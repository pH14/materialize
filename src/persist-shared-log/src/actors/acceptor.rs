// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Acceptor: blind group commit via persist `WriteHandle`.
//!
//! Proposals are appended unconditionally via `compare_and_append`.
//!
//! Uses an open-loop design: as soon as a flush completes, the next one starts
//! immediately if there are pending proposals — no timer delay.
//!
//! # Future: pipelined batch building
//!
//! Currently `compare_and_append` does parquet encoding and blob upload
//! internally, so we can't overlap encoding of batch N+1 with the write of
//! batch N. Persist's `BatchBuilder` API allows splitting batch construction
//! (parquet encode + blob upload) from the CAS (`compare_and_append_batch`).
//! Using that split, we could optimistically build the next batch at timestamp
//! T+1 while the current batch's CAS is in flight, and submit it immediately
//! on success. On `UpperMismatch` (rare, only with multiple writers) the
//! pre-built batch would need to be discarded and rebuilt.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use bytes::Bytes;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::retry::Retry;
use mz_persist::generated::consensus_service::{ProtoAppendResponse, ProtoLogProposal};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use prost::Message;

use super::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema, extract_shard_name};
use crate::metrics::AcceptorMetrics;
use crate::{AcceptorConfig, AcceptorError};

// ---------------------------------------------------------------------------
// Setup batch IDs
// ---------------------------------------------------------------------------

/// Batch ID for the bulk snapshot written during shard setup.
pub const BULK_SNAPSHOT_BATCH_ID: u64 = 1;
/// Batch ID for the delta snapshot written during shard setup.
pub const DELTA_SNAPSHOT_BATCH_ID: u64 = 2;
/// First batch ID used for regular traffic after setup completes.
pub const FIRST_REGULAR_BATCH_ID: u64 = 3;

/// Commands dispatched to the acceptor.
pub enum PersistAcceptorCommand {
    /// Append a pre-encoded proposal. Reply after the next flush.
    ///
    /// The proposal is already serialized to protobuf bytes by the caller
    /// (in the handle's `append()` method), so encoding is parallelized
    /// across callers rather than serialized in the acceptor's flush loop.
    Append {
        proposal: Proposal,
        encoded_len: usize,
        reply: oneshot::Sender<Result<ProtoAppendResponse, AcceptorError>>,
    },
    /// Flush barrier: reply after all preceding proposals have been flushed.
    /// Used in tests to force deterministic flush boundaries.
    #[allow(dead_code)]
    Flush { reply: oneshot::Sender<()> },
}

/// A typed handle to the acceptor's command channel.
#[derive(Debug, Clone)]
pub struct PersistAcceptorHandle {
    tx: mpsc::Sender<PersistAcceptorCommand>,
}

impl PersistAcceptorHandle {
    pub fn new(tx: mpsc::Sender<PersistAcceptorCommand>) -> Self {
        PersistAcceptorHandle { tx }
    }

    pub async fn flush(&self) -> Result<(), AcceptorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistAcceptorCommand::Flush { reply: reply_tx })
            .await
            .map_err(|_| AcceptorError::Shutdown)?;
        reply_rx.await.map_err(|_| AcceptorError::DroppedReply)
    }
}

#[async_trait::async_trait]
impl crate::Acceptor for PersistAcceptorHandle {
    async fn append(
        &self,
        proposal: ProtoLogProposal,
    ) -> Result<ProtoAppendResponse, AcceptorError> {
        // Pre-encode the proposal into protobuf bytes here, in the caller's
        // task, so that encoding is parallelized across all writers rather than
        // serialized in the acceptor's flush loop.
        let encoded_len = proposal.encoded_len();
        let encoded = Proposal {
            encoded: Bytes::from(proposal.encode_to_vec()),
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistAcceptorCommand::Append {
                proposal: encoded,
                encoded_len,
                reply: reply_tx,
            })
            .await
            .map_err(|_| AcceptorError::Shutdown)?;
        reply_rx.await.map_err(|_| AcceptorError::DroppedReply)?
    }
}

// ---------------------------------------------------------------------------
// Pending buffer
// ---------------------------------------------------------------------------

/// An item in the pending buffer: either a proposal or a flush barrier.
enum PendingItem {
    Append(PendingAppend),
    /// A flush barrier. Resolved after all preceding proposals in this batch
    /// have been committed.
    FlushBarrier(oneshot::Sender<()>),
}

struct PendingAppend {
    proposal: Proposal,
    encoded_len: usize,
    reply: oneshot::Sender<Result<ProtoAppendResponse, AcceptorError>>,
    received_at: tokio::time::Instant,
}

// ---------------------------------------------------------------------------
// Acceptor
// ---------------------------------------------------------------------------

/// The acceptor.
///
/// A passive state machine that buffers proposals and flushes them via
/// `compare_and_append`. The persist shard upper frontier serves as the batch
/// number — batch number derives from upper, not the other way around.
///
/// Separates **mechanism** (buffering and flushing) from **policy** (when to
/// flush). The production policy is implemented by [`run()`](Self::run), but
/// callers can also drive the acceptor directly via [`handle_command`],
/// [`flush`], and [`drain_ready_commands`] for deterministic testing.
pub struct PersistAcceptor {
    pending: Vec<PendingItem>,
    rx: mpsc::Receiver<PersistAcceptorCommand>,
    metrics: AcceptorMetrics,
    /// ShardId of the log shard this acceptor writes to, included in receipts.
    log_shard_id: String,
    /// Partition map epoch this acceptor was created under.
    epoch: u64,
    /// Watch receiver for the current retraction source.
    ///
    /// The factory updates this when learners become available or change.
    /// The acceptor clones the current `Arc` before each poll so it holds
    /// the lock for as little time as possible.
    retraction_sources: watch::Receiver<Arc<dyn crate::RetractionSource>>,
    /// Buffered retractions waiting to be included in the next flush.
    buffered_retractions: Vec<(OrderedKey, Proposal)>,
    /// Flush counter for periodic retraction polling.
    flush_count: u64,
    /// Poll for retractions every N flushes.
    retraction_poll_interval: u64,
}

impl PersistAcceptor {
    /// Creates a new acceptor and returns a handle.
    ///
    /// The batch number is derived from the `WriteHandle`'s `upper()` at flush
    /// time — no explicit `set_batch_number` needed.
    pub fn new(
        config: AcceptorConfig,
        metrics: AcceptorMetrics,
        log_shard_id: ShardId,
        epoch: u64,
        retraction_sources: watch::Receiver<Arc<dyn crate::RetractionSource>>,
    ) -> (Self, PersistAcceptorHandle) {
        let (tx, rx) = mpsc::channel(config.queue_depth);

        let acceptor = PersistAcceptor {
            pending: Vec::new(),
            rx,
            metrics,
            log_shard_id: log_shard_id.to_string(),
            epoch,
            retraction_sources,
            buffered_retractions: Vec::new(),
            flush_count: 0,
            retraction_poll_interval: 100,
        };
        let handle = PersistAcceptorHandle::new(tx);
        (acceptor, handle)
    }

    /// Returns true if there are pending proposals or flush barriers.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Push a command into the pending buffer.
    pub fn handle_command(&mut self, cmd: PersistAcceptorCommand) {
        match cmd {
            PersistAcceptorCommand::Append {
                proposal,
                encoded_len,
                reply,
            } => {
                self.pending.push(PendingItem::Append(PendingAppend {
                    proposal,
                    encoded_len,
                    reply,
                    received_at: tokio::time::Instant::now(),
                }));
            }
            PersistAcceptorCommand::Flush { reply } => {
                self.metrics.flush_explicit_triggered.inc();
                self.pending.push(PendingItem::FlushBarrier(reply));
            }
        }
    }

    /// Drain all immediately-available commands from the channel without
    /// blocking. Maximizes batching after a flush completes.
    pub fn drain_ready_commands(&mut self) {
        while let Ok(cmd) = self.rx.try_recv() {
            self.handle_command(cmd);
        }
    }

    /// Flush all pending proposals via `compare_and_append`.
    ///
    /// Takes the pending buffer, resolves reply oneshots, and returns. Retry
    /// logic for `UpperMismatch` is handled internally. Returns `Err` on fatal
    /// error (InvalidUsage or retries exhausted).
    ///
    /// The caller decides when to call this — that's the policy. This method
    /// is the mechanism.
    pub async fn flush(
        &mut self,
        write: &mut WriteHandle<OrderedKey, Proposal, u64, i64>,
    ) -> Result<(), String> {
        let pending = std::mem::take(&mut self.pending);
        let retractions = std::mem::take(&mut self.buffered_retractions);
        flush_inner(
            write,
            pending,
            &retractions,
            &self.metrics,
            &self.log_shard_id,
            self.epoch,
        )
        .await
    }

    /// Poll the retraction source and buffer results for the next flush.
    async fn poll_retractions(&mut self, current_upper: u64) {
        // Clone the Arc to release the watch lock before the async call.
        let source = self.retraction_sources.borrow().clone();
        let retractions = source.get_retractions(current_upper).await;
        if retractions.is_empty() {
            return;
        }
        debug!(count = retractions.len(), "polled retractions from source");
        // Deduplicate against existing buffer by OrderedKey.
        let existing: std::collections::BTreeSet<_> = self
            .buffered_retractions
            .iter()
            .map(|(k, _)| k.clone())
            .collect();
        for (key, proposal) in retractions {
            if !existing.contains(&key) {
                self.buffered_retractions.push((key, proposal));
            }
        }
    }

    /// Runs the acceptor loop until the channel closes or a fatal error occurs.
    ///
    /// This is the **production policy**: open-loop, flush immediately when
    /// there are pending proposals, drain commands after each flush.
    pub async fn run(mut self, mut write: WriteHandle<OrderedKey, Proposal, u64, i64>) {
        info!(
            shard_id = %self.log_shard_id,
            epoch = self.epoch,
            "acceptor starting"
        );
        loop {
            if self.has_pending() || !self.buffered_retractions.is_empty() {
                if let Err(e) = self.flush(&mut write).await {
                    error!("acceptor shutting down: {}", e);
                    break;
                }
                self.flush_count += 1;

                // Poll for retractions every Nth flush.
                if self.flush_count % self.retraction_poll_interval == 0 {
                    let upper = write.upper().as_option().copied().unwrap_or(u64::MAX);
                    self.poll_retractions(upper).await;
                }

                self.drain_ready_commands();
                continue;
            }

            // Nothing pending — wait for a command.
            match self.rx.recv().await {
                Some(cmd) => self.handle_command(cmd),
                None => break,
            }
        }

        // Drain any remaining pending items before shutting down.
        if self.has_pending() {
            let _ = self.flush(&mut write).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

/// Retry configuration for compare_and_append in the flush loop.
fn flush_retry() -> Retry {
    Retry::default()
        .initial_backoff(Duration::from_millis(1))
        .factor(2.0)
        .clamp_backoff(Duration::from_millis(100))
        .max_tries(10)
}

/// Flush pending items via `compare_and_append`.
///
/// Proposal reply oneshots and flush barrier oneshots are resolved inside this
/// function. Returns `Err` on fatal error (InvalidUsage, sealed shard, or
/// retries exhausted).
async fn flush_inner(
    write: &mut WriteHandle<OrderedKey, Proposal, u64, i64>,
    pending: Vec<PendingItem>,
    retractions: &[(OrderedKey, Proposal)],
    metrics: &AcceptorMetrics,
    log_shard_id: &str,
    epoch: u64,
) -> Result<(), String> {
    if pending.is_empty() {
        return Ok(());
    }

    let flush_start = tokio::time::Instant::now();

    // Split pending items into proposals and flush barriers.
    let mut proposals = Vec::new();
    let mut batch_bytes: usize = 0;
    let mut replies = Vec::new();
    let mut barriers = Vec::new();
    for item in pending {
        match item {
            PendingItem::Append(p) => {
                metrics
                    .proposal_queue_seconds
                    .observe((flush_start - p.received_at).as_secs_f64());
                batch_bytes += p.encoded_len;
                proposals.push(p.proposal);
                replies.push(p.reply);
            }
            PendingItem::FlushBarrier(reply) => barriers.push(reply),
        }
    }

    // If there are only barriers and no proposals or retractions, resolve
    // them immediately — no compare_and_append needed.
    if proposals.is_empty() && retractions.is_empty() {
        for barrier in barriers {
            let _ = barrier.send(());
        }
        return Ok(());
    }

    let num_proposals = proposals.len();

    let retry = flush_retry().into_retry_stream();
    tokio::pin!(retry);

    let write_start = tokio::time::Instant::now();

    while let Some(state) = retry.next().await {
        // Read the (possibly updated) upper and derive batch_number.
        let upper = write.upper().clone();
        let raw_upper = match upper.as_option() {
            Some(u) => *u,
            None => {
                // Upper is the empty antichain — the shard has been sealed.
                warn!(log_shard = log_shard_id, "log shard sealed");
                for reply in replies {
                    let _ = reply.send(Err(AcceptorError::Sealed));
                }
                return Err("log shard sealed".to_string());
            }
        };
        // Skip T=0: listen(as_of=since) where since=[0] treats T=0 as an
        // empty snapshot, so writing at T=0 would be invisible to the
        // learner. After the first batch, raw_upper >= 2 so .max(1) is a
        // no-op.
        let batch_number = raw_upper.max(1);

        debug!(
            batch = batch_number,
            proposals = num_proposals,
            attempt = state.i,
            "persist acceptor flush"
        );

        // Build updates at the current batch_number. Each proposal gets an
        // OrderedKey with (batch_id, position, shard) for stable ordering
        // through compaction. Proposal clone is O(1) via Bytes refcounting.
        let mut updates: Vec<_> = proposals
            .iter()
            .enumerate()
            .map(|(position, p)| {
                let shard = extract_shard_name(&p.encoded);
                let key = OrderedKey {
                    batch_id: batch_number,
                    position: u32::try_from(position).expect("batch position fits u32"),
                    shard,
                };
                ((key, p.clone()), batch_number, 1i64)
            })
            .collect();

        // Include buffered retractions as -1 diffs in the same batch.
        for (key, proposal) in retractions {
            updates.push(((key.clone(), proposal.clone()), batch_number, -1i64));
        }

        let new_upper = Antichain::from_elem(batch_number + 1);

        match write.compare_and_append(&updates, upper, new_upper).await {
            Ok(Ok(())) => {
                // Success — resolve proposal replies and flush barriers.
                for (position, reply) in replies.into_iter().enumerate() {
                    let _ = reply.send(Ok(ProtoAppendResponse {
                        batch_number,
                        position: u32::try_from(position).expect("batch position fits u32"),
                        log_shard: log_shard_id.to_string(),
                        epoch,
                    }));
                }
                for barrier in barriers {
                    let _ = barrier.send(());
                }

                metrics.flush_count.inc();
                metrics
                    .flush_proposals_per_batch
                    .observe(f64::cast_lossy(num_proposals));
                metrics
                    .flush_latency_seconds
                    .observe(flush_start.elapsed().as_secs_f64());
                metrics
                    .object_store_log_write_bytes
                    .inc_by(u64::cast_from(batch_bytes));
                metrics.object_store_log_writes.inc();
                metrics
                    .object_store_log_write_latency_seconds
                    .observe(write_start.elapsed().as_secs_f64());

                debug!(
                    batch = batch_number,
                    proposals = num_proposals,
                    "persist acceptor flush committed"
                );
                return Ok(());
            }
            Ok(Err(upper_mismatch)) => {
                // Check if the shard was sealed by someone else.
                if upper_mismatch.current.as_option().is_none() {
                    warn!(
                        log_shard = log_shard_id,
                        "log shard sealed (upper mismatch)"
                    );
                    for reply in replies {
                        let _ = reply.send(Err(AcceptorError::Sealed));
                    }
                    return Err("log shard sealed".to_string());
                }
                // Another writer advanced the upper — retryable.
                // WriteHandle auto-updates its cached upper on mismatch.
                metrics.object_store_write_retries.inc();
                let actual = upper_mismatch
                    .current
                    .as_option()
                    .copied()
                    .unwrap_or(u64::MAX);
                warn!(
                    expected = batch_number,
                    actual_upper = actual,
                    attempt = state.i,
                    "persist acceptor upper mismatch, retrying"
                );
                continue;
            }
            Err(invalid_usage) => {
                let msg = format!("persist internal error: {}", invalid_usage);
                error!("{}", msg);
                for reply in replies {
                    let _ = reply.send(Err(AcceptorError::Command(msg.clone())));
                }
                return Err(msg);
            }
        }
    }

    let msg = "persist acceptor flush failed: retries exhausted after repeated upper mismatch";
    error!("{}", msg);
    for reply in replies {
        let _ = reply.send(Err(AcceptorError::Command(msg.to_string())));
    }
    Err(msg.to_string())
}

// ---------------------------------------------------------------------------
// Predecessor setup: bulk snapshot (batch_id=1) + delta snapshot (batch_id=2)
// ---------------------------------------------------------------------------

/// A predecessor shard and the CriticalSince frontier at which to read it.
pub type PredecessorSpec = (ShardId, Antichain<u64>);

/// Copy the consolidated snapshot of each predecessor shard (at its
/// CriticalSince) into the new shard at `BULK_SNAPSHOT_BATCH_ID`.
///
/// Entries are blind-copied: proposals are opaque bytes, only OrderedKeys are
/// re-keyed to `(batch_id, position, shard)`. The learner applies CaS
/// semantics when it processes these entries from its subscribe.
async fn write_bulk_snapshot(
    write: &mut WriteHandle<OrderedKey, Proposal, u64, i64>,
    client: &PersistClient,
    predecessors: &[PredecessorSpec],
    range: &crate::RangeAssignment,
) {
    let current_upper = write.upper().as_option().copied().unwrap_or(u64::MAX);
    if current_upper >= DELTA_SNAPSHOT_BATCH_ID {
        info!(
            "bulk snapshot already written (upper={}), skipping",
            current_upper
        );
        return;
    }

    if predecessors.is_empty() {
        info!("no predecessors, advancing upper through bulk snapshot (empty)");
        let upper = write.upper().clone();
        let new_upper = Antichain::from_elem(DELTA_SNAPSHOT_BATCH_ID);
        let empty: Vec<((OrderedKey, Proposal), u64, i64)> = vec![];
        match write.compare_and_append(&empty, upper, new_upper).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                info!("upper mismatch during empty bulk snapshot, another acceptor won");
            }
            Err(e) => {
                error!("invalid usage during empty bulk snapshot: {}", e);
            }
        }
        return;
    }

    let key_schema = Arc::new(OrderedKeySchema);
    let val_schema = Arc::new(ProposalSchema);
    let mut snapshot_entries: Vec<((OrderedKey, Proposal), u64, i64)> = Vec::new();
    let mut position: u32 = 0;

    for (pred_shard, since) in predecessors {
        let (_, mut read) = client
            .open::<OrderedKey, Proposal, u64, i64>(
                *pred_shard,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("acceptor-predecessor-snapshot"),
                false,
            )
            .await
            .expect("failed to open predecessor for snapshot");

        let snapshot = read
            .snapshot_and_fetch(since.clone())
            .await
            .expect("snapshot of predecessor failed");

        for ((key, proposal), _ts, diff) in snapshot {
            if !range.contains_partition_key(&key.shard) {
                continue;
            }
            let new_key = OrderedKey {
                batch_id: BULK_SNAPSHOT_BATCH_ID,
                position,
                shard: key.shard.clone(),
            };
            snapshot_entries.push(((new_key, proposal.clone()), BULK_SNAPSHOT_BATCH_ID, diff));
            position += 1;
        }
    }

    info!(
        entries = snapshot_entries.len(),
        "writing bulk snapshot at batch_id={}", BULK_SNAPSHOT_BATCH_ID
    );

    let upper = write.upper().clone();
    let new_upper = Antichain::from_elem(DELTA_SNAPSHOT_BATCH_ID);
    match write
        .compare_and_append(&snapshot_entries, upper, new_upper)
        .await
    {
        Ok(Ok(())) => {
            info!("bulk snapshot written successfully");
        }
        Ok(Err(_)) => {
            info!("upper mismatch during bulk snapshot, another acceptor won");
        }
        Err(e) => {
            error!("invalid usage during bulk snapshot: {}", e);
        }
    }
}

/// Copy delta events from each predecessor (between its CriticalSince and its
/// seal) into the new shard at `DELTA_SNAPSHOT_BATCH_ID`.
///
/// Uses `listen` to read only the events after the snapshot point, avoiding the
/// need to skip the snapshot region that `subscribe` would include.
async fn write_delta_snapshot(
    write: &mut WriteHandle<OrderedKey, Proposal, u64, i64>,
    client: &PersistClient,
    predecessors: &[PredecessorSpec],
    range: &crate::RangeAssignment,
) {
    use mz_persist_client::read::ListenEvent;

    let current_upper = write.upper().as_option().copied().unwrap_or(u64::MAX);
    if current_upper >= FIRST_REGULAR_BATCH_ID {
        info!(
            "delta snapshot already written (upper={}), skipping",
            current_upper
        );
        return;
    }

    if predecessors.is_empty() {
        info!("no predecessors, advancing upper through delta snapshot (empty)");
        let upper = write.upper().clone();
        let new_upper = Antichain::from_elem(FIRST_REGULAR_BATCH_ID);
        let empty: Vec<((OrderedKey, Proposal), u64, i64)> = vec![];
        match write.compare_and_append(&empty, upper, new_upper).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                info!("upper mismatch during empty delta snapshot, another acceptor won");
            }
            Err(e) => {
                error!("invalid usage during empty delta snapshot: {}", e);
            }
        }
        return;
    }

    let key_schema = Arc::new(OrderedKeySchema);
    let val_schema = Arc::new(ProposalSchema);
    let mut delta_entries: Vec<((OrderedKey, Proposal), u64, i64)> = Vec::new();
    let mut position: u32 = 0;

    for (pred_shard, since) in predecessors {
        let (_, read) = client
            .open::<OrderedKey, Proposal, u64, i64>(
                *pred_shard,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("acceptor-predecessor-delta"),
                false,
            )
            .await
            .expect("failed to open predecessor for delta");

        let mut listen = read
            .listen(since.clone())
            .await
            .expect("listen to predecessor for delta failed");

        'outer: loop {
            let events = listen.fetch_next().await;
            for event in &events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        if frontier.is_empty() {
                            break 'outer;
                        }
                    }
                    ListenEvent::Updates(updates) => {
                        for ((key, proposal), _ts, diff) in updates {
                            if !range.contains_partition_key(&key.shard) {
                                continue;
                            }
                            let new_key = OrderedKey {
                                batch_id: DELTA_SNAPSHOT_BATCH_ID,
                                position,
                                shard: key.shard.clone(),
                            };
                            delta_entries.push((
                                (new_key, proposal.clone()),
                                DELTA_SNAPSHOT_BATCH_ID,
                                *diff,
                            ));
                            position += 1;
                        }
                    }
                }
            }
        }
    }

    info!(
        entries = delta_entries.len(),
        "writing delta snapshot at batch_id={}", DELTA_SNAPSHOT_BATCH_ID
    );

    let upper = write.upper().clone();
    let new_upper = Antichain::from_elem(FIRST_REGULAR_BATCH_ID);
    match write
        .compare_and_append(&delta_entries, upper, new_upper)
        .await
    {
        Ok(Ok(())) => {
            info!("delta snapshot written successfully");
        }
        Ok(Err(_)) => {
            info!("upper mismatch during delta snapshot, another acceptor won");
        }
        Err(e) => {
            error!("invalid usage during delta snapshot: {}", e);
        }
    }

    let shard = write.shard_id();
    info!(
        %shard,
        range_lo = range.lo,
        range_hi = range.hi_exclusive,
        "setup batches complete, acceptor ready for regular traffic"
    );
}

// ---------------------------------------------------------------------------
// Spawn helper
// ---------------------------------------------------------------------------

impl PersistAcceptor {
    /// Opens a persist shard and spawns the acceptor as a tokio task.
    ///
    /// Setup batches are injected as the first actor command: the bulk snapshot
    /// (batch_id=1) and delta snapshot (batch_id=2) are written before any
    /// regular proposals are processed. Regular traffic starts at batch_id=3.
    pub async fn spawn(
        config: AcceptorConfig,
        client: &PersistClient,
        shard_id: ShardId,
        metrics: AcceptorMetrics,
        epoch: u64,
        retraction_sources: watch::Receiver<Arc<dyn crate::RetractionSource>>,
        predecessors: Vec<PredecessorSpec>,
        range: crate::RangeAssignment,
    ) -> (PersistAcceptorHandle, mz_ore::task::JoinHandle<()>) {
        let write = client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::new(OrderedKeySchema),
                Arc::new(ProposalSchema),
                Diagnostics::from_purpose("persist-shared-log-acceptor"),
            )
            .await
            .expect("failed to open persist shard for acceptor");

        let (acceptor, handle) = Self::new(config, metrics, shard_id, epoch, retraction_sources);
        let client = client.clone();
        let task = mz_ore::task::spawn(|| "persist-acceptor", async move {
            // Setup runs as the first action inside the actor task. The bulk
            // snapshot and delta snapshot advance the shard upper through
            // BULK_SNAPSHOT_BATCH_ID and DELTA_SNAPSHOT_BATCH_ID before the
            // acceptor enters its normal flush loop. External callers (e.g.,
            // the meta actor) can monitor progress by listening on the shard.
            let mut write = write;
            write_bulk_snapshot(&mut write, &client, &predecessors, &range).await;
            write_delta_snapshot(&mut write, &client, &predecessors, &range).await;
            acceptor.run(write).await;
        });

        (handle, task)
    }
}
