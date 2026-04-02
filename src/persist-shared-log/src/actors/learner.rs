// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Learner: tails a persist shard subscription, materializes state, serves reads.
//!
//! A passive state machine that separates **mechanism** (applying events,
//! serving reads, flushing retractions) from **policy** (when to fetch upper,
//! when to sweep pending retractions). The production policy is implemented by
//! [`PersistLearner::run()`], but callers can also drive the learner directly
//! via [`on_events`](PersistLearner::on_events),
//! [`on_upper`](PersistLearner::on_upper),
//! [`on_command`](PersistLearner::on_command), and
//! [`flush_pending_retractions`](PersistLearner::flush_pending_retractions) for deterministic testing.
//!
//! ## EventSource trait
//!
//! The learner is generic over [`EventSource`], which abstracts where listen
//! events come from. In production, [`ChannelEventSource`] wraps a dedicated
//! task running `Subscribe::fetch_next()`. In tests, a mock can deliver events
//! deterministically.
//!
//! ## Read linearization
//!
//! Reads are linearized against the shard upper (the latest committed timestamp
//! across all writers). The caller provides the upper via [`on_upper`] after
//! fetching it — the learner does not own the upper handle.
//!
//! To amortize the cost of upper fetches across concurrent reads, the
//! production [`run()`](PersistLearner::run) loop uses the "bus-stand"
//! optimization: a single `fetch_recent_upper()` call is shared by all reads
//! that arrive while it's in flight.
//!
//! ## Listen task isolation
//!
//! `Listen::fetch_next()` is **not cancel-safe**: it mutates the listen frontier
//! partway through execution, so dropping it mid-await can lose data.
//! [`ChannelEventSource`] isolates this by running the subscribe in a dedicated
//! task that feeds events through an mpsc channel.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use mz_ore::cast::CastFrom;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoLogProposal, ProtoScanResponse,
    ProtoTruncateResponse, ProtoVersionedData, proto_log_proposal,
};
use mz_persist_client::read::{ListenEvent, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};

use super::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::LearnerError;
use crate::metrics::LearnerMetrics;

/// Per-shard committed state.
#[derive(Debug, Clone, Default)]
struct ShardState {
    /// Committed entries, ordered by seqno.
    entries: Vec<VersionedEntry>,
}

/// A versioned data entry.
#[derive(Debug, Clone)]
struct VersionedEntry {
    seqno: u64,
    data: Bytes,
    /// The persist-level key for this entry, needed for retraction.
    ordered_key: OrderedKey,
    /// The persist-level value for this entry, needed for retraction.
    proposal: Proposal,
}

/// Configuration for the persist-backed learner.
#[derive(Debug, Clone)]
pub struct PersistLearnerConfig {
    /// Depth of the command channel.
    pub queue_depth: usize,
    /// How often the learner flushes pending retractions.
    pub retraction_interval: Duration,
}

impl Default for PersistLearnerConfig {
    fn default() -> Self {
        PersistLearnerConfig {
            queue_depth: 4096,
            retraction_interval: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// StateMachine
// ---------------------------------------------------------------------------

/// The replicated state machine that applies proposals from the shared log.
///
/// Takes CAS and truncate proposals, evaluates them against current state, and
/// maintains the resulting key→versions mapping. This is the SMR (state machine
/// replication) core — it processes the log deterministically so all replicas
/// converge to the same state.
///
/// `pending_retractions` are stored on [`LogShardState`] rather than here,
/// because retractions must be written to the specific log shard that sourced
/// the proposal. The apply methods return retractions for the caller to route.
struct StateMachine {
    shards: BTreeMap<String, ShardState>,
    /// Running total of entries across all shards, maintained incrementally.
    total_entries: usize,
    /// Running total of approximate bytes across all shards, maintained
    /// incrementally. Avoids an O(total_entries) traversal on every batch.
    approx_bytes: usize,

    /// Every OrderedKey we've seen with diff=+1 that hasn't been retracted (-1).
    /// Used to assert no negative multiplicities.
    live_keys: BTreeSet<OrderedKey>,
}

/// Per-log-shard state held alongside the global `StateMachine`.
///
/// During static sharding (Phase 1-3), each learner has exactly one of these.
/// During reconfiguration (Phase 4), a learner may temporarily hold multiple
/// `LogShardState` instances while transitioning between log shards.
pub(crate) struct LogShardState<E: EventSource> {
    /// Event source (persist subscribe channel).
    pub(crate) event_source: E,
    /// The listen frontier, mirrored from Progress events.
    pub(crate) listen_frontier: Antichain<u64>,
    /// Result cache: batch_number → vec of proposal results.
    pub(crate) results: BTreeMap<u64, Vec<ProposalResult>>,
    /// Clients waiting for results from specific batches.
    pub(crate) result_waiters: BTreeMap<u64, Vec<ResultWaiter>>,
    /// Proposals eligible for retraction, accumulated during apply.
    /// Drained by the periodic retraction sweep.
    pub(crate) pending_retractions: BTreeMap<OrderedKey, Proposal>,
    /// Clients waiting for retractions at a specific frontier.
    /// Woken when listen_frontier advances past the requested frontier.
    pub(crate) retraction_waiters: Vec<RetractionWaiter>,
}

/// A pending get_retractions request waiting for the listen frontier to advance.
pub(crate) struct RetractionWaiter {
    frontier: u64,
    reply: oneshot::Sender<Vec<(OrderedKey, Proposal)>>,
}

impl StateMachine {
    fn new() -> Self {
        StateMachine {
            shards: BTreeMap::new(),
            total_entries: 0,
            approx_bytes: 0,
            live_keys: BTreeSet::new(),
        }
    }

    /// Apply a CAS proposal with diff=+1. Returns the CAS result and an
    /// optional retraction (if the CAS was rejected, the proposal is waste).
    fn apply_cas(
        &mut self,
        cas: mz_persist::generated::consensus_service::ProtoCasProposal,
        ordered_key: OrderedKey,
        proposal: Proposal,
    ) -> (ProtoCompareAndSetResponse, Option<(OrderedKey, Proposal)>) {
        let current_seqno = self
            .shards
            .get(&cas.key)
            .and_then(|s| s.entries.last())
            .map(|e| e.seqno);

        let committed = current_seqno == cas.expected;

        let retraction = if committed {
            let data_len = cas.data.len();
            let entry = VersionedEntry {
                seqno: cas.new_seqno,
                data: Bytes::from(cas.data),
                ordered_key,
                proposal,
            };
            self.shards.entry(cas.key).or_default().entries.push(entry);
            self.total_entries += 1;
            self.approx_bytes += data_len;
            None
        } else {
            Some((ordered_key, proposal))
        };

        (ProtoCompareAndSetResponse { committed }, retraction)
    }

    /// Apply a truncate proposal with diff=+1. Returns the truncate result
    /// and a list of retractions (removed entries + the truncate proposal itself).
    fn apply_truncate(
        &mut self,
        trunc: &mz_persist::generated::consensus_service::ProtoTruncateProposal,
        ordered_key: OrderedKey,
        proposal: Proposal,
    ) -> (
        Result<ProtoTruncateResponse, String>,
        Vec<(OrderedKey, Proposal)>,
    ) {
        let shard = match self.shards.get(&trunc.key) {
            Some(s) if !s.entries.is_empty() => s,
            _ => {
                return (
                    Err(format!("no data at key: {}", trunc.key)),
                    vec![(ordered_key, proposal)],
                );
            }
        };

        let head_seqno = shard.entries.last().unwrap().seqno;

        if trunc.seqno > head_seqno {
            return (
                Err(format!(
                    "upper bound too high for truncate: {}",
                    trunc.seqno
                )),
                vec![(ordered_key, proposal)],
            );
        }

        let shard = self.shards.get_mut(&trunc.key).unwrap();
        let keep_from = shard.entries.partition_point(|e| e.seqno < trunc.seqno);
        let removed_bytes: usize = shard.entries[..keep_from]
            .iter()
            .map(|e| e.data.len())
            .sum();

        let mut retractions: Vec<(OrderedKey, Proposal)> = shard.entries[..keep_from]
            .iter()
            .map(|e| (e.ordered_key.clone(), e.proposal.clone()))
            .collect();
        shard.entries.drain(..keep_from);
        self.total_entries -= keep_from;
        self.approx_bytes -= removed_bytes;

        // The truncate proposal itself is also a retraction.
        retractions.push((ordered_key, proposal));

        (
            Ok(ProtoTruncateResponse {
                deleted: Some(u64::cast_from(keep_from)),
            }),
            retractions,
        )
    }

    /// Handle a retraction (diff=-1). Removes the entry from live_keys and
    /// from materialized state if applicable.
    ///
    /// The caller is responsible for pruning the retracted key from the
    /// `LogShardState.pending_retractions` map.
    fn apply_retraction(&mut self, ordered_key: &OrderedKey) {
        // Assert: we must have seen this key with +1 before.
        assert!(
            self.live_keys.remove(ordered_key),
            "negative multiplicity: retraction for OrderedKey not in live_keys: {:?}",
            ordered_key,
        );

        // If this was a committed CAS entry still in state, remove it.
        if let Some(shard) = self.shards.get_mut(&ordered_key.shard) {
            if let Some(idx) = shard
                .entries
                .iter()
                .position(|e| e.ordered_key == *ordered_key)
            {
                let removed = shard.entries.remove(idx);
                self.total_entries -= 1;
                self.approx_bytes -= removed.data.len();
            }
        }
    }

    fn head(&self, key: &str) -> ProtoHeadResponse {
        let data = self
            .shards
            .get(key)
            .and_then(|s| s.entries.last())
            .map(|e| ProtoVersionedData {
                seqno: e.seqno,
                data: e.data.to_vec(),
            });
        ProtoHeadResponse { data }
    }

    fn scan(&self, key: &str, from: u64, limit: u64) -> ProtoScanResponse {
        let data = if let Some(shard) = self.shards.get(key) {
            let from_idx = shard.entries.partition_point(|e| e.seqno < from);
            let lim = usize::try_from(limit).unwrap_or(usize::MAX);
            let slice = &shard.entries[from_idx..];
            let slice = &slice[..usize::min(lim, slice.len())];
            slice
                .iter()
                .map(|e| ProtoVersionedData {
                    seqno: e.seqno,
                    data: e.data.to_vec(),
                })
                .collect()
        } else {
            Vec::new()
        };
        ProtoScanResponse { data }
    }

    fn keys(&self) -> Vec<String> {
        self.shards.keys().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands dispatched to the persist-backed learner.
pub enum PersistLearnerCommand {
    Head {
        key: String,
        reply: oneshot::Sender<ProtoHeadResponse>,
        received_at: tokio::time::Instant,
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        reply: oneshot::Sender<ProtoScanResponse>,
        received_at: tokio::time::Instant,
    },
    ListKeys {
        reply: oneshot::Sender<Vec<String>>,
        received_at: tokio::time::Instant,
    },
    AwaitCasResult {
        batch_number: u64,
        position: u32,
        reply: oneshot::Sender<ProtoCompareAndSetResponse>,
        received_at: tokio::time::Instant,
    },
    AwaitTruncateResult {
        batch_number: u64,
        position: u32,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
        received_at: tokio::time::Instant,
    },
    /// Return a snapshot of pending retractions for proposals with
    /// `batch_id < frontier`. The frontier is capped to the learner's current
    /// listen frontier so we never return retractions for batches the learner
    /// hasn't processed yet. The learner retains the entries — they're only
    /// removed when the -1 diffs arrive via subscription.
    GetRetractions {
        frontier: u64,
        reply: oneshot::Sender<Vec<(OrderedKey, Proposal)>>,
    },
}

/// A read command waiting for linearization.
#[allow(dead_code)] // received_at on ListKeys is present for uniformity
enum ReadCommand {
    Head {
        key: String,
        reply: oneshot::Sender<ProtoHeadResponse>,
        received_at: tokio::time::Instant,
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        reply: oneshot::Sender<ProtoScanResponse>,
        received_at: tokio::time::Instant,
    },
    ListKeys {
        reply: oneshot::Sender<Vec<String>>,
        received_at: tokio::time::Instant,
    },
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// A typed handle to the persist-backed learner's command channel.
#[derive(Debug, Clone)]
pub struct PersistLearnerHandle {
    tx: mpsc::Sender<PersistLearnerCommand>,
}

impl PersistLearnerHandle {
    pub fn new(tx: mpsc::Sender<PersistLearnerCommand>) -> Self {
        PersistLearnerHandle { tx }
    }

    pub async fn head(&self, key: String) -> Result<ProtoHeadResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::Head {
                key,
                reply: reply_tx,
                received_at: tokio::time::Instant::now(),
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
    ) -> Result<ProtoScanResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::Scan {
                key,
                from,
                limit,
                reply: reply_tx,
                received_at: tokio::time::Instant::now(),
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn list_keys(&self) -> Result<Vec<String>, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::ListKeys {
                reply: reply_tx,
                received_at: tokio::time::Instant::now(),
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn await_cas_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoCompareAndSetResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::AwaitCasResult {
                batch_number,
                position,
                reply: reply_tx,
                received_at: tokio::time::Instant::now(),
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }

    pub async fn await_truncate_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoTruncateResponse, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::AwaitTruncateResult {
                batch_number,
                position,
                reply: reply_tx,
                received_at: tokio::time::Instant::now(),
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx
            .await
            .map_err(|_| LearnerError::DroppedReply)?
            .map_err(LearnerError::Command)
    }

    /// Return a snapshot of pending retractions with `batch_id < frontier`.
    /// Non-destructive — entries stay in the learner until confirmed via
    /// subscription.
    pub async fn get_retractions(
        &self,
        frontier: u64,
    ) -> Result<Vec<(OrderedKey, Proposal)>, LearnerError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PersistLearnerCommand::GetRetractions {
                frontier,
                reply: reply_tx,
            })
            .await
            .map_err(|_| LearnerError::Shutdown)?;
        reply_rx.await.map_err(|_| LearnerError::DroppedReply)
    }
}

#[async_trait::async_trait]
impl crate::Learner for PersistLearnerHandle {
    async fn head(&self, key: String) -> Result<ProtoHeadResponse, LearnerError> {
        self.head(key).await
    }

    async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
    ) -> Result<ProtoScanResponse, LearnerError> {
        self.scan(key, from, limit).await
    }

    async fn list_keys(&self) -> Result<Vec<String>, LearnerError> {
        self.list_keys().await
    }

    async fn await_cas_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoCompareAndSetResponse, LearnerError> {
        self.await_cas_result(batch_number, position).await
    }

    async fn await_truncate_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoTruncateResponse, LearnerError> {
        self.await_truncate_result(batch_number, position).await
    }
}

// ---------------------------------------------------------------------------
// Result storage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) enum ProposalResult {
    Cas(ProtoCompareAndSetResponse),
    Truncate(Result<ProtoTruncateResponse, String>),
}

pub(crate) enum ResultWaiter {
    Cas {
        position: u32,
        reply: oneshot::Sender<ProtoCompareAndSetResponse>,
        received_at: tokio::time::Instant,
    },
    Truncate {
        position: u32,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, String>>,
        received_at: tokio::time::Instant,
    },
}

// ---------------------------------------------------------------------------
// EventSource
// ---------------------------------------------------------------------------

/// A source of listen events for the learner.
///
/// In production, [`ChannelEventSource`] wraps a dedicated task running
/// `Subscribe::fetch_next()` with events fed through an mpsc channel. In tests,
/// a mock can deliver events deterministically.
#[async_trait::async_trait]
pub trait EventSource: Send {
    /// Wait for the next batch of listen events.
    /// Returns `None` when the source is exhausted or shut down.
    async fn next_events(
        &mut self,
    ) -> Option<Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>>;
}

/// Production event source: receives events from a dedicated listen task via an
/// mpsc channel. The task isolates the non-cancel-safe `Subscribe::fetch_next()`
/// from the learner's select loop.
pub struct ChannelEventSource {
    event_rx: mpsc::Receiver<Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>>,
    _listen_task: mz_ore::task::JoinHandle<()>,
}

impl ChannelEventSource {
    /// Create a new channel event source from a persist `Subscribe`.
    ///
    /// Spawns a dedicated task that calls `fetch_next()` in a loop and feeds
    /// events through an mpsc channel. The `Subscribe` first delivers a
    /// snapshot of all existing data, then switches to incremental updates.
    pub fn new(subscribe: Subscribe<OrderedKey, Proposal, u64, i64>) -> Self {
        let (event_tx, event_rx) = mpsc::channel(256);
        let listen_task = spawn_listen_task(subscribe, event_tx);
        ChannelEventSource {
            event_rx,
            _listen_task: listen_task,
        }
    }
}

#[async_trait::async_trait]
impl EventSource for ChannelEventSource {
    async fn next_events(
        &mut self,
    ) -> Option<Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>> {
        self.event_rx.recv().await
    }
}

/// Spawns a dedicated task that runs `Subscribe::fetch_next()` in a loop,
/// sending events through a channel.
fn spawn_listen_task(
    mut subscribe: Subscribe<OrderedKey, Proposal, u64, i64>,
    event_tx: mpsc::Sender<Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>>,
) -> mz_ore::task::JoinHandle<()> {
    mz_ore::task::spawn(|| "persist-subscribe", async move {
        loop {
            let events = subscribe.fetch_next().await;
            if event_tx.send(events).await.is_err() {
                // Learner dropped the receiver — shut down.
                break;
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Learner
// ---------------------------------------------------------------------------

/// The learner.
///
/// A passive state machine that applies proposals from a persist shard
/// subscription and serves reads. Generic over [`EventSource`] to allow both
/// production (channel-backed) and test (mock) event delivery.
///
/// Separates **mechanism** (applying events, serving reads, flushing
/// retractions) from **policy** (when to fetch upper, when to sweep pending retractions).
/// The caller drives the learner via [`on_events`](Self::on_events),
/// [`on_upper`](Self::on_upper), [`on_command`](Self::on_command), and
/// [`flush_pending_retractions`](Self::flush_pending_retractions). The production
/// [`run()`](Self::run) method is one such driver.
pub struct PersistLearner<E: EventSource = ChannelEventSource> {
    /// Global: client shard state, accumulated across all log shards.
    state: StateMachine,

    /// Per-log-shard state. In static sharding (Phase 1-3), this is a single
    /// entry. During reconfiguration (Phase 4), may temporarily hold multiple.
    log_shard: LogShardState<E>,

    // --- Configuration ---
    #[allow(dead_code)] // Retraction interval removed; config kept for future use.
    config: PersistLearnerConfig,

    // --- Metrics ---
    metrics: LearnerMetrics,

    // --- Channels ---
    cmd_rx: mpsc::Receiver<PersistLearnerCommand>,

    // --- Bus-stand linearization ---
    /// Reads waiting for the current upper fetch to complete.
    pending_reads: Vec<ReadCommand>,
    /// Reads keyed by linearization target timestamp, waiting for the listen
    /// frontier to reach their target before they can be served.
    linearizing_reads: BTreeMap<u64, Vec<ReadCommand>>,
}

impl PersistLearner<ChannelEventSource> {
    /// Creates a new persist-backed learner with a [`ChannelEventSource`] and
    /// returns a handle.
    ///
    /// The `Subscribe` delivers a snapshot of existing shard state followed by
    /// incremental updates. A dedicated task feeds events through a channel to
    /// avoid cancel-safety issues with `fetch_next()` in a `select!`.
    pub fn new(
        config: PersistLearnerConfig,
        subscribe: Subscribe<OrderedKey, Proposal, u64, i64>,
        metrics: LearnerMetrics,
    ) -> (Self, PersistLearnerHandle) {
        let event_source = ChannelEventSource::new(subscribe);
        Self::new_with_event_source(config, event_source, metrics)
    }
}

impl<E: EventSource> PersistLearner<E> {
    /// Creates a new learner with a custom event source and returns a handle.
    ///
    /// This constructor allows tests to inject a mock [`EventSource`] for
    /// deterministic event delivery.
    pub fn new_with_event_source(
        config: PersistLearnerConfig,
        event_source: E,
        metrics: LearnerMetrics,
    ) -> (Self, PersistLearnerHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(config.queue_depth);

        let log_shard = LogShardState {
            event_source,
            listen_frontier: Antichain::from_elem(0),
            results: BTreeMap::new(),
            result_waiters: BTreeMap::new(),
            pending_retractions: BTreeMap::new(),
            retraction_waiters: Vec::new(),
        };

        let learner = PersistLearner {
            state: StateMachine::new(),
            log_shard,
            config,
            metrics,
            cmd_rx,
            pending_reads: Vec::new(),
            linearizing_reads: BTreeMap::new(),
        };
        let handle = PersistLearnerHandle::new(cmd_tx);
        (learner, handle)
    }

    // -------------------------------------------------------------------
    // Event-level public API
    // -------------------------------------------------------------------

    /// Process a batch of listen events and wake any linearizing reads.
    ///
    /// This is the primary entry point for advancing the learner's state.
    /// Internally calls `process_listen_events` (applies proposals to the
    /// state machine, updates the listen frontier) then `wake_linearizing_reads`
    /// (serves any reads whose linearization target has been reached).
    pub fn on_events(&mut self, events: Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>) {
        self.process_listen_events(events);
        self.wake_linearizing_reads();
    }

    /// Returns the listen frontier from the current log shard.
    pub fn listen_frontier(&self) -> &Antichain<u64> {
        &self.log_shard.listen_frontier
    }

    /// An upper fetch completed: assign linearization targets and wake reads.
    ///
    /// Moves all pending reads into the linearizing set with the given upper
    /// as their target, then checks whether any can already be served.
    pub fn on_upper(&mut self, upper: Antichain<u64>) {
        let target = match upper.as_option().copied() {
            Some(t) => t,
            None => {
                // Upper is [] (sealed). The shard is closed — no more writes
                // will arrive. Drop all pending reads without replying; the
                // handle interprets a dropped reply as LearnerError::DroppedReply,
                // and the router retries against the new shard's learners.
                self.pending_reads.clear();
                return;
            }
        };
        // Assign the fetched upper as the linearization target for all pending
        // reads, then check if any can be served immediately.
        let pending = std::mem::take(&mut self.pending_reads);
        self.linearizing_reads
            .entry(target)
            .or_default()
            .extend(pending);
        self.wake_linearizing_reads();
    }

    /// Handle a learner command.
    ///
    /// Read commands go into the pending reads set (awaiting an upper fetch).
    /// Result-await commands are resolved immediately if the result is cached,
    /// or buffered for later wakeup.
    pub fn on_command(&mut self, cmd: PersistLearnerCommand) {
        self.handle_command(cmd);
    }

    /// Returns true if there are reads waiting for an upper fetch.
    ///
    /// When this returns true, the caller should fetch the shard upper and
    /// pass it to [`on_upper`](Self::on_upper).
    pub fn has_pending_reads(&self) -> bool {
        !self.pending_reads.is_empty()
    }

    /// Returns true if there are pending retractions waiting to be flushed.
    ///
    /// When this returns true, the caller may choose to call
    /// [`flush_pending_retractions`](Self::flush_pending_retractions).
    pub fn has_pending_retractions(&self) -> bool {
        !self.log_shard.pending_retractions.is_empty()
    }

    // -------------------------------------------------------------------
    // Production policy: run loop
    // -------------------------------------------------------------------

    /// Runs the learner loop. This is the **production policy**.
    ///
    /// Fetches events from the event source, fetches upper when reads are
    /// pending, sweeps retractions on a timer, and handles commands.
    pub async fn run(mut self, upper_handle: WriteHandle<OrderedKey, Proposal, u64, i64>) {
        // Spawn a dedicated task for upper fetches. This avoids wasted
        // consensus reads: the old approach recreated the fetch_recent_upper
        // future each select! iteration, so in-flight RPCs were cancelled
        // whenever a higher-priority branch resolved first.
        let (upper_request_tx, mut upper_request_rx) = mpsc::channel::<()>(1);
        let (upper_result_tx, mut upper_result_rx) = mpsc::unbounded_channel::<Antichain<u64>>();
        // When run() returns, upper_request_tx is dropped, which causes the
        // spawned task's recv() to return None and exit.
        let _upper_task = mz_ore::task::spawn(|| "persist-learner-upper", async move {
            let mut handle = upper_handle;
            while upper_request_rx.recv().await.is_some() {
                let upper = handle.fetch_recent_upper().await.clone();
                if upper_result_tx.send(upper).is_err() {
                    break;
                }
            }
        });

        let mut fetch_in_flight = false;

        loop {
            // Request an upper fetch if reads are waiting and no fetch is
            // already in flight.
            if self.has_pending_reads() && !fetch_in_flight {
                if upper_request_tx.try_send(()).is_ok() {
                    fetch_in_flight = true;
                }
            }

            tokio::select! {
                biased;
                // cancel-safety: per tokio docs
                events = self.log_shard.event_source.next_events() => {
                    match events {
                        Some(events) => self.on_events(events),
                        None => {
                            warn!("event source closed");
                            return;
                        }
                    }
                }
                // cancel-safety: channel recv is cancel-safe per tokio docs
                Some(upper) = upper_result_rx.recv() => {
                    fetch_in_flight = false;
                    self.on_upper(upper);
                }
                // cancel-safety: per tokio docs
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.on_command(cmd),
                        None => return,
                    }
                }
            }
        }
    }

    /// Process a batch of listen events from the listen task channel.
    fn process_listen_events(
        &mut self,
        events: Vec<ListenEvent<u64, ((OrderedKey, Proposal), u64, i64)>>,
    ) {
        // Collect updates grouped by timestamp (batch number).
        let mut updates_by_ts: BTreeMap<u64, Vec<(OrderedKey, Proposal, i64)>> = BTreeMap::new();
        for event in events {
            match event {
                ListenEvent::Updates(updates) => {
                    for ((key, proposal), ts, diff) in updates {
                        assert!(diff == 1 || diff == -1, "unexpected diff: {diff}");
                        updates_by_ts
                            .entry(ts)
                            .or_default()
                            .push((key, proposal, diff));
                    }
                }
                ListenEvent::Progress(frontier) => {
                    self.log_shard.listen_frontier = frontier;
                    self.wake_retraction_waiters();
                }
            }
        }

        // Apply each batch in timestamp order.
        for (batch_number, mut entries) in updates_by_ts {
            // Sort by (batch_id, position) for stable ordering through compaction
            entries.sort_by(|a, b| {
                a.0.batch_id
                    .cmp(&b.0.batch_id)
                    .then(a.0.position.cmp(&b.0.position))
            });
            self.apply_batch(batch_number, entries);
        }
    }

    /// Apply a single batch of proposals at the given timestamp.
    fn apply_batch(&mut self, batch_number: u64, entries: Vec<(OrderedKey, Proposal, i64)>) {
        let batch_start = tokio::time::Instant::now();
        let num_entries = entries.len();
        debug!(
            batch_number,
            entries = num_entries,
            "applying persist batch"
        );

        // Results are indexed by position within the batch.
        // Only +1 diff entries produce results.
        let mut batch_results = Vec::new();

        for (key, proposal_data, diff) in entries {
            if diff == 1 {
                // Insertion: track in live_keys, apply proposal, produce result.
                self.state.live_keys.insert(key.clone());

                match ProtoLogProposal::decode(proposal_data.encoded.as_ref()) {
                    Ok(proposal) => match proposal.op {
                        Some(proto_log_proposal::Op::Cas(cas)) => {
                            let (result, retraction) =
                                self.state.apply_cas(cas, key.clone(), proposal_data);
                            if result.committed {
                                self.metrics.cas_committed.inc();
                            } else {
                                self.metrics.cas_rejected.inc();
                            }
                            if let Some((rk, rp)) = retraction {
                                self.log_shard.pending_retractions.insert(rk, rp);
                            }
                            while batch_results.len() <= usize::cast_from(key.position) {
                                batch_results.push(None);
                            }
                            batch_results[usize::cast_from(key.position)] =
                                Some(ProposalResult::Cas(result));
                        }
                        Some(proto_log_proposal::Op::Truncate(trunc)) => {
                            let (result, retractions) =
                                self.state
                                    .apply_truncate(&trunc, key.clone(), proposal_data);
                            self.metrics.truncate_ops.inc();
                            for (rk, rp) in retractions {
                                self.log_shard.pending_retractions.insert(rk, rp);
                            }
                            while batch_results.len() <= usize::cast_from(key.position) {
                                batch_results.push(None);
                            }
                            batch_results[usize::cast_from(key.position)] =
                                Some(ProposalResult::Truncate(result));
                        }
                        None => {
                            warn!(batch_number, "proposal with no op, skipping");
                            self.log_shard
                                .pending_retractions
                                .insert(key.clone(), proposal_data);
                            while batch_results.len() <= usize::cast_from(key.position) {
                                batch_results.push(None);
                            }
                            batch_results[usize::cast_from(key.position)] =
                                Some(ProposalResult::Cas(ProtoCompareAndSetResponse {
                                    committed: false,
                                }));
                        }
                    },
                    Err(e) => {
                        warn!(batch_number, "failed to decode proposal: {}, skipping", e);
                        self.log_shard
                            .pending_retractions
                            .insert(key.clone(), proposal_data);
                        while batch_results.len() <= usize::cast_from(key.position) {
                            batch_results.push(None);
                        }
                        batch_results[usize::cast_from(key.position)] =
                            Some(ProposalResult::Cas(ProtoCompareAndSetResponse {
                                committed: false,
                            }));
                    }
                }
            } else {
                debug_assert_eq!(diff, -1);
                // Retraction: remove from live_keys + state, prune pending retractions.
                self.state.apply_retraction(&key);
                self.log_shard.pending_retractions.remove(&key);

                // Clean up the result for this retracted proposal.
                self.log_shard
                    .results
                    .get_mut(&key.batch_id)
                    .map(|results| {
                        if let Some(slot) = results.get_mut(usize::cast_from(key.position)) {
                            *slot = ProposalResult::Cas(ProtoCompareAndSetResponse {
                                committed: false,
                            });
                        }
                    });
            }
        }

        // Convert Option<ProposalResult> to ProposalResult for storage.
        let batch_results: Vec<ProposalResult> = batch_results.into_iter().flatten().collect();

        self.log_shard.results.insert(batch_number, batch_results);
        self.metrics.batches_materialized.inc();
        self.metrics
            .batch_materialize_latency_seconds
            .observe(batch_start.elapsed().as_secs_f64());

        // Update state gauges from incrementally maintained counters (O(1)).
        self.metrics
            .active_shards
            .set(i64::try_from(self.state.shards.len()).expect("shard count"));
        self.metrics
            .total_entries
            .set(i64::try_from(self.state.total_entries).expect("entry count"));
        self.metrics
            .approx_bytes
            .set(i64::try_from(self.state.approx_bytes).expect("byte count"));

        self.wake_result_waiters(batch_number);
    }

    // -----------------------------------------------------------------------
    // Command handling
    // -----------------------------------------------------------------------

    fn handle_command(&mut self, cmd: PersistLearnerCommand) {
        match cmd {
            PersistLearnerCommand::Head {
                key,
                reply,
                received_at,
            } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                self.pending_reads.push(ReadCommand::Head {
                    key,
                    reply,
                    received_at,
                });
            }
            PersistLearnerCommand::Scan {
                key,
                from,
                limit,
                reply,
                received_at,
            } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                self.pending_reads.push(ReadCommand::Scan {
                    key,
                    from,
                    limit,
                    reply,
                    received_at,
                });
            }
            PersistLearnerCommand::ListKeys { reply, received_at } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                self.pending_reads
                    .push(ReadCommand::ListKeys { reply, received_at });
            }
            PersistLearnerCommand::AwaitCasResult {
                batch_number,
                position,
                reply,
                received_at,
            } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                if let Some(results) = self.log_shard.results.get(&batch_number) {
                    if let Some(ProposalResult::Cas(result)) =
                        results.get(usize::cast_from(position))
                    {
                        self.metrics
                            .cas_result_seconds
                            .observe(received_at.elapsed().as_secs_f64());
                        let _ = reply.send(result.clone());
                        return;
                    }
                }
                self.log_shard
                    .result_waiters
                    .entry(batch_number)
                    .or_default()
                    .push(ResultWaiter::Cas {
                        position,
                        reply,
                        received_at,
                    });
            }
            PersistLearnerCommand::AwaitTruncateResult {
                batch_number,
                position,
                reply,
                received_at,
            } => {
                self.metrics
                    .cmd_queue_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                if let Some(results) = self.log_shard.results.get(&batch_number) {
                    if let Some(ProposalResult::Truncate(result)) =
                        results.get(usize::cast_from(position))
                    {
                        self.metrics
                            .truncate_result_seconds
                            .observe(received_at.elapsed().as_secs_f64());
                        let _ = reply.send(result.clone());
                        return;
                    }
                }
                self.log_shard
                    .result_waiters
                    .entry(batch_number)
                    .or_default()
                    .push(ResultWaiter::Truncate {
                        position,
                        reply,
                        received_at,
                    });
            }
            PersistLearnerCommand::GetRetractions { frontier, reply } => {
                let listen_frontier = self
                    .log_shard
                    .listen_frontier
                    .as_option()
                    .copied()
                    .unwrap_or(0);
                if listen_frontier >= frontier {
                    // We've processed all updates through the requested
                    // frontier — return the exact retraction set.
                    let retractions: Vec<_> = self
                        .log_shard
                        .pending_retractions
                        .iter()
                        .filter(|(key, _)| key.batch_id < frontier)
                        .map(|(key, proposal)| (key.clone(), proposal.clone()))
                        .collect();
                    let _ = reply.send(retractions);
                } else {
                    // Haven't caught up yet — park the request until the
                    // listen frontier advances past the requested frontier.
                    self.log_shard
                        .retraction_waiters
                        .push(RetractionWaiter { frontier, reply });
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Learner retractions
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Bus-stand read linearization
    // -----------------------------------------------------------------------

    /// Serve any linearizing reads whose target has been reached by the listen.
    fn wake_linearizing_reads(&mut self) {
        let frontier = self
            .log_shard
            .listen_frontier
            .as_option()
            .copied()
            .unwrap_or(u64::MAX);
        // Reads with target <= frontier are ready. split_off at frontier+1
        // so that entries with key == frontier stay in the ready set.
        let ready = if frontier == u64::MAX {
            std::mem::take(&mut self.linearizing_reads)
        } else {
            let still_waiting = self.linearizing_reads.split_off(&(frontier + 1));
            std::mem::replace(&mut self.linearizing_reads, still_waiting)
        };

        for (_target, cmds) in ready {
            for cmd in cmds {
                self.serve_read(cmd);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    fn serve_read(&self, cmd: ReadCommand) {
        match cmd {
            ReadCommand::Head {
                key,
                reply,
                received_at,
            } => {
                self.metrics.head_ops.inc();
                self.metrics
                    .head_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                let _ = reply.send(self.state.head(&key));
            }
            ReadCommand::Scan {
                key,
                from,
                limit,
                reply,
                received_at,
            } => {
                self.metrics.scan_ops.inc();
                self.metrics
                    .scan_seconds
                    .observe(received_at.elapsed().as_secs_f64());
                let _ = reply.send(self.state.scan(&key, from, limit));
            }
            ReadCommand::ListKeys {
                reply,
                received_at: _,
            } => {
                self.metrics.list_keys_ops.inc();
                let _ = reply.send(self.state.keys());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Waiter management
    // -----------------------------------------------------------------------

    fn wake_result_waiters(&mut self, batch_number: u64) {
        let waiters = match self.log_shard.result_waiters.remove(&batch_number) {
            Some(w) => w,
            None => return,
        };
        let results = match self.log_shard.results.get(&batch_number) {
            Some(r) => r,
            None => return,
        };

        for waiter in waiters {
            match waiter {
                ResultWaiter::Cas {
                    position,
                    reply,
                    received_at,
                } => {
                    if let Some(ProposalResult::Cas(result)) =
                        results.get(usize::cast_from(position))
                    {
                        self.metrics
                            .cas_result_seconds
                            .observe(received_at.elapsed().as_secs_f64());
                        let _ = reply.send(result.clone());
                    }
                }
                ResultWaiter::Truncate {
                    position,
                    reply,
                    received_at,
                } => {
                    if let Some(ProposalResult::Truncate(result)) =
                        results.get(usize::cast_from(position))
                    {
                        self.metrics
                            .truncate_result_seconds
                            .observe(received_at.elapsed().as_secs_f64());
                        let _ = reply.send(result.clone());
                    }
                }
            }
        }
    }

    /// Wake retraction waiters whose requested frontier is now ≤ the listen
    /// frontier. Called after the listen frontier advances.
    fn wake_retraction_waiters(&mut self) {
        let listen_frontier = self
            .log_shard
            .listen_frontier
            .as_option()
            .copied()
            .unwrap_or(0);

        // Drain waiters whose frontier has been reached.
        let mut remaining = Vec::new();
        for waiter in self.log_shard.retraction_waiters.drain(..) {
            if listen_frontier >= waiter.frontier {
                let retractions: Vec<_> = self
                    .log_shard
                    .pending_retractions
                    .iter()
                    .filter(|(key, _)| key.batch_id < waiter.frontier)
                    .map(|(key, proposal)| (key.clone(), proposal.clone()))
                    .collect();
                let _ = waiter.reply.send(retractions);
            } else {
                remaining.push(waiter);
            }
        }
        self.log_shard.retraction_waiters = remaining;
    }
}

impl PersistLearner<ChannelEventSource> {
    /// Opens a persist shard and spawns the learner as a tokio task.
    ///
    /// The learner subscribes to its own shard and processes events. It does
    /// NOT read predecessor shards — the acceptor is responsible for writing
    /// predecessor state into the shard (batch_id=1 bulk snapshot, batch_id=2
    /// delta snapshot) before accepting regular traffic. The learner just sees
    /// those entries via its subscribe and applies CaS/truncate semantics.
    pub async fn spawn(
        config: PersistLearnerConfig,
        client: &PersistClient,
        shard_id: ShardId,
        metrics: LearnerMetrics,
    ) -> (PersistLearnerHandle, mz_ore::task::JoinHandle<()>) {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        let (upper_handle, read) = client
            .open::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("persist-shared-log-learner"),
                false,
            )
            .await
            .expect("failed to open persist shard for learner");

        // NOTE: We do NOT advance upper past T=0 here. The acceptor always
        // writes setup batches at batch_id=1 and batch_id=2 (even when empty),
        // which advances upper. The learner's subscribe unblocks when the
        // acceptor writes batch_id=1.

        let since = read.since().clone();
        let subscribe = read
            .subscribe(since)
            .await
            .expect("subscribe should succeed");

        let (learner, handle) = Self::new(config, subscribe, metrics);

        let task = mz_ore::task::spawn(|| "persist-learner", async move {
            learner.run(upper_handle).await;
        });

        (handle, task)
    }
}
