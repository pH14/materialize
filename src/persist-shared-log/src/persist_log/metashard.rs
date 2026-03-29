// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metashard actor: maintains the partition map, coordinates reconfigurations,
//! and manages acceptor/learner lifecycle.
//!
//! The metashard actor holds a [`MetashardState`] in memory and serves lookups.
//! On reconfiguration, it orchestrates the full lifecycle: validate → spawn new
//! actors → seal old shards → update partition map → swap routing.
//!
//! Follows the same actor pattern as the acceptor and learner: a passive state
//! machine driven by a command channel, with a handle type for sending commands.

use std::collections::BTreeMap;
use std::sync::Arc;

use timely::progress::Antichain;
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{debug, info};

use bytes::Bytes;
use prost::Message;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::{ProtoCasProposal, ProtoLogProposal, proto_log_proposal};
use mz_persist_client::critical::{CriticalReaderId, Opaque, SinceHandle};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};

use crate::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
use crate::persist_log::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::sharded_service::RoutingState;
use crate::{AcceptorConfig, MetashardError, PartitionMap, RangeAssignment, ReconfigurationPlan};

// ---------------------------------------------------------------------------
// Metashard state
// ---------------------------------------------------------------------------

/// Log shard status in the metashard.
#[derive(Debug, Clone, PartialEq)]
pub enum LogShardStatus {
    /// Actively accepting writes.
    Active,
    /// Sealed (upper = []), no more writes. Data still readable.
    Sealed,
    /// Finalized: snapshot downstream covers this shard's state, data can be
    /// compacted away.
    Finalized,
}

/// Per-log-shard metadata tracked by the metashard.
#[derive(Debug, Clone)]
pub struct LogShardInfo {
    pub status: LogShardStatus,
    pub epoch_created: u64,
    pub epoch_sealed: Option<u64>,
    pub range: RangeAssignment,
    /// The log shard(s) this one succeeded for overlapping ranges.
    /// Multiple predecessors occur in fan-in (merge) reconfigurations.
    pub predecessors: Vec<ShardId>,
    /// Whether this shard contains T=0 snapshot entries from its predecessor.
    pub has_snapshot: bool,
}

/// Status of a reconfiguration intent.
#[derive(Debug, Clone, PartialEq)]
pub enum IntentStatus {
    /// Intent written, not yet started.
    Preparing,
    /// Old shards sealed.
    Sealed,
    /// Snapshots written, new actors spawned.
    Committed,
}

/// A durable reconfiguration intent. Written to the metashard persist shard
/// before sealing, so that a crash mid-reconfiguration can be detected and
/// completed on restart.
#[derive(Debug, Clone)]
pub struct ReconfigurationIntent {
    pub epoch: u64,
    pub plan: ReconfigurationPlan,
    pub status: IntentStatus,
}

/// The metashard actor's in-memory materialized state.
#[derive(Debug, Clone)]
pub struct MetashardState {
    /// Current configuration epoch.
    pub epoch: u64,
    /// The authoritative partition map.
    pub partition_map: PartitionMap,
    /// Per-log-shard metadata.
    pub log_shards: BTreeMap<ShardId, LogShardInfo>,
    /// In-flight reconfiguration intent (if any).
    pub pending_intent: Option<ReconfigurationIntent>,
}

impl MetashardState {
    /// Create initial state with a single log shard covering the entire range.
    pub fn single(log_shard: ShardId) -> Self {
        let range = RangeAssignment {
            lo: 0x00,
            hi_exclusive: 0x100,
            log_shard,
        };
        let mut log_shards = BTreeMap::new();
        log_shards.insert(
            log_shard,
            LogShardInfo {
                status: LogShardStatus::Active,
                epoch_created: 0,
                epoch_sealed: None,
                range: range.clone(),
                predecessors: Vec::new(),
                has_snapshot: false,
            },
        );
        MetashardState {
            epoch: 0,
            partition_map: PartitionMap::single(log_shard),
            log_shards,
            pending_intent: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands dispatched to the metashard actor.
pub enum MetashardCommand {
    /// Look up which log shard owns a client shard.
    Lookup {
        client_shard: String,
        reply: oneshot::Sender<Result<ShardId, MetashardError>>,
    },
    /// Return the current partition map.
    GetPartitionMap {
        reply: oneshot::Sender<Result<PartitionMap, MetashardError>>,
    },
    /// Return the current epoch.
    GetEpoch {
        reply: oneshot::Sender<Result<u64, MetashardError>>,
    },
    /// Execute a reconfiguration.
    Reconfigure {
        plan: ReconfigurationPlan,
        reply: oneshot::Sender<Result<u64, MetashardError>>,
    },
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// A typed handle to the metashard actor's command channel.
#[derive(Debug, Clone)]
pub struct PersistMetashardHandle {
    tx: mpsc::Sender<MetashardCommand>,
}

impl PersistMetashardHandle {
    pub fn new(tx: mpsc::Sender<MetashardCommand>) -> Self {
        PersistMetashardHandle { tx }
    }
}

#[async_trait::async_trait]
impl crate::Metashard for PersistMetashardHandle {
    async fn lookup(&self, client_shard: &str) -> Result<ShardId, MetashardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetashardCommand::Lookup {
                client_shard: client_shard.to_string(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| MetashardError::Shutdown)?;
        reply_rx.await.map_err(|_| MetashardError::DroppedReply)?
    }

    async fn partition_map(&self) -> Result<PartitionMap, MetashardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetashardCommand::GetPartitionMap { reply: reply_tx })
            .await
            .map_err(|_| MetashardError::Shutdown)?;
        reply_rx.await.map_err(|_| MetashardError::DroppedReply)?
    }

    async fn current_epoch(&self) -> Result<u64, MetashardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetashardCommand::GetEpoch { reply: reply_tx })
            .await
            .map_err(|_| MetashardError::Shutdown)?;
        reply_rx.await.map_err(|_| MetashardError::DroppedReply)?
    }

    async fn reconfigure(&self, plan: ReconfigurationPlan) -> Result<u64, MetashardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetashardCommand::Reconfigure {
                plan,
                reply: reply_tx,
            })
            .await
            .map_err(|_| MetashardError::Shutdown)?;
        reply_rx.await.map_err(|_| MetashardError::DroppedReply)?
    }
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

/// The metashard actor.
///
/// Maintains an in-memory [`MetashardState`] and serves commands from the
/// handle. On reconfiguration, orchestrates: validate → spawn new actors →
/// seal old shards → update partition map → swap routing state.
pub struct PersistMetashardActor {
    state: MetashardState,
    rx: mpsc::Receiver<MetashardCommand>,
    /// PersistClient for creating new log shard persist shards and spawning actors.
    persist_client: PersistClient,
    /// Metrics registry for spawning new acceptors and learners.
    #[allow(dead_code)]
    metrics_registry: MetricsRegistry,
    /// Handle to the ShardedService's routing state, for atomic swaps during reconfiguration.
    routing: Arc<RwLock<RoutingState<PersistAcceptorHandle, PersistLearnerHandle>>>,
    /// Whether a reconfiguration is currently in progress.
    reconfiguring: bool,
    /// Optional persist shard for durable metashard state. When provided, the
    /// partition map and reconfiguration intents are written here so they survive
    /// process restarts. The shard uses a special convention: key
    /// `"__metashard_epoch_{N}"` with the serialized partition map as data.
    metashard_shard_id: Option<ShardId>,
    /// Write handle for the metashard persist shard (if any).
    metashard_write: Option<mz_persist_client::write::WriteHandle<OrderedKey, Proposal, u64, i64>>,
}

impl PersistMetashardActor {
    /// Create a new metashard actor.
    pub fn new(
        state: MetashardState,
        queue_depth: usize,
        persist_client: PersistClient,
        metrics_registry: MetricsRegistry,
        routing: Arc<RwLock<RoutingState<PersistAcceptorHandle, PersistLearnerHandle>>>,
    ) -> (Self, PersistMetashardHandle) {
        let (tx, rx) = mpsc::channel(queue_depth);
        let actor = PersistMetashardActor {
            state,
            rx,
            persist_client,
            metrics_registry,
            routing,
            reconfiguring: false,
            metashard_shard_id: None,
            metashard_write: None,
        };
        let handle = PersistMetashardHandle::new(tx);
        (actor, handle)
    }

    /// Configure this actor to persist its state to a persist shard.
    /// Reads existing state from the shard on startup to recover pending
    /// intents from a previous crash. Must be called before `run()`.
    pub async fn with_durable_state(mut self, shard_id: ShardId) -> Self {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        let mut write = self
            .persist_client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("metashard-durable-state"),
            )
            .await
            .expect("open metashard persist shard writer");

        // Advance past T=0 if fresh.
        if write.upper().as_option() == Some(&0) {
            write.advance_upper(&Antichain::from_elem(1)).await;
        }

        // Read existing state to recover pending intents.
        let (_, read) = self
            .persist_client
            .open::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("metashard-durable-state-read"),
                false,
            )
            .await
            .expect("open metashard persist shard reader");

        let since = read.since().clone();
        let mut subscribe = read
            .subscribe(since)
            .await
            .expect("subscribe to metashard shard");

        // Read the latest state entry.
        let mut latest_data: Option<String> = None;
        loop {
            let events = subscribe.fetch_next().await;
            let mut done = false;
            for event in &events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        // Stop when we've read up to the upper.
                        if frontier.as_option().copied() >= write.upper().as_option().copied() {
                            done = true;
                        }
                    }
                    ListenEvent::Updates(updates) => {
                        for ((key, proposal), _ts, diff) in updates {
                            if *diff == 1 && key.shard == "__metashard" {
                                latest_data =
                                    Some(String::from_utf8_lossy(&proposal.encoded).to_string());
                            }
                        }
                    }
                }
            }
            if done {
                break;
            }
        }

        // Parse the latest state: restore partition map, epoch, and pending intent.
        if let Some(data) = latest_data {
            let mut persisted_epoch: Option<u64> = None;
            let mut persisted_ranges: Vec<RangeAssignment> = Vec::new();
            let mut intent_status = None;
            let mut intent_epoch = None;
            let mut intent_ranges = Vec::new();

            for line in data.lines() {
                if let Some(epoch_str) = line.strip_prefix("epoch=") {
                    persisted_epoch = epoch_str.parse::<u64>().ok();
                } else if let Some(range_str) = line.strip_prefix("range=") {
                    if let Some((range_part, shard_str)) = range_str.split_once(':') {
                        if let Some((lo_str, hi_str)) = range_part.split_once('-') {
                            if let (Ok(lo), Ok(hi), Ok(shard)) = (
                                u8::from_str_radix(lo_str, 16),
                                u16::from_str_radix(hi_str, 16),
                                shard_str.parse::<ShardId>(),
                            ) {
                                persisted_ranges.push(RangeAssignment {
                                    lo,
                                    hi_exclusive: hi,
                                    log_shard: shard,
                                });
                            }
                        }
                    }
                } else if let Some(pred_str) = line.strip_prefix("predecessor=") {
                    // Format: "shard_id:predecessor_shard_id"
                    if let Some((shard_str, pred_shard_str)) = pred_str.split_once(':') {
                        if let (Ok(shard), Ok(pred)) = (
                            shard_str.parse::<ShardId>(),
                            pred_shard_str.parse::<ShardId>(),
                        ) {
                            // Store in log_shards for use during rebuild_routing_from_state.
                            self.state
                                .log_shards
                                .entry(shard)
                                .or_insert_with(|| LogShardInfo {
                                    status: LogShardStatus::Active,
                                    epoch_created: 0,
                                    epoch_sealed: None,
                                    range: RangeAssignment {
                                        lo: 0,
                                        hi_exclusive: 0,
                                        log_shard: shard,
                                    },
                                    predecessors: Vec::new(),
                                    has_snapshot: false,
                                })
                                .predecessors.push(pred);
                        }
                    }
                } else if let Some(status) = line.strip_prefix("intent_status=") {
                    intent_status = Some(status.to_string());
                } else if let Some(epoch) = line.strip_prefix("intent_epoch=") {
                    intent_epoch = epoch.parse::<u64>().ok();
                } else if let Some(range_str) = line.strip_prefix("intent_range=") {
                    // Parse "lo-hi:shard_id"
                    if let Some((range_part, shard_str)) = range_str.split_once(':') {
                        if let Some((lo_str, hi_str)) = range_part.split_once('-') {
                            if let (Ok(lo), Ok(hi), Ok(shard)) = (
                                u8::from_str_radix(lo_str, 16),
                                u16::from_str_radix(hi_str, 16),
                                shard_str.parse::<ShardId>(),
                            ) {
                                intent_ranges.push(RangeAssignment {
                                    lo,
                                    hi_exclusive: hi,
                                    log_shard: shard,
                                });
                            }
                        }
                    }
                }
            }

            if let (Some(status_str), Some(epoch)) = (intent_status, intent_epoch) {
                let status = match status_str.as_str() {
                    "Preparing" => IntentStatus::Preparing,
                    "Sealed" => IntentStatus::Sealed,
                    "Committed" => IntentStatus::Committed,
                    _ => IntentStatus::Preparing,
                };

                if !intent_ranges.is_empty() {
                    let plan = ReconfigurationPlan {
                        expected_epoch: epoch.saturating_sub(1),
                        new_partition_map: PartitionMap {
                            epoch,
                            ranges: intent_ranges,
                        },
                    };
                    self.state.pending_intent = Some(ReconfigurationIntent {
                        epoch,
                        plan,
                        status,
                    });
                    info!(
                        epoch,
                        "recovered pending reconfiguration intent from durable state"
                    );
                }
            }

            // Restore the persisted partition map and epoch.
            if let Some(epoch) = persisted_epoch {
                if !persisted_ranges.is_empty() {
                    let map = PartitionMap {
                        epoch,
                        ranges: persisted_ranges,
                    };
                    if map.validate().is_ok() {
                        info!(
                            epoch,
                            num_ranges = map.ranges.len(),
                            "restored partition map from durable state"
                        );
                        self.state.epoch = epoch;
                        self.state.partition_map = map;
                    }
                }
            }
        }

        self.metashard_shard_id = Some(shard_id);
        self.metashard_write = Some(write);
        self
    }

    /// Persist the current metashard state to the durable shard (if configured).
    async fn persist_state(&mut self) {
        let write = match self.metashard_write.as_mut() {
            Some(w) => w,
            None => return, // No durable shard configured.
        };

        // Serialize the full metashard state including the pending intent and
        // its ReconfigurationPlan. Format is line-delimited key=value pairs.
        let mut lines = Vec::new();
        lines.push(format!("epoch={}", self.state.epoch));
        for r in &self.state.partition_map.ranges {
            lines.push(format!(
                "range={:02x}-{:03x}:{}",
                r.lo, r.hi_exclusive, r.log_shard
            ));
        }
        // Persist predecessor chains so recovery can use spawn_with_predecessors.
        for (shard_id, info) in &self.state.log_shards {
            for pred in &info.predecessors {
                lines.push(format!("predecessor={}:{}", shard_id, pred));
            }
        }
        if let Some(ref intent) = self.state.pending_intent {
            lines.push(format!("intent_status={:?}", intent.status));
            lines.push(format!("intent_epoch={}", intent.epoch));
            // Serialize the target partition map from the plan.
            for r in &intent.plan.new_partition_map.ranges {
                lines.push(format!(
                    "intent_range={:02x}-{:03x}:{}",
                    r.lo, r.hi_exclusive, r.log_shard
                ));
            }
        }
        let data = lines.join("\n");

        let batch_number = write
            .upper()
            .as_option()
            .copied()
            .unwrap_or(1)
            .max(1);

        let key = OrderedKey {
            batch_id: batch_number,
            position: 0,
            shard: "__metashard".to_string(),
        };
        let proposal = Proposal {
            encoded: Bytes::from(data.into_bytes()),
        };

        let upper = write.upper().clone();
        let new_upper = Antichain::from_elem(batch_number + 1);
        match write
            .compare_and_append(&[((key, proposal), batch_number, 1i64)], upper, new_upper)
            .await
        {
            Ok(Ok(())) => {
                debug!(epoch = self.state.epoch, "persisted metashard state");
            }
            Ok(Err(_)) => {
                debug!("metashard state persist upper mismatch (concurrent writer)");
            }
            Err(e) => {
                tracing::error!("metashard state persist error: {e}");
            }
        }
    }

    /// Rebuild the routing state from the current metashard state.
    ///
    /// Spawns fresh acceptors and learners for every shard in the partition map
    /// and swaps the routing state. Uses `spawn_with_predecessors` when
    /// predecessor info is available, so recovered learners can replay sealed
    /// predecessor shards and reconstruct carried-forward state.
    async fn rebuild_routing_from_state(&self) {
        let map = &self.state.partition_map;
        let mut acceptors = BTreeMap::new();
        let mut learners = BTreeMap::new();

        for range in &map.ranges {
            let shard_id = range.log_shard;
            let shard_registry = MetricsRegistry::new();
            let acceptor_metrics = AcceptorMetrics::register(&shard_registry);
            let learner_metrics = LearnerMetrics::register(&shard_registry);

            let (acceptor_handle, _task) = PersistAcceptor::spawn(
                AcceptorConfig::default(),
                &self.persist_client,
                shard_id,
                acceptor_metrics,
                self.state.epoch,
            )
            .await;

            // Walk the full transitive predecessor chain so that multi-hop
            // carried-forward state (L1→L2→L4) is reconstructed on recovery.
            let predecessors = self.transitive_predecessors(shard_id);

            let learner_handle = if predecessors.is_empty() {
                let (handle, _task) = PersistLearner::spawn(
                    PersistLearnerConfig::default(),
                    &self.persist_client,
                    shard_id,
                    learner_metrics,
                )
                .await;
                handle
            } else {
                info!(
                    %shard_id,
                    predecessors = ?predecessors,
                    "spawning recovered learner with predecessor replay"
                );
                let (handle, _task, replay_done_rx) =
                    PersistLearner::spawn_with_predecessors(
                        PersistLearnerConfig::default(),
                        &self.persist_client,
                        shard_id,
                        predecessors,
                        learner_metrics,
                    )
                    .await;
                // Wait for predecessor replay to complete before proceeding.
                if replay_done_rx.await.is_err() {
                    tracing::error!(
                        %shard_id,
                        "predecessor replay task died during recovery; \
                         learner may be missing carried-forward state"
                    );
                }
                handle
            };

            info!(%shard_id, "spawned recovered actor");
            acceptors.insert(shard_id, acceptor_handle);
            learners.insert(shard_id, learner_handle);
        }

        let mut routing = self.routing.write().await;
        *routing = RoutingState {
            partition_map: map.clone(),
            acceptors,
            learners,
        };
        drop(routing);
        info!(
            epoch = self.state.epoch,
            num_shards = map.ranges.len(),
            "rebuilt routing from recovered metashard state"
        );
    }

    /// Walk the predecessor chain transitively for a shard, returning all
    /// ancestors in replay order (oldest first).
    ///
    /// For example, if L4's predecessor is L2 and L2's predecessor is L1,
    /// this returns [L1, L2].
    fn transitive_predecessors(&self, shard_id: ShardId) -> Vec<ShardId> {
        let mut visited = std::collections::BTreeSet::new();
        let preds = self
            .state
            .log_shards
            .get(&shard_id)
            .map(|info| info.predecessors.clone())
            .unwrap_or_default();
        let mut chain = Vec::new();
        for pred in &preds {
            if visited.insert(*pred) {
                // Recurse: get this predecessor's own ancestors first (older→newer).
                chain.extend(self.transitive_predecessors_inner(*pred, &mut visited));
                chain.push(*pred);
            }
        }
        chain
    }

    /// Recursive helper for transitive_predecessors.
    fn transitive_predecessors_inner(
        &self,
        shard_id: ShardId,
        visited: &mut std::collections::BTreeSet<ShardId>,
    ) -> Vec<ShardId> {
        let preds = self
            .state
            .log_shards
            .get(&shard_id)
            .map(|info| info.predecessors.clone())
            .unwrap_or_default();
        let mut chain = Vec::new();
        for pred in &preds {
            if visited.insert(*pred) {
                chain.extend(self.transitive_predecessors_inner(*pred, visited));
                chain.push(*pred);
            }
        }
        chain
    }

    /// Handle a non-reconfigure command (fast, synchronous).
    fn on_query(&self, cmd: MetashardCommand) {
        match cmd {
            MetashardCommand::Lookup {
                client_shard,
                reply,
            } => {
                let result = Ok(self.state.partition_map.route(&client_shard));
                let _ = reply.send(result);
            }
            MetashardCommand::GetPartitionMap { reply } => {
                let _ = reply.send(Ok(self.state.partition_map.clone()));
            }
            MetashardCommand::GetEpoch { reply } => {
                let _ = reply.send(Ok(self.state.epoch));
            }
            MetashardCommand::Reconfigure { .. } => {
                unreachable!("Reconfigure handled separately in run loop")
            }
        }
    }

    /// Replay a predecessor shard and extract its committed head state.
    #[allow(dead_code)] // Used for future background snapshot writing after finalization.
    ///
    /// Works on both sealed shards (frontier reaches []) and active shards
    /// (stops when the listen frontier catches up to the upper at the time
    /// this method was called). This allows snapshotting BEFORE the seal.
    ///
    /// Returns a map of client_shard_key → (seqno, data).
    async fn replay_predecessor_head_state(
        &self,
        predecessor: ShardId,
    ) -> Result<BTreeMap<String, (u64, Vec<u8>)>, String> {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        let (write_handle, read) = self
            .persist_client
            .open::<OrderedKey, Proposal, u64, i64>(
                predecessor,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("metashard-snapshot-replay"),
                false,
            )
            .await
            .map_err(|e| format!("open predecessor for snapshot: {e}"))?;

        // Record the current upper — we'll read up to this point.
        let target_upper = write_handle
            .upper()
            .as_option()
            .copied()
            .unwrap_or(u64::MAX);

        let since = read.since().clone();
        let mut subscribe = read
            .subscribe(since)
            .await
            .map_err(|e| format!("subscribe to predecessor: {e:?}"))?;

        let mut head_state: BTreeMap<String, (u64, Vec<u8>)> = BTreeMap::new();

        loop {
            let events = subscribe.fetch_next().await;
            let mut done = false;
            for event in &events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        // Stop if sealed (frontier=[]) or caught up to target.
                        if frontier.is_empty() {
                            done = true;
                        } else if let Some(ts) = frontier.as_option() {
                            if *ts >= target_upper {
                                done = true;
                            }
                        }
                    }
                    ListenEvent::Updates(updates) => {
                        for ((_key, proposal_data), _ts, diff) in updates {
                            if *diff != 1 {
                                continue;
                            }
                            let proposal: ProtoLogProposal =
                                match Message::decode(proposal_data.encoded.as_ref()) {
                                    Ok(p) => p,
                                    Err(_) => continue,
                                };
                            if let Some(proto_log_proposal::Op::Cas(cas)) = proposal.op {
                                let current_seqno =
                                    head_state.get(&cas.key).map(|(s, _)| *s);
                                if current_seqno == cas.expected {
                                    head_state
                                        .insert(cas.key, (cas.new_seqno, cas.data));
                                }
                            }
                        }
                    }
                }
            }
            if done {
                break;
            }
        }

        Ok(head_state)
    }

    /// Write a combined head state as CaS proposals to a new shard, filtered
    /// by the destination range. Only entries whose `partition_key` falls within
    /// `dest_range` are written. Returns the number of entries written.
    #[allow(dead_code)] // Kept for future background snapshot writing after finalization.
    async fn write_snapshot_entries(
        &self,
        new_shard: ShardId,
        dest_range: &RangeAssignment,
        head_state: &BTreeMap<String, (u64, Vec<u8>)>,
    ) -> Result<usize, String> {
        // Filter head_state by partition key against the destination range.
        let filtered: Vec<(&String, &(u64, Vec<u8>))> = head_state
            .iter()
            .filter(|(key, _)| {
                let pk = crate::partition_key(key);
                pk >= dest_range.lo && u16::from(pk) < dest_range.hi_exclusive
            })
            .collect();

        if filtered.is_empty() {
            return Ok(0);
        }

        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        let mut write = self
            .persist_client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                new_shard,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("metashard-snapshot-write"),
            )
            .await
            .map_err(|e| format!("open writer for snapshot: {e}"))?;

        if write.upper().as_option() == Some(&0) {
            write.advance_upper(&Antichain::from_elem(1)).await;
        }

        let batch_number = 1u64;
        let mut updates = Vec::new();
        for (position, (shard_key, (seqno, data))) in filtered.iter().enumerate() {
            let cas = ProtoCasProposal {
                key: (*shard_key).clone(),
                expected: None,
                new_seqno: *seqno,
                data: data.clone(),
            };
            let proposal = ProtoLogProposal {
                op: Some(proto_log_proposal::Op::Cas(cas)),
            };
            let encoded = Proposal {
                encoded: Bytes::from(proposal.encode_to_vec()),
            };
            let ordered_key = OrderedKey {
                batch_id: batch_number,
                position: u32::try_from(position).expect("position fits u32"),
                shard: (*shard_key).clone(),
            };
            updates.push(((ordered_key, encoded), batch_number, 1i64));
        }

        let upper = write.upper().clone();
        let new_upper = Antichain::from_elem(batch_number + 1);
        match write.compare_and_append(&updates, upper, new_upper).await {
            Ok(Ok(())) => {
                info!(
                    %new_shard,
                    entries = filtered.len(),
                    "wrote range-filtered snapshot to new shard"
                );
                Ok(filtered.len())
            }
            Ok(Err(mismatch)) => Err(format!("snapshot write upper mismatch: {mismatch:?}")),
            Err(e) => Err(format!("snapshot write error: {e}")),
        }
    }

    /// Execute a reconfiguration.
    ///
    /// This is the core reconfiguration protocol:
    /// 1. Validate epoch
    /// 2. Identify new and retiring log shards
    /// 3. Spawn new acceptors + learners
    /// 4. Seal retiring log shards
    /// 5. Update partition map and swap routing state
    /// - Before sealing, new learners should subscribe to old shards and catch up
    /// - Acquire CriticalSince on old shard at the catch-up point
    /// - Write snapshot entries (T=0) to new shard during pre-hydration
    /// - Write delta entries (T=1) after seal
    /// - Release CriticalSince after delta confirmed
    /// See doc/reference/05_horizontal_sharding.md Section 12 for the full protocol.
    async fn do_reconfigure(&mut self, plan: ReconfigurationPlan) -> Result<u64, MetashardError> {
        // Phase 0: Validate.
        if plan.expected_epoch != self.state.epoch {
            return Err(MetashardError::EpochMismatch {
                expected: plan.expected_epoch,
                actual: self.state.epoch,
            });
        }
        plan.new_partition_map
            .validate()
            .map_err(|e| MetashardError::Command(format!("invalid partition map: {e}")))?;

        let old_map = self.state.partition_map.clone();
        let new_map = plan.new_partition_map.clone();
        let new_epoch = self.state.epoch + 1;

        // Identify which log shards are new and which are retiring.
        let old_shards: std::collections::BTreeSet<ShardId> =
            old_map.ranges.iter().map(|r| r.log_shard).collect();
        let new_shards: std::collections::BTreeSet<ShardId> =
            new_map.ranges.iter().map(|r| r.log_shard).collect();

        let added: Vec<ShardId> = new_shards.difference(&old_shards).copied().collect();
        let retiring: Vec<ShardId> = old_shards.difference(&new_shards).copied().collect();

        info!(
            old_epoch = self.state.epoch,
            new_epoch,
            added = ?added,
            retiring = ?retiring,
            "starting reconfiguration"
        );

        // Phase 0: Write ReconfigurationIntent (durable crash recovery marker).
        self.state.pending_intent = Some(ReconfigurationIntent {
            epoch: new_epoch,
            plan: plan.clone(),
            status: IntentStatus::Preparing,
        });
        self.persist_state().await;

        // Phase 0.5: Acquire CriticalSince on retiring shards to prevent
        // compaction during predecessor replay. Uses a deterministic reader ID
        // derived from the new epoch so it can be recovered after crash.
        let mut critical_holds: Vec<SinceHandle<OrderedKey, Proposal, u64, i64>> = Vec::new();
        for &shard_id in &retiring {
            let reader_id: CriticalReaderId = format!(
                "c{:0>8}-{:04x}-0000-0000-000000000000",
                new_epoch, shard_id.to_string().len()
            )
            .parse()
            .expect("valid CriticalReaderId");

            let handle = self
                .persist_client
                .open_critical_since::<OrderedKey, Proposal, u64, i64>(
                    shard_id,
                    reader_id,
                    Opaque::encode(&0i64),
                    Diagnostics::from_purpose("metashard-reconfig-critical-since"),
                )
                .await
                .expect("open_critical_since should succeed");

            info!(%shard_id, "acquired CriticalSince hold for predecessor replay");
            critical_holds.push(handle);
        }

        // NOTE: We intentionally do NOT write snapshot CaS rows to new shards
        // during the transition. The predecessor replay (spawn_with_predecessors)
        // handles state carryforward. Writing snapshot rows here would conflict:
        // the learner's predecessor replay builds state in memory first, then
        // when it processes the new shard's events, the expected=None snapshot
        // rows fail CaS evaluation (state already exists), get queued for
        // retraction, and are eventually deleted — making the carried-forward
        // state self-deleting.
        //
        // Snapshots for cold-start recovery (after old shard finalization) can
        // be written as a separate background step once the learner is caught up.
        let mut new_acceptors = BTreeMap::new();
        let mut new_learners = BTreeMap::new();
        let mut replay_done_receivers: Vec<tokio::sync::oneshot::Receiver<()>> = Vec::new();

        // Phase 2: Seal retiring log shards.
        // This happens AFTER snapshots are written, minimizing the
        // unavailability window. The old acceptors see the seal and return
        // AcceptorError::Sealed; the serving layer retries with the new routing.
        for &shard_id in &retiring {
            let key_schema = Arc::new(OrderedKeySchema);
            let val_schema = Arc::new(ProposalSchema);
            let mut write = self
                .persist_client
                .open_writer::<OrderedKey, Proposal, u64, i64>(
                    shard_id,
                    key_schema,
                    val_schema,
                    Diagnostics::from_purpose("metashard-seal"),
                )
                .await
                .expect("open writer for sealing");

            write.advance_upper(&Antichain::new()).await;
            info!(%shard_id, "sealed log shard");

            if let Some(info) = self.state.log_shards.get_mut(&shard_id) {
                info.status = LogShardStatus::Sealed;
                info.epoch_sealed = Some(new_epoch);
            }
        }

        // Update intent: sealed.
        if let Some(ref mut intent) = self.state.pending_intent {
            intent.status = IntentStatus::Sealed;
        }

        // Phase 2.5: Spawn actors for new log shards.
        for &shard_id in &added {
            let new_range = new_map
                .ranges
                .iter()
                .find(|r| r.log_shard == shard_id)
                .expect("new shard must be in new partition map");
            let predecessors: Vec<ShardId> = old_map
                .ranges
                .iter()
                .filter(|r| {
                    u16::from(new_range.lo) < r.hi_exclusive
                        && u16::from(r.lo) < new_range.hi_exclusive
                })
                .map(|r| r.log_shard)
                .filter(|s| retiring.contains(s))
                .collect();

            let shard_registry = MetricsRegistry::new();
            let acceptor_metrics = AcceptorMetrics::register(&shard_registry);
            let learner_metrics = LearnerMetrics::register(&shard_registry);

            let (acceptor_handle, _task) = PersistAcceptor::spawn(
                AcceptorConfig::default(),
                &self.persist_client,
                shard_id,
                acceptor_metrics,
                new_epoch,
            )
            .await;

            // Always use spawn_with_predecessors when predecessors exist.
            // Predecessor replay ensures the learner sees the complete history
            // from the old shard, including any tail proposals between the
            // snapshot point and the seal.
            let learner_handle = if predecessors.is_empty() {
                let (handle, _task) = PersistLearner::spawn(
                    PersistLearnerConfig::default(),
                    &self.persist_client,
                    shard_id,
                    learner_metrics,
                )
                .await;
                handle
            } else {
                info!(
                    %shard_id,
                    predecessors = ?predecessors,
                    "spawning learner with predecessor chain replay"
                );
                let (handle, _task, replay_done_rx) =
                    PersistLearner::spawn_with_predecessors(
                        PersistLearnerConfig::default(),
                        &self.persist_client,
                        shard_id,
                        predecessors,
                        learner_metrics,
                    )
                    .await;
                replay_done_receivers.push(replay_done_rx);
                handle
            };

            info!(%shard_id, "spawned actors for new log shard");
            new_acceptors.insert(shard_id, acceptor_handle);
            new_learners.insert(shard_id, learner_handle);
        }

        // Phase 3: Wait for all predecessor replays to complete BEFORE
        // committing the new epoch. If any replay fails, bail out — nothing
        // has been committed yet, CriticalSince holds are still active, and
        // the old routing is still in place.
        for rx in replay_done_receivers {
            if rx.await.is_err() {
                return Err(MetashardError::Command(
                    "predecessor replay task died before completing; \
                     reconfiguration aborted to preserve predecessor data"
                        .to_string(),
                ));
            }
        }
        info!("all predecessor replays complete — committing new epoch");

        // Phase 4: Build new routing state and swap atomically.
        let mut routing = self.routing.write().await;

        // Carry forward acceptors/learners for shards that remain.
        let mut all_acceptors = BTreeMap::new();
        let mut all_learners = BTreeMap::new();

        for range in &new_map.ranges {
            if let Some(a) = routing.acceptors.get(&range.log_shard) {
                all_acceptors.insert(range.log_shard, a.clone());
            } else if let Some(a) = new_acceptors.remove(&range.log_shard) {
                all_acceptors.insert(range.log_shard, a);
            } else {
                return Err(MetashardError::Command(format!(
                    "no acceptor for log shard {} in new partition map",
                    range.log_shard
                )));
            }

            if let Some(l) = routing.learners.get(&range.log_shard) {
                all_learners.insert(range.log_shard, l.clone());
            } else if let Some(l) = new_learners.remove(&range.log_shard) {
                all_learners.insert(range.log_shard, l);
            } else {
                return Err(MetashardError::Command(format!(
                    "no learner for log shard {} in new partition map",
                    range.log_shard
                )));
            }
        }

        // Update routing state and metashard state atomically.
        let new_partition_map = PartitionMap {
            epoch: new_epoch,
            ranges: new_map.ranges.clone(),
        };

        *routing = RoutingState {
            partition_map: new_partition_map.clone(),
            acceptors: all_acceptors,
            learners: all_learners,
        };
        drop(routing);

        // Track new log shards in metashard state.
        for range in &new_map.ranges {
            if added.contains(&range.log_shard) {
                // Find ALL old shards this range overlaps with (predecessors).
                // A merge reconfiguration can have multiple predecessors.
                let predecessors: Vec<ShardId> = old_map
                    .ranges
                    .iter()
                    .filter(|r| {
                        u16::from(range.lo) < r.hi_exclusive
                            && u16::from(r.lo) < range.hi_exclusive
                    })
                    .map(|r| r.log_shard)
                    .collect();

                self.state.log_shards.insert(
                    range.log_shard,
                    LogShardInfo {
                        status: LogShardStatus::Active,
                        epoch_created: new_epoch,
                        epoch_sealed: None,
                        range: range.clone(),
                        predecessors,
                        has_snapshot: false,
                    },
                );
            }
        }

        self.state.epoch = new_epoch;
        self.state.partition_map = new_partition_map;
        // Clear the intent — reconfiguration committed successfully.
        self.state.pending_intent = None;

        // Persist the updated state durably.
        self.persist_state().await;

        // Phase 6: Release CriticalSince holds on retired shards.
        // Predecessor replays were confirmed complete in Phase 3, so the
        // holds are no longer needed.
        for mut hold in critical_holds {
            let opaque = hold.opaque().clone();
            match hold
                .compare_and_downgrade_since(&opaque, (&opaque, &Antichain::new()))
                .await
            {
                Ok(_) => {
                    info!("released CriticalSince hold");
                }
                Err(actual) => {
                    info!(?actual, "CriticalSince was fenced (another process released it)");
                }
            }
        }

        info!(new_epoch, "reconfiguration complete");
        Ok(new_epoch)
    }

    /// Run the actor loop until the command channel closes.
    pub async fn run(mut self) {
        info!(
            epoch = self.state.epoch,
            num_ranges = self.state.partition_map.ranges.len(),
            num_log_shards = self.state.log_shards.len(),
            "metashard actor starting"
        );

        // If the recovered epoch differs from what the bootstrap routing was
        // built with, rebuild routing from the recovered partition map. This
        // handles the case where a reconfiguration committed in a previous run
        // and the process restarted with stale CLI bootstrap arguments.
        {
            let routing_epoch = self.routing.read().await.partition_map.epoch;
            if self.state.epoch != routing_epoch {
                info!(
                    recovered_epoch = self.state.epoch,
                    bootstrap_epoch = routing_epoch,
                    "recovered epoch differs from bootstrap — rebuilding routing"
                );
                self.rebuild_routing_from_state().await;
            }
        }

        // Check for a pending reconfiguration intent from a previous crash.
        // Resume the reconfiguration from the last completed phase.
        if let Some(intent) = self.state.pending_intent.take() {
            info!(
                epoch = intent.epoch,
                status = ?intent.status,
                "found pending reconfiguration intent — resuming"
            );
            match intent.status {
                IntentStatus::Committed => {
                    // Already committed — just clear the intent.
                    info!(epoch = intent.epoch, "intent already committed, clearing");
                }
                IntentStatus::Preparing | IntentStatus::Sealed => {
                    // Need to re-run the reconfiguration. The plan tells us
                    // what the target partition map should be.
                    info!(
                        epoch = intent.epoch,
                        "resuming reconfiguration from {:?}",
                        intent.status
                    );
                    match self.do_reconfigure(intent.plan).await {
                        Ok(new_epoch) => {
                            info!(new_epoch, "crash recovery reconfiguration completed");
                        }
                        Err(e) => {
                            tracing::error!(
                                "crash recovery reconfiguration failed: {} — \
                                 manual intervention may be required",
                                e
                            );
                        }
                    }
                }
            }
        }

        loop {
            match self.rx.recv().await {
                Some(MetashardCommand::Reconfigure { plan, reply }) => {
                    if self.reconfiguring {
                        let _ = reply.send(Err(MetashardError::ReconfigurationInProgress));
                        continue;
                    }
                    self.reconfiguring = true;
                    let result = self.do_reconfigure(plan).await;
                    self.reconfiguring = false;
                    let _ = reply.send(result);
                }
                Some(cmd) => {
                    debug!("metashard command received");
                    self.on_query(cmd);
                }
                None => {
                    info!("metashard actor shutting down (channel closed)");
                    break;
                }
            }
        }
    }

    /// Spawn the metashard actor as a tokio task.
    pub fn spawn(
        state: MetashardState,
        queue_depth: usize,
        persist_client: PersistClient,
        metrics_registry: MetricsRegistry,
        routing: Arc<RwLock<RoutingState<PersistAcceptorHandle, PersistLearnerHandle>>>,
    ) -> (PersistMetashardHandle, mz_ore::task::JoinHandle<()>) {
        let (actor, handle) = Self::new(
            state,
            queue_depth,
            persist_client,
            metrics_registry,
            routing,
        );
        let task = mz_ore::task::spawn(|| "persist-metashard", actor.run());
        (handle, task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Metashard;

    fn test_shard(suffix: &str) -> ShardId {
        format!("s{:0>32}", suffix)
            .parse()
            .expect("valid shard id")
    }

    /// Test that requires a full PersistClient — uses the spawn_for_test helper.
    /// For unit tests of the static query path, we skip the full actor and test
    /// the handle directly via the simple new() + spawn pattern.
    #[tokio::test]
    async fn metashard_lookup_routes_correctly() {
        // For this test we use a lightweight approach: create the actor without
        // a PersistClient (won't reconfigure, just serves queries).
        let s1 = test_shard("1");
        let s2 = test_shard("2");
        let state = MetashardState {
            epoch: 1,
            partition_map: PartitionMap {
                epoch: 1,
                ranges: vec![
                    RangeAssignment {
                        lo: 0x00,
                        hi_exclusive: 0x80,
                        log_shard: s1,
                    },
                    RangeAssignment {
                        lo: 0x80,
                        hi_exclusive: 0x100,
                        log_shard: s2,
                    },
                ],
            },
            log_shards: BTreeMap::new(),
            pending_intent: None,
        };

        // Create a minimal actor (query-only, no reconfiguration capability).
        let (tx, rx) = mpsc::channel(64);
        let handle = PersistMetashardHandle::new(tx);

        // Spawn a minimal query-only loop.
        let actor_state = state.clone();
        mz_ore::task::spawn(|| "test-metashard", async move {
            let mut rx = rx;
            loop {
                match rx.recv().await {
                    Some(MetashardCommand::Lookup { client_shard, reply }) => {
                        let _ = reply.send(Ok(actor_state.partition_map.route(&client_shard)));
                    }
                    Some(MetashardCommand::GetEpoch { reply }) => {
                        let _ = reply.send(Ok(actor_state.epoch));
                    }
                    Some(MetashardCommand::GetPartitionMap { reply }) => {
                        let _ = reply.send(Ok(actor_state.partition_map.clone()));
                    }
                    Some(MetashardCommand::Reconfigure { reply, .. }) => {
                        let _ = reply.send(Err(MetashardError::Command(
                            "not supported in test".into(),
                        )));
                    }
                    None => break,
                }
            }
        });

        // "s0a..." → partition key 0x0a → first range → s1
        let result = handle
            .lookup("s0a000000-0000-0000-0000-000000000000")
            .await
            .unwrap();
        assert_eq!(result, s1);

        // "sff..." → partition key 0xff → second range → s2
        let result = handle
            .lookup("sff000000-0000-0000-0000-000000000000")
            .await
            .unwrap();
        assert_eq!(result, s2);

        // Verify epoch
        assert_eq!(handle.current_epoch().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn metashard_returns_partition_map() {
        let s1 = test_shard("1");
        let state = MetashardState::single(s1);

        let (tx, rx) = mpsc::channel(64);
        let handle = PersistMetashardHandle::new(tx);
        let actor_state = state.clone();
        mz_ore::task::spawn(|| "test-metashard", async move {
            let mut rx = rx;
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    MetashardCommand::GetPartitionMap { reply } => {
                        let _ = reply.send(Ok(actor_state.partition_map.clone()));
                    }
                    _ => {}
                }
            }
        });

        let map = handle.partition_map().await.unwrap();
        assert_eq!(map, state.partition_map);
    }
}
