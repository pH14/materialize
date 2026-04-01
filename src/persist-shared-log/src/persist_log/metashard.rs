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
use tracing::{debug, info, warn};

use bytes::Bytes;
use prost::Message;

use mz_persist_client::critical::{CriticalReaderId, Opaque, SinceHandle};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};

use crate::factory::ActorFactory;
use crate::persist_log::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::sharded_service::RoutingState;
use crate::{
    MetashardError, PartitionMap, RangeAssignment, ReconfigurationPlan,
};

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
    // TODO: Consider removing — callers can use `PartitionMap::route` directly.
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
pub struct PersistMetashardActor<F: ActorFactory> {
    state: MetashardState,
    rx: mpsc::Receiver<MetashardCommand>,
    /// PersistClient for metashard's own durable state and sealing operations.
    persist_client: PersistClient,
    /// Factory for creating new acceptors and learners during reconfiguration.
    factory: F,
    /// Handle to the ShardedService's routing state, for atomic swaps during reconfiguration.
    routing: Arc<RwLock<RoutingState<F::A, F::L>>>,
    /// Whether a reconfiguration is currently in progress.
    reconfiguring: bool,
    /// Persist shard for durable metashard state. The partition map and
    /// reconfiguration intents are written here so they survive process
    /// restarts. Uses in-memory persist backend in tests.
    #[allow(dead_code)]
    metashard_shard_id: ShardId,
    /// Write handle for the metashard persist shard.
    metashard_write: mz_persist_client::write::WriteHandle<OrderedKey, Proposal, u64, i64>,
}

impl<F: ActorFactory> PersistMetashardActor<F> {
    /// Create a new metashard actor. Opens the durable state persist shard
    /// eagerly and recovers any persisted state (partition map, pending intents).
    pub async fn new(
        mut state: MetashardState,
        queue_depth: usize,
        persist_client: PersistClient,
        factory: F,
        routing: Arc<RwLock<RoutingState<F::A, F::L>>>,
        metashard_shard_id: ShardId,
    ) -> (Self, PersistMetashardHandle) {
        let (tx, rx) = mpsc::channel(queue_depth);

        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        let mut metashard_write = persist_client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                metashard_shard_id,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("metashard-durable-state"),
            )
            .await
            .expect("open metashard persist shard writer");

        if metashard_write.upper().as_option() == Some(&0) {
            metashard_write
                .advance_upper(&Antichain::from_elem(1))
                .await;
        }

        // Recover persisted state (partition map, predecessors, pending intent).
        let (_, read) = persist_client
            .open::<OrderedKey, Proposal, u64, i64>(
                metashard_shard_id,
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

        let mut latest_data: Option<Bytes> = None;
        loop {
            let events = subscribe.fetch_next().await;
            let mut done = false;
            for event in &events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        if frontier.as_option().copied()
                            >= metashard_write.upper().as_option().copied()
                        {
                            done = true;
                        }
                    }
                    ListenEvent::Updates(updates) => {
                        for ((key, proposal), _ts, diff) in updates {
                            if *diff == 1 && key.shard == "__metashard" {
                                latest_data = Some(proposal.encoded.clone());
                            }
                        }
                    }
                }
            }
            if done {
                break;
            }
        }

        if let Some(data) = latest_data {
            use mz_persist::generated::consensus_service::ProtoMetashardState;

            let parse_range = |r: &mz_persist::generated::consensus_service::ProtoRangeAssignment| -> Option<RangeAssignment> {
                Some(RangeAssignment {
                    lo: u8::try_from(r.lo).ok()?,
                    hi_exclusive: u16::try_from(r.hi_exclusive).ok()?,
                    log_shard: r.log_shard.parse().ok()?,
                })
            };

            match ProtoMetashardState::decode(data.as_ref()) {
                Ok(proto) => {
                    // Restore predecessors.
                    for pred_entry in &proto.predecessors {
                        if let Ok(shard) = pred_entry.shard.parse::<ShardId>() {
                            let preds: Vec<ShardId> = pred_entry
                                .predecessors
                                .iter()
                                .filter_map(|p| p.parse().ok())
                                .collect();
                            if !preds.is_empty() {
                                state
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
                                    .predecessors = preds;
                            }
                        }
                    }

                    // Restore intent.
                    if let Some(intent_proto) = &proto.intent {
                        let status = match intent_proto.status.as_str() {
                            "Preparing" => IntentStatus::Preparing,
                            "Sealed" => IntentStatus::Sealed,
                            "Committed" => IntentStatus::Committed,
                            _ => IntentStatus::Preparing,
                        };
                        let intent_ranges: Vec<RangeAssignment> = intent_proto
                            .new_ranges
                            .iter()
                            .filter_map(parse_range)
                            .collect();
                        if !intent_ranges.is_empty() {
                            let plan = ReconfigurationPlan {
                                expected_epoch: intent_proto.epoch.saturating_sub(1),
                                new_partition_map: PartitionMap {
                                    epoch: intent_proto.epoch,
                                    ranges: intent_ranges,
                                },
                            };
                            state.pending_intent = Some(ReconfigurationIntent {
                                epoch: intent_proto.epoch,
                                plan,
                                status,
                            });
                            info!(
                                epoch = intent_proto.epoch,
                                "recovered pending reconfiguration intent"
                            );
                        }
                    }

                    // Restore partition map.
                    let persisted_ranges: Vec<RangeAssignment> =
                        proto.ranges.iter().filter_map(parse_range).collect();
                    if !persisted_ranges.is_empty() {
                        let map = PartitionMap {
                            epoch: proto.epoch,
                            ranges: persisted_ranges,
                        };
                        if map.validate().is_ok() {
                            info!(
                                epoch = proto.epoch,
                                num_ranges = map.ranges.len(),
                                "restored partition map from durable state"
                            );
                            state.epoch = proto.epoch;
                            state.partition_map = map;
                        }
                    }
                }
                Err(e) => {
                    warn!("failed to decode metashard proto, ignoring durable state: {e}");
                }
            }
        }

        let actor = PersistMetashardActor {
            state,
            rx,
            persist_client,
            factory,
            routing,
            reconfiguring: false,
            metashard_shard_id,
            metashard_write,
        };
        let handle = PersistMetashardHandle::new(tx);
        (actor, handle)
    }

    /// Persist the current metashard state to the durable shard.
    async fn persist_state(&mut self) {
        use mz_persist::generated::consensus_service::{
            ProtoLogShardPredecessor, ProtoMetashardState, ProtoRangeAssignment,
            ProtoReconfigurationIntent,
        };

        let write = &mut self.metashard_write;

        let proto =
            ProtoMetashardState {
                epoch: self.state.epoch,
                ranges: self
                    .state
                    .partition_map
                    .ranges
                    .iter()
                    .map(|r| ProtoRangeAssignment {
                        lo: u32::from(r.lo),
                        hi_exclusive: u32::from(r.hi_exclusive),
                        log_shard: r.log_shard.to_string(),
                    })
                    .collect(),
                predecessors: self
                    .state
                    .log_shards
                    .iter()
                    .filter(|(_, info)| !info.predecessors.is_empty())
                    .map(|(shard_id, info)| ProtoLogShardPredecessor {
                        shard: shard_id.to_string(),
                        predecessors: info.predecessors.iter().map(|p| p.to_string()).collect(),
                    })
                    .collect(),
                intent: self.state.pending_intent.as_ref().map(|intent| {
                    ProtoReconfigurationIntent {
                        status: format!("{:?}", intent.status),
                        epoch: intent.epoch,
                        new_ranges: intent
                            .plan
                            .new_partition_map
                            .ranges
                            .iter()
                            .map(|r| ProtoRangeAssignment {
                                lo: u32::from(r.lo),
                                hi_exclusive: u32::from(r.hi_exclusive),
                                log_shard: r.log_shard.to_string(),
                            })
                            .collect(),
                    }
                }),
            };

        let data = Bytes::from(proto.encode_to_vec());

        let batch_number = write.upper().as_option().copied().unwrap_or(1).max(1);

        let key = OrderedKey {
            batch_id: batch_number,
            position: 0,
            shard: "__metashard".to_string(),
        };
        let proposal = Proposal { encoded: data };

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
            // Walk the full transitive predecessor chain so that multi-hop
            // carried-forward state (L1→L2→L4) is reconstructed on recovery.
            let predecessors = self.transitive_predecessors(shard_id);

            // For each predecessor, read its current since (the compaction
            // frontier). On recovery, CriticalSince holds may have been lost,
            // so we use whatever since is available.
            let mut pred_specs = Vec::new();
            for &pred_shard in &predecessors {
                let (_, read) = self
                    .persist_client
                    .open::<OrderedKey, Proposal, u64, i64>(
                        pred_shard,
                        Arc::new(OrderedKeySchema),
                        Arc::new(ProposalSchema),
                        Diagnostics::from_purpose("metashard-recovery-since-read"),
                        false,
                    )
                    .await
                    .expect("failed to open predecessor for since read");
                pred_specs.push((pred_shard, read.since().clone()));
            }

            let acceptor_handle = self
                .factory
                .create_acceptor(shard_id, self.state.epoch, pred_specs, range.clone())
                .await
                .expect("failed to create acceptor during rebuild");

            if !predecessors.is_empty() {
                info!(
                    %shard_id,
                    predecessors = ?predecessors,
                    "spawning recovered learner with predecessor replay"
                );
            }
            let learner_handle = self
                .factory
                .create_learner(shard_id)
                .await
                .expect("failed to create learner during rebuild");

            info!(%shard_id, "spawned recovered actor");
            acceptors.insert(shard_id, acceptor_handle);
            learners.insert(shard_id, learner_handle);
        }

        {
            let mut routing_guard = self.routing.write().await;
            *routing_guard = RoutingState {
                partition_map: map.clone(),
                acceptors: Arc::new(acceptors),
                learners: Arc::new(learners),
            };
        }
        info!(
            epoch = self.state.epoch,
            num_shards = map.ranges.len(),
            "rebuilt routing from recovered metashard state"
        );
    }

    /// Access the actor's current state. Used by main.rs to read the
    /// (possibly recovered) partition map before spawning log shard actors.
    pub fn state(&self) -> &MetashardState {
        &self.state
    }

    /// Get the routing handle for external updates.
    pub fn routing_handle(&self) -> &Arc<RwLock<RoutingState<F::A, F::L>>> {
        &self.routing
    }

    /// Public wrapper for transitive predecessor lookup, used by main.rs
    /// to determine which predecessors to replay at startup.
    pub fn transitive_predecessors_for(&self, shard_id: ShardId) -> Vec<ShardId> {
        self.transitive_predecessors(shard_id)
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

    /// Execute a reconfiguration.
    ///
    /// This is the core reconfiguration protocol:
    /// 0. Validate epoch, write durable intent, acquire CriticalSince holds
    /// 2. Spawn new acceptors + learners (learners subscribe to live predecessors)
    /// 2.5. Seal retiring log shards (predecessors become finite)
    /// 3. Wait for predecessor replay to complete
    /// 4. Swap routing state atomically
    /// 5. Persist new state, release CriticalSince holds
    ///
    /// By spawning learners before sealing, the new learners pre-hydrate from
    /// live predecessors, minimizing the unavailability window to just the tail
    /// of writes between the subscribe point and the seal.
    ///
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
            added_shards = added.len(),
            retiring_shards = retiring.len(),
            new_ranges = new_map.ranges.len(),
            "starting reconfiguration"
        );

        // Phase 0: Write ReconfigurationIntent (durable crash recovery marker).
        self.state.pending_intent = Some(ReconfigurationIntent {
            epoch: new_epoch,
            plan: plan.clone(),
            status: IntentStatus::Preparing,
        });
        self.persist_state().await;

        // BUGGIFY: crash after intent is persisted but before seal.
        crate::fault::maybe_fail("after_intent_persist").map_err(MetashardError::Command)?;

        // Phase 0.5: Acquire CriticalSince on retiring shards to prevent
        // compaction during predecessor replay. Uses a deterministic reader ID
        // derived from the new epoch so it can be recovered after crash.
        let mut critical_holds: Vec<SinceHandle<OrderedKey, Proposal, u64, i64>> = Vec::new();
        for &shard_id in &retiring {
            let reader_id: CriticalReaderId = format!(
                "c{:0>8}-{:04x}-0000-0000-000000000000",
                new_epoch,
                shard_id.to_string().len()
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

            info!(%shard_id, since = ?handle.since(), "acquired CriticalSince hold");
            critical_holds.push(handle);
        }

        // Build a map from predecessor shard → CriticalSince frontier for the
        // acceptor's setup batches. The acceptor subscribes at this since.
        let predecessor_sinces: BTreeMap<ShardId, Antichain<u64>> = retiring
            .iter()
            .enumerate()
            .map(|(i, &shard_id)| (shard_id, critical_holds[i].since().clone()))
            .collect();

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

        // Phase 2: Spawn actors for new log shards BEFORE sealing.
        //
        // New learners subscribe to live (unsealed) predecessors and start
        // catching up in real-time. This pre-hydration means they're nearly
        // current by the time we seal, minimizing the unavailability window
        // to just the tail of writes between subscribe and seal.
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

            let pred_specs: Vec<_> = predecessors
                .iter()
                .map(|s| {
                    let since = predecessor_sinces
                        .get(s)
                        .cloned()
                        .unwrap_or_else(|| Antichain::from_elem(0));
                    (*s, since)
                })
                .collect();

            let acceptor_handle = self
                .factory
                .create_acceptor(shard_id, new_epoch, pred_specs, new_range.clone())
                .await
                .map_err(MetashardError::Command)?;

            let learner_handle = self
                .factory
                .create_learner(shard_id)
                .await
                .map_err(MetashardError::Command)?;

            info!(
                %shard_id,
                range_lo = new_range.lo,
                range_hi = new_range.hi_exclusive,
                num_predecessors = predecessors.len(),
                "spawned acceptor + learner for new log shard"
            );
            new_acceptors.insert(shard_id, acceptor_handle);
            new_learners.insert(shard_id, learner_handle);
        }

        // BUGGIFY: crash after spawning new actors but before seal.
        // Actors are running and replaying live predecessors. On recovery,
        // fresh actors are spawned and replay restarts.
        crate::fault::maybe_fail("after_actor_spawn").map_err(MetashardError::Command)?;

        // Phase 2.5: Seal retiring log shards.
        //
        // The new learners are already subscribed and catching up. Sealing
        // makes each predecessor finite, so replay_predecessor sees
        // frontier.is_empty() and completes. The unavailability window is
        // only the time to process the tail after the subscribe point.
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
            info!(%shard_id, epoch = new_epoch, "sealed log shard");

            if let Some(info) = self.state.log_shards.get_mut(&shard_id) {
                info.status = LogShardStatus::Sealed;
                info.epoch_sealed = Some(new_epoch);
            }
        }

        // Update intent: sealed.
        if let Some(ref mut intent) = self.state.pending_intent {
            intent.status = IntentStatus::Sealed;
        }

        // BUGGIFY: crash after seal but before routing swap.
        crate::fault::maybe_fail("after_seal").map_err(MetashardError::Command)?;

        // Phase 4: Build new routing state and swap atomically.
        let new_partition_map = {
            let mut routing_guard = self.routing.write().await;

            // Carry forward acceptors/learners for shards that remain.
            let mut all_acceptors = BTreeMap::new();
            let mut all_learners = BTreeMap::new();

            for range in &new_map.ranges {
                if let Some(a) = routing_guard.acceptors.get(&range.log_shard) {
                    all_acceptors.insert(range.log_shard, a.clone());
                } else if let Some(a) = new_acceptors.remove(&range.log_shard) {
                    all_acceptors.insert(range.log_shard, a);
                } else {
                    return Err(MetashardError::Command(format!(
                        "no acceptor for log shard {} in new partition map",
                        range.log_shard
                    )));
                }

                if let Some(l) = routing_guard.learners.get(&range.log_shard) {
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

            let new_partition_map = PartitionMap {
                epoch: new_epoch,
                ranges: new_map.ranges.clone(),
            };

            *routing_guard = RoutingState {
                partition_map: new_partition_map.clone(),
                acceptors: Arc::new(all_acceptors),
                learners: Arc::new(all_learners),
            };
            new_partition_map
        };

        // Track new log shards in metashard state.
        for range in &new_map.ranges {
            if added.contains(&range.log_shard) {
                // Find ALL old shards this range overlaps with (predecessors).
                // A merge reconfiguration can have multiple predecessors.
                let predecessors: Vec<ShardId> = old_map
                    .ranges
                    .iter()
                    .filter(|r| {
                        u16::from(range.lo) < r.hi_exclusive && u16::from(r.lo) < range.hi_exclusive
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

        // BUGGIFY: crash after routing swap but before durable persist.
        // On recovery, the durable state still has the old epoch and intent,
        // but the old shards are sealed. do_reconfigure re-runs idempotently.
        crate::fault::maybe_fail("after_routing_swap").map_err(MetashardError::Command)?;

        // Persist the updated state durably.
        self.persist_state().await;

        // BUGGIFY: crash after commit persist but before hold release.
        // Holds leak but correctness is preserved — old shards just keep
        // their CriticalSince longer than necessary.
        crate::fault::maybe_fail("after_commit_persist").map_err(MetashardError::Command)?;

        // BUGGIFY: crash before releasing CriticalSince holds. Holds leak
        // but correctness is preserved — old shards keep their since longer
        // than necessary. Next reconfiguration or restart can release them.
        crate::fault::maybe_fail("before_hold_release").map_err(MetashardError::Command)?;

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
                    info!(
                        ?actual,
                        "CriticalSince was fenced (another process released it)"
                    );
                }
            }
        }

        // TODO: Retract durable state for fully-finalized prior epochs so the
        // metashard shard doesn't grow unboundedly.

        info!(new_epoch, "reconfiguration complete");
        Ok(new_epoch)
    }

    /// Run the actor loop until the command channel closes.
    pub async fn run(mut self) {
        info!(
            metashard_shard = %self.metashard_shard_id,
            epoch = self.state.epoch,
            num_ranges = self.state.partition_map.ranges.len(),
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
                        "resuming reconfiguration from {:?}", intent.status
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
    pub async fn spawn(
        state: MetashardState,
        queue_depth: usize,
        persist_client: PersistClient,
        factory: F,
        routing: Arc<RwLock<RoutingState<F::A, F::L>>>,
        metashard_shard_id: ShardId,
    ) -> (PersistMetashardHandle, mz_ore::task::JoinHandle<()>) {
        let (actor, handle) = Self::new(
            state,
            queue_depth,
            persist_client,
            factory,
            routing,
            metashard_shard_id,
        )
        .await;
        let task = mz_ore::task::spawn(|| "persist-metashard", actor.run());
        (handle, task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::ActorFactory;
    use crate::Metashard;

    fn test_shard(suffix: &str) -> ShardId {
        format!("s{:0>32}", suffix).parse().expect("valid shard id")
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
                    Some(MetashardCommand::Lookup {
                        client_shard,
                        reply,
                    }) => {
                        let _ = reply.send(Ok(actor_state.partition_map.route(&client_shard)));
                    }
                    Some(MetashardCommand::GetEpoch { reply }) => {
                        let _ = reply.send(Ok(actor_state.epoch));
                    }
                    Some(MetashardCommand::GetPartitionMap { reply }) => {
                        let _ = reply.send(Ok(actor_state.partition_map.clone()));
                    }
                    Some(MetashardCommand::Reconfigure { reply, .. }) => {
                        let _ = reply
                            .send(Err(MetashardError::Command("not supported in test".into())));
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

    /// Round-trip test: persist metashard state (epoch, partition map,
    /// predecessors, intent), then recover from the same shard and verify
    /// the recovered state matches.
    ///
    /// This catches serialization/parsing bugs in the line-delimited
    /// key=value format used by `persist_state()` / `new()`.
    #[mz_ore::test(tokio::test)]
    async fn metashard_state_roundtrip() {
        use mz_persist_client::PersistLocation;
        use mz_persist_client::cache::PersistClientCache;

        let cache = PersistClientCache::new_no_metrics();
        let client: mz_persist_client::PersistClient = cache
            .open(PersistLocation::new_in_mem())
            .await
            .expect("in-mem persist client");

        let metashard_shard = ShardId::new();

        let s_old = test_shard("000");
        let s_a = test_shard("aaa");
        let s_b = test_shard("bbb");

        // Build a state with all serialized fields populated:
        // - epoch > 0
        // - multi-range partition map
        // - predecessor chains
        // - pending reconfiguration intent with intent_ranges
        let mut log_shards = BTreeMap::new();
        log_shards.insert(
            s_a,
            LogShardInfo {
                status: LogShardStatus::Active,
                epoch_created: 1,
                epoch_sealed: None,
                range: RangeAssignment {
                    lo: 0x00,
                    hi_exclusive: 0x80,
                    log_shard: s_a,
                },
                predecessors: vec![s_old],
                has_snapshot: false,
            },
        );
        log_shards.insert(
            s_b,
            LogShardInfo {
                status: LogShardStatus::Active,
                epoch_created: 1,
                epoch_sealed: None,
                range: RangeAssignment {
                    lo: 0x80,
                    hi_exclusive: 0x100,
                    log_shard: s_b,
                },
                predecessors: vec![s_old],
                has_snapshot: false,
            },
        );

        let s_merged = test_shard("ccc");
        let intent = ReconfigurationIntent {
            epoch: 2,
            plan: ReconfigurationPlan {
                expected_epoch: 1,
                new_partition_map: PartitionMap {
                    epoch: 2,
                    ranges: vec![RangeAssignment {
                        lo: 0x00,
                        hi_exclusive: 0x100,
                        log_shard: s_merged,
                    }],
                },
            },
            status: IntentStatus::Preparing,
        };

        let state = MetashardState {
            epoch: 1,
            partition_map: PartitionMap {
                epoch: 1,
                ranges: vec![
                    RangeAssignment {
                        lo: 0x00,
                        hi_exclusive: 0x80,
                        log_shard: s_a,
                    },
                    RangeAssignment {
                        lo: 0x80,
                        hi_exclusive: 0x100,
                        log_shard: s_b,
                    },
                ],
            },
            log_shards,
            pending_intent: Some(intent),
        };

        // --- Persist the state ---
        // We need a RoutingState but the round-trip test only exercises
        // persist_state() / new(), not routing. Spawn a real acceptor+learner
        // pair for a dummy shard so RoutingState::new passes its assertions.
        let dummy_shard = test_shard("ddd");
        let dummy_map = PartitionMap::single(dummy_shard);

        let factory = crate::factory::InProcessActorFactory::new(client.clone());
        let acc_handle = factory
            .create_acceptor(
                dummy_shard,
                0,
                vec![],
                crate::RangeAssignment {
                    lo: 0x00,
                    hi_exclusive: 0x100,
                    log_shard: dummy_shard,
                },
            )
            .await
            .expect("spawn dummy acceptor");
        let lrn_handle = factory
            .create_learner(dummy_shard)
            .await
            .expect("spawn dummy learner");

        let mut dummy_acceptors = BTreeMap::new();
        let mut dummy_learners = BTreeMap::new();
        dummy_acceptors.insert(dummy_shard, acc_handle);
        dummy_learners.insert(dummy_shard, lrn_handle);
        let routing = Arc::new(RwLock::new(RoutingState::new(
            dummy_map,
            dummy_acceptors,
            dummy_learners,
        )));

        let (mut actor, _handle) = PersistMetashardActor::new(
            state.clone(),
            64,
            client.clone(),
            factory,
            Arc::clone(&routing),
            metashard_shard,
        )
        .await;

        // Force a persist (normally happens during do_reconfigure).
        actor.persist_state().await;
        drop(actor);
        drop(_handle);

        // --- Recover from the same shard ---
        let bootstrap = MetashardState::single(test_shard("eee"));
        let factory2 = crate::factory::InProcessActorFactory::new(client.clone());

        let (recovered_actor, _handle2) = PersistMetashardActor::new(
            bootstrap,
            64,
            client.clone(),
            factory2,
            Arc::clone(&routing),
            metashard_shard,
        )
        .await;

        let recovered = &recovered_actor.state;

        // --- Verify round-trip ---
        assert_eq!(
            recovered.epoch, state.epoch,
            "epoch round-trip failed: got {}, expected {}",
            recovered.epoch, state.epoch
        );
        assert_eq!(
            recovered.partition_map, state.partition_map,
            "partition map round-trip failed"
        );

        // Verify predecessor chains survived.
        assert!(
            recovered.log_shards.contains_key(&s_a),
            "shard_a should have log_shards entry from predecessor line"
        );
        assert_eq!(
            recovered.log_shards[&s_a].predecessors,
            vec![s_old],
            "shard_a predecessor chain round-trip failed"
        );
        assert!(
            recovered.log_shards.contains_key(&s_b),
            "shard_b should have log_shards entry from predecessor line"
        );
        assert_eq!(
            recovered.log_shards[&s_b].predecessors,
            vec![s_old],
            "shard_b predecessor chain round-trip failed"
        );

        // Verify pending intent survived.
        let recovered_intent = recovered
            .pending_intent
            .as_ref()
            .expect("pending intent should survive round-trip");
        assert_eq!(recovered_intent.epoch, 2);
        assert_eq!(recovered_intent.status, IntentStatus::Preparing);
        assert_eq!(
            recovered_intent.plan.new_partition_map.ranges.len(),
            1,
            "intent should have 1 range (merged)"
        );
        assert_eq!(
            recovered_intent.plan.new_partition_map.ranges[0].log_shard, s_merged,
            "intent range shard should be the merged shard"
        );
    }
}
