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
    /// The log shard this one succeeded for overlapping ranges.
    pub predecessor: Option<ShardId>,
    /// Whether this shard contains T=0 snapshot entries from its predecessor.
    pub has_snapshot: bool,
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
                predecessor: None,
                has_snapshot: false,
            },
        );
        MetashardState {
            epoch: 0,
            partition_map: PartitionMap::single(log_shard),
            log_shards,
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
        };
        let handle = PersistMetashardHandle::new(tx);
        (actor, handle)
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

    /// Replay a sealed predecessor shard and write its head state as snapshot
    /// CaS proposals to a new shard. This makes the new shard self-contained.
    ///
    /// Returns the number of client shard entries written.
    async fn write_snapshot_to_new_shard(
        &self,
        predecessor: ShardId,
        new_shard: ShardId,
    ) -> Result<usize, String> {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        // Replay predecessor to extract head state.
        let (_, read) = self
            .persist_client
            .open::<OrderedKey, Proposal, u64, i64>(
                predecessor,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("metashard-snapshot-replay"),
                false,
            )
            .await
            .map_err(|e| format!("open predecessor for snapshot: {e}"))?;

        let since = read.since().clone();
        let mut subscribe = read
            .subscribe(since)
            .await
            .map_err(|e| format!("subscribe to predecessor: {e:?}"))?;

        // Build head state per client shard: key → (seqno, data).
        // We evaluate CaS proposals in order to track only committed state.
        let mut head_state: BTreeMap<String, (u64, Vec<u8>)> = BTreeMap::new();

        loop {
            let events = subscribe.fetch_next().await;
            let mut done = false;
            for event in &events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        if frontier.is_empty() {
                            done = true;
                        }
                    }
                    ListenEvent::Updates(updates) => {
                        for ((_key, proposal_data), _ts, diff) in updates {
                            if *diff != 1 {
                                continue; // Skip retractions for head tracking.
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

        if head_state.is_empty() {
            return Ok(0);
        }

        // Write head state as CaS proposals to the new shard.
        let mut write = self
            .persist_client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                new_shard,
                Arc::clone(&key_schema),
                Arc::clone(&val_schema),
                Diagnostics::from_purpose("metashard-snapshot-write"),
            )
            .await
            .map_err(|e| format!("open writer for snapshot: {e}"))?;

        // Advance upper past T=0 if needed.
        if write.upper().as_option() == Some(&0) {
            write.advance_upper(&Antichain::from_elem(1)).await;
        }

        // Write at T=1 (T=0 is skipped per learner convention).
        let batch_number = 1u64;
        let mut updates = Vec::new();
        for (position, (shard_key, (seqno, data))) in head_state.iter().enumerate() {
            let cas = ProtoCasProposal {
                key: shard_key.clone(),
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
                shard: shard_key.clone(),
            };
            updates.push(((ordered_key, encoded), batch_number, 1i64));
        }

        let upper = write.upper().clone();
        let new_upper = Antichain::from_elem(batch_number + 1);
        match write.compare_and_append(&updates, upper, new_upper).await {
            Ok(Ok(())) => {
                info!(
                    %new_shard,
                    entries = head_state.len(),
                    "wrote snapshot to new shard"
                );
                Ok(head_state.len())
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

        let old_map = &self.state.partition_map;
        let new_map = &plan.new_partition_map;
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

        // Phase 1: Seal retiring log shards.
        for &shard_id in &retiring {
            let key_schema = Arc::new(crate::persist_log::OrderedKeySchema);
            let val_schema = Arc::new(crate::persist_log::ProposalSchema);
            let mut write = self
                .persist_client
                .open_writer::<crate::persist_log::OrderedKey, crate::persist_log::Proposal, u64, i64>(
                    shard_id,
                    key_schema,
                    val_schema,
                    mz_persist_client::Diagnostics::from_purpose("metashard-seal"),
                )
                .await
                .expect("open writer for sealing");

            // Advance the upper to the empty antichain to seal the shard.
            write.advance_upper(&Antichain::new()).await;
            info!(%shard_id, "sealed log shard");

            // Track in metashard state.
            if let Some(info) = self.state.log_shards.get_mut(&shard_id) {
                info.status = LogShardStatus::Sealed;
                info.epoch_sealed = Some(new_epoch);
            }
        }

        // Phase 2: Write snapshots to new shards + spawn actors.
        let mut new_acceptors = BTreeMap::new();
        let mut new_learners = BTreeMap::new();

        for &shard_id in &added {
            // Find predecessor shard(s) for this new log shard.
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

            // Write snapshot: replay predecessor(s) and write head state to new shard.
            let mut snapshot_entries = 0;
            for &pred in &predecessors {
                match self.write_snapshot_to_new_shard(pred, shard_id).await {
                    Ok(n) => snapshot_entries += n,
                    Err(e) => {
                        info!(
                            %shard_id,
                            %pred,
                            error = %e,
                            "snapshot write failed, learner will use chain replay"
                        );
                    }
                }
            }

            // Spawn acceptor + learner.
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

            // If snapshot was written, learner just replays its own shard.
            // If snapshot failed, fall back to chain replay via predecessors.
            let (learner_handle, _task) = if snapshot_entries > 0 || predecessors.is_empty() {
                PersistLearner::spawn(
                    PersistLearnerConfig::default(),
                    &self.persist_client,
                    shard_id,
                    learner_metrics,
                )
                .await
            } else {
                info!(
                    %shard_id,
                    predecessors = ?predecessors,
                    "falling back to chain replay (no snapshot)"
                );
                PersistLearner::spawn_with_predecessors(
                    PersistLearnerConfig::default(),
                    &self.persist_client,
                    shard_id,
                    predecessors,
                    learner_metrics,
                )
                .await
            };

            info!(%shard_id, snapshot_entries, "spawned actors for new log shard");
            new_acceptors.insert(shard_id, acceptor_handle);
            new_learners.insert(shard_id, learner_handle);
        }

        // Phase 3: Build new routing state and swap atomically.
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
                // Find which old shard this range overlaps with (predecessor).
                let predecessor = old_map
                    .ranges
                    .iter()
                    .find(|r| {
                        // The new range overlaps with the old range if they share any key space.
                        u16::from(range.lo) < r.hi_exclusive
                            && u16::from(r.lo) < range.hi_exclusive
                    })
                    .map(|r| r.log_shard);

                self.state.log_shards.insert(
                    range.log_shard,
                    LogShardInfo {
                        status: LogShardStatus::Active,
                        epoch_created: new_epoch,
                        epoch_sealed: None,
                        range: range.clone(),
                        predecessor,
                        has_snapshot: false,
                    },
                );
            }
        }

        self.state.epoch = new_epoch;
        self.state.partition_map = new_partition_map;

        // Phase 5: Release CriticalSince holds on retired shards.
        // The new learners have finished replaying predecessors by now
        // (spawn_with_predecessors blocks until replay is complete before
        // entering the run loop), so the holds are no longer needed.
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
