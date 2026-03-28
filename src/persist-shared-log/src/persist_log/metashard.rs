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

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::{PersistClient, ShardId};

use crate::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
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

    /// Execute a reconfiguration.
    ///
    /// This is the core reconfiguration protocol:
    /// 1. Validate epoch
    /// 2. Identify new and retiring log shards
    /// 3. Spawn new acceptors + learners
    /// 4. Seal retiring log shards
    /// 5. Update partition map and swap routing state
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

        // Phase 1: Spawn new acceptors + learners for added log shards.
        let mut new_acceptors = BTreeMap::new();
        let mut new_learners = BTreeMap::new();

        for &shard_id in &added {
            // Each shard gets its own metrics registry to avoid double-registration
            // when spawning multiple shards (AcceptorMetrics/LearnerMetrics use
            // fixed metric names).
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

            let (learner_handle, _task) = PersistLearner::spawn(
                PersistLearnerConfig::default(),
                &self.persist_client,
                shard_id,
                learner_metrics,
            )
            .await;

            info!(%shard_id, "spawned new acceptor + learner for reconfiguration");
            new_acceptors.insert(shard_id, acceptor_handle);
            new_learners.insert(shard_id, learner_handle);
        }

        // Phase 3: Seal retiring log shards.
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
