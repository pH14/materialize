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
//! The metashard actor holds a [`MetaState`] in memory and serves lookups.
//! On reconfiguration, it orchestrates the full lifecycle: validate → spawn new
//! actors → seal old shards → update partition map → swap routing.
//!
//! Follows the same actor pattern as the acceptor and learner: a passive state
//! machine driven by a command channel, with a handle type for sending commands.

use std::collections::BTreeMap;
use std::sync::Arc;

use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use bytes::Bytes;
use prost::Message;

use mz_persist_client::critical::{CriticalReaderId, Opaque, SinceHandle};
use mz_persist_client::{Diagnostics, PersistClient, ShardId};

use crate::actors::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::factory::ActorFactory;
use crate::{MetaError, PartitionMap, RangeAssignment, ReconfigurationPlan};

// ---------------------------------------------------------------------------
// Metashard state
// ---------------------------------------------------------------------------

/// Per-log-shard metadata tracked by the metashard.
///
/// The presence of non-empty `predecessors` indicates a reconfiguration is in
/// progress for this shard. All other shard status (sealed, snapshot progress)
/// is derived from real-world state inspection, not stored durably.
#[derive(Debug, Clone)]
pub struct LogShardInfo {
    pub epoch_created: u64,
    pub range: RangeAssignment,
    /// The log shard(s) this one succeeded for overlapping ranges.
    /// Non-empty means reconfiguration is still in progress — the reconcile
    /// loop will drive predecessors to completion and clear this field.
    pub predecessors: Vec<ShardId>,
}

/// Durable status of the metashard state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum MetaStatus {
    /// All shards reconciled, ready for a new reconfiguration.
    Completed,
    /// A reconfiguration is in progress. Predecessors indicate remaining work.
    Reconfiguring,
}

/// The metashard actor's in-memory materialized state.
///
/// Persisted durably to the metashard persist shard. The reconcile loop reads
/// this state and drives the world toward it.
#[derive(Debug, Clone)]
pub struct MetaState {
    /// Current configuration epoch.
    pub(crate) epoch: u64,
    /// The authoritative partition map.
    pub(crate) partition_map: PartitionMap,
    /// Per-log-shard metadata.
    pub(crate) log_shards: BTreeMap<ShardId, LogShardInfo>,
    /// Monotonically increasing leader ID.
    pub(crate) leader_id: Option<u64>,
    /// Durable status: `Completed` when reconciled, `Reconfiguring` during
    /// an in-progress reconfiguration.
    pub(crate) status: MetaStatus,
}

impl MetaState {
    /// Create initial bootstrap state from a partition map.
    pub fn new(partition_map: PartitionMap) -> Self {
        let log_shards = partition_map
            .ranges
            .iter()
            .map(|r| {
                (
                    r.log_shard,
                    LogShardInfo {
                        epoch_created: 0,
                        range: r.clone(),
                        predecessors: Vec::new(),
                    },
                )
            })
            .collect();
        MetaState {
            epoch: 0,
            partition_map,
            log_shards,
            leader_id: None,
            status: MetaStatus::Completed,
        }
    }

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
                epoch_created: 0,
                range: range.clone(),
                predecessors: Vec::new(),
            },
        );
        MetaState {
            epoch: 0,
            partition_map: PartitionMap::single(log_shard),
            leader_id: None,
            status: MetaStatus::Completed,
            log_shards,
        }
    }
}

// ---------------------------------------------------------------------------
// Proto encode / decode helpers
// ---------------------------------------------------------------------------

use mz_persist::generated::consensus_service::{
    ProtoLogShardPredecessor, ProtoMetaStatus, ProtoMetashardState, ProtoRangeAssignment,
};

/// Parse a `ProtoRangeAssignment` into a `RangeAssignment`.
pub fn parse_proto_range(r: &ProtoRangeAssignment) -> Option<RangeAssignment> {
    Some(RangeAssignment {
        lo: u8::try_from(r.lo).ok()?,
        hi_exclusive: u16::try_from(r.hi_exclusive).ok()?,
        log_shard: r.log_shard.parse().ok()?,
    })
}

/// Encode `MetaState` into protobuf bytes for durable storage.
pub fn encode_meta_state(state: &MetaState) -> Bytes {
    let proto = ProtoMetashardState {
        epoch: state.epoch,
        ranges: state
            .partition_map
            .ranges
            .iter()
            .map(|r| ProtoRangeAssignment {
                lo: u32::from(r.lo),
                hi_exclusive: u32::from(r.hi_exclusive),
                log_shard: r.log_shard.to_string(),
            })
            .collect(),
        predecessors: state
            .log_shards
            .iter()
            .filter(|(_, info)| !info.predecessors.is_empty())
            .map(|(shard_id, info)| ProtoLogShardPredecessor {
                shard: shard_id.to_string(),
                predecessors: info.predecessors.iter().map(|p| p.to_string()).collect(),
            })
            .collect(),
        intent: None,
        leader_id: state.leader_id.unwrap_or(0),
        status: match state.status {
            MetaStatus::Completed => ProtoMetaStatus::Completed.into(),
            MetaStatus::Reconfiguring => ProtoMetaStatus::Reconfiguring.into(),
        },
    };
    Bytes::from(proto.encode_to_vec())
}

/// Decode protobuf bytes into a fresh `MetaState`.
///
/// Returns the partition map, epoch, and predecessor chains from the durable
/// representation. Uses struct destructuring on the proto to ensure all fields
/// are handled.
pub fn decode_meta_state(data: &[u8]) -> Result<MetaState, String> {
    let proto = ProtoMetashardState::decode(data)
        .map_err(|e| format!("failed to decode metashard proto: {e}"))?;

    let ProtoMetashardState {
        epoch,
        ranges: proto_ranges,
        predecessors: proto_predecessors,
        intent: _, // legacy field, ignored
        leader_id,
        status: proto_status,
    } = proto;

    let ranges: Vec<RangeAssignment> = proto_ranges.iter().filter_map(parse_proto_range).collect();

    if ranges.is_empty() {
        return Err("decoded empty partition map".to_string());
    }

    let partition_map = PartitionMap {
        epoch,
        ranges: ranges.clone(),
    };
    partition_map
        .validate()
        .map_err(|e| format!("invalid recovered partition map: {e}"))?;

    // Build log_shards: one entry per range, then overlay predecessors.
    let mut log_shards = BTreeMap::new();
    for range in &ranges {
        log_shards.insert(
            range.log_shard,
            LogShardInfo {
                epoch_created: epoch,
                range: range.clone(),
                predecessors: Vec::new(),
            },
        );
    }
    for pred_entry in &proto_predecessors {
        if let Ok(shard) = pred_entry.shard.parse::<ShardId>() {
            let preds: Vec<ShardId> = pred_entry
                .predecessors
                .iter()
                .filter_map(|p| p.parse().ok())
                .collect();
            if let Some(info) = log_shards.get_mut(&shard) {
                info.predecessors = preds;
            }
        }
    }

    info!(
        epoch,
        num_ranges = ranges.len(),
        "restored partition map from durable state"
    );

    let status = match ProtoMetaStatus::try_from(proto_status) {
        Ok(ProtoMetaStatus::Reconfiguring) => MetaStatus::Reconfiguring,
        _ => MetaStatus::Completed, // Unknown or Completed → Completed
    };

    Ok(MetaState {
        epoch,
        partition_map,
        log_shards,
        leader_id: if leader_id == 0 {
            None
        } else {
            Some(leader_id)
        },
        status,
    })
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Result of the `Reconcile` command.
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    pub epoch: u64,
    pub partition_map: PartitionMap,
    pub fully_reconciled: bool,
}

/// Commands dispatched to the metashard actor.
pub enum MetaCommand {
    /// Claim leadership by CAS-writing an incremented leader_id.
    /// Must succeed before Reconcile or Reconfigure can proceed.
    ClaimLeadership {
        reply: oneshot::Sender<Result<u64, MetaError>>,
    },
    /// Read durable state, ensure actors exist, drive any in-progress
    /// reconfiguration to completion. Requires active leadership.
    Reconcile {
        reply: oneshot::Sender<Result<ReconcileResult, MetaError>>,
    },
    /// Return the current partition map.
    GetPartitionMap {
        reply: oneshot::Sender<Result<PartitionMap, MetaError>>,
    },
    /// Return the current epoch.
    GetEpoch {
        reply: oneshot::Sender<Result<u64, MetaError>>,
    },
    /// Execute a reconfiguration.
    Reconfigure {
        plan: ReconfigurationPlan,
        reply: oneshot::Sender<Result<u64, MetaError>>,
    },
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// A typed handle to the metashard actor's command channel.
#[derive(Debug, Clone)]
pub struct PersistMetaHandle {
    tx: mpsc::Sender<MetaCommand>,
}

impl PersistMetaHandle {
    pub fn new(tx: mpsc::Sender<MetaCommand>) -> Self {
        PersistMetaHandle { tx }
    }

    /// Claim leadership by CAS-writing an incremented leader_id.
    pub async fn claim_leadership(&self) -> Result<u64, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::ClaimLeadership { reply: reply_tx })
            .await
            .map_err(|_| MetaError::Shutdown)?;
        reply_rx.await.map_err(|_| MetaError::DroppedReply)?
    }

    /// Reconcile: read durable state, ensure actors, drive predecessors.
    /// Requires active leadership.
    pub async fn reconcile(&self) -> Result<ReconcileResult, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::Reconcile { reply: reply_tx })
            .await
            .map_err(|_| MetaError::Shutdown)?;
        reply_rx.await.map_err(|_| MetaError::DroppedReply)?
    }
}

#[async_trait::async_trait]
impl crate::Metashard for PersistMetaHandle {
    async fn partition_map(&self) -> Result<PartitionMap, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::GetPartitionMap { reply: reply_tx })
            .await
            .map_err(|_| MetaError::Shutdown)?;
        reply_rx.await.map_err(|_| MetaError::DroppedReply)?
    }

    async fn current_epoch(&self) -> Result<u64, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::GetEpoch { reply: reply_tx })
            .await
            .map_err(|_| MetaError::Shutdown)?;
        reply_rx.await.map_err(|_| MetaError::DroppedReply)?
    }

    async fn reconfigure(&self, plan: ReconfigurationPlan) -> Result<u64, MetaError> {
        // Step 1: CAS-write the new desired state.
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::Reconfigure {
                plan,
                reply: reply_tx,
            })
            .await
            .map_err(|_| MetaError::Shutdown)?;
        let epoch = reply_rx.await.map_err(|_| MetaError::DroppedReply)??;

        // Step 2: Reconcile — drive the world toward the new state.
        self.reconcile().await?;

        Ok(epoch)
    }
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

/// The metashard actor.
///
/// Maintains an in-memory [`MetaState`] and serves commands from the
/// handle. On reconfiguration, orchestrates: validate → spawn new actors →
/// seal old shards → update partition map → swap routing state.
pub struct PersistMetaActor<F: ActorFactory> {
    state: MetaState,
    rx: mpsc::Receiver<MetaCommand>,
    /// PersistClient for metashard's own durable state and sealing operations.
    persist_client: PersistClient,
    /// Factory for creating new acceptors and learners during reconfiguration.
    factory: F,
    /// Transient leader ID — set after a successful `ClaimLeadership` CAS.
    /// Only the actor whose `leader_id` matches `self.state.leader_id` is
    /// allowed to reconcile or reconfigure.
    leader_id: Option<u64>,
    /// Persist shard for durable metashard state. The partition map and
    /// reconfiguration intents are written here so they survive process
    /// restarts. Uses in-memory persist backend in tests.
    #[allow(dead_code)]
    metashard_shard_id: ShardId,
    /// Write handle for the metashard persist shard.
    metashard_write: mz_persist_client::write::WriteHandle<OrderedKey, Proposal, u64, i64>,
}

impl<F: ActorFactory> PersistMetaActor<F> {
    /// Create a new metashard actor. Opens the metashard persist shard write
    /// handle but does NOT recover durable state — recovery is performed by
    /// the `Recover` command, injected as the first command after actor startup.
    pub async fn new(
        state: MetaState,
        queue_depth: usize,
        persist_client: PersistClient,
        factory: F,
        metashard_shard_id: ShardId,
    ) -> (Self, PersistMetaHandle) {
        let (tx, rx) = mpsc::channel(queue_depth);

        let mut metashard_write = persist_client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                metashard_shard_id,
                Arc::new(OrderedKeySchema),
                Arc::new(ProposalSchema),
                Diagnostics::from_purpose("metashard-durable-state"),
            )
            .await
            .expect("open metashard persist shard writer");

        // Advance upper past T=0 so subscribers can distinguish "no data" from
        // "initial state". This is a one-time bootstrap, cheap and idempotent.
        if metashard_write.upper().as_option() == Some(&0) {
            metashard_write
                .advance_upper(&Antichain::from_elem(1))
                .await;
        }

        let actor = PersistMetaActor {
            state,
            rx,
            persist_client,
            factory,
            leader_id: None,
            metashard_shard_id,
            metashard_write,
        };
        let handle = PersistMetaHandle::new(tx);
        (actor, handle)
    }

    /// Persist an arbitrary metashard state to the durable shard.
    ///
    /// Used by `do_reconfigure` to persist a new state value before swapping
    /// it into `self.state`, ensuring the durable state is always consistent
    /// with the in-memory state.
    ///
    /// Returns `Err(MetaError::Fenced)` if another meta actor has written to
    /// the shard (upper mismatch). In that case, `self.state` is refreshed
    /// from the durable state so the caller can see what the current state is.
    ///
    /// TODO: Retract old metashard entries atomically in the same
    /// compare_and_append batch. Good opportunities: when persisting a new
    /// leader_id (ClaimLeadership) or after reconfiguration completes
    /// (CommitReconciliation). This prevents the metashard shard from growing
    /// unboundedly.
    /// CAS-write the new state to the metashard shard. On success, replaces
    /// `self.state` with the new value. On CAS failure, refreshes `self.state`
    /// from the durable shard and returns `Fenced`.
    async fn persist_state_value(&mut self, new_state: MetaState) -> Result<(), MetaError> {
        let write = &mut self.metashard_write;
        let data = encode_meta_state(&new_state);

        let batch_number = write.upper().as_option().copied().unwrap_or(1).max(1);

        // TODO: The metashard shard reuses OrderedKey/Proposal from log shards
        // with a magic key "__metashard". It should have its own key/value types
        // that make the schema self-describing. The routing task also opens the
        // metashard shard with OrderedKeySchema/ProposalSchema which is confusing.
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
                debug!(epoch = new_state.epoch, "persisted metashard state");
                self.state = new_state;
                Ok(())
            }
            Ok(Err(_upper_mismatch)) => {
                let stale_epoch = new_state.epoch;
                warn!(
                    epoch = stale_epoch,
                    "metashard CAS failed: another writer advanced the upper"
                );
                self.fetch_latest_state().await;
                Err(MetaError::Fenced {
                    stale_epoch,
                    current_epoch: self.state.epoch,
                })
            }
            Err(e) => Err(MetaError::Command(format!("metashard persist error: {e}"))),
        }
    }

    // -----------------------------------------------------------------------
    // Leadership
    // -----------------------------------------------------------------------

    /// Claim leadership by reading durable state, incrementing the leader_id,
    /// and CAS-writing it back. On success, sets `self.leader_id` to the new
    /// value and returns it. On CAS failure (another actor wrote), returns
    /// `Fenced`.
    async fn do_claim_leadership(&mut self) -> Result<u64, MetaError> {
        self.fetch_latest_state().await;

        let new_leader_id = self.state.leader_id.unwrap_or(0) + 1;
        let mut claim_state = self.state.clone();
        claim_state.leader_id = Some(new_leader_id);

        self.persist_state_value(claim_state).await?;
        self.leader_id = Some(new_leader_id);

        info!(leader_id = new_leader_id, "claimed meta actor leadership");
        Ok(new_leader_id)
    }

    // -----------------------------------------------------------------------
    // Reconciliation helpers
    // -----------------------------------------------------------------------

    /// Read the latest durable state from the metashard persist shard and
    /// replace `self.state`. Used by both reconcile (on entry) and CAS failure
    /// recovery.
    /// Fetch the latest metashard state from persist and replace `self.state`.
    async fn fetch_latest_state(&mut self) {
        use mz_persist_client::read::ListenEvent;
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        // Fetch the true shard upper from persist. The cached upper on our
        // write handle may be stale if another meta actor wrote concurrently.
        let true_upper = self.metashard_write.fetch_recent_upper().await.clone();

        let (_, read) = self
            .persist_client
            .open::<OrderedKey, Proposal, u64, i64>(
                self.metashard_shard_id,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("metashard-read-durable"),
                false,
            )
            .await
            .expect("open metashard reader");

        let since = read.since().clone();
        let mut subscribe = read
            .subscribe(since)
            .await
            .expect("subscribe to metashard shard");

        let mut latest_data: Option<Bytes> = None;
        'outer: loop {
            let events = subscribe.fetch_next().await;
            for event in &events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        if frontier.as_option().copied() >= true_upper.as_option().copied() {
                            break 'outer;
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
        }

        if let Some(data) = latest_data {
            match decode_meta_state(data.as_ref()) {
                Ok(recovered) => self.state = recovered,
                Err(e) => warn!("fetch_latest_state: {e}"),
            }
        }
    }

    /// Check if a shard is sealed (upper is the empty antichain).
    async fn is_shard_sealed(&self, shard_id: ShardId) -> bool {
        let write = self
            .persist_client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::new(OrderedKeySchema),
                Arc::new(ProposalSchema),
                Diagnostics::from_purpose("metashard-check-sealed"),
            )
            .await
            .expect("open writer to check sealed");
        write.upper().is_empty()
    }

    /// Wait for a shard's upper to advance to at least `target`.
    /// Wait for a shard's upper to advance to at least `target`.
    /// Uses `listen` which blocks until the frontier advances past the as_of.
    async fn wait_for_upper(&self, shard_id: ShardId, target: u64) {
        let (_, read) = self
            .persist_client
            .open::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::new(OrderedKeySchema),
                Arc::new(ProposalSchema),
                Diagnostics::from_purpose("metashard-wait-upper"),
                false,
            )
            .await
            .expect("open reader for upper wait");

        // listen(as_of) blocks until the frontier advances past as_of.
        // If upper is already >= target, listen returns immediately.
        let mut listen = read
            .listen(Antichain::from_elem(target.saturating_sub(1)))
            .await
            .expect("listen for upper progress");
        let _ = listen.fetch_next().await;
    }

    /// Seal a shard by advancing its upper to the empty antichain.
    async fn seal_shard(&self, shard_id: ShardId) {
        let mut write = self
            .persist_client
            .open_writer::<OrderedKey, Proposal, u64, i64>(
                shard_id,
                Arc::new(OrderedKeySchema),
                Arc::new(ProposalSchema),
                Diagnostics::from_purpose("metashard-seal"),
            )
            .await
            .expect("open writer for sealing");

        write.advance_upper(&Antichain::new()).await;
        info!(%shard_id, "sealed log shard");
    }

    /// Acquire CriticalSince holds on a set of shards using deterministic
    /// reader IDs derived from the epoch. Re-acquirable after crash.
    async fn acquire_critical_holds(
        &self,
        shard_ids: &[ShardId],
        epoch: u64,
    ) -> Vec<SinceHandle<OrderedKey, Proposal, u64, i64>> {
        let mut holds = Vec::new();
        for &shard_id in shard_ids {
            let reader_id: CriticalReaderId = format!(
                "c{:0>8}-{:04x}-0000-0000-000000000000",
                epoch,
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
            holds.push(handle);
        }
        holds
    }

    /// Release CriticalSince holds by downgrading since to the empty antichain.
    async fn release_critical_holds(
        &self,
        holds: Vec<SinceHandle<OrderedKey, Proposal, u64, i64>>,
    ) {
        for mut hold in holds {
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
    }

    /// Build predecessor specs using the actual CriticalSince frontiers from
    /// the acquired holds. This ensures the acceptor subscribes at the correct
    /// frontier, not T=0.
    fn build_predecessor_specs(
        &self,
        shard_id: ShardId,
        holds: &[SinceHandle<OrderedKey, Proposal, u64, i64>],
        predecessor_shards: &[ShardId],
    ) -> Vec<(ShardId, Antichain<u64>)> {
        self.state
            .log_shards
            .get(&shard_id)
            .map(|info| {
                info.predecessors
                    .iter()
                    .map(|p| {
                        let idx = predecessor_shards
                            .iter()
                            .position(|s| s == p)
                            .expect("predecessor must have a CriticalSince hold");
                        (*p, holds[idx].since().clone())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    // -----------------------------------------------------------------------
    // Reconcile: idempotent control loop
    // -----------------------------------------------------------------------

    /// Drive the world toward the desired state. Completely idempotent — safe
    /// to call at any point in the lifecycle. Each step inspects real-world
    /// state and skips if already done.
    ///
    /// Used on startup (replaces Recover) and after every Reconfigure.
    async fn do_reconcile(&mut self) -> Result<ReconcileResult, MetaError> {
        // Step 1: Read durable state from meta shard.
        self.fetch_latest_state().await;

        // Step 2: Ensure actors exist for shards WITHOUT predecessors.
        // Shards with predecessors (reconfiguration in progress) are created
        // in execute_reconfiguration after CriticalSince holds are acquired,
        // so the acceptor gets the correct since frontier.
        for range in self.state.partition_map.ranges.clone() {
            let shard_id = range.log_shard;
            let has_predecessors = self
                .state
                .log_shards
                .get(&shard_id)
                .map_or(false, |info| !info.predecessors.is_empty());
            if has_predecessors {
                continue;
            }
            if let Err(e) = self
                .factory
                .create_acceptor(shard_id, self.state.epoch, vec![], range)
                .await
            {
                tracing::error!(%shard_id, "failed to create acceptor: {e}");
            }
            if let Err(e) = self.factory.create_learner(shard_id).await {
                tracing::error!(%shard_id, "failed to create learner: {e}");
            }
        }

        // Step 3: Branch on durable status.
        match self.state.status {
            MetaStatus::Completed => {
                info!(
                    epoch = self.state.epoch,
                    num_shards = self.state.partition_map.ranges.len(),
                    "reconciled — status is Completed"
                );
                Ok(ReconcileResult {
                    epoch: self.state.epoch,
                    partition_map: self.state.partition_map.clone(),
                    fully_reconciled: true,
                })
            }
            MetaStatus::Reconfiguring => self.execute_reconfiguration().await,
        }
    }

    /// Drive an in-progress reconfiguration to completion: acquire holds,
    /// wait for snapshots, seal predecessors, clear predecessors, and
    /// transition status to `Completed`.
    async fn execute_reconfiguration(&mut self) -> Result<ReconcileResult, MetaError> {
        use crate::actors::acceptor::{DELTA_SNAPSHOT_BATCH_ID, FIRST_REGULAR_BATCH_ID};

        let shards_with_predecessors: Vec<(ShardId, Vec<ShardId>)> = self
            .state
            .log_shards
            .iter()
            .filter(|(_, info)| !info.predecessors.is_empty())
            .map(|(id, info)| (*id, info.predecessors.clone()))
            .collect();

        let all_predecessors: Vec<ShardId> = {
            let mut set = std::collections::BTreeSet::new();
            for (_, preds) in &shards_with_predecessors {
                set.extend(preds.iter().copied());
            }
            set.into_iter().collect()
        };

        info!(
            epoch = self.state.epoch,
            new_shards = shards_with_predecessors.len(),
            predecessors = all_predecessors.len(),
            "executing reconfiguration"
        );

        // BUGGIFY: crash before predecessor processing.
        crate::fault::maybe_fail("after_actor_spawn").map_err(MetaError::Command)?;

        // Acquire CriticalSince holds on predecessors.
        let critical_holds = self
            .acquire_critical_holds(&all_predecessors, self.state.epoch)
            .await;

        // Create actors for shards with predecessors, using the CriticalSince
        // frontiers as the subscribe point for bulk snapshots.
        for range in self.state.partition_map.ranges.clone() {
            let shard_id = range.log_shard;
            let pred_specs =
                self.build_predecessor_specs(shard_id, &critical_holds, &all_predecessors);
            if pred_specs.is_empty() {
                continue; // Already created in do_reconcile step 2.
            }
            if let Err(e) = self
                .factory
                .create_acceptor(shard_id, self.state.epoch, pred_specs, range)
                .await
            {
                tracing::error!(%shard_id, "failed to create acceptor: {e}");
            }
            if let Err(e) = self.factory.create_learner(shard_id).await {
                tracing::error!(%shard_id, "failed to create learner: {e}");
            }
        }

        // Wait for bulk snapshots on new shards.
        for (shard_id, _) in &shards_with_predecessors {
            self.wait_for_upper(*shard_id, DELTA_SNAPSHOT_BATCH_ID)
                .await;
            info!(%shard_id, "bulk snapshot complete");
        }

        // Seal predecessor shards (idempotent — skip if already sealed).
        for &pred_id in &all_predecessors {
            if !self.is_shard_sealed(pred_id).await {
                self.seal_shard(pred_id).await;
            }
        }

        // BUGGIFY: crash after seal.
        crate::fault::maybe_fail("after_seal").map_err(MetaError::Command)?;

        // Wait for delta snapshots on new shards.
        for (shard_id, _) in &shards_with_predecessors {
            self.wait_for_upper(*shard_id, FIRST_REGULAR_BATCH_ID).await;
            info!(%shard_id, "delta snapshot complete");
        }

        // Build completed state: clear predecessors, set status to Completed.
        // REIVEW: I'd rather have new_state be done through Rust destructuring so we
        // are extremely precise about how each field is cloned and modified into the
        // next version
        let mut new_state = self.state.clone();
        for (shard_id, _) in &shards_with_predecessors {
            if let Some(info) = new_state.log_shards.get_mut(shard_id) {
                info.predecessors.clear();
            }
        }
        new_state.status = MetaStatus::Completed;

        // BUGGIFY: crash before persist.
        crate::fault::maybe_fail("after_routing_swap").map_err(MetaError::Command)?;

        self.persist_state_value(new_state).await?;

        // BUGGIFY: crash after persist but before cleanup.
        crate::fault::maybe_fail("after_commit_persist").map_err(MetaError::Command)?;

        // Release CriticalSince holds.
        crate::fault::maybe_fail("before_hold_release").map_err(MetaError::Command)?;
        self.release_critical_holds(critical_holds).await;

        // Stop retired actor processes.
        for &pred_id in &all_predecessors {
            self.factory.stop_shard(pred_id).await;
        }

        info!(
            epoch = self.state.epoch,
            "reconfiguration execution complete"
        );

        Ok(ReconcileResult {
            epoch: self.state.epoch,
            partition_map: self.state.partition_map.clone(),
            fully_reconciled: true,
        })
    }

    /// Handle a non-reconfigure command (fast, synchronous).
    fn on_query(&self, cmd: MetaCommand) {
        match cmd {
            MetaCommand::GetPartitionMap { reply } => {
                let _ = reply.send(Ok(self.state.partition_map.clone()));
            }
            MetaCommand::GetEpoch { reply } => {
                let _ = reply.send(Ok(self.state.epoch));
            }
            MetaCommand::Reconfigure { .. }
            | MetaCommand::Reconcile { .. }
            | MetaCommand::ClaimLeadership { .. } => {
                unreachable!("handled separately in run loop")
            }
        }
    }

    // -----------------------------------------------------------------------
    // Reconfigure: record desired state via CAS
    // -----------------------------------------------------------------------

    /// Record a new desired state via CAS. Lightweight — does not execute
    /// the reconfiguration. The reconcile loop drives the world toward the
    /// new state after this returns.
    ///
    /// Validates: epoch matches, current state is fully reconciled (all
    /// predecessors empty), new partition map is valid. Builds new MetaState
    /// with predecessor chains for added shards, CAS-writes to meta shard.
    async fn do_reconfigure(&mut self, plan: ReconfigurationPlan) -> Result<u64, MetaError> {
        if plan.expected_epoch != self.state.epoch {
            return Err(MetaError::EpochMismatch {
                expected: plan.expected_epoch,
                actual: self.state.epoch,
            });
        }

        // Reject if a reconfiguration is still in progress.
        if self.state.status != MetaStatus::Completed {
            return Err(MetaError::ReconfigurationInProgress);
        }

        plan.new_partition_map
            .validate()
            .map_err(|e| MetaError::Command(format!("invalid partition map: {e}")))?;

        let old_map = &self.state.partition_map;
        let new_map = &plan.new_partition_map;
        let new_epoch = self.state.epoch + 1;

        let old_shards: std::collections::BTreeSet<ShardId> =
            old_map.ranges.iter().map(|r| r.log_shard).collect();
        let new_shards: std::collections::BTreeSet<ShardId> =
            new_map.ranges.iter().map(|r| r.log_shard).collect();
        let added: std::collections::BTreeSet<ShardId> =
            new_shards.difference(&old_shards).copied().collect();

        let mut new_log_shards = BTreeMap::new();
        for range in &new_map.ranges {
            let predecessors = if added.contains(&range.log_shard) {
                old_map
                    .ranges
                    .iter()
                    .filter(|r| {
                        u16::from(range.lo) < r.hi_exclusive && u16::from(r.lo) < range.hi_exclusive
                    })
                    .map(|r| r.log_shard)
                    .collect()
            } else {
                Vec::new()
            };
            new_log_shards.insert(
                range.log_shard,
                LogShardInfo {
                    epoch_created: if added.contains(&range.log_shard) {
                        new_epoch
                    } else {
                        self.state
                            .log_shards
                            .get(&range.log_shard)
                            .map(|i| i.epoch_created)
                            .unwrap_or(new_epoch)
                    },
                    range: range.clone(),
                    predecessors,
                },
            );
        }

        let new_state = MetaState {
            epoch: new_epoch,
            partition_map: PartitionMap {
                epoch: new_epoch,
                ranges: new_map.ranges.clone(),
            },
            log_shards: new_log_shards,
            leader_id: self.state.leader_id,
            status: MetaStatus::Reconfiguring,
        };

        info!(
            old_epoch = self.state.epoch,
            new_epoch,
            added_shards = added.len(),
            new_ranges = new_map.ranges.len(),
            "reconfigure: CAS-writing new desired state"
        );

        // BUGGIFY: crash after building state but before CAS.
        crate::fault::maybe_fail("after_intent_persist").map_err(MetaError::Command)?;

        self.persist_state_value(new_state).await?;

        info!(new_epoch, "reconfigure: new desired state committed");
        Ok(new_epoch)
    }

    // -----------------------------------------------------------------------
    // Run loop + spawn
    // -----------------------------------------------------------------------

    /// Run the actor loop until the command channel closes.
    pub async fn run(mut self) {
        info!(
            metashard_shard = %self.metashard_shard_id,
            epoch = self.state.epoch,
            num_ranges = self.state.partition_map.ranges.len(),
            "metashard actor starting"
        );

        loop {
            match self.rx.recv().await {
                Some(MetaCommand::ClaimLeadership { reply }) => {
                    let result = self.do_claim_leadership().await;
                    let fenced = matches!(&result, Err(MetaError::Fenced { .. }));
                    let _ = reply.send(result);
                    if fenced {
                        info!("meta actor fenced during leadership claim, shutting down");
                        break;
                    }
                }
                Some(MetaCommand::Reconcile { reply }) => {
                    let result = self.do_reconcile().await;
                    let fenced = matches!(&result, Err(MetaError::Fenced { .. }));
                    let _ = reply.send(result);
                    if fenced {
                        info!("meta actor fenced during reconciliation, shutting down");
                        break;
                    }
                }
                Some(MetaCommand::Reconfigure { plan, reply }) => {
                    let result = self.do_reconfigure(plan).await;
                    let fenced = matches!(&result, Err(MetaError::Fenced { .. }));
                    let _ = reply.send(result);
                    if fenced {
                        info!("meta actor fenced during reconfiguration, shutting down");
                        break;
                    }
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
    ///
    /// Sends `ClaimLeadership` then `Reconcile` as the first two commands.
    pub async fn spawn(
        state: MetaState,
        queue_depth: usize,
        persist_client: PersistClient,
        factory: F,
        metashard_shard_id: ShardId,
    ) -> (
        PersistMetaHandle,
        ReconcileResult,
        mz_ore::task::JoinHandle<()>,
    ) {
        let (actor, handle) = Self::new(
            state,
            queue_depth,
            persist_client,
            factory,
            metashard_shard_id,
        )
        .await;
        let task = mz_ore::task::spawn(|| "persist-metashard", actor.run());

        handle
            .claim_leadership()
            .await
            .expect("meta leadership claim failed");
        let result = handle
            .reconcile()
            .await
            .expect("meta reconciliation failed");

        (handle, result, task)
    }
}

// NOTE: Old do_reconfigure (400+ lines), do_recover, and old run loop were
// deleted. Replaced by the Reconcile/Reconfigure control loop pattern above.
//
// The dead code that was here has been removed. If you need to reference the
// old implementation, check git history.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Metashard;

    fn test_shard(suffix: &str) -> ShardId {
        format!("s{:0>32}", suffix).parse().expect("valid shard id")
    }

    #[mz_ore::test]
    fn partition_map_routes_correctly() {
        let s1 = test_shard("1");
        let s2 = test_shard("2");
        let map = PartitionMap {
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
        };

        // "s0a..." → partition key 0x0a → first range → s1
        assert_eq!(map.route("s0a000000-0000-0000-0000-000000000000"), s1);

        // "sff..." → partition key 0xff → second range → s2
        assert_eq!(map.route("sff000000-0000-0000-0000-000000000000"), s2);
    }

    #[tokio::test]
    async fn metashard_returns_partition_map() {
        let s1 = test_shard("1");
        let state = MetaState::single(s1);

        let (tx, rx) = mpsc::channel(64);
        let handle = PersistMetaHandle::new(tx);
        let actor_state = state.clone();
        mz_ore::task::spawn(|| "test-metashard", async move {
            let mut rx = rx;
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    MetaCommand::GetPartitionMap { reply } => {
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
    /// predecessors), then recover from the same shard and verify the
    /// recovered state matches.
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
        // - predecessor chains (non-empty = reconfig in progress)
        let mut log_shards = BTreeMap::new();
        log_shards.insert(
            s_a,
            LogShardInfo {
                epoch_created: 1,
                range: RangeAssignment {
                    lo: 0x00,
                    hi_exclusive: 0x80,
                    log_shard: s_a,
                },
                predecessors: vec![s_old],
            },
        );
        log_shards.insert(
            s_b,
            LogShardInfo {
                epoch_created: 1,
                range: RangeAssignment {
                    lo: 0x80,
                    hi_exclusive: 0x100,
                    log_shard: s_b,
                },
                predecessors: vec![s_old],
            },
        );

        let state = MetaState {
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
            leader_id: Some(42),
            status: MetaStatus::Reconfiguring,
        };

        // --- Persist the state ---
        let factory = crate::factory::InProcessActorFactory::new(client.clone());

        let (mut actor, _handle) =
            PersistMetaActor::new(state.clone(), 64, client.clone(), factory, metashard_shard)
                .await;

        // Force a persist via persist_state_value.
        actor
            .persist_state_value(actor.state.clone())
            .await
            .expect("persist failed");
        drop(actor);
        drop(_handle);

        // --- Recover from the same shard ---
        let bootstrap = MetaState::single(test_shard("eee"));
        let factory2 = crate::factory::InProcessActorFactory::new(client.clone());

        let (mut recovered_actor, _handle2) =
            PersistMetaActor::new(bootstrap, 64, client.clone(), factory2, metashard_shard).await;

        // Trigger reconciliation (reads durable state).
        recovered_actor.fetch_latest_state().await;

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
            "shard_a should have log_shards entry"
        );
        assert_eq!(
            recovered.log_shards[&s_a].predecessors,
            vec![s_old],
            "shard_a predecessor chain round-trip failed"
        );
        assert!(
            recovered.log_shards.contains_key(&s_b),
            "shard_b should have log_shards entry"
        );
        assert_eq!(
            recovered.log_shards[&s_b].predecessors,
            vec![s_old],
            "shard_b predecessor chain round-trip failed"
        );

        // Verify status survived.
        assert_eq!(
            recovered.status,
            MetaStatus::Reconfiguring,
            "status round-trip failed"
        );
    }
}
