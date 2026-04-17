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

use std::collections::HashMap;
use std::sync::Arc;

use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use arrow::array::{BinaryArray, BinaryBuilder};
use bytes::{BufMut, Bytes};
use prost::Message;

use mz_persist_client::critical::{CriticalReaderId, Opaque, SinceHandle};
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::Codec;
use mz_persist_types::codec_impls::{
    SimpleColumnarData, SimpleColumnarDecoder, SimpleColumnarEncoder, UnitSchema,
};
use mz_persist_types::columnar::Schema;
use mz_persist_types::stats::NoneStats;

use crate::actors::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::factory::ActorFactory;
use crate::{MetaError, PartitionMap, RangeAssignment, ReconfigurationPlan};

// ---------------------------------------------------------------------------
// Metashard state
// ---------------------------------------------------------------------------

/// A set of log shards with their range assignments.
///
/// Used in [`MetaState`] to describe the outgoing (`start_state`) or
/// desired (`target_state`) shard set.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardSet {
    pub ranges: Vec<RangeAssignment>,
}

/// The metashard actor's durable state.
///
/// Stored as the key in the metashard persist shard (value is `()`). The
/// reconcile loop reads this state and drives the world toward it.
///
/// When `start_state` is `None`, the configuration is stable and `target_state`
/// describes the live shards. When `start_state` is `Some`, a reconfiguration
/// is in progress: `start_state` holds the outgoing shards and `target_state`
/// holds the incoming shards. Predecessors for each target shard are computed
/// at runtime from range overlap — they are not stored.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetaState {
    /// Monotonically increasing; bumped on each reconfiguration.
    /// Used to generate deterministic CriticalSince reader IDs.
    pub(crate) epoch: u64,
    /// Monotonically increasing leader ID. Incremented on each ClaimLeadership.
    pub(crate) leader_id: Option<u64>,
    /// `None` when stable; `Some` during an in-progress reconfiguration.
    /// Contains the outgoing shard set.
    pub(crate) start_state: Option<ShardSet>,
    /// The live (stable) or desired (reconfiguring) shard set.
    pub(crate) target_state: ShardSet,
}

impl MetaState {
    /// Create initial bootstrap state from a partition map.
    pub fn new(partition_map: PartitionMap) -> Self {
        MetaState {
            epoch: 0,
            leader_id: None,
            start_state: None,
            target_state: ShardSet {
                ranges: partition_map.ranges,
            },
        }
    }

    /// Create initial state with a single log shard covering the entire range.
    pub fn single(log_shard: ShardId) -> Self {
        MetaState {
            epoch: 0,
            leader_id: None,
            start_state: None,
            target_state: ShardSet {
                ranges: PartitionMap::single(log_shard).ranges,
            },
        }
    }

    /// Derive the current partition map from `target_state` and `epoch`.
    pub(crate) fn partition_map(&self) -> PartitionMap {
        PartitionMap {
            epoch: self.epoch,
            ranges: self.target_state.ranges.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// MetaState codec + columnar schema
// ---------------------------------------------------------------------------

use mz_persist::generated::consensus_service::{
    ProtoMetashardStateV2, ProtoRangeAssignment, ProtoShardSet,
};

fn parse_proto_range(r: &ProtoRangeAssignment) -> Option<RangeAssignment> {
    Some(RangeAssignment {
        lo: u8::try_from(r.lo).ok()?,
        hi_exclusive: u16::try_from(r.hi_exclusive).ok()?,
        log_shard: r.log_shard.parse().ok()?,
    })
}

fn encode_shard_set(s: &ShardSet) -> ProtoShardSet {
    ProtoShardSet {
        ranges: s
            .ranges
            .iter()
            .map(|r| ProtoRangeAssignment {
                lo: u32::from(r.lo),
                hi_exclusive: u32::from(r.hi_exclusive),
                log_shard: r.log_shard.to_string(),
            })
            .collect(),
    }
}

fn decode_shard_set(proto: ProtoShardSet) -> Result<ShardSet, String> {
    let ranges: Vec<RangeAssignment> = proto.ranges.iter().filter_map(parse_proto_range).collect();
    if ranges.is_empty() {
        return Err("decoded empty shard set".to_string());
    }
    Ok(ShardSet { ranges })
}

fn encode_meta_state(state: &MetaState) -> Bytes {
    let proto = ProtoMetashardStateV2 {
        epoch: state.epoch,
        leader_id: state.leader_id.unwrap_or(0),
        target_state: Some(encode_shard_set(&state.target_state)),
        start_state: state.start_state.as_ref().map(encode_shard_set),
    };
    Bytes::from(proto.encode_to_vec())
}

fn decode_meta_state(data: &[u8]) -> Result<MetaState, String> {
    let proto = ProtoMetashardStateV2::decode(data)
        .map_err(|e| format!("failed to decode metashard proto: {e}"))?;

    let ProtoMetashardStateV2 {
        epoch,
        leader_id,
        target_state,
        start_state,
    } = proto;

    let target = decode_shard_set(target_state.ok_or("missing target_state in proto")?)?;
    PartitionMap {
        epoch,
        ranges: target.ranges.clone(),
    }
    .validate()
    .map_err(|e| format!("invalid recovered target_state: {e}"))?;

    let start = start_state.map(decode_shard_set).transpose()?;

    Ok(MetaState {
        epoch,
        leader_id: if leader_id == 0 {
            None
        } else {
            Some(leader_id)
        },
        start_state: start,
        target_state: target,
    })
}

/// Schema for the metashard persist shard key ([`MetaState`]).
///
/// Encodes `MetaState` as a single binary column containing proto bytes.
/// The value type is `()`, using [`UnitSchema`].
#[derive(Debug, PartialEq)]
pub struct MetaStateSchema;

impl Codec for MetaState {
    type Storage = ();
    type Schema = MetaStateSchema;

    fn codec_name() -> String {
        "MetaState".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put(encode_meta_state(self).as_ref());
    }

    fn decode<'a>(buf: &'a [u8], _schema: &MetaStateSchema) -> Result<Self, String> {
        decode_meta_state(buf)
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        MetaStateSchema
    }
}

impl SimpleColumnarData for MetaState {
    type ArrowBuilder = BinaryBuilder;
    type ArrowColumn = BinaryArray;

    fn goodbytes(builder: &Self::ArrowBuilder) -> usize {
        builder.values_slice().len()
    }

    fn push(&self, builder: &mut Self::ArrowBuilder) {
        builder.append_value(encode_meta_state(self).as_ref());
    }

    fn push_null(builder: &mut Self::ArrowBuilder) {
        builder.append_null();
    }

    fn read(&mut self, idx: usize, column: &Self::ArrowColumn) {
        *self = decode_meta_state(column.value(idx)).expect("valid MetaState in shard");
    }
}

impl Schema<MetaState> for MetaStateSchema {
    type ArrowColumn = BinaryArray;
    type Statistics = NoneStats;
    type Decoder = SimpleColumnarDecoder<MetaState>;
    type Encoder = SimpleColumnarEncoder<MetaState>;

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SimpleColumnarEncoder::default())
    }

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        Ok(SimpleColumnarDecoder::new(col))
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Result of the `Reconcile` command.
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    pub epoch: u64,
    pub partition_map: PartitionMap,
}

/// Commands dispatched to the metashard actor.
pub enum MetaCommand {
    /// Claim leadership by CAS-writing an incremented leader_id.
    /// Must succeed before Reconcile or PlanReconfiguration can proceed.
    ClaimLeadership {
        reply: oneshot::Sender<Result<u64, MetaError>>,
    },
    /// Read durable state, ensure actors exist, drive any in-progress
    /// reconfiguration to completion. Requires active leadership.
    Reconcile {
        reply: oneshot::Sender<Result<ReconcileResult, MetaError>>,
    },
    /// Return the current in-memory actor state.
    GetState {
        reply: oneshot::Sender<Result<MetaState, MetaError>>,
    },
    /// Record a new desired partition map via CAS. Lightweight — does not
    /// execute the reconfiguration. Call `Reconcile` afterward to drive the
    /// world toward the new state.
    PlanReconfiguration {
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

    /// Drive the world toward durable state. Requires active leadership.
    pub async fn reconcile(&self) -> Result<ReconcileResult, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::Reconcile { reply: reply_tx })
            .await
            .map_err(|_| MetaError::Shutdown)?;
        reply_rx.await.map_err(|_| MetaError::DroppedReply)?
    }

    async fn get_state(&self) -> Result<MetaState, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::GetState { reply: reply_tx })
            .await
            .map_err(|_| MetaError::Shutdown)?;
        reply_rx.await.map_err(|_| MetaError::DroppedReply)?
    }
}

#[async_trait::async_trait]
impl crate::Metashard for PersistMetaHandle {
    async fn partition_map(&self) -> Result<PartitionMap, MetaError> {
        Ok(self.get_state().await?.partition_map())
    }

    async fn current_epoch(&self) -> Result<u64, MetaError> {
        Ok(self.get_state().await?.epoch)
    }

    async fn plan_reconfiguration(&self, plan: ReconfigurationPlan) -> Result<u64, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::PlanReconfiguration {
                plan,
                reply: reply_tx,
            })
            .await
            .map_err(|_| MetaError::Shutdown)?;
        reply_rx.await.map_err(|_| MetaError::DroppedReply)?
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
    /// Write handle for the metashard persist shard.
    /// Key: MetaState (proto-encoded), Value: () (unit).
    metashard_write: mz_persist_client::write::WriteHandle<MetaState, (), u64, i64>,
    /// Read handle for the metashard persist shard. Held open to avoid
    /// re-registering a leased reader on every `fetch_latest_state` call.
    /// Cloned (via `ReadHandle::clone`) before each subscribe since
    /// `subscribe` consumes the handle.
    metashard_read: mz_persist_client::read::ReadHandle<MetaState, (), u64, i64>,
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

        let (mut metashard_write, metashard_read) = persist_client
            .open::<MetaState, (), u64, i64>(
                metashard_shard_id,
                Arc::new(MetaStateSchema),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("metashard-durable-state"),
                false,
            )
            .await
            .expect("open metashard persist shard");

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
            metashard_write,
            metashard_read,
        };
        let handle = PersistMetaHandle::new(tx);
        (actor, handle)
    }

    /// CAS-write a new state to the metashard persist shard.
    ///
    /// Appends `new_state` (diff=+1) and retracts `self.state` (diff=-1) in
    /// the same batch, keeping the shard bounded to a single live entry.
    /// On success, replaces `self.state` with `new_state`. On CAS failure
    /// (another actor wrote concurrently), returns `Fenced`.
    async fn update_durable_meta_state(&mut self, new_state: MetaState) -> Result<(), MetaError> {
        let ts = self
            .metashard_write
            .upper()
            .as_option()
            .copied()
            .unwrap_or(1)
            .max(1);
        let upper = self.metashard_write.upper().clone();
        let new_upper = Antichain::from_elem(ts + 1);

        // Retract the previous state unless this is the first write (ts == 1).
        let mut updates: Vec<((MetaState, ()), u64, i64)> =
            vec![((new_state.clone(), ()), ts, 1i64)];
        if ts > 1 {
            updates.push(((self.state.clone(), ()), ts, -1i64));
        }

        match self
            .metashard_write
            .compare_and_append(&updates, upper, new_upper)
            .await
        {
            Ok(Ok(())) => {
                debug!(epoch = new_state.epoch, "persisted metashard state");
                self.state = new_state;
                Ok(())
            }
            Ok(Err(_upper_mismatch)) => {
                let stale_epoch = self.state.epoch;
                warn!(
                    epoch = stale_epoch,
                    "metashard CAS failed: another writer advanced the upper"
                );
                Err(MetaError::Fenced { stale_epoch })
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
        let ts = self
            .metashard_write
            .upper()
            .as_option()
            .copied()
            .unwrap_or(1)
            .max(1);
        if ts > 1 {
            self.fetch_latest_state().await;
        }

        let new_leader_id = self.state.leader_id.unwrap_or(0) + 1;
        let mut claim_state = self.state.clone();
        claim_state.leader_id = Some(new_leader_id);

        self.update_durable_meta_state(claim_state).await?;
        self.leader_id = Some(new_leader_id);

        info!(leader_id = new_leader_id, "claimed meta actor leadership");
        Ok(new_leader_id)
    }

    // -----------------------------------------------------------------------
    // Reconciliation helpers
    // -----------------------------------------------------------------------

    /// Fetch the latest metashard state from persist and replace `self.state`.
    ///
    /// Reads a consolidated snapshot at `metashard_upper - 1`. Each write
    /// retracts the previous state atomically, so after consolidation exactly
    /// one entry with diff=+1 remains. If the shard has no data yet (upper ==
    /// {1} from bootstrap), `self.state` is left unchanged.
    async fn fetch_latest_state(&mut self) {
        let metashard_upper = self.metashard_write.fetch_recent_upper().await.clone();

        let Some(&upper_ts) = metashard_upper.as_option() else {
            return;
        };
        if upper_ts == 0 {
            return;
        }

        let as_of = Antichain::from_elem(upper_ts - 1);
        let updates: Vec<((MetaState, ()), u64, i64)> = self
            .metashard_read
            .snapshot_and_fetch(as_of)
            .await
            .expect("as_of is within [since, upper)");

        if let Some(((state, ()), _, _)) = updates.into_iter().find(|(_, _, diff)| *diff == 1) {
            debug!(
                epoch = state.epoch,
                num_target_ranges = state.target_state.ranges.len(),
                reconfiguring = state.start_state.is_some(),
                "fetched latest metashard state from durable storage",
            );
            self.state = state;
        }
    }


    /// Acquire CriticalSince holds on a set of shards using deterministic
    /// reader IDs derived from the epoch. Re-acquirable after crash.
    async fn acquire_critical_holds(
        &self,
        shard_ids: &[ShardId],
        epoch: u64,
    ) -> Vec<SinceHandle<OrderedKey, Proposal, u64, i64>> {
        let mut holds: Vec<SinceHandle<OrderedKey, Proposal, u64, i64>> = Vec::new();
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

    /// Compute the predecessor list for each target shard from range overlap.
    ///
    /// For each shard in `target`, returns the shards in `start` whose key
    /// range overlaps. Only target shards with at least one predecessor are
    /// included. This replaces the stored `predecessors` field from the old
    /// design — predecessors are now always computed, never stored durably.
    fn compute_target_shard_predecessors(
        start: &ShardSet,
        target: &ShardSet,
    ) -> Vec<(ShardId, Vec<ShardId>)> {
        target
            .ranges
            .iter()
            .filter_map(|new_r| {
                let predecessors: Vec<ShardId> = start
                    .ranges
                    .iter()
                    .filter(|old_r| {
                        u16::from(new_r.lo) < old_r.hi_exclusive
                            && u16::from(old_r.lo) < new_r.hi_exclusive
                    })
                    .map(|r| r.log_shard)
                    .collect();
                if predecessors.is_empty() {
                    None
                } else {
                    Some((new_r.log_shard, predecessors))
                }
            })
            .collect()
    }

    /// Build predecessor specs with the actual CriticalSince frontiers from
    /// the acquired holds. Ensures each acceptor subscribes at the correct
    /// frontier rather than T=0.
    fn build_predecessor_specs(
        predecessors: &[ShardId],
        holds: &[SinceHandle<OrderedKey, Proposal, u64, i64>],
        all_predecessor_shards: &[ShardId],
    ) -> Vec<(ShardId, Antichain<u64>)> {
        predecessors
            .iter()
            .map(|p| {
                let idx = all_predecessor_shards
                    .iter()
                    .position(|s| s == p)
                    .expect("predecessor must have a CriticalSince hold");
                (*p, holds[idx].since().clone())
            })
            .collect()
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
        self.fetch_latest_state().await;

        if self.state.start_state.is_some() {
            return self.execute_reconfiguration().await;
        }

        // Stable: ensure acceptors and learners exist for all target shards.
        // Shards being reconfigured are created in execute_reconfiguration
        // after CriticalSince holds are acquired so the acceptor gets the
        // correct subscribe frontier.
        for range in self.state.target_state.ranges.clone() {
            let shard_id = range.log_shard;
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

        info!(
            epoch = self.state.epoch,
            num_shards = self.state.target_state.ranges.len(),
            "reconciled"
        );
        Ok(ReconcileResult {
            epoch: self.state.epoch,
            partition_map: self.state.partition_map(),
        })
    }

    /// Drive an in-progress reconfiguration to completion.
    ///
    /// Computes predecessor relationships from range overlap between
    /// `start_state` and `target_state`, then executes the full lifecycle:
    /// acquire CriticalSince holds → create new shard actors → wait for bulk
    /// snapshots → seal predecessors → wait for delta snapshots → commit
    /// completed state (start_state = None) → release holds → stop old actors.
    async fn execute_reconfiguration(&mut self) -> Result<ReconcileResult, MetaError> {
        use crate::actors::acceptor::{BULK_SNAPSHOT_BATCH_ID, DELTA_SNAPSHOT_BATCH_ID};

        let start = self
            .state
            .start_state
            .as_ref()
            .expect("called only during reconfiguration");
        let target_shard_predecessors =
            Self::compute_target_shard_predecessors(start, &self.state.target_state);

        let all_predecessors: Vec<ShardId> = target_shard_predecessors
            .iter()
            .flat_map(|(_, preds)| preds.iter().copied())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        info!(
            epoch = self.state.epoch,
            new_shards = target_shard_predecessors.len(),
            predecessors = all_predecessors.len(),
            "executing reconfiguration"
        );

        // BUGGIFY: crash before predecessor processing.
        crate::fault::maybe_fail("after_actor_spawn").map_err(MetaError::Command)?;

        // Acquire CriticalSince holds on all predecessor shards (idempotent: re-acquired after crash using deterministic reader IDs).
        let critical_holds = self
            .acquire_critical_holds(&all_predecessors, self.state.epoch)
            .await;

        // Open one write handle per predecessor shard, reused for the sealed
        // check and the seal operation — avoids re-registering handles on each call.
        let mut pred_writes: HashMap<
            ShardId,
            mz_persist_client::write::WriteHandle<OrderedKey, Proposal, u64, i64>,
        > = HashMap::new();
        for &pred_id in &all_predecessors {
            let write = self
                .persist_client
                .open_writer::<OrderedKey, Proposal, u64, i64>(
                    pred_id,
                    Arc::new(OrderedKeySchema),
                    Arc::new(ProposalSchema),
                    Diagnostics::from_purpose("metashard-seal"),
                )
                .await
                .expect("open predecessor shard writer");
            pred_writes.insert(pred_id, write);
        }

        // Open one write handle per target shard, reused for both upper waits
        // via wait_for_upper_past — simpler than read+listen and no clone needed.
        let mut target_writes: HashMap<
            ShardId,
            mz_persist_client::write::WriteHandle<OrderedKey, Proposal, u64, i64>,
        > = HashMap::new();
        for (target_shard_id, _) in &target_shard_predecessors {
            let write = self
                .persist_client
                .open_writer::<OrderedKey, Proposal, u64, i64>(
                    *target_shard_id,
                    Arc::new(OrderedKeySchema),
                    Arc::new(ProposalSchema),
                    Diagnostics::from_purpose("metashard-wait-upper"),
                )
                .await
                .expect("open target shard writer");
            target_writes.insert(*target_shard_id, write);
        }

        // Create actors for new shards that have predecessors.
        //
        // In distributed (ProcessActorFactory) mode each call forks a child
        // process and blocks until it binds its gRPC Unix socket, so spawning
        // N shards sequentially used to take ~1s × N. Fire them all off in
        // parallel.
        let factory = &self.factory;
        let epoch = self.state.epoch;
        let spawns = self.state.target_state.ranges.clone().into_iter().filter_map(
            |range| -> Option<_> {
                let shard_id = range.log_shard;
                let predecessors = target_shard_predecessors
                    .iter()
                    .find(|(id, _)| *id == shard_id)
                    .map(|(_, p)| p.as_slice())
                    .unwrap_or(&[]);
                if predecessors.is_empty() {
                    return None;
                }
                let pred_specs = Self::build_predecessor_specs(
                    predecessors,
                    &critical_holds,
                    &all_predecessors,
                );
                Some(async move {
                    factory
                        .create_acceptor(shard_id, epoch, pred_specs, range)
                        .await
                        .map_err(|e| format!("failed to create acceptor for {shard_id}: {e}"))?;
                    factory
                        .create_learner(shard_id)
                        .await
                        .map_err(|e| format!("failed to create learner for {shard_id}: {e}"))?;
                    Ok::<(), String>(())
                })
            },
        );
        futures::future::try_join_all(spawns)
            .await
            .map_err(MetaError::Command)?;

        // Wait for bulk snapshots on new (target) shards, in parallel. Each
        // wait borrows a distinct WriteHandle so the futures don't contend.
        let bulk_waits = target_writes.iter_mut().map(|(shard_id, write)| async move {
            write
                .wait_for_upper_past(&Antichain::from_elem(BULK_SNAPSHOT_BATCH_ID))
                .await;
            info!(%shard_id, "bulk snapshot complete");
        });
        futures::future::join_all(bulk_waits).await;

        // Seal predecessor shards (idempotent: already-sealed shards are skipped).
        let seals = pred_writes.iter_mut().map(|(pred_id, write)| async move {
            if !write.upper().is_empty() {
                write.advance_upper(&Antichain::new()).await;
                info!(%pred_id, "sealed log shard");
            }
        });
        futures::future::join_all(seals).await;

        // BUGGIFY: crash after seal.
        crate::fault::maybe_fail("after_seal").map_err(MetaError::Command)?;

        // Wait for delta snapshots on new (target) shards, in parallel.
        let delta_waits = target_writes.iter_mut().map(|(shard_id, write)| async move {
            write
                .wait_for_upper_past(&Antichain::from_elem(DELTA_SNAPSHOT_BATCH_ID))
                .await;
            info!(%shard_id, "delta snapshot complete");
        });
        futures::future::join_all(delta_waits).await;

        // Commit: clear start_state to mark reconfiguration complete.
        let new_state = MetaState {
            epoch: self.state.epoch,
            leader_id: self.state.leader_id,
            start_state: None,
            target_state: self.state.target_state.clone(),
        };

        // BUGGIFY: crash before persist.
        crate::fault::maybe_fail("after_routing_swap").map_err(MetaError::Command)?;

        self.update_durable_meta_state(new_state).await?;

        // BUGGIFY: crash after persist but before cleanup.
        crate::fault::maybe_fail("after_commit_persist").map_err(MetaError::Command)?;

        crate::fault::maybe_fail("before_hold_release").map_err(MetaError::Command)?;
        self.release_critical_holds(critical_holds).await;

        for &pred_id in &all_predecessors {
            self.factory.stop_shard(pred_id).await;
        }

        info!(epoch = self.state.epoch, "reconfiguration complete");

        Ok(ReconcileResult {
            epoch: self.state.epoch,
            partition_map: self.state.partition_map(),
        })
    }

    // -----------------------------------------------------------------------
    // PlanReconfiguration: record desired state via CAS
    // -----------------------------------------------------------------------

    /// Record a new desired state via CAS. Lightweight — does not execute the
    /// reconfiguration. The reconcile loop drives the world toward the new
    /// state after this returns.
    ///
    /// Sets `start_state` to the current `target_state` (outgoing) and
    /// `target_state` to the new partition map (incoming). Predecessor
    /// relationships are computed at reconcile time from range overlap.
    ///
    /// Requires active leadership — returns `Fenced` if this actor is not the current leader.
    async fn do_plan_reconfiguration(
        &mut self,
        plan: ReconfigurationPlan,
    ) -> Result<u64, MetaError> {
        if self.state.start_state.is_some() {
            return Err(MetaError::ReconfigurationInProgress);
        }

        if self.leader_id != self.state.leader_id {
            return Err(MetaError::Fenced {
                stale_epoch: self.state.epoch,
            });
        }

        if plan.expected_epoch != self.state.epoch {
            return Err(MetaError::EpochMismatch {
                expected: plan.expected_epoch,
                actual: self.state.epoch,
            });
        }

        plan.new_partition_map
            .validate()
            .map_err(|e| MetaError::Command(format!("invalid partition map: {e}")))?;

        let new_epoch = self.state.epoch + 1;
        let new_state = MetaState {
            epoch: new_epoch,
            leader_id: self.state.leader_id,
            start_state: Some(self.state.target_state.clone()),
            target_state: ShardSet {
                ranges: plan.new_partition_map.ranges,
            },
        };

        info!(
            old_epoch = self.state.epoch,
            new_epoch,
            new_ranges = new_state.target_state.ranges.len(),
            "plan_reconfiguration: CAS-writing new desired state"
        );

        self.update_durable_meta_state(new_state).await?;

        // BUGGIFY: crash after intent is durably persisted (start_state=Some committed).
        // On restart, the actor sees the pending reconfiguration and resumes it.
        crate::fault::maybe_fail("after_intent_persist").map_err(MetaError::Command)?;

        info!(
            new_epoch,
            "plan_reconfiguration: new desired state committed"
        );
        Ok(new_epoch)
    }

    // -----------------------------------------------------------------------
    // Run loop + spawn
    // -----------------------------------------------------------------------

    /// Run the actor loop until the command channel closes.
    pub async fn run(mut self) {
        info!(
            metashard_shard = %self.metashard_write.shard_id(),
            epoch = self.state.epoch,
            num_ranges = self.state.target_state.ranges.len(),
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
                Some(MetaCommand::PlanReconfiguration { plan, reply }) => {
                    let result = self.do_plan_reconfiguration(plan).await;
                    let fenced = matches!(&result, Err(MetaError::Fenced { .. }));
                    let _ = reply.send(result);
                    if fenced {
                        info!("meta actor fenced during plan_reconfiguration, shutting down");
                        break;
                    }
                }
                Some(MetaCommand::GetState { reply }) => {
                    let _ = reply.send(Ok(self.state.clone()));
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
                    MetaCommand::GetState { reply } => {
                        let _ = reply.send(Ok(actor_state.clone()));
                    }
                    _ => {}
                }
            }
        });

        let map = handle.partition_map().await.unwrap();
        assert_eq!(map, state.partition_map());
    }

    /// Round-trip test: persist metashard state, then recover from the same
    /// shard and verify the recovered state matches.
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

        // Build a state with all fields populated:
        // - epoch > 0, leader_id set
        // - start_state: Some (reconfiguration in progress)
        // - target_state: two-shard split
        let state = MetaState {
            epoch: 1,
            leader_id: Some(42),
            start_state: Some(ShardSet {
                ranges: vec![RangeAssignment {
                    lo: 0x00,
                    hi_exclusive: 0x100,
                    log_shard: s_old,
                }],
            }),
            target_state: ShardSet {
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
        };

        // --- Persist the state ---
        let factory = crate::factory::InProcessActorFactory::new(client.clone());

        let (mut actor, _handle) =
            PersistMetaActor::new(state.clone(), 64, client.clone(), factory, metashard_shard)
                .await;

        actor
            .update_durable_meta_state(actor.state.clone())
            .await
            .expect("persist failed");
        drop(actor);
        drop(_handle);

        // --- Recover from the same shard ---
        let bootstrap = MetaState::single(test_shard("eee"));
        let factory2 = crate::factory::InProcessActorFactory::new(client.clone());

        let (mut recovered_actor, _handle2) =
            PersistMetaActor::new(bootstrap, 64, client.clone(), factory2, metashard_shard).await;

        recovered_actor.fetch_latest_state().await;

        let recovered = &recovered_actor.state;

        // --- Verify round-trip ---
        assert_eq!(recovered.epoch, state.epoch, "epoch round-trip failed");
        assert_eq!(
            recovered.leader_id, state.leader_id,
            "leader_id round-trip failed"
        );
        assert_eq!(
            recovered.start_state, state.start_state,
            "start_state round-trip failed"
        );
        assert_eq!(
            recovered.target_state, state.target_state,
            "target_state round-trip failed"
        );
    }
}
