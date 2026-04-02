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
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};

use crate::actors::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::factory::ActorFactory;
use crate::{MetaError, PartitionMap, RangeAssignment, ReconfigurationPlan};

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
pub struct MetaState {
    /// Current configuration epoch.
    pub epoch: u64,
    /// The authoritative partition map.
    pub partition_map: PartitionMap,
    /// Per-log-shard metadata.
    pub log_shards: BTreeMap<ShardId, LogShardInfo>,
    /// In-flight reconfiguration intent (if any).
    pub pending_intent: Option<ReconfigurationIntent>,
}

impl MetaState {
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
        MetaState {
            epoch: 0,
            partition_map: PartitionMap::single(log_shard),
            log_shards,
            pending_intent: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Proto encode / decode helpers
// ---------------------------------------------------------------------------

use mz_persist::generated::consensus_service::{
    ProtoLogShardPredecessor, ProtoMetashardState, ProtoRangeAssignment, ProtoReconfigurationIntent,
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
        intent: state
            .pending_intent
            .as_ref()
            .map(|intent| ProtoReconfigurationIntent {
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
            }),
    };
    Bytes::from(proto.encode_to_vec())
}

/// Decode protobuf bytes into updates applied to a `MetaState`.
///
/// Restores the partition map, predecessor chains, and any pending
/// reconfiguration intent from the durable representation.
pub fn decode_meta_state(data: &[u8], state: &mut MetaState) -> Result<(), String> {
    let proto = ProtoMetashardState::decode(data)
        .map_err(|e| format!("failed to decode metashard proto: {e}"))?;

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
            .filter_map(parse_proto_range)
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
        proto.ranges.iter().filter_map(parse_proto_range).collect();
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

    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Result of the `Recover` command, providing the recovered state to callers.
#[derive(Debug, Clone)]
pub struct RecoverResult {
    pub epoch: u64,
    pub partition_map: PartitionMap,
}

/// Commands dispatched to the metashard actor.
pub enum MetaCommand {
    /// Recover durable state from the metashard persist shard, resume pending
    /// intents, and create actors for all shards. Injected as the first
    /// command after actor startup.
    Recover {
        reply: oneshot::Sender<Result<RecoverResult, MetaError>>,
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

    /// Recover durable state and create actors. Sent as the first command
    /// after actor startup.
    pub async fn recover(&self) -> Result<RecoverResult, MetaError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::Recover { reply: reply_tx })
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
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetaCommand::Reconfigure {
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
            reconfiguring: false,
            metashard_shard_id,
            metashard_write,
        };
        let handle = PersistMetaHandle::new(tx);
        (actor, handle)
    }

    /// Recover durable state from the metashard persist shard, resume any
    /// pending reconfiguration intent, and create actors for all shards in
    /// the partition map.
    async fn do_recover(&mut self) -> Result<RecoverResult, MetaError> {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        // Read persisted state (partition map, predecessors, pending intent).
        let (_, read) = self
            .persist_client
            .open::<OrderedKey, Proposal, u64, i64>(
                self.metashard_shard_id,
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
        let upper = self.metashard_write.upper().clone();
        'outer: loop {
            let events = subscribe.fetch_next().await;
            for event in &events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        if frontier.as_option().copied() >= upper.as_option().copied() {
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
            if let Err(e) = decode_meta_state(data.as_ref(), &mut self.state) {
                warn!("{e}, ignoring durable state");
            }
        }

        // Persist state so subscribers (routing task) can discover the partition map.
        self.persist_state().await;

        // Resume pending reconfiguration intent from a previous crash.
        if let Some(intent) = self.state.pending_intent.take() {
            info!(
                epoch = intent.epoch,
                status = ?intent.status,
                "found pending reconfiguration intent — resuming"
            );
            match intent.status {
                IntentStatus::Committed => {
                    info!(epoch = intent.epoch, "intent already committed, clearing");
                }
                IntentStatus::Preparing | IntentStatus::Sealed => {
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

        // Create actors for all shards in the partition map. The factory is
        // idempotent — if actors were already created during crash recovery
        // reconfiguration above, this returns cached handles.
        for range in self.state.partition_map.ranges.clone() {
            let shard_id = range.log_shard;
            // Pass predecessors from recovered state (not empty vec).
            let pred_specs: Vec<_> = self
                .state
                .log_shards
                .get(&shard_id)
                .map(|info| {
                    info.predecessors
                        .iter()
                        .map(|p| (*p, Antichain::from_elem(0u64)))
                        .collect()
                })
                .unwrap_or_default();
            if let Err(e) = self
                .factory
                .create_acceptor(shard_id, self.state.epoch, pred_specs, range)
                .await
            {
                tracing::error!(%shard_id, "failed to create acceptor at startup: {e}");
            }
            if let Err(e) = self.factory.create_learner(shard_id).await {
                tracing::error!(%shard_id, "failed to create learner at startup: {e}");
            }
        }
        info!(
            num_shards = self.state.partition_map.ranges.len(),
            "created actors for all shards in partition map"
        );

        Ok(RecoverResult {
            epoch: self.state.epoch,
            partition_map: self.state.partition_map.clone(),
        })
    }

    /// Persist the current metashard state to the durable shard.
    ///
    /// Used during recovery where fencing is not yet relevant. For
    /// reconfiguration, use `persist_state_value` directly to handle
    /// `Fenced` errors.
    async fn persist_state(&mut self) {
        if let Err(e) = self.persist_state_value(&self.state.clone()).await {
            tracing::error!("persist_state failed: {e}");
        }
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
    async fn persist_state_value(&mut self, state: &MetaState) -> Result<(), MetaError> {
        let write = &mut self.metashard_write;
        let data = encode_meta_state(state);

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
                debug!(epoch = state.epoch, "persisted metashard state");
                Ok(())
            }
            Ok(Err(_upper_mismatch)) => {
                warn!(
                    epoch = state.epoch,
                    "metashard CAS failed: another writer advanced the upper"
                );
                // Re-read durable state to learn the current epoch.
                self.refresh_from_durable_state().await;
                Err(MetaError::Fenced {
                    stale_epoch: state.epoch,
                    current_epoch: self.state.epoch,
                })
            }
            Err(e) => Err(MetaError::Command(format!("metashard persist error: {e}"))),
        }
    }

    /// Re-read the metashard persist shard and update `self.state` with the
    /// current durable state. Used after a CAS failure to learn what the
    /// winning writer committed.
    async fn refresh_from_durable_state(&mut self) {
        let key_schema = Arc::new(OrderedKeySchema);
        let val_schema = Arc::new(ProposalSchema);

        let (_, read) = self
            .persist_client
            .open::<OrderedKey, Proposal, u64, i64>(
                self.metashard_shard_id,
                key_schema,
                val_schema,
                Diagnostics::from_purpose("metashard-refresh"),
                false,
            )
            .await
            .expect("open metashard reader for refresh");

        let since = read.since().clone();
        let mut subscribe = read
            .subscribe(since)
            .await
            .expect("subscribe to metashard shard for refresh");

        let mut latest_data: Option<Bytes> = None;
        let upper = self.metashard_write.upper().clone();
        'outer: loop {
            let events = subscribe.fetch_next().await;
            for event in &events {
                match event {
                    ListenEvent::Progress(frontier) => {
                        if frontier.as_option().copied() >= upper.as_option().copied() {
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
            if let Err(e) = decode_meta_state(data.as_ref(), &mut self.state) {
                warn!("refresh_from_durable_state: {e}");
            }
        }
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
            MetaCommand::Reconfigure { .. } | MetaCommand::Recover { .. } => {
                unreachable!("Reconfigure and Recover handled separately in run loop")
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
    async fn do_reconfigure(&mut self, plan: ReconfigurationPlan) -> Result<u64, MetaError> {
        // Phase 0: Validate.
        if plan.expected_epoch != self.state.epoch {
            return Err(MetaError::EpochMismatch {
                expected: plan.expected_epoch,
                actual: self.state.epoch,
            });
        }
        plan.new_partition_map
            .validate()
            .map_err(|e| MetaError::Command(format!("invalid partition map: {e}")))?;

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
        // Build a new state with the intent set, persist it, then swap.
        let mut intent_state = self.state.clone();
        intent_state.pending_intent = Some(ReconfigurationIntent {
            epoch: new_epoch,
            plan: plan.clone(),
            status: IntentStatus::Preparing,
        });
        self.persist_state_value(&intent_state).await?;
        self.state = intent_state;

        // BUGGIFY: crash after intent is persisted but before seal.
        crate::fault::maybe_fail("after_intent_persist").map_err(MetaError::Command)?;

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
        // Phase 2: Spawn actors for new log shards BEFORE sealing.
        //
        // New learners subscribe to live (unsealed) predecessors and start
        // catching up in real-time. This pre-hydration means they're nearly
        // current by the time we seal, minimizing the unavailability window
        // to just the tail of writes between subscribe and seal.
        //
        // The factory caches handles internally. The Router's routing
        // task will pick up the new actors when it processes the updated
        // partition map from the metashard persist shard.
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

            let _ = self
                .factory
                .create_acceptor(shard_id, new_epoch, pred_specs, new_range.clone())
                .await
                .map_err(MetaError::Command)?;

            let _ = self
                .factory
                .create_learner(shard_id)
                .await
                .map_err(MetaError::Command)?;

            info!(
                %shard_id,
                range_lo = new_range.lo,
                range_hi = new_range.hi_exclusive,
                num_predecessors = predecessors.len(),
                "spawned acceptor + learner for new log shard"
            );
        }

        // BUGGIFY: crash after spawning new actors but before seal.
        // Actors are running and replaying live predecessors. On recovery,
        // fresh actors are spawned and replay restarts.
        crate::fault::maybe_fail("after_actor_spawn").map_err(MetaError::Command)?;

        // Phase 2.25: Wait for bulk snapshots to complete on all new shards.
        //
        // Each new acceptor writes its bulk snapshot at BULK_SNAPSHOT_BATCH_ID
        // and advances the shard upper to DELTA_SNAPSHOT_BATCH_ID when done.
        // We open a listen on each new shard and block until the frontier
        // advances past that point. This ensures all predecessor data at the
        // CriticalSince is captured before we seal, minimizing the window
        // where old shards accept writes after new shards are ready.
        {
            use crate::actors::acceptor::BULK_SNAPSHOT_BATCH_ID;
            let key_schema = Arc::new(OrderedKeySchema);
            let val_schema = Arc::new(ProposalSchema);
            for &shard_id in &added {
                let (_, read) = self
                    .persist_client
                    .open::<OrderedKey, Proposal, u64, i64>(
                        shard_id,
                        Arc::clone(&key_schema),
                        Arc::clone(&val_schema),
                        Diagnostics::from_purpose("metashard-bulk-snapshot-wait"),
                        false,
                    )
                    .await
                    .expect("open reader for bulk snapshot wait");
                let mut listen = read
                    .listen(Antichain::from_elem(BULK_SNAPSHOT_BATCH_ID))
                    .await
                    .expect("listen for bulk snapshot progress");
                // Blocks until the acceptor advances upper past BULK_SNAPSHOT_BATCH_ID.
                let _ = listen.fetch_next().await;
                info!(%shard_id, "bulk snapshot complete");
            }
        }

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
        }

        // BUGGIFY: crash after seal but before persist.
        crate::fault::maybe_fail("after_seal").map_err(MetaError::Command)?;

        // Phase 4: Build the final state atomically, persist, then swap.
        //
        // All state mutations (sealed shards, new shards, epoch, partition map,
        // cleared intent) are accumulated in a new MetaState value. self.state
        // is only updated after the persist succeeds, ensuring consistency
        // between durable and in-memory state.
        let mut new_state = self.state.clone();

        // Mark sealed shards.
        for &shard_id in &retiring {
            if let Some(info) = new_state.log_shards.get_mut(&shard_id) {
                info.status = LogShardStatus::Sealed;
                info.epoch_sealed = Some(new_epoch);
            }
        }

        // Add new log shards with predecessor chains.
        for range in &new_map.ranges {
            if added.contains(&range.log_shard) {
                let predecessors: Vec<ShardId> = old_map
                    .ranges
                    .iter()
                    .filter(|r| {
                        u16::from(range.lo) < r.hi_exclusive && u16::from(r.lo) < range.hi_exclusive
                    })
                    .map(|r| r.log_shard)
                    .collect();

                new_state.log_shards.insert(
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

        new_state.epoch = new_epoch;
        new_state.partition_map = PartitionMap {
            epoch: new_epoch,
            ranges: new_map.ranges.clone(),
        };
        new_state.pending_intent = None;

        // BUGGIFY: crash after building new state but before durable persist.
        // On recovery, the durable state still has the old epoch and intent,
        // but the old shards are sealed. do_reconfigure re-runs idempotently.
        crate::fault::maybe_fail("after_routing_swap").map_err(MetaError::Command)?;

        // Persist the new state, then swap into self.state.
        self.persist_state_value(&new_state).await?;
        self.state = new_state;

        // BUGGIFY: crash after commit persist but before hold release.
        // Holds leak but correctness is preserved — old shards just keep
        // their CriticalSince longer than necessary.
        crate::fault::maybe_fail("after_commit_persist").map_err(MetaError::Command)?;

        // BUGGIFY: crash before releasing CriticalSince holds. Holds leak
        // but correctness is preserved — old shards keep their since longer
        // than necessary. Next reconfiguration or restart can release them.
        crate::fault::maybe_fail("before_hold_release").map_err(MetaError::Command)?;

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

        // Stop retired shard processes (if the factory supports it).
        for &shard_id in &retiring {
            self.factory.stop_shard(shard_id).await;
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

        loop {
            match self.rx.recv().await {
                Some(MetaCommand::Recover { reply }) => {
                    let result = self.do_recover().await;
                    let _ = reply.send(result);
                }
                Some(MetaCommand::Reconfigure { plan, reply }) => {
                    if self.reconfiguring {
                        let _ = reply.send(Err(MetaError::ReconfigurationInProgress));
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
    ///
    /// Injects a `Recover` command as the first message, which reads durable
    /// state, resumes pending intents, and creates actors for all shards.
    /// Returns the recovered state so callers can log/use the partition map.
    pub async fn spawn(
        state: MetaState,
        queue_depth: usize,
        persist_client: PersistClient,
        factory: F,
        metashard_shard_id: ShardId,
    ) -> (
        PersistMetaHandle,
        RecoverResult,
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

        // Inject recovery as the first command.
        let result = handle.recover().await.expect("meta recovery failed");

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
            pending_intent: Some(intent),
        };

        // --- Persist the state ---
        let factory = crate::factory::InProcessActorFactory::new(client.clone());

        let (mut actor, _handle) =
            PersistMetaActor::new(state.clone(), 64, client.clone(), factory, metashard_shard)
                .await;

        // Force a persist (normally happens during do_reconfigure).
        actor.persist_state().await;
        drop(actor);
        drop(_handle);

        // --- Recover from the same shard ---
        let bootstrap = MetaState::single(test_shard("eee"));
        let factory2 = crate::factory::InProcessActorFactory::new(client.clone());

        let (mut recovered_actor, _handle2) =
            PersistMetaActor::new(bootstrap, 64, client.clone(), factory2, metashard_shard).await;

        // Trigger recovery (previously happened inside new()).
        recovered_actor.do_recover().await.expect("recovery failed");

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
