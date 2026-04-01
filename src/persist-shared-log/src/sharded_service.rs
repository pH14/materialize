// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sharded gRPC service: routes client requests to the correct acceptor/learner
//! based on the partition map.
//!
//! For each incoming request, the service extracts the client shard key, looks
//! up the owning log shard in the partition map, and routes to the corresponding
//! acceptor and learner.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLog;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoCompareAndSetResponse, ProtoHeadRequest, ProtoHeadResponse,
    ProtoListKeysRequest, ProtoListKeysResponse, ProtoLogProposal, ProtoMetashardState,
    ProtoReconfigureRequest, ProtoReconfigureResponse, ProtoScanRequest, ProtoScanResponse,
    ProtoTruncateRequest, ProtoTruncateResponse, proto_log_proposal,
};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};

use crate::factory::ActorFactory;
use crate::persist_log::metashard::PersistMetashardHandle;
use crate::persist_log::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::{Acceptor, Learner, Metashard, PartitionMap, RangeAssignment, ReconfigurationPlan};

// ---------------------------------------------------------------------------
// ShardedRetractionSource
// ---------------------------------------------------------------------------

/// Implements [`RetractionSource`] by fanning out to learner replicas and
/// returning the first response.
pub struct ShardedRetractionSource {
    learners: Vec<crate::persist_log::learner::PersistLearnerHandle>,
}

impl ShardedRetractionSource {
    pub fn new(learners: Vec<crate::persist_log::learner::PersistLearnerHandle>) -> Self {
        ShardedRetractionSource { learners }
    }
}

#[async_trait::async_trait]
impl crate::RetractionSource for ShardedRetractionSource {
    async fn get_retractions(
        &self,
        frontier: u64,
    ) -> Vec<(OrderedKey, Proposal)> {
        for learner in &self.learners {
            match learner.get_retractions(frontier).await {
                Ok(retractions) => return retractions,
                Err(_) => continue,
            }
        }
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// RoutingSnapshot
// ---------------------------------------------------------------------------

/// An immutable snapshot of routing state.
#[derive(Clone, Debug)]
pub struct RoutingSnapshot<A: Acceptor, L: Learner> {
    pub partition_map: PartitionMap,
    pub acceptors: Arc<BTreeMap<ShardId, A>>,
    pub learners: Arc<BTreeMap<ShardId, L>>,
}

impl<A: Acceptor, L: Learner> RoutingSnapshot<A, L> {
    pub fn new(
        partition_map: PartitionMap,
        acceptors: BTreeMap<ShardId, A>,
        learners: BTreeMap<ShardId, L>,
    ) -> Self {
        for range in &partition_map.ranges {
            assert!(
                acceptors.contains_key(&range.log_shard),
                "missing acceptor for log shard {}",
                range.log_shard
            );
            assert!(
                learners.contains_key(&range.log_shard),
                "missing learner for log shard {}",
                range.log_shard
            );
        }
        RoutingSnapshot {
            partition_map,
            acceptors: Arc::new(acceptors),
            learners: Arc::new(learners),
        }
    }

    pub fn empty() -> Self {
        RoutingSnapshot {
            partition_map: PartitionMap { epoch: 0, ranges: vec![] },
            acceptors: Arc::new(BTreeMap::new()),
            learners: Arc::new(BTreeMap::new()),
        }
    }
}

/// Backward-compat alias.
pub type RoutingState<A, L> = RoutingSnapshot<A, L>;

// ---------------------------------------------------------------------------
// ShardedService
// ---------------------------------------------------------------------------

/// A sharded gRPC service that routes requests by partition key.
pub struct ShardedService<A: Acceptor, L: Learner> {
    routing: Arc<RwLock<RoutingSnapshot<A, L>>>,
    /// Signaled when routing changes (e.g., after reconfiguration).
    routing_notify: Arc<tokio::sync::Notify>,
    metashard: Option<PersistMetashardHandle>,
}

impl<A: Acceptor, L: Learner> std::fmt::Debug for ShardedService<A, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedService").finish_non_exhaustive()
    }
}

impl<A: Acceptor, L: Learner> ShardedService<A, L> {
    pub fn new(
        partition_map: PartitionMap,
        acceptors: BTreeMap<ShardId, A>,
        learners: BTreeMap<ShardId, L>,
    ) -> Self {
        let snapshot = RoutingSnapshot::new(partition_map, acceptors, learners);
        ShardedService {
            routing: Arc::new(RwLock::new(snapshot)),
            routing_notify: Arc::new(tokio::sync::Notify::new()),
            metashard: None,
        }
    }

    pub fn from_routing(routing: Arc<RwLock<RoutingSnapshot<A, L>>>) -> Self {
        ShardedService {
            routing,
            routing_notify: Arc::new(tokio::sync::Notify::new()),
            metashard: None,
        }
    }

    pub fn routing_handle(&self) -> Arc<RwLock<RoutingSnapshot<A, L>>> {
        Arc::clone(&self.routing)
    }

    pub fn routing_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.routing_notify)
    }

    pub fn with_metashard(mut self, handle: PersistMetashardHandle) -> Self {
        self.metashard = Some(handle);
        self
    }
}

/// Spawn a background task that subscribes to the metashard persist shard and
/// updates the ShardedService's routing state when the partition map changes.
///
/// This decouples the ShardedService from the metashard actor — they communicate
/// only through the persist shard. The task uses the `ActorFactory` to create
/// handles for new shards (idempotent — returns cached handles if already created).
pub async fn spawn_routing_task<F: ActorFactory>(
    persist_client: &PersistClient,
    metashard_shard_id: ShardId,
    factory: F,
    routing: Arc<RwLock<RoutingSnapshot<F::A, F::L>>>,
    routing_notify: Arc<tokio::sync::Notify>,
) {
    let key_schema = Arc::new(OrderedKeySchema);
    let val_schema = Arc::new(ProposalSchema);

    let (_, read) = persist_client
        .open::<OrderedKey, Proposal, u64, i64>(
            metashard_shard_id,
            key_schema,
            val_schema,
            Diagnostics::from_purpose("routing-task-subscribe"),
            false,
        )
        .await
        .expect("open metashard shard for routing subscription");

    let since = read.since().clone();
    let subscribe = read
        .subscribe(since)
        .await
        .expect("subscribe to metashard shard");

    // The subscribe is handed to the background task which processes all
    // events (initial catchup + ongoing updates). The ShardedService starts
    // with empty routing and updates when the metashard writes its state.
    mz_ore::task::spawn(|| "routing-task", async move {
        let mut subscribe = subscribe;
        loop {
            let events = subscribe.fetch_next().await;
            let mut new_data: Option<Bytes> = None;
            for event in events {
                match event {
                    ListenEvent::Progress(_) => {}
                    ListenEvent::Updates(updates) => {
                        for ((key, proposal), _ts, diff) in updates {
                            if diff == 1 && key.shard == "__metashard" {
                                new_data = Some(proposal.encoded.clone());
                            }
                        }
                    }
                }
            }
            if let Some(data) = new_data {
                if let Some(snapshot) = decode_and_build_snapshot(&data, &factory).await {
                    let epoch = snapshot.partition_map.epoch;
                    *routing.write().await = snapshot;
                    routing_notify.notify_waiters();
                    info!(epoch, "routing task: applied partition map update");
                }
            }
        }
    });
}

/// Decode a `ProtoMetashardState` and build a `RoutingSnapshot` using the factory.
async fn decode_and_build_snapshot<F: ActorFactory>(
    data: &[u8],
    factory: &F,
) -> Option<RoutingSnapshot<F::A, F::L>> {
    let proto = match ProtoMetashardState::decode(data) {
        Ok(p) => p,
        Err(e) => {
            warn!("failed to decode metashard state: {e}");
            return None;
        }
    };

    let ranges: Vec<RangeAssignment> = proto
        .ranges
        .iter()
        .filter_map(|r| {
            Some(RangeAssignment {
                lo: u8::try_from(r.lo).ok()?,
                hi_exclusive: u16::try_from(r.hi_exclusive).ok()?,
                log_shard: r.log_shard.parse().ok()?,
            })
        })
        .collect();

    if ranges.is_empty() {
        return None;
    }

    let partition_map = PartitionMap {
        epoch: proto.epoch,
        ranges: ranges.clone(),
    };
    if partition_map.validate().is_err() {
        warn!("decoded invalid partition map, ignoring");
        return None;
    }

    let mut acceptors = BTreeMap::new();
    let mut learners = BTreeMap::new();

    // Create acceptors first (setup batches advance upper), then learners.
    for range in &ranges {
        let shard_id = range.log_shard;
        match factory.create_acceptor(shard_id, proto.epoch, vec![], range.clone()).await {
            Ok(a) => { acceptors.insert(shard_id, a); }
            Err(e) => {
                warn!(%shard_id, "failed to create acceptor: {e}");
                return None;
            }
        }
    }
    for range in &ranges {
        let shard_id = range.log_shard;
        match factory.create_learner(shard_id).await {
            Ok(l) => { learners.insert(shard_id, l); }
            Err(e) => {
                warn!(%shard_id, "failed to create learner: {e}");
                return None;
            }
        }
    }

    Some(RoutingSnapshot {
        partition_map,
        acceptors: Arc::new(acceptors),
        learners: Arc::new(learners),
    })
}

// ---------------------------------------------------------------------------
// PersistSharedLog gRPC implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl<A: Acceptor, L: Learner> PersistSharedLog for ShardedService<A, L> {
    async fn head(
        &self,
        request: tonic::Request<ProtoHeadRequest>,
    ) -> Result<tonic::Response<ProtoHeadResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, "sharded head");
        let routing = self.routing.read().await;
        let log_shard = routing.partition_map.route(&req.key);
        let learner = routing
            .learners
            .get(&log_shard)
            .expect("partition map routes to known log shard");
        let resp = learner.head(req.key).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, from = req.from, limit = req.limit, "sharded scan");
        let routing = self.routing.read().await;
        let log_shard = routing.partition_map.route(&req.key);
        let learner = routing
            .learners
            .get(&log_shard)
            .expect("partition map routes to known log shard");
        let resp = learner.scan(req.key, req.from, req.limit).await?;
        Ok(tonic::Response::new(resp))
    }

    type ListKeysStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoListKeysResponse, tonic::Status>>;

    async fn list_keys(
        &self,
        _request: tonic::Request<ProtoListKeysRequest>,
    ) -> Result<tonic::Response<Self::ListKeysStream>, tonic::Status> {
        debug!("sharded list_keys");
        let routing = self.routing.read().await;
        let mut all_keys = std::collections::BTreeSet::new();
        for learner in routing.learners.values() {
            match learner.list_keys().await {
                Ok(keys) => {
                    all_keys.extend(keys);
                }
                Err(e) => return Err(tonic::Status::from(e)),
            }
        }
        drop(routing);

        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel(64);
        mz_ore::task::spawn(|| "sharded-list-keys-stream", async move {
            for key in all_keys {
                if stream_tx
                    .send(Ok(ProtoListKeysResponse { key }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(stream_rx),
        ))
    }

    async fn compare_and_set(
        &self,
        request: tonic::Request<ProtoCompareAndSetRequest>,
    ) -> Result<tonic::Response<ProtoCompareAndSetResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, "sharded compare_and_set");

        let new = req
            .new
            .ok_or_else(|| tonic::Status::invalid_argument("missing new"))?;

        loop {
            let proposal = ProtoLogProposal {
                op: Some(proto_log_proposal::Op::Cas(
                    mz_persist::generated::consensus_service::ProtoCasProposal {
                        key: req.key.clone(),
                        expected: req.expected,
                        new_seqno: new.seqno,
                        data: new.data.clone(),
                    },
                )),
            };

            let (acceptor, learner) = {
                let routing = self.routing.read().await;
                let log_shard = routing.partition_map.route(&req.key);
                let a = routing
                    .acceptors
                    .get(&log_shard)
                    .expect("partition map routes to known log shard")
                    .clone();
                let l = routing
                    .learners
                    .get(&log_shard)
                    .expect("partition map routes to known log shard")
                    .clone();
                (a, l)
            };

            match acceptor.append(proposal).await {
                Ok(receipt) => {
                    let result = learner
                        .await_cas_result(receipt.batch_number, receipt.position)
                        .await?;
                    return Ok(tonic::Response::new(result));
                }
                Err(crate::AcceptorError::Sealed | crate::AcceptorError::Shutdown) => {
                    debug!(
                        key = %req.key,
                        "acceptor sealed/shutdown, waiting for routing update"
                    );
                    // Wait for a routing update notification, with timeout.
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        self.routing_notify.notified(),
                    )
                    .await
                    {
                        Ok(()) => continue,
                        Err(_) => {
                            return Err(tonic::Status::unavailable(
                                "acceptor sealed and no routing update received within timeout",
                            ));
                        }
                    }
                }
                Err(e) => return Err(tonic::Status::from(e)),
            }
        }
    }

    async fn truncate(
        &self,
        request: tonic::Request<ProtoTruncateRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, seqno = req.seqno, "sharded truncate");

        loop {
            let proposal = ProtoLogProposal {
                op: Some(proto_log_proposal::Op::Truncate(
                    mz_persist::generated::consensus_service::ProtoTruncateProposal {
                        key: req.key.clone(),
                        seqno: req.seqno,
                    },
                )),
            };

            let (acceptor, learner) = {
                let routing = self.routing.read().await;
                let log_shard = routing.partition_map.route(&req.key);
                let a = routing
                    .acceptors
                    .get(&log_shard)
                    .expect("partition map routes to known log shard")
                    .clone();
                let l = routing
                    .learners
                    .get(&log_shard)
                    .expect("partition map routes to known log shard")
                    .clone();
                (a, l)
            };

            match acceptor.append(proposal).await {
                Ok(receipt) => {
                    let result = learner
                        .await_truncate_result(receipt.batch_number, receipt.position)
                        .await?;
                    return Ok(tonic::Response::new(result));
                }
                Err(crate::AcceptorError::Sealed | crate::AcceptorError::Shutdown) => {
                    debug!(
                        key = %req.key,
                        "acceptor sealed/shutdown during truncate, waiting for routing update"
                    );
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        self.routing_notify.notified(),
                    )
                    .await
                    {
                        Ok(()) => continue,
                        Err(_) => {
                            return Err(tonic::Status::unavailable(
                                "acceptor sealed and no routing update received within timeout",
                            ));
                        }
                    }
                }
                Err(e) => return Err(tonic::Status::from(e)),
            }
        }
    }

    async fn reconfigure(
        &self,
        request: tonic::Request<ProtoReconfigureRequest>,
    ) -> Result<tonic::Response<ProtoReconfigureResponse>, tonic::Status> {
        let req = request.into_inner();
        let num_shards = usize::try_from(req.num_shards).expect("num_shards fits usize");
        if num_shards == 0 {
            return Err(tonic::Status::invalid_argument(
                "num_shards must be at least 1",
            ));
        }

        let metashard = self
            .metashard
            .as_ref()
            .ok_or_else(|| {
                tonic::Status::unimplemented("reconfiguration not available (no metashard)")
            })?;

        let current_epoch = metashard
            .current_epoch()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let range_size = 256 / num_shards;
        let mut ranges = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let lo = u8::try_from(i * range_size).expect("range start fits u8");
            let hi_exclusive = if i == num_shards - 1 {
                0x100u16
            } else {
                u16::try_from((i + 1) * range_size).expect("range end fits u16")
            };
            ranges.push(RangeAssignment {
                lo,
                hi_exclusive,
                log_shard: ShardId::new(),
            });
        }

        let new_map = PartitionMap {
            epoch: current_epoch + 1,
            ranges,
        };

        info!(
            current_epoch,
            num_shards,
            "Reconfigure RPC: splitting to {} shards",
            num_shards
        );

        let new_epoch = metashard
            .reconfigure(ReconfigurationPlan {
                expected_epoch: current_epoch,
                new_partition_map: new_map,
            })
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(tonic::Response::new(ProtoReconfigureResponse {
            new_epoch,
            num_shards: u32::try_from(num_shards).expect("num_shards fits u32"),
        }))
    }
}
