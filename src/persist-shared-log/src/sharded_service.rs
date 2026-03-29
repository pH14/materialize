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
//! Replaces `PersistSharedLogGrpcService` for multi-shard deployments. For each
//! incoming request, the service extracts the client shard key, looks up the
//! owning log shard in the partition map, and routes to the corresponding
//! acceptor and learner.
//!
//! The routing state is held behind an `Arc<RwLock<...>>` so that the metashard
//! actor can atomically swap it during reconfiguration.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::debug;

use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLog;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoCompareAndSetResponse, ProtoHeadRequest, ProtoHeadResponse,
    ProtoListKeysRequest, ProtoListKeysResponse, ProtoLogProposal, ProtoScanRequest,
    ProtoScanResponse, ProtoTruncateRequest, ProtoTruncateResponse, proto_log_proposal,
};
use mz_persist_client::ShardId;

use crate::{Acceptor, Learner, PartitionMap};

// ---------------------------------------------------------------------------
// RoutingState
// ---------------------------------------------------------------------------

/// The routing state that can be atomically swapped during reconfiguration.
#[derive(Debug)]
pub struct RoutingState<A: Acceptor, L: Learner> {
    pub partition_map: PartitionMap,
    pub acceptors: BTreeMap<ShardId, A>,
    pub learners: BTreeMap<ShardId, L>,
}

impl<A: Acceptor, L: Learner> RoutingState<A, L> {
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
        RoutingState {
            partition_map,
            acceptors,
            learners,
        }
    }
}

// ---------------------------------------------------------------------------
// ShardedService
// ---------------------------------------------------------------------------

/// A sharded gRPC service that routes requests by partition key.
///
/// The routing state (partition map + acceptor/learner pools) is behind an
/// `Arc<RwLock<...>>` so that the metashard actor can atomically swap it
/// during reconfiguration without blocking in-flight requests.
pub struct ShardedService<A: Acceptor, L: Learner> {
    routing: Arc<RwLock<RoutingState<A, L>>>,
}

// Manual Debug impl to avoid requiring Debug on A, L.
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
        let routing = RoutingState::new(partition_map, acceptors, learners);
        ShardedService {
            routing: Arc::new(RwLock::new(routing)),
        }
    }

    /// Get a handle to the routing state for external updates (e.g., from the
    /// metashard actor during reconfiguration).
    pub fn routing_handle(&self) -> Arc<RwLock<RoutingState<A, L>>> {
        Arc::clone(&self.routing)
    }
}

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
        // Drop the read lock before streaming.
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

        // Retry loop: if the acceptor returns Sealed, refresh routing and retry
        // on the replacement shard. Bounded to 3 attempts.
        for attempt in 0..3u32 {
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
                Err(crate::AcceptorError::Sealed) => {
                    debug!(
                        key = %req.key,
                        attempt,
                        "acceptor sealed, refreshing routing and retrying"
                    );
                    // Brief yield to let the routing swap propagate.
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(e) => return Err(tonic::Status::from(e)),
            }
        }

        Err(tonic::Status::unavailable(
            "acceptor sealed after 3 retry attempts; reconfiguration may be in progress",
        ))
    }

    async fn truncate(
        &self,
        request: tonic::Request<ProtoTruncateRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, seqno = req.seqno, "sharded truncate");

        for attempt in 0..3u32 {
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
                Err(crate::AcceptorError::Sealed) => {
                    debug!(
                        key = %req.key,
                        attempt,
                        "acceptor sealed during truncate, refreshing routing"
                    );
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(e) => return Err(tonic::Status::from(e)),
            }
        }

        Err(tonic::Status::unavailable(
            "acceptor sealed after 3 retry attempts; reconfiguration may be in progress",
        ))
    }
}
