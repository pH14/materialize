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

use std::collections::BTreeMap;

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
// ShardedService
// ---------------------------------------------------------------------------

/// A sharded gRPC service that routes requests by partition key.
///
/// Holds one acceptor and one learner per log shard. The `partition_map`
/// determines routing. In Phase 1 (static sharding), the map is fixed at
/// construction. Phase 4 will add dynamic updates via metashard subscription.
#[derive(Debug)]
pub struct ShardedService<A: Acceptor, L: Learner> {
    partition_map: PartitionMap,
    acceptors: BTreeMap<ShardId, A>,
    learners: BTreeMap<ShardId, L>,
}

impl<A: Acceptor, L: Learner> ShardedService<A, L> {
    pub fn new(
        partition_map: PartitionMap,
        acceptors: BTreeMap<ShardId, A>,
        learners: BTreeMap<ShardId, L>,
    ) -> Self {
        // Validate that we have an acceptor and learner for every log shard in the map.
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
        ShardedService {
            partition_map,
            acceptors,
            learners,
        }
    }

    fn route_acceptor(&self, client_shard: &str) -> &A {
        let log_shard = self.partition_map.route(client_shard);
        self.acceptors
            .get(&log_shard)
            .expect("partition map routes to known log shard")
    }

    fn route_learner(&self, client_shard: &str) -> &L {
        let log_shard = self.partition_map.route(client_shard);
        self.learners
            .get(&log_shard)
            .expect("partition map routes to known log shard")
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
        let learner = self.route_learner(&req.key);
        let resp = learner.head(req.key).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, from = req.from, limit = req.limit, "sharded scan");
        let learner = self.route_learner(&req.key);
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
        // Fan out to all learners, merge results.
        let mut all_keys = std::collections::BTreeSet::new();
        for learner in self.learners.values() {
            match learner.list_keys().await {
                Ok(keys) => {
                    all_keys.extend(keys);
                }
                Err(e) => return Err(tonic::Status::from(e)),
            }
        }

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

        let proposal = ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Cas(
                mz_persist::generated::consensus_service::ProtoCasProposal {
                    key: req.key.clone(),
                    expected: req.expected,
                    new_seqno: new.seqno,
                    data: new.data,
                },
            )),
        };

        let acceptor = self.route_acceptor(&req.key);
        let receipt = acceptor.append(proposal).await?;

        let learner = self.route_learner(&req.key);
        let result = learner
            .await_cas_result(receipt.batch_number, receipt.position)
            .await?;

        Ok(tonic::Response::new(result))
    }

    async fn truncate(
        &self,
        request: tonic::Request<ProtoTruncateRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, seqno = req.seqno, "sharded truncate");

        let proposal = ProtoLogProposal {
            op: Some(proto_log_proposal::Op::Truncate(
                mz_persist::generated::consensus_service::ProtoTruncateProposal {
                    key: req.key.clone(),
                    seqno: req.seqno,
                },
            )),
        };

        let acceptor = self.route_acceptor(&req.key);
        let receipt = acceptor.append(proposal).await?;

        let learner = self.route_learner(&req.key);
        let result = learner
            .await_truncate_result(receipt.batch_number, receipt.position)
            .await?;

        Ok(tonic::Response::new(result))
    }
}
