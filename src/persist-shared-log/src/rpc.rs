// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC remote handles and server adapters for multi-process actor communication.
//!
//! This module provides two sides of the transport abstraction:
//!
//! **Client handles** (`GrpcAcceptorHandle`, `GrpcLearnerHandle`): implement the
//! `Acceptor` and `Learner` traits by forwarding calls over gRPC to a remote
//! actor process. These can be plugged into `Router<GrpcAcceptorHandle,
//! GrpcLearnerHandle>` to create a stateless router that connects to remote
//! actors.
//!
//! **Server adapters** (`AcceptorGrpcServer`, `LearnerGrpcServer`): implement the
//! tonic-generated `ConsensusAcceptor` and `ConsensusLearner` gRPC server traits
//! by delegating to in-process actor handles. Each standalone actor binary wraps
//! its local `PersistAcceptorHandle`/`PersistLearnerHandle` in one of these and
//! serves it via tonic.
//!
//! Together, these enable the same actor code to run either in-process (via mpsc
//! channel handles) or as separate OS processes (via gRPC), without changing
//! `Router` or the metashard.
//!
//! See also: `persist-client/src/rpc.rs` for the Persist PubSub pattern that
//! inspired this design.

use bytes::Bytes;
use tokio_stream::StreamExt;
use tracing::debug;

use mz_persist::generated::consensus_service::consensus_acceptor_client::ConsensusAcceptorClient;
use mz_persist::generated::consensus_service::consensus_acceptor_server;
use mz_persist::generated::consensus_service::consensus_learner_client::ConsensusLearnerClient;
use mz_persist::generated::consensus_service::consensus_learner_server;
use mz_persist::generated::consensus_service::{
    ProtoAppendRequest, ProtoAppendResponse, ProtoAwaitResultRequest,
    ProtoCompareAndSetResponse, ProtoGetRetractionsRequest, ProtoHeadRequest,
    ProtoHeadResponse, ProtoListKeysRequest, ProtoListKeysResponse, ProtoLogProposal,
    ProtoScanRequest, ProtoScanResponse, ProtoTruncateResponse,
};

use crate::persist_log::acceptor::PersistAcceptorHandle;
use crate::persist_log::learner::PersistLearnerHandle;
use crate::persist_log::{OrderedKey, Proposal};
use crate::{Acceptor, AcceptorError, LearnerError, Metashard};

// ---------------------------------------------------------------------------
// Error conversions (tonic::Status -> domain errors)
// ---------------------------------------------------------------------------

fn status_to_acceptor_error(status: tonic::Status) -> AcceptorError {
    match status.code() {
        tonic::Code::Unavailable => AcceptorError::Shutdown,
        tonic::Code::FailedPrecondition => AcceptorError::Sealed,
        _ => AcceptorError::Command(status.message().to_string()),
    }
}

fn status_to_learner_error(status: tonic::Status) -> LearnerError {
    match status.code() {
        tonic::Code::Unavailable => LearnerError::Shutdown,
        _ => LearnerError::Command(status.message().to_string()),
    }
}

// ===========================================================================
// Client handles (gRPC client -> implements domain trait)
// ===========================================================================

// ---------------------------------------------------------------------------
// GrpcAcceptorHandle
// ---------------------------------------------------------------------------

/// A handle to a remote acceptor process, communicating over gRPC.
///
/// Implements the `Acceptor` trait so it can be used interchangeably with
/// the in-process `PersistAcceptorHandle` in `Router` and
/// `RoutingSnapshot`.
#[derive(Debug, Clone)]
pub struct GrpcAcceptorHandle {
    client: ConsensusAcceptorClient<tonic::transport::Channel>,
}

impl GrpcAcceptorHandle {
    /// Create a handle from an existing tonic channel.
    pub fn from_channel(channel: tonic::transport::Channel) -> Self {
        GrpcAcceptorHandle {
            client: ConsensusAcceptorClient::new(channel),
        }
    }

    /// Connect to a remote acceptor at the given address.
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = ConsensusAcceptorClient::connect(addr).await?;
        Ok(GrpcAcceptorHandle { client })
    }

    /// Connect to a remote acceptor over a Unix domain socket.
    pub async fn connect_unix(socket_path: &str) -> Result<Self, anyhow::Error> {
        let channel = crate::uds::connect_uds(socket_path).await?;
        Ok(GrpcAcceptorHandle {
            client: ConsensusAcceptorClient::new(channel),
        })
    }

    /// Connect to a remote acceptor, retrying until success or timeout.
    /// Auto-detects Unix sockets (paths starting with `/`) vs TCP URLs.
    pub async fn connect_with_retry(
        addr: String,
        timeout: std::time::Duration,
    ) -> Result<Self, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let result: Result<Self, String> = if addr.starts_with('/') {
                Self::connect_unix(&addr).await.map_err(|e| e.to_string())
            } else {
                Self::connect(addr.clone()).await.map_err(|e| e.to_string())
            };
            match result {
                Ok(handle) => return Ok(handle),
                Err(e) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(format!(
                            "failed to connect to acceptor at {} after {:?}: {}",
                            addr, timeout, e
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::Acceptor for GrpcAcceptorHandle {
    async fn append(
        &self,
        proposal: ProtoLogProposal,
    ) -> Result<ProtoAppendResponse, AcceptorError> {
        let request = ProtoAppendRequest {
            proposal: Some(proposal),
        };
        let response = self
            .client
            .clone()
            .append(request)
            .await
            .map_err(status_to_acceptor_error)?;
        Ok(response.into_inner())
    }
}

// ---------------------------------------------------------------------------
// GrpcLearnerHandle
// ---------------------------------------------------------------------------

/// A handle to a remote learner process, communicating over gRPC.
///
/// Implements the `Learner` trait so it can be used interchangeably with
/// the in-process `PersistLearnerHandle`.
#[derive(Debug, Clone)]
pub struct GrpcLearnerHandle {
    client: ConsensusLearnerClient<tonic::transport::Channel>,
}

impl GrpcLearnerHandle {
    /// Create a handle from an existing tonic channel.
    pub fn from_channel(channel: tonic::transport::Channel) -> Self {
        GrpcLearnerHandle {
            client: ConsensusLearnerClient::new(channel),
        }
    }

    /// Connect to a remote learner at the given address.
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = ConsensusLearnerClient::connect(addr).await?;
        Ok(GrpcLearnerHandle { client })
    }

    /// Connect to a remote learner over a Unix domain socket.
    pub async fn connect_unix(socket_path: &str) -> Result<Self, anyhow::Error> {
        let channel = crate::uds::connect_uds(socket_path).await?;
        Ok(GrpcLearnerHandle {
            client: ConsensusLearnerClient::new(channel),
        })
    }

    /// Connect to a remote learner, retrying until success or timeout.
    /// Auto-detects Unix sockets (paths starting with `/`) vs TCP URLs.
    pub async fn connect_with_retry(
        addr: String,
        timeout: std::time::Duration,
    ) -> Result<Self, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let result: Result<Self, String> = if addr.starts_with('/') {
                Self::connect_unix(&addr).await.map_err(|e| e.to_string())
            } else {
                Self::connect(addr.clone()).await.map_err(|e| e.to_string())
            };
            match result {
                Ok(handle) => return Ok(handle),
                Err(e) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(format!(
                            "failed to connect to learner at {} after {:?}: {}",
                            addr, timeout, e
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::Learner for GrpcLearnerHandle {
    async fn head(&self, key: String) -> Result<ProtoHeadResponse, LearnerError> {
        let request = ProtoHeadRequest { key };
        let response = self
            .client
            .clone()
            .head(request)
            .await
            .map_err(status_to_learner_error)?;
        Ok(response.into_inner())
    }

    async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
    ) -> Result<ProtoScanResponse, LearnerError> {
        let request = ProtoScanRequest { key, from, limit };
        let response = self
            .client
            .clone()
            .scan(request)
            .await
            .map_err(status_to_learner_error)?;
        Ok(response.into_inner())
    }

    async fn list_keys(&self) -> Result<Vec<String>, LearnerError> {
        let request = ProtoListKeysRequest {};
        let mut stream = self
            .client
            .clone()
            .list_keys(request)
            .await
            .map_err(status_to_learner_error)?
            .into_inner();

        let mut keys = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(resp) => keys.push(resp.key),
                Err(e) => return Err(status_to_learner_error(e)),
            }
        }
        Ok(keys)
    }

    async fn await_cas_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoCompareAndSetResponse, LearnerError> {
        let request = ProtoAwaitResultRequest {
            batch_number,
            position,
        };
        let response = self
            .client
            .clone()
            .await_cas_result(request)
            .await
            .map_err(status_to_learner_error)?;
        Ok(response.into_inner())
    }

    async fn await_truncate_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoTruncateResponse, LearnerError> {
        let request = ProtoAwaitResultRequest {
            batch_number,
            position,
        };
        let response = self
            .client
            .clone()
            .await_truncate_result(request)
            .await
            .map_err(status_to_learner_error)?;
        Ok(response.into_inner())
    }
}

// ---------------------------------------------------------------------------
// GrpcRetractionSource
// ---------------------------------------------------------------------------

/// A `RetractionSource` that fetches retractions from a remote learner via gRPC.
///
/// Used by acceptor processes in multi-process mode: the acceptor needs to poll
/// the learner for pending retractions, but the learner runs in a different
/// process.
pub struct GrpcRetractionSource {
    client: ConsensusLearnerClient<tonic::transport::Channel>,
}

impl GrpcRetractionSource {
    pub fn new(client: ConsensusLearnerClient<tonic::transport::Channel>) -> Self {
        GrpcRetractionSource { client }
    }

    pub async fn from_addr(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = ConsensusLearnerClient::connect(addr).await?;
        Ok(GrpcRetractionSource { client })
    }
}

#[async_trait::async_trait]
impl crate::RetractionSource for GrpcRetractionSource {
    async fn get_retractions(
        &self,
        frontier: u64,
    ) -> Vec<(OrderedKey, Proposal)> {
        let request = ProtoGetRetractionsRequest { through_upper: frontier };
        match self.client.clone().get_retractions(request).await {
            Ok(response) => {
                let proto = response.into_inner();
                proto
                    .entries
                    .into_iter()
                    .map(|e| {
                        let key = OrderedKey {
                            batch_id: e.batch_id,
                            position: e.position,
                            shard: e.shard,
                        };
                        let proposal = Proposal {
                            encoded: Bytes::from(e.proposal),
                        };
                        (key, proposal)
                    })
                    .collect()
            }
            Err(e) => {
                debug!("gRPC get_retractions failed: {}", e);
                Vec::new()
            }
        }
    }
}

// ===========================================================================
// Server adapters (in-process handle -> gRPC server trait)
// ===========================================================================

// ---------------------------------------------------------------------------
// AcceptorGrpcServer
// ---------------------------------------------------------------------------

/// Server-side adapter: implements the `ConsensusAcceptor` gRPC service trait
/// by delegating to an in-process `PersistAcceptorHandle`.
///
/// Used by standalone acceptor binaries: the `PersistAcceptor` runs as a local
/// tokio task, and this adapter exposes it over gRPC.
pub struct AcceptorGrpcServer {
    handle: PersistAcceptorHandle,
}

impl AcceptorGrpcServer {
    pub fn new(handle: PersistAcceptorHandle) -> Self {
        AcceptorGrpcServer { handle }
    }
}

#[tonic::async_trait]
impl consensus_acceptor_server::ConsensusAcceptor for AcceptorGrpcServer {
    async fn append(
        &self,
        request: tonic::Request<ProtoAppendRequest>,
    ) -> Result<tonic::Response<ProtoAppendResponse>, tonic::Status> {
        let proposal = request
            .into_inner()
            .proposal
            .ok_or_else(|| tonic::Status::invalid_argument("missing proposal"))?;
        let response = self
            .handle
            .append(proposal)
            .await
            .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(response))
    }
}

// ---------------------------------------------------------------------------
// LearnerGrpcServer
// ---------------------------------------------------------------------------

/// Server-side adapter: implements the `ConsensusLearner` gRPC service trait
/// by delegating to an in-process `PersistLearnerHandle`.
pub struct LearnerGrpcServer {
    handle: PersistLearnerHandle,
}

impl LearnerGrpcServer {
    pub fn new(handle: PersistLearnerHandle) -> Self {
        LearnerGrpcServer { handle }
    }
}

#[tonic::async_trait]
impl consensus_learner_server::ConsensusLearner for LearnerGrpcServer {
    async fn await_cas_result(
        &self,
        request: tonic::Request<ProtoAwaitResultRequest>,
    ) -> Result<tonic::Response<ProtoCompareAndSetResponse>, tonic::Status> {
        let req = request.into_inner();
        let response = self
            .handle
            .await_cas_result(req.batch_number, req.position)
            .await
            .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(response))
    }

    async fn await_truncate_result(
        &self,
        request: tonic::Request<ProtoAwaitResultRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        let response = self
            .handle
            .await_truncate_result(req.batch_number, req.position)
            .await
            .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(response))
    }

    async fn head(
        &self,
        request: tonic::Request<ProtoHeadRequest>,
    ) -> Result<tonic::Response<ProtoHeadResponse>, tonic::Status> {
        let req = request.into_inner();
        let response = self.handle.head(req.key).await.map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(response))
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        let response = self
            .handle
            .scan(req.key, req.from, req.limit)
            .await
            .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(response))
    }

    type ListKeysStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoListKeysResponse, tonic::Status>>;

    async fn list_keys(
        &self,
        _request: tonic::Request<ProtoListKeysRequest>,
    ) -> Result<tonic::Response<Self::ListKeysStream>, tonic::Status> {
        let keys = self.handle.list_keys().await.map_err(tonic::Status::from)?;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        mz_ore::task::spawn(|| "learner-grpc-list-keys-stream", async move {
            for key in keys {
                if tx.send(Ok(ProtoListKeysResponse { key })).await.is_err() {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }

    async fn get_retractions(
        &self,
        request: tonic::Request<ProtoGetRetractionsRequest>,
    ) -> Result<
        tonic::Response<mz_persist::generated::consensus_service::ProtoGetRetractionsResponse>,
        tonic::Status,
    > {
        use mz_persist::generated::consensus_service::{
            ProtoGetRetractionsResponse, ProtoRetractionEntry,
        };

        let req = request.into_inner();
        let retractions = self
            .handle
            .get_retractions(req.through_upper)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let entries: Vec<ProtoRetractionEntry> = retractions
            .into_iter()
            .map(|(key, proposal)| ProtoRetractionEntry {
                batch_id: key.batch_id,
                position: key.position,
                shard: key.shard,
                proposal: proposal.encoded.to_vec(),
            })
            .collect();

        Ok(tonic::Response::new(ProtoGetRetractionsResponse { entries }))
    }
}

// ---------------------------------------------------------------------------
// MetashardGrpcServer
// ---------------------------------------------------------------------------

/// Server-side adapter: implements the `ConsensusMetashard` gRPC service trait
/// by delegating to an in-process `PersistMetashardHandle`.
///
/// Used by standalone metashard binaries so operators can `grpcurl` the
/// partition map to discover shard IDs before starting acceptors/learners.
pub struct MetashardGrpcServer {
    handle: crate::persist_log::metashard::PersistMetashardHandle,
}

impl MetashardGrpcServer {
    pub fn new(handle: crate::persist_log::metashard::PersistMetashardHandle) -> Self {
        MetashardGrpcServer { handle }
    }
}

#[tonic::async_trait]
impl mz_persist::generated::consensus_service::consensus_metashard_server::ConsensusMetashard
    for MetashardGrpcServer
{
    async fn get_partition_map(
        &self,
        _request: tonic::Request<
            mz_persist::generated::consensus_service::ProtoGetPartitionMapRequest,
        >,
    ) -> Result<
        tonic::Response<mz_persist::generated::consensus_service::ProtoGetPartitionMapResponse>,
        tonic::Status,
    > {
        use mz_persist::generated::consensus_service::{
            ProtoGetPartitionMapResponse, ProtoMetashardState, ProtoRangeAssignment,
        };

        let partition_map = self
            .handle
            .partition_map()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let epoch = self
            .handle
            .current_epoch()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let ranges = partition_map
            .ranges
            .iter()
            .map(|r| ProtoRangeAssignment {
                lo: u32::from(r.lo),
                hi_exclusive: u32::from(r.hi_exclusive),
                log_shard: r.log_shard.to_string(),
            })
            .collect();

        let state = ProtoMetashardState {
            epoch,
            ranges,
            predecessors: vec![],
            intent: None,
        };

        Ok(tonic::Response::new(ProtoGetPartitionMapResponse {
            state: Some(state),
        }))
    }
}

// ---------------------------------------------------------------------------
// Convenience re-exports for standalone binaries
// ---------------------------------------------------------------------------

pub use mz_persist::generated::consensus_service::consensus_acceptor_server::ConsensusAcceptorServer;
pub use mz_persist::generated::consensus_service::consensus_learner_server::ConsensusLearnerServer;
pub use mz_persist::generated::consensus_service::consensus_metashard_server::ConsensusMetashardServer;
