// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Router actor: routes client requests to the correct acceptor/learner based
//! on the partition map.
//!
//! Follows the same actor pattern as acceptor, learner, and meta: a passive
//! state machine driven by command channels, with a handle type for sending
//! commands. Uses two channels (like the learner's event source + command
//! channel pattern):
//!
//! - **`cmd_rx`**: client commands (Head, Scan, ListKeys, CAS, Truncate)
//! - **`routing_rx`**: routing snapshots from the routing task
//!
//! A biased `tokio::select!` ensures routing updates take priority, so parked
//! commands are retried promptly after reconfiguration.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLog;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoCompareAndSetResponse, ProtoHeadRequest, ProtoHeadResponse,
    ProtoListKeysRequest, ProtoListKeysResponse, ProtoLogProposal, ProtoMetashardState,
    ProtoScanRequest, ProtoScanResponse, ProtoTruncateRequest, ProtoTruncateResponse,
    ProtoVersionedData, proto_log_proposal,
};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};

use crate::actors::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::{Acceptor, Learner, PartitionMap, RangeAssignment};

// ---------------------------------------------------------------------------
// HandleResolver
// ---------------------------------------------------------------------------

/// Resolves shard IDs to acceptor and learner handles.
///
/// Used by the routing task to connect to existing actors discovered via the
/// partition map. Unlike `ActorFactory`, this does not create actors — it
/// finds handles to actors that already exist.
#[async_trait::async_trait]
pub trait HandleResolver: Send + Sync + 'static {
    type A: Acceptor;
    type L: Learner;

    /// Get a handle to the acceptor for the given shard.
    async fn resolve_acceptor(&self, shard_id: ShardId) -> Result<Self::A, String>;

    /// Get handles to all learner replicas for the given shard.
    async fn resolve_learners(&self, shard_id: ShardId) -> Result<Vec<Self::L>, String>;
}

// ---------------------------------------------------------------------------
// RoutingSnapshot
// ---------------------------------------------------------------------------

/// An immutable snapshot of routing state.
#[derive(Clone, Debug)]
pub struct RoutingSnapshot<A: Acceptor, L: Learner> {
    pub partition_map: PartitionMap,
    pub acceptors: Arc<BTreeMap<ShardId, A>>,
    pub learners: Arc<BTreeMap<ShardId, Vec<L>>>,
}

impl<A: Acceptor, L: Learner> RoutingSnapshot<A, L> {
    pub fn new(
        partition_map: PartitionMap,
        acceptors: BTreeMap<ShardId, A>,
        learners: BTreeMap<ShardId, Vec<L>>,
    ) -> Self {
        for range in &partition_map.ranges {
            assert!(
                acceptors.contains_key(&range.log_shard),
                "missing acceptor for log shard {}",
                range.log_shard
            );
            assert!(
                learners
                    .get(&range.log_shard)
                    .map_or(false, |v| !v.is_empty()),
                "missing learner(s) for log shard {}",
                range.log_shard
            );
        }
        RoutingSnapshot {
            partition_map,
            acceptors: Arc::new(acceptors),
            learners: Arc::new(learners),
        }
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands dispatched to the router actor from clients.
pub enum RouterCommand {
    Head {
        key: String,
        reply: oneshot::Sender<Result<ProtoHeadResponse, tonic::Status>>,
    },
    Scan {
        key: String,
        from: u64,
        limit: u64,
        reply: oneshot::Sender<Result<ProtoScanResponse, tonic::Status>>,
    },
    ListKeys {
        reply: oneshot::Sender<Result<Vec<String>, tonic::Status>>,
    },
    CompareAndSet {
        key: String,
        expected: Option<u64>,
        new: ProtoVersionedData,
        reply: oneshot::Sender<Result<ProtoCompareAndSetResponse, tonic::Status>>,
    },
    Truncate {
        key: String,
        seqno: u64,
        reply: oneshot::Sender<Result<ProtoTruncateResponse, tonic::Status>>,
    },
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// A typed handle to the router actor's command channel.
#[derive(Debug, Clone)]
pub struct RouterHandle {
    tx: mpsc::Sender<RouterCommand>,
}

impl RouterHandle {
    pub fn new(tx: mpsc::Sender<RouterCommand>) -> Self {
        RouterHandle { tx }
    }

    pub async fn head(&self, key: String) -> Result<ProtoHeadResponse, tonic::Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RouterCommand::Head {
                key,
                reply: reply_tx,
            })
            .await
            .map_err(|_| tonic::Status::unavailable("router shut down"))?;
        reply_rx
            .await
            .map_err(|_| tonic::Status::unavailable("router dropped reply"))?
    }

    pub async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
    ) -> Result<ProtoScanResponse, tonic::Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RouterCommand::Scan {
                key,
                from,
                limit,
                reply: reply_tx,
            })
            .await
            .map_err(|_| tonic::Status::unavailable("router shut down"))?;
        reply_rx
            .await
            .map_err(|_| tonic::Status::unavailable("router dropped reply"))?
    }

    pub async fn list_keys(&self) -> Result<Vec<String>, tonic::Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RouterCommand::ListKeys { reply: reply_tx })
            .await
            .map_err(|_| tonic::Status::unavailable("router shut down"))?;
        reply_rx
            .await
            .map_err(|_| tonic::Status::unavailable("router dropped reply"))?
    }

    pub async fn compare_and_set(
        &self,
        key: String,
        expected: Option<u64>,
        new: ProtoVersionedData,
    ) -> Result<ProtoCompareAndSetResponse, tonic::Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RouterCommand::CompareAndSet {
                key,
                expected,
                new,
                reply: reply_tx,
            })
            .await
            .map_err(|_| tonic::Status::unavailable("router shut down"))?;
        reply_rx
            .await
            .map_err(|_| tonic::Status::unavailable("router dropped reply"))?
    }

    pub async fn truncate(
        &self,
        key: String,
        seqno: u64,
    ) -> Result<ProtoTruncateResponse, tonic::Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(RouterCommand::Truncate {
                key,
                seqno,
                reply: reply_tx,
            })
            .await
            .map_err(|_| tonic::Status::unavailable("router shut down"))?;
        reply_rx
            .await
            .map_err(|_| tonic::Status::unavailable("router dropped reply"))?
    }
}

// ---------------------------------------------------------------------------
// gRPC service implementation on the handle
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl PersistSharedLog for RouterHandle {
    async fn head(
        &self,
        request: tonic::Request<ProtoHeadRequest>,
    ) -> Result<tonic::Response<ProtoHeadResponse>, tonic::Status> {
        let req = request.into_inner();
        let resp = RouterHandle::head(self, req.key).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        let resp = RouterHandle::scan(self, req.key, req.from, req.limit).await?;
        Ok(tonic::Response::new(resp))
    }

    type ListKeysStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoListKeysResponse, tonic::Status>>;

    async fn list_keys(
        &self,
        _request: tonic::Request<ProtoListKeysRequest>,
    ) -> Result<tonic::Response<Self::ListKeysStream>, tonic::Status> {
        let keys = RouterHandle::list_keys(self).await?;
        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel(64);
        mz_ore::task::spawn(|| "sharded-list-keys-stream", async move {
            for key in keys {
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
        let new = req
            .new
            .ok_or_else(|| tonic::Status::invalid_argument("missing new"))?;
        let resp = RouterHandle::compare_and_set(self, req.key, req.expected, new).await?;
        Ok(tonic::Response::new(resp))
    }

    async fn truncate(
        &self,
        request: tonic::Request<ProtoTruncateRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        let resp = RouterHandle::truncate(self, req.key, req.seqno).await?;
        Ok(tonic::Response::new(resp))
    }
}

// ---------------------------------------------------------------------------
// Router actor
// ---------------------------------------------------------------------------

/// Router actor: owns routing state and processes client commands.
///
/// Two input channels (biased select, routing updates take priority):
/// - `routing_rx`: routing snapshots from the routing task
/// - `cmd_rx`: client commands from the handle
///
/// Commands that arrive before routing is available, or that hit a sealed
/// acceptor, are parked in `pending` and retried on the next routing update.
pub struct Router<A: Acceptor, L: Learner> {
    cmd_rx: mpsc::Receiver<RouterCommand>,
    routing_rx: mpsc::Receiver<RoutingSnapshot<A, L>>,
    /// Commands that arrived before routing, retried on first routing update.
    pending: Vec<RouterCommand>,
    /// Commands parked due to sealed acceptors, retried on routing updates.
    retry_rx: mpsc::Receiver<RouterCommand>,
    retry_tx: mpsc::Sender<RouterCommand>,
    routing: Option<RoutingSnapshot<A, L>>,
    /// Round-robin counter for distributing reads across learner replicas.
    learner_counter: usize,
}

impl<A: Acceptor, L: Learner> Router<A, L> {
    /// Create a new router with no initial routing.
    ///
    /// Returns `(actor, handle, routing_tx)`. Pass `routing_tx` to
    /// [`spawn_routing_task`] so it can push partition map updates.
    pub fn new(queue_depth: usize) -> (Self, RouterHandle, mpsc::Sender<RoutingSnapshot<A, L>>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(queue_depth);
        let (routing_tx, routing_rx) = mpsc::channel(4);
        let (retry_tx, retry_rx) = mpsc::channel(queue_depth);
        let router = Router {
            cmd_rx,
            routing_rx,
            pending: Vec::new(),
            retry_rx,
            retry_tx,
            routing: None,
            learner_counter: 0,
        };
        let handle = RouterHandle::new(cmd_tx);
        (router, handle, routing_tx)
    }

    /// Create a router with pre-populated routing (for tests).
    pub fn with_routing(
        queue_depth: usize,
        snapshot: RoutingSnapshot<A, L>,
    ) -> (Self, RouterHandle, mpsc::Sender<RoutingSnapshot<A, L>>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(queue_depth);
        let (routing_tx, routing_rx) = mpsc::channel(4);
        let (retry_tx, retry_rx) = mpsc::channel(queue_depth);
        let router = Router {
            cmd_rx,
            routing_rx,
            pending: Vec::new(),
            retry_rx,
            retry_tx,
            routing: Some(snapshot),
            learner_counter: 0,
        };
        let handle = RouterHandle::new(cmd_tx);
        (router, handle, routing_tx)
    }

    /// Run the actor until all senders are dropped.
    pub async fn run(mut self) {
        loop {
            // TODO: This sleep is a workaround for tokio's paused-time tests
            // and turmoil sims. With paused time, persist's Subscribe needs
            // time to advance to deliver events; without a sleep here, parked
            // commands block indefinitely because the routing task's subscribe
            // never fires. In production (real time), this is unnecessary.
            if !self.pending.is_empty() {
                tokio::time::sleep(std::time::Duration::from_nanos(1)).await;
            }

            tokio::select! {
                biased;
                Some(snapshot) = self.routing_rx.recv() => {
                    self.on_routing_update(snapshot);
                }
                Some(cmd) = self.retry_rx.recv() => {
                    self.dispatch(cmd);
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    self.dispatch(cmd);
                }
                else => break,
            }
        }
    }

    fn on_routing_update(&mut self, snapshot: RoutingSnapshot<A, L>) {
        info!(
            epoch = snapshot.partition_map.epoch,
            num_ranges = snapshot.partition_map.ranges.len(),
            "router: applied routing update"
        );
        self.routing = Some(snapshot);

        // Retry all parked commands with the new routing.
        let pending = std::mem::take(&mut self.pending);
        for cmd in pending {
            self.dispatch(cmd);
        }
    }

    /// Route a command and spawn a task to execute it. Returns immediately.
    fn dispatch(&mut self, cmd: RouterCommand) {
        let routing = match &self.routing {
            Some(r) => r.clone(),
            None => {
                self.pending.push(cmd);
                return;
            }
        };

        match cmd {
            RouterCommand::Head { key, reply } => {
                let learner = self.pick_learner(&routing, &key);
                let retry_tx = self.retry_tx.clone();
                mz_ore::task::spawn(|| "router-head", async move {
                    match learner.head(key.clone()).await {
                        Ok(resp) => {
                            let _ = reply.send(Ok(resp));
                        }
                        Err(_) => {
                            debug!(%key, "learner failed, parking read for routing update");
                            let _ = retry_tx.send(RouterCommand::Head { key, reply }).await;
                        }
                    }
                });
            }
            RouterCommand::Scan {
                key,
                from,
                limit,
                reply,
            } => {
                let learner = self.pick_learner(&routing, &key);
                let retry_tx = self.retry_tx.clone();
                mz_ore::task::spawn(|| "router-scan", async move {
                    match learner.scan(key.clone(), from, limit).await {
                        Ok(resp) => {
                            let _ = reply.send(Ok(resp));
                        }
                        Err(_) => {
                            debug!(%key, "learner failed, parking scan for routing update");
                            let _ = retry_tx
                                .send(RouterCommand::Scan { key, from, limit, reply })
                                .await;
                        }
                    }
                });
            }
            RouterCommand::ListKeys { reply } => {
                let learners: Vec<L> = routing
                    .learners
                    .values()
                    .map(|replicas| replicas[0].clone())
                    .collect();
                mz_ore::task::spawn(|| "router-list-keys", async move {
                    let mut all_keys = std::collections::BTreeSet::new();
                    for learner in &learners {
                        match learner.list_keys().await {
                            Ok(keys) => all_keys.extend(keys),
                            Err(e) => {
                                let _ = reply.send(Err(tonic::Status::from(e)));
                                return;
                            }
                        }
                    }
                    let _ = reply.send(Ok(all_keys.into_iter().collect()));
                });
            }
            RouterCommand::CompareAndSet {
                key,
                expected,
                new,
                reply,
            } => {
                let acceptor = self.pick_acceptor(&routing, &key);
                let learner = self.pick_learner(&routing, &key);
                let retry_tx = self.retry_tx.clone();
                mz_ore::task::spawn(|| "router-cas", async move {
                    let proposal = ProtoLogProposal {
                        op: Some(proto_log_proposal::Op::Cas(
                            mz_persist::generated::consensus_service::ProtoCasProposal {
                                key: key.clone(),
                                expected,
                                new_seqno: new.seqno,
                                data: new.data.clone(),
                            },
                        )),
                    };
                    match acceptor.append(proposal).await {
                        Ok(receipt) => {
                            let result = learner
                                .await_cas_result(receipt.batch_number, receipt.position)
                                .await
                                .map_err(tonic::Status::from);
                            let _ = reply.send(result);
                        }
                        Err(
                            crate::AcceptorError::Sealed | crate::AcceptorError::Shutdown,
                        ) => {
                            debug!(
                                %key,
                                "acceptor sealed/shutdown, parking CAS for routing update"
                            );
                            let _ = retry_tx
                                .send(RouterCommand::CompareAndSet {
                                    key,
                                    expected,
                                    new,
                                    reply,
                                })
                                .await;
                        }
                        Err(e) => {
                            let _ = reply.send(Err(tonic::Status::from(e)));
                        }
                    }
                });
            }
            RouterCommand::Truncate { key, seqno, reply } => {
                let acceptor = self.pick_acceptor(&routing, &key);
                let learner = self.pick_learner(&routing, &key);
                let retry_tx = self.retry_tx.clone();
                mz_ore::task::spawn(|| "router-truncate", async move {
                    let proposal = ProtoLogProposal {
                        op: Some(proto_log_proposal::Op::Truncate(
                            mz_persist::generated::consensus_service::ProtoTruncateProposal {
                                key: key.clone(),
                                seqno,
                            },
                        )),
                    };
                    match acceptor.append(proposal).await {
                        Ok(receipt) => {
                            let result = learner
                                .await_truncate_result(receipt.batch_number, receipt.position)
                                .await
                                .map_err(tonic::Status::from);
                            let _ = reply.send(result);
                        }
                        Err(
                            crate::AcceptorError::Sealed | crate::AcceptorError::Shutdown,
                        ) => {
                            debug!(
                                %key,
                                "acceptor sealed/shutdown, parking truncate for routing update"
                            );
                            let _ = retry_tx
                                .send(RouterCommand::Truncate { key, seqno, reply })
                                .await;
                        }
                        Err(e) => {
                            let _ = reply.send(Err(tonic::Status::from(e)));
                        }
                    }
                });
            }
        }
    }

    fn pick_acceptor(&self, routing: &RoutingSnapshot<A, L>, key: &str) -> A {
        let log_shard = routing.partition_map.route(key);
        routing
            .acceptors
            .get(&log_shard)
            .expect("partition map routes to known log shard")
            .clone()
    }

    fn pick_learner(&mut self, routing: &RoutingSnapshot<A, L>, key: &str) -> L {
        let log_shard = routing.partition_map.route(key);
        let replicas = routing
            .learners
            .get(&log_shard)
            .expect("partition map routes to known log shard");
        let idx = self.learner_counter % replicas.len();
        self.learner_counter = self.learner_counter.wrapping_add(1);
        replicas[idx].clone()
    }
}

// ---------------------------------------------------------------------------
// Routing task
// ---------------------------------------------------------------------------

/// Spawn a background task that subscribes to the metashard persist shard and
/// pushes routing updates to the router actor.
///
/// This decouples the Router from the metashard actor — they communicate
/// only through the persist shard. The task uses a [`HandleResolver`] to
/// connect to existing actors discovered via the partition map.
pub async fn spawn_routing_task<R: HandleResolver>(
    persist_client: &PersistClient,
    metashard_shard_id: ShardId,
    resolver: R,
    routing_tx: mpsc::Sender<RoutingSnapshot<R::A, R::L>>,
) {
    // The metashard shard reuses the same (OrderedKey, Proposal) schema as log
    // shards. Its state is stored as a single key "__metashard" with a serialized
    // ProtoMetashardState as the proposal payload.
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
                if let Some(snapshot) = decode_and_build_snapshot(&data, &resolver).await {
                    let map = &snapshot.partition_map;
                    info!("routing task: new partition map\n{map}");
                    if routing_tx.send(snapshot).await.is_err() {
                        info!("routing task: router shut down, exiting");
                        return;
                    }
                }
            }
        }
    });
}

/// Decode a `ProtoMetashardState` and build a `RoutingSnapshot` using the
/// resolver to connect to existing actors.
async fn decode_and_build_snapshot<R: HandleResolver>(
    data: &[u8],
    resolver: &R,
) -> Option<RoutingSnapshot<R::A, R::L>> {
    use super::meta::parse_proto_range;

    let proto = match ProtoMetashardState::decode(data) {
        Ok(p) => p,
        Err(e) => {
            warn!("failed to decode metashard state: {e}");
            return None;
        }
    };

    let ranges: Vec<RangeAssignment> = proto.ranges.iter().filter_map(parse_proto_range).collect();

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

    for range in &ranges {
        let shard_id = range.log_shard;
        match resolver.resolve_acceptor(shard_id).await {
            Ok(a) => {
                acceptors.insert(shard_id, a);
            }
            Err(e) => {
                warn!(%shard_id, "failed to resolve acceptor: {e}");
                return None;
            }
        }
        match resolver.resolve_learners(shard_id).await {
            Ok(replicas) => {
                learners.insert(shard_id, replicas);
            }
            Err(e) => {
                warn!(%shard_id, "failed to resolve learners: {e}");
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
