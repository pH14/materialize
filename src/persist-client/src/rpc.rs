// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! gRPC-based implementations of Persist PubSub client and server.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime};

use futures::Stream;
use prost::Message;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream, TcpListenerStream};
use tokio_stream::StreamExt;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::transport::Endpoint;
use tonic::{Extensions, Request, Response, Status, Streaming};
use tracing::{debug, error, info, info_span, warn};

use mz_ore::cast::CastFrom;
use mz_ore::collections::{HashMap, HashSet};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::retry::RetryResult;
use mz_persist::location::VersionedData;
use mz_proto::{ProtoType, RustType};

use crate::cache::StateCache;
use crate::internal::metrics::PubSubServerMetrics;
use crate::internal::service::proto_persist_pub_sub_client::ProtoPersistPubSubClient;
use crate::internal::service::proto_persist_pub_sub_server::ProtoPersistPubSubServer;
use crate::internal::service::{
    proto_persist_pub_sub_server, proto_pub_sub_message, ProtoPubSubMessage, ProtoPushDiff,
    ProtoSubscribe, ProtoUnsubscribe,
};
use crate::metrics::Metrics;
use crate::ShardId;

/// Top-level Trait to create a PubSubClient.
///
/// Returns a [PubSubClientConnection] with a [PubSubSender] for issuing RPCs to the PubSub
/// server, and a [PubSubReceiver] that receives messages, such as state diffs.
pub trait PersistPubSubClient {
    /// Receive handles with which to push and subscribe to diffs.
    fn connect(config: PersistPubSubClientConfig, metrics: Arc<Metrics>) -> PubSubClientConnection;
}

/// Wrapper type for a matching [PubSubSender] and [PubSubReceiver] client pair.
#[derive(Debug)]
pub struct PubSubClientConnection {
    /// The sender client to Persist PubSub.
    pub sender: Arc<dyn PubSubSender>,
    /// The receiver client to Persist PubSub.
    pub receiver: Box<dyn PubSubReceiver>,
}

impl PubSubClientConnection {
    /// Creates a new [PubSubClientConnection] from a matching [PubSubSender] and [PubSubReceiver].
    pub fn new(sender: Arc<dyn PubSubSender>, receiver: Box<dyn PubSubReceiver>) -> Self {
        Self { sender, receiver }
    }
}

/// The public send-side client to Persist PubSub.
pub trait PubSubSender: std::fmt::Debug + Send + Sync {
    /// Push a diff to subscribers.
    fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData);

    /// Subscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    /// Returns a token that, when dropped, will unsubscribe the client from the
    /// shard.
    ///
    /// This call is idempotent and is a no-op for an already subscribed shard.
    fn subscribe(self: Arc<Self>, shard_id: &ShardId) -> Arc<ShardSubscriptionToken>;
}

/// The internal send-side client trait to Persist PubSub, responsible for issuing RPCs.
trait PubSubSenderInternal: std::fmt::Debug + Send + Sync {
    /// Push a diff to subscribers.
    fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData);

    /// Subscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    ///
    /// This call is idempotent and is a no-op for an already subscribed shard.
    fn subscribe(&self, shard_id: &ShardId);

    /// Unsubscribe the corresponding [PubSubReceiver] from diffs for the given shard.
    /// Users should not need to call this method directly, as it will be called
    /// automatically when the [ShardSubscriptionToken] returned by [PubSubSender::subscribe]
    /// is dropped.
    ///
    /// This call is idempotent and is a no-op for already unsubscribed shards.
    fn unsubscribe(&self, shard_id: &ShardId);
}

/// The receive-side client to Persist PubSub.
///
/// Returns diffs (and maybe in the future, blobs) for any shards subscribed to
/// by the corresponding `PubSubSender`.
pub trait PubSubReceiver:
    Stream<Item = ProtoPubSubMessage> + Send + Unpin + std::fmt::Debug
{
}

impl<T> PubSubReceiver for T where
    T: Stream<Item = ProtoPubSubMessage> + Send + Unpin + std::fmt::Debug
{
}

/// A token corresponding to a subscription to diffs for a particular shard.
///
/// When dropped, the client that originated the token will be unsubscribed
/// from further diffs to the shard.
pub struct ShardSubscriptionToken {
    pub(crate) shard_id: ShardId,
    sender: Arc<dyn PubSubSenderInternal>,
}

impl Debug for ShardSubscriptionToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShardSubscriptionToken({})", self.shard_id)
    }
}

impl Drop for ShardSubscriptionToken {
    fn drop(&mut self) {
        self.sender.unsubscribe(&self.shard_id);
    }
}

/// A gRPC-based implementation of a Persist PubSub server.
#[derive(Debug)]
pub struct PersistGrpcPubSubServer {
    state: Arc<PubSubState>,
}

impl PersistGrpcPubSubServer {
    /// Creates a new [PersistGrpcPubSubServer].
    pub fn new(metrics_registry: &MetricsRegistry) -> Self {
        let metrics = PubSubServerMetrics::new(metrics_registry);
        let state = Arc::new(PubSubState {
            connection_id_counter: AtomicUsize::new(0),
            shard_subscribers: Default::default(),
            connections: Default::default(),
            metrics: Arc::new(metrics),
        });

        PersistGrpcPubSubServer { state }
    }

    /// Creates a client to [PersistGrpcPubSubServer] that is directly connected
    /// to the server, avoiding the need for network calls or message serde.
    pub fn new_direct_client(&self) -> PubSubClientConnection {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let sender: Arc<dyn PubSubSender> = Arc::new(SubscriptionTrackingSender::new(Arc::new(
            Arc::clone(&self.state).new_connection(tx),
        )));

        PubSubClientConnection {
            sender,
            receiver: Box::new(
                ReceiverStream::new(rx)
                    .filter_map(|x| Some(x.expect("cannot receive grpc errors locally"))),
            ),
        }
    }

    /// Starts the gRPC server. Consumes `self` and runs until the task is cancelled.
    pub async fn serve(self, listen_addr: SocketAddr) -> Result<(), anyhow::Error> {
        tonic::transport::Server::builder()
            .add_service(ProtoPersistPubSubServer::new(self))
            .serve(listen_addr)
            .await?;
        Ok(())
    }

    /// Starts the gRPC server with the given listener stream.
    /// Consumes `self` and runs until the task is cancelled.
    #[cfg(test)]
    pub async fn serve_with_stream(self, listener: TcpListenerStream) -> Result<(), anyhow::Error> {
        tonic::transport::Server::builder()
            .add_service(ProtoPersistPubSubServer::new(self))
            .serve_with_incoming(listener)
            .await?;
        Ok(())
    }
}

/// A gRPC metadata key to indicate the caller id of a client.
pub const PERSIST_PUBSUB_CALLER_KEY: &str = "persist-pubsub-caller-id";

/// Client configuration for connecting to a remote PubSub server.
#[derive(Debug)]
pub struct PersistPubSubClientConfig {
    /// Connection address for the pubsub server, e.g. `http://localhost:6879`
    pub addr: String,
    /// A caller ID for the client. Used for debugging.
    pub caller_id: String,
}

/// A [PersistPubSubClient] implementation backed by gRPC.
///
/// Returns a [PubSubClientConnection] backed by channels that submit and receive
/// messages to and from a long-lived bidirectional gRPC stream. The gRPC stream
/// will be transparently reestablished if the connection is lost.
#[derive(Debug)]
pub struct GrpcPubSubClient;

impl PersistPubSubClient for GrpcPubSubClient {
    fn connect(config: PersistPubSubClientConfig, metrics: Arc<Metrics>) -> PubSubClientConnection {
        // Create a stable channel for our client to transmit message into our gRPC stream. We use a
        // broadcast to allow us to create new Receivers on demand, in case the underlying gRPC stream
        // is swapped out (e.g. due to connection failure). It is expected that only 1 Receiver is
        // ever active at a given time.
        let (rpc_requests, _) = tokio::sync::broadcast::channel(20);
        // Create a stable channel to receive messages from our gRPC stream. The input end lives inside
        // a task that continuously reads from the active gRPC stream, decoupling the `PubSubReceiver`
        // from the lifetime of a specific gRPC connection.
        let (receiver_input, receiver_output) = tokio::sync::mpsc::channel(20);

        let pubsub_sender = Arc::new(SubscriptionTrackingSender::new(Arc::new(
            GrpcPubSubSender {
                metrics: Arc::clone(&metrics),
                requests: rpc_requests.clone(),
                subscribes: Default::default(),
            },
        )));
        let sender = Arc::clone(&pubsub_sender);
        mz_ore::task::spawn(
            || format!("persist::pubsub::grpc::connection"),
            async move {
                let mut metadata = MetadataMap::new();
                metadata.insert(
                    AsciiMetadataKey::from_static(PERSIST_PUBSUB_CALLER_KEY),
                    AsciiMetadataValue::try_from(&config.caller_id)
                        .unwrap_or_else(|_| AsciiMetadataValue::from_static("unknown")),
                );

                loop {
                    println!("trying to connect");
                    let client = mz_ore::retry::Retry::default()
                        .clamp_backoff(Duration::from_secs(60))
                        .retry_async(|_| async {
                            println!("connecting to pubsub");
                            metrics
                                .pubsub_client
                                .grpc_connection
                                .connect_call_attempt_count
                                .inc();
                            let endpoint = match Endpoint::from_str(&config.addr) {
                                Ok(endpoint) => endpoint,
                                Err(err) => return RetryResult::FatalErr(err),
                            };
                            ProtoPersistPubSubClient::connect(
                                endpoint.timeout(Duration::from_secs(5)),
                            )
                            .await
                            .into()
                        })
                        .await;

                    let mut client = match client {
                        Ok(client) => client,
                        Err(err) => {
                            error!("fatal error connecting to persist pubsub: {:?}", err);
                            return;
                        }
                    };

                    metrics
                        .pubsub_client
                        .grpc_connection
                        .connection_established_count
                        .inc();
                    println!("created pubsub client to: {:?}", config.addr);

                    let mut broadcast = BroadcastStream::new(rpc_requests.subscribe());
                    let broadcast_errors = metrics
                        .pubsub_client
                        .grpc_connection
                        .broadcast_recv_lagged_count
                        .clone();
                    let pubsub_request = Request::from_parts(
                        metadata.clone(),
                        Extensions::default(),
                        async_stream::stream! {
                            while let Some(x) = broadcast.next().await {
                                println!("sending message: {:?}", x);
                                match x {
                                    Ok(x) => yield x,
                                    Err(BroadcastStreamRecvError::Lagged(i)) => {
                                        broadcast_errors.inc_by(i);
                                    }
                                }
                            }
                        },
                    );

                    let mut responses = match client.pub_sub(pubsub_request).await {
                        Ok(response) => response.into_inner(),
                        Err(err) => {
                            warn!("pub_sub rpc error: {:?}", err);
                            continue;
                        }
                    };

                    // shard subscriptions are tracked by connection on the server, so if our
                    // gRPC stream is ever swapped out, we must inform the server which shards
                    // our client intended to be subscribed to.
                    sender.reconnect();

                    loop {
                        debug!("awaiting next response");
                        match responses.next().await {
                            Some(Ok(message)) => {
                                match receiver_input.send(message).await {
                                    Ok(_) => {}
                                    // if the receiver has dropped, end the task to drop
                                    // our no-longer-needed grpc connection. in practice,
                                    // this should only occur during shutdown.
                                    Err(_) => {
                                        info!("closing pubsub grpc client connection");
                                        return;
                                    }
                                }
                            }
                            Some(Err(err)) => {
                                metrics.pubsub_client.grpc_connection.grpc_error_count.inc();
                                warn!("pubsub client error: {:?}", err);
                                break;
                            }
                            None => break,
                        }
                    }
                }
            },
        );

        PubSubClientConnection {
            sender: pubsub_sender,
            receiver: Box::new(ReceiverStream::new(receiver_output)),
        }
    }
}

/// Spawns a Tokio task that reads a [PubSubReceiver], applying diffs to
/// the [StateCache].
pub(crate) fn subscribe_state_cache_to_pubsub(
    cache: Arc<StateCache>,
    mut pubsub_receiver: Box<dyn PubSubReceiver>,
) -> JoinHandle<()> {
    mz_ore::task::spawn(|| "persist::rpc::push_responses", async move {
        cache.metrics.pubsub_client.receiver.connected.set(1);
        while let Some(res) = pubsub_receiver.next().await {
            // let timestamp =
            //     u128::from_le_bytes(<[u8; 16]>::try_from(res.timestamp.as_slice()).expect("WIP"));
            match res.message {
                Some(proto_pub_sub_message::Message::PushDiff(diff)) => {
                    info!("received diff: {:?}", diff);
                    cache.metrics.pubsub_client.receiver.push_received.inc();
                    let shard_id = diff.shard_id.into_rust().expect("valid shard id");
                    let diff = VersionedData {
                        seqno: diff.seqno.into_rust().expect("valid SeqNo"),
                        data: diff.diff,
                    };
                    debug!(
                        "client got diff {} {} {}",
                        shard_id,
                        diff.seqno,
                        diff.data.len()
                    );
                    cache.apply_diff(&shard_id, diff);
                    // let now = SystemTime::now()
                    //     .duration_since(SystemTime::UNIX_EPOCH)
                    //     .expect("failed to get millis since epoch")
                    //     .as_micros();
                    // let latency = now.saturating_sub(timestamp) as f64;
                    // cache
                    //     .metrics
                    //     .pubsub_client
                    //     .receiver
                    //     .approx_diff_latency
                    //     .observe(latency);
                }
                ref msg @ None | ref msg @ Some(_) => {
                    warn!("pubsub client received unexpected message: {:?}", msg);
                    cache
                        .metrics
                        .pubsub_client
                        .receiver
                        .unknown_message_received
                        .inc();
                }
            }
        }
        cache.metrics.pubsub_client.receiver.connected.set(0);
    })
}

/// An internal, gRPC-backed implementation of [PubSubSender].
struct GrpcPubSubSender {
    metrics: Arc<Metrics>,
    requests: tokio::sync::broadcast::Sender<ProtoPubSubMessage>,
}

impl Debug for GrpcPubSubSender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GrpcPubSubSender")
    }
}

impl PubSubSenderInternal for GrpcPubSubSender {
    fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData) {
        let seqno = diff.seqno.clone();
        let diff = ProtoPushDiff {
            shard_id: shard_id.into_proto(),
            seqno: diff.seqno.into_proto(),
            diff: diff.data.clone(),
        };
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get millis since epoch")
            .as_micros();
        let msg = ProtoPubSubMessage {
            timestamp: now.to_le_bytes().to_vec(),
            message: Some(proto_pub_sub_message::Message::PushDiff(diff)),
        };
        let size = msg.encoded_len();
        let metrics = &self.metrics.pubsub_client.sender.push;
        match self.requests.send(msg) {
            Ok(i) => {
                metrics.succeeded.inc();
                metrics.bytes_sent.inc_by(u64::cast_from(size));
                debug!("pushed ({}, {}) to {} listeners", shard_id, seqno, i);
            }
            Err(err) => {
                metrics.failed.inc();
                error!("{}", err);
            }
        }
    }

    fn subscribe(&self, shard_id: &ShardId) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get millis since epoch")
            .as_micros();
        let msg = ProtoPubSubMessage {
            timestamp: now.to_le_bytes().to_vec(),
            message: Some(proto_pub_sub_message::Message::Subscribe(ProtoSubscribe {
                shard_id: shard_id.into_proto(),
            })),
        };
        let size = msg.encoded_len();
        let metrics = &self.metrics.pubsub_client.sender.subscribe;
        match self.requests.send(msg) {
            Ok(_) => {
                metrics.succeeded.inc();
                metrics.bytes_sent.inc_by(u64::cast_from(size));
                info!("subscribed to {}", shard_id);
            }
            Err(err) => {
                metrics.failed.inc();
                error!("error subscribing to {}: {}", shard_id, err);
            }
        }
    }

    fn unsubscribe(&self, shard_id: &ShardId) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get millis since epoch")
            .as_micros();
        let msg = ProtoPubSubMessage {
            timestamp: now.to_le_bytes().to_vec(),
            message: Some(proto_pub_sub_message::Message::Unsubscribe(
                ProtoUnsubscribe {
                    shard_id: shard_id.into_proto(),
                },
            )),
        };
        let size = msg.encoded_len();
        let metrics = &self.metrics.pubsub_client.sender.unsubscribe;
        match self.requests.send(msg) {
            Ok(_) => {
                metrics.succeeded.inc();
                metrics.bytes_sent.inc_by(u64::cast_from(size));
                debug!("unsubscribed from {}", shard_id);
            }
            Err(err) => {
                metrics.failed.inc();
                error!("error unsubscribing from {}: {}", shard_id, err);
            }
        }
    }
}

#[derive(Debug)]
struct SubscriptionTrackingSender {
    delegate: Arc<dyn PubSubSenderInternal>,
    subscribes: Arc<Mutex<BTreeMap<ShardId, Weak<ShardSubscriptionToken>>>>,
}

impl SubscriptionTrackingSender {
    fn new(sender: Arc<dyn PubSubSenderInternal>) -> Self {
        Self {
            delegate: sender,
            subscribes: Default::default(),
        }
    }

    fn reconnect(&self) {
        let mut subscribes = self.subscribes.lock().expect("lock");
        subscribes.retain(|shard_id, token| {
            if token.upgrade().is_none() {
                false
            } else {
                debug!("reconnecting to: {}", shard_id);
                self.delegate.subscribe(shard_id);
                true
            }
        })
    }
}

impl PubSubSender for SubscriptionTrackingSender {
    fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData) {
        self.delegate.push_diff(shard_id, diff)
    }

    fn subscribe(self: Arc<Self>, shard_id: &ShardId) -> Arc<ShardSubscriptionToken> {
        let mut subscribes = self.subscribes.lock().expect("lock");
        if let Some(token) = subscribes.get(shard_id) {
            match token.upgrade() {
                None => assert!(subscribes.remove(shard_id).is_some()),
                Some(token) => {
                    return Arc::clone(&token);
                }
            }
        }

        let pubsub_sender = Arc::clone(&self.delegate);
        let token = Arc::new(ShardSubscriptionToken {
            shard_id: *shard_id,
            sender: pubsub_sender,
        });

        assert!(subscribes
            .insert(*shard_id, Arc::downgrade(&token))
            .is_none());

        self.delegate.subscribe(shard_id);

        token
    }
}

/// A wrapper intended to provide client-side metrics for a client
/// directly that communicates directly with the server state, such
/// as one created by [PersistGrpcPubSubServer::new_direct_client].
#[derive(Debug)]
pub struct MetricsDirectPubSubSender {
    metrics: Arc<Metrics>,
    pubsub_sender: Arc<dyn PubSubSender>,
}

impl MetricsDirectPubSubSender {
    /// Returns a new [MetricsDirectPubSubSender], wrapping the given
    /// `Arc<dyn PubSubSender>`'s calls to provide client-side metrics.
    pub fn new(pubsub_sender: Arc<dyn PubSubSender>, metrics: Arc<Metrics>) -> Self {
        Self {
            pubsub_sender,
            metrics,
        }
    }
}

impl PubSubSender for MetricsDirectPubSubSender {
    fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData) {
        self.metrics
            .pubsub_client
            .sender
            .push
            .bytes_sent
            .inc_by(u64::cast_from(diff.data.len()));
        self.pubsub_sender.push_diff(shard_id, diff);
        self.metrics.pubsub_client.sender.push.succeeded.inc();
    }

    fn subscribe(self: Arc<Self>, shard_id: &ShardId) -> Arc<ShardSubscriptionToken> {
        let token = Arc::clone(&self.pubsub_sender).subscribe(shard_id);
        self.metrics.pubsub_client.sender.subscribe.succeeded.inc();
        token
    }
}

/// Internal state of a PubSub server implementation.
#[derive(Debug)]
pub(crate) struct PubSubState {
    /// Assigns a unique ID to each incoming connection.
    connection_id_counter: AtomicUsize,
    /// Maintains a mapping of `ShardId --> [ConnectionId -> Tx]`.
    shard_subscribers:
        Arc<RwLock<BTreeMap<ShardId, BTreeMap<usize, Sender<Result<ProtoPubSubMessage, Status>>>>>>,
    /// Active connections.
    connections: Arc<RwLock<HashSet<usize>>>,
    /// Server-side metrics.
    metrics: Arc<PubSubServerMetrics>,
}

impl PubSubState {
    fn new_connection(
        self: Arc<Self>,
        notifier: Sender<Result<ProtoPubSubMessage, Status>>,
    ) -> PubSubConnection {
        let connection_id = self.connection_id_counter.fetch_add(1, Ordering::SeqCst);
        {
            let mut connections = self.connections.write().expect("lock");
            debug!("inserting connid: {}", connection_id);
            assert!(connections.insert(connection_id));
        }

        self.metrics.active_connections.inc();
        PubSubConnection {
            connection_id,
            notifier,
            state: self,
        }
    }

    fn remove_connection(&self, connection_id: usize) {
        let now = Instant::now();

        {
            debug!("removing connid: {}", connection_id);
            let mut connections = self.connections.write().expect("lock");
            assert!(
                connections.remove(&connection_id),
                "unknown connection id: {}",
                connection_id
            );
        }

        {
            let mut subscribers = self.shard_subscribers.write().expect("lock poisoned");
            for (_shard, connections) in subscribers.iter_mut() {
                connections.remove(&connection_id);
            }
        }

        self.metrics
            .connection_cleanup_seconds
            .inc_by(now.elapsed().as_secs_f64());
        self.metrics.active_connections.dec();
    }

    fn push_diff(&self, connection_id: usize, shard_id: &ShardId, data: &VersionedData) {
        let now = Instant::now();
        self.metrics.push_call_count.inc();

        assert!(
            self.connections
                .read()
                .expect("lock")
                .contains(&connection_id),
            "unknown connection id: {}",
            connection_id
        );

        let subscribers = self.shard_subscribers.read().expect("lock poisoned");
        if let Some(subscribed_connections) = subscribers.get(shard_id) {
            let mut num_sent = 0;
            let mut data_size = 0;

            for (subscribed_conn_id, tx) in subscribed_connections {
                // skip sending the diff back to the original sender
                if *subscribed_conn_id == connection_id {
                    continue;
                }
                debug!(
                    "server forwarding req to {} conns {} {} {}",
                    subscribed_conn_id,
                    &shard_id,
                    data.seqno,
                    data.data.len()
                );
                let req = ProtoPubSubMessage {
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("failed to get millis since epoch")
                        .as_micros()
                        .to_le_bytes()
                        .to_vec(),
                    message: Some(proto_pub_sub_message::Message::PushDiff(ProtoPushDiff {
                        seqno: data.seqno.into_proto(),
                        shard_id: shard_id.to_string(),
                        diff: Bytes::clone(&data.data),
                    })),
                };
                data_size = req.encoded_len();
                match tx.try_send(Ok(req)) {
                    Ok(_) => {
                        num_sent += 1;
                    }
                    Err(TrySendError::Full(_)) => {
                        self.metrics.broadcasted_diff_dropped_channel_full.inc();
                    }
                    Err(TrySendError::Closed(_)) => {}
                };
            }

            self.metrics.broadcasted_diff_count.inc_by(num_sent);
            self.metrics
                .broadcasted_diff_bytes
                .inc_by(num_sent * u64::cast_from(data_size));
        }

        self.metrics
            .push_seconds
            .inc_by(now.elapsed().as_secs_f64());
    }

    fn subscribe(
        &self,
        connection_id: usize,
        notifier: Sender<Result<ProtoPubSubMessage, Status>>,
        shard_id: &ShardId,
    ) {
        let now = Instant::now();
        self.metrics.subscribe_call_count.inc();

        assert!(
            self.connections
                .read()
                .expect("lock")
                .contains(&connection_id),
            "unknown connection id: {}",
            connection_id
        );

        {
            let mut subscribed_shards = self.shard_subscribers.write().expect("lock poisoned");
            subscribed_shards
                .entry(*shard_id)
                .or_default()
                .insert(connection_id, notifier);
        }

        self.metrics
            .subscribe_seconds
            .inc_by(now.elapsed().as_secs_f64());
    }

    fn unsubscribe(&self, connection_id: usize, shard_id: &ShardId) {
        let now = Instant::now();
        self.metrics.unsubscribe_call_count.inc();

        assert!(
            self.connections
                .read()
                .expect("lock")
                .contains(&connection_id),
            "unknown connection id: {}",
            connection_id
        );

        {
            let mut subscribed_shards = self.shard_subscribers.write().expect("lock poisoned");
            if let Some(subscribed_connections) = subscribed_shards.get_mut(shard_id) {
                subscribed_connections.remove(&connection_id);
            }
        }

        self.metrics
            .unsubscribe_seconds
            .inc_by(now.elapsed().as_secs_f64());
    }

    #[cfg(test)]
    fn new_for_test() -> Self {
        Self {
            connection_id_counter: AtomicUsize::new(0),
            shard_subscribers: Default::default(),
            connections: Default::default(),
            metrics: Arc::new(PubSubServerMetrics::new(&MetricsRegistry::new())),
        }
    }

    #[cfg(test)]
    fn active_connections(&self) -> HashSet<usize> {
        self.connections.read().expect("lock").clone()
    }

    #[cfg(test)]
    fn subscriptions(&self, connection_id: usize) -> HashSet<ShardId> {
        let mut shards = HashSet::new();

        let subscribers = self.shard_subscribers.read().expect("lock");
        for (shard, subscribed_connections) in subscribers.iter() {
            if subscribed_connections.contains_key(&connection_id) {
                shards.insert(*shard);
            }
        }

        shards
    }

    #[cfg(test)]
    fn shard_subscription_counts(&self) -> HashMap<ShardId, usize> {
        let mut shards = HashMap::new();

        let subscribers = self.shard_subscribers.read().expect("lock");
        for (shard, subscribed_connections) in subscribers.iter() {
            shards.insert(*shard, subscribed_connections.len());
        }

        shards
    }
}

/// An active connection of [PubSubState].
///
/// When dropped, removes itself from [PubSubState], clearing all of its subscriptions.
#[derive(Debug)]
pub(crate) struct PubSubConnection {
    connection_id: usize,
    notifier: Sender<Result<ProtoPubSubMessage, Status>>,
    state: Arc<PubSubState>,
}

impl PubSubSenderInternal for PubSubConnection {
    fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData) {
        self.state.push_diff(self.connection_id, shard_id, diff)
    }

    fn subscribe(&self, shard_id: &ShardId) {
        self.state
            .subscribe(self.connection_id, self.notifier.clone(), shard_id)
    }

    fn unsubscribe(&self, shard_id: &ShardId) {
        self.state.unsubscribe(self.connection_id, shard_id)
    }
}

impl Drop for PubSubConnection {
    fn drop(&mut self) {
        self.state.remove_connection(self.connection_id)
    }
}

#[async_trait]
impl proto_persist_pub_sub_server::ProtoPersistPubSub for PersistGrpcPubSubServer {
    type PubSubStream = Pin<Box<dyn Stream<Item = Result<ProtoPubSubMessage, Status>> + Send>>;

    async fn pub_sub(
        &self,
        request: Request<Streaming<ProtoPubSubMessage>>,
    ) -> Result<Response<Self::PubSubStream>, Status> {
        let root_span = info_span!("persist::push::server");
        let _guard = root_span.enter();
        let caller_id = request
            .metadata()
            .get(AsciiMetadataKey::from_static(PERSIST_PUBSUB_CALLER_KEY))
            .map(|key| key.to_str().ok())
            .flatten()
            .map(|key| key.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        println!("incoming pubsub stream from: {:?}", caller_id);

        let mut in_stream = request.into_inner();

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let server_state = Arc::clone(&self.state);
        // this spawn here to cleanup after connection error / disconnect, otherwise the stream
        // would not be polled after the connection drops. in our case, we want to clear the
        // connection and its subscriptions from our shared state when it drops.
        tokio::spawn(async move {
            let root_span = info_span!("persist::push::server_conn");
            let _guard = root_span.enter();

            println!("new connection task");
            let connection = server_state.new_connection(tx);
            while let Some(result) = in_stream.next().await {
                let req = match result {
                    Ok(req) => req,
                    Err(err) => {
                        warn!("pubsub connection err: {}", err);
                        break;
                    }
                };

                match req.message {
                    None => {}
                    Some(proto_pub_sub_message::Message::PushDiff(req)) => {
                        println!("server received diff");
                        let shard_id = req.shard_id.parse().expect("valid shard id");
                        let diff = VersionedData {
                            seqno: req.seqno.into_rust().expect("WIP"),
                            data: req.diff.clone(),
                        };
                        connection.push_diff(&shard_id, &diff);
                    }
                    Some(proto_pub_sub_message::Message::Subscribe(diff)) => {
                        println!("server received subscribe");
                        let shard_id = diff.shard_id.parse().expect("valid shard id");
                        connection.subscribe(&shard_id);
                    }
                    Some(proto_pub_sub_message::Message::Unsubscribe(diff)) => {
                        println!("server received unsubscribe");
                        let shard_id = diff.shard_id.parse().expect("valid shard id");
                        connection.unsubscribe(&shard_id);
                    }
                }
            }
            println!("push stream to {} ended", caller_id);
        });

        let out_stream: Self::PubSubStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(out_stream))
    }
}

#[cfg(test)]
mod pubsub_state {
    use std::sync::Arc;

    use bytes::Bytes;
    use mz_ore::collections::HashSet;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::Receiver;
    use tonic::Status;

    use mz_persist::location::{SeqNo, VersionedData};
    use mz_proto::RustType;

    use crate::internal::service::proto_pub_sub_message::Message;
    use crate::internal::service::ProtoPubSubMessage;
    use crate::rpc::{PubSubSenderInternal, PubSubState};
    use crate::ShardId;

    const SHARD_ID_0: ShardId = ShardId([0u8; 16]);
    const SHARD_ID_1: ShardId = ShardId([1u8; 16]);

    const VERSIONED_DATA_0: VersionedData = VersionedData {
        seqno: SeqNo(0),
        data: Bytes::from_static(&[0, 1, 2, 3]),
    };

    const VERSIONED_DATA_1: VersionedData = VersionedData {
        seqno: SeqNo(1),
        data: Bytes::from_static(&[4, 5, 6, 7]),
    };

    #[test]
    #[should_panic(expected = "unknown connection id: 100")]
    fn test_zero_connections_push_diff() {
        let state = Arc::new(PubSubState::new_for_test());
        state.push_diff(100, &SHARD_ID_0, &VERSIONED_DATA_0);
    }

    #[test]
    #[should_panic(expected = "unknown connection id: 100")]
    fn test_zero_connections_subscribe() {
        let state = Arc::new(PubSubState::new_for_test());
        let (tx, _) = tokio::sync::mpsc::channel(100);
        state.subscribe(100, tx, &SHARD_ID_0);
    }

    #[test]
    #[should_panic(expected = "unknown connection id: 100")]
    fn test_zero_connections_unsubscribe() {
        let state = Arc::new(PubSubState::new_for_test());
        state.unsubscribe(100, &SHARD_ID_0);
    }

    #[test]
    #[should_panic(expected = "unknown connection id: 100")]
    fn test_zero_connections_remove() {
        let state = Arc::new(PubSubState::new_for_test());
        state.remove_connection(100)
    }

    #[test]
    fn test_single_connection() {
        let state = Arc::new(PubSubState::new_for_test());

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let connection = Arc::clone(&state).new_connection(tx);

        assert_eq!(
            state.active_connections(),
            HashSet::from([connection.connection_id])
        );

        // no messages should have been broadcasted yet
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        connection.push_diff(
            &SHARD_ID_0,
            &VersionedData {
                seqno: SeqNo::minimum(),
                data: Bytes::new(),
            },
        );

        // server should not broadcast a message back to originating client
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        // a connection can subscribe to a shard
        connection.subscribe(&SHARD_ID_0);
        assert_eq!(
            state.subscriptions(connection.connection_id),
            HashSet::from([SHARD_ID_0.clone()])
        );

        // a connection can unsubscribe
        connection.unsubscribe(&SHARD_ID_0);
        assert!(state.subscriptions(connection.connection_id).is_empty());

        // a connection can subscribe to many shards
        connection.subscribe(&SHARD_ID_0);
        connection.subscribe(&SHARD_ID_1);
        assert_eq!(
            state.subscriptions(connection.connection_id),
            HashSet::from([SHARD_ID_0, SHARD_ID_1])
        );

        // and to a single shard many times idempotently
        connection.subscribe(&SHARD_ID_0);
        connection.subscribe(&SHARD_ID_0);
        assert_eq!(
            state.subscriptions(connection.connection_id),
            HashSet::from([SHARD_ID_0, SHARD_ID_1])
        );

        // dropping the connection should unsubscribe all shards and unregister the connection
        let connection_id = connection.connection_id;
        drop(connection);
        assert!(state.subscriptions(connection_id).is_empty());
        assert!(state.active_connections().is_empty());
    }

    #[test]
    fn test_many_connection() {
        let state = Arc::new(PubSubState::new_for_test());

        let (tx1, mut rx1) = tokio::sync::mpsc::channel(100);
        let conn1 = Arc::clone(&state).new_connection(tx1);

        let (tx2, mut rx2) = tokio::sync::mpsc::channel(100);
        let conn2 = Arc::clone(&state).new_connection(tx2);

        let (tx3, mut rx3) = tokio::sync::mpsc::channel(100);
        let conn3 = Arc::clone(&state).new_connection(tx3);

        conn1.subscribe(&SHARD_ID_0);
        conn2.subscribe(&SHARD_ID_0);
        conn2.subscribe(&SHARD_ID_1);

        assert_eq!(
            state.active_connections(),
            HashSet::from([
                conn1.connection_id,
                conn2.connection_id,
                conn3.connection_id
            ])
        );

        // broadcast a diff to a shard subscribed to by several connections
        conn3.push_diff(&SHARD_ID_0, &VERSIONED_DATA_0);
        assert_push(&mut rx1, &SHARD_ID_0, &VERSIONED_DATA_0);
        assert_push(&mut rx2, &SHARD_ID_0, &VERSIONED_DATA_0);
        assert!(matches!(rx3.try_recv(), Err(TryRecvError::Empty)));

        // broadcast a diff shared by publisher. it should not receive the diff back.
        conn1.push_diff(&SHARD_ID_0, &VERSIONED_DATA_0);
        assert!(matches!(rx1.try_recv(), Err(TryRecvError::Empty)));
        assert_push(&mut rx2, &SHARD_ID_0, &VERSIONED_DATA_0);
        assert!(matches!(rx3.try_recv(), Err(TryRecvError::Empty)));

        // broadcast a diff to a shard subscribed to by one connection
        conn3.push_diff(&SHARD_ID_1, &VERSIONED_DATA_1);
        assert!(matches!(rx1.try_recv(), Err(TryRecvError::Empty)));
        assert_push(&mut rx2, &SHARD_ID_1, &VERSIONED_DATA_1);
        assert!(matches!(rx3.try_recv(), Err(TryRecvError::Empty)));

        // broadcast a diff to a shard subscribed to by no connections
        conn2.unsubscribe(&SHARD_ID_1);
        conn3.push_diff(&SHARD_ID_1, &VERSIONED_DATA_1);
        assert!(matches!(rx1.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(rx2.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(rx3.try_recv(), Err(TryRecvError::Empty)));

        // dropping connections unsubscribes them
        let conn1_id = conn1.connection_id;
        drop(conn1);
        conn3.push_diff(&SHARD_ID_0, &VERSIONED_DATA_0);
        assert!(matches!(rx1.try_recv(), Err(TryRecvError::Disconnected)));
        assert_push(&mut rx2, &SHARD_ID_0, &VERSIONED_DATA_0);
        assert!(matches!(rx3.try_recv(), Err(TryRecvError::Empty)));

        assert!(state.subscriptions(conn1_id).is_empty());
        assert_eq!(
            state.subscriptions(conn2.connection_id),
            HashSet::from([SHARD_ID_0])
        );
        assert_eq!(state.subscriptions(conn3.connection_id), HashSet::new());
        assert_eq!(
            state.active_connections(),
            HashSet::from([conn2.connection_id, conn3.connection_id])
        );
    }

    fn assert_push(
        rx: &mut Receiver<Result<ProtoPubSubMessage, Status>>,
        shard: &ShardId,
        data: &VersionedData,
    ) {
        let message = rx
            .try_recv()
            .expect("message in channel")
            .expect("pubsub")
            .message
            .expect("proto contains message");
        match message {
            Message::PushDiff(x) => {
                assert_eq!(x.shard_id, shard.into_proto());
                assert_eq!(x.seqno, data.seqno.into_proto());
                assert_eq!(x.diff, data.data);
            }
            Message::Subscribe(_) | Message::Unsubscribe(_) => panic!("unexpected message type"),
        };
    }
}

#[cfg(test)]
mod grpc {
    use crate::cfg::PersistConfig;
    use crate::internal::service::proto_pub_sub_message::Message;
    use crate::internal::service::ProtoPubSubMessage;
    use crate::metrics::Metrics;
    use crate::rpc::{
        GrpcPubSubClient, PersistGrpcPubSubServer, PersistPubSubClient, PersistPubSubClientConfig,
        PubSubState,
    };
    use crate::ShardId;
    use bytes::Bytes;
    use futures_util::FutureExt;
    use mz_ore::collections::HashMap;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist::location::{SeqNo, VersionedData};
    use mz_proto::RustType;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_stream::StreamExt;

    const SHARD_ID_0: ShardId = ShardId([0u8; 16]);
    const SHARD_ID_1: ShardId = ShardId([1u8; 16]);
    const VERSIONED_DATA_0: VersionedData = VersionedData {
        seqno: SeqNo(0),
        data: Bytes::from_static(&[0, 1, 2, 3]),
    };
    const VERSIONED_DATA_1: VersionedData = VersionedData {
        seqno: SeqNo(1),
        data: Bytes::from_static(&[4, 5, 6, 7]),
    };

    // NB: we use separate runtimes for client and server throughout these tests to cleanly drop
    // ALL tasks (including spawned child tasks) associated with one end of a connection, to most
    // closely model an actual disconnect.

    #[test]
    fn grpc_server() {
        let metrics = Arc::new(Metrics::new(
            &PersistConfig::new_for_tests(),
            &MetricsRegistry::new(),
        ));
        let server_runtime = tokio::runtime::Runtime::new().expect("server runtime");
        let client_runtime = tokio::runtime::Runtime::new().expect("client runtime");

        // start the server
        let (addr, tcp_listener_stream) = server_runtime.block_on(new_tcp_listener());
        let server_state = server_runtime.block_on(spawn_server(tcp_listener_stream));

        // start a client.
        client_runtime.spawn(async move {
            let client = GrpcPubSubClient::connect(
                PersistPubSubClientConfig {
                    addr: format!("http://{}", addr.to_string()),
                    caller_id: "client".to_string(),
                },
                metrics,
            );
            let _token = client.sender.subscribe(&SHARD_ID_0);
            tokio::time::sleep(Duration::MAX).await;
        });

        // wait until the client is connected and subscribed
        server_runtime.block_on(async {
            poll_until_true(Duration::from_secs(10), || {
                server_state.active_connections().len() == 1
            })
            .await;
            poll_until_true(Duration::from_secs(2), || {
                server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 1)])
            })
            .await
        });

        // drop the client
        client_runtime.shutdown_timeout(Duration::from_secs(2));

        // server should notice the client dropping and clean up its state
        server_runtime.block_on(async {
            poll_until_true(Duration::from_secs(10), || {
                server_state.active_connections().is_empty()
            })
            .await;
            poll_until_true(Duration::from_secs(2), || {
                server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 0)])
            })
            .await
        });
    }

    #[test]
    fn grpc_client_sender_reconnects() {
        let metrics = Arc::new(Metrics::new(
            &PersistConfig::new_for_tests(),
            &MetricsRegistry::new(),
        ));
        let server_runtime = tokio::runtime::Runtime::new().expect("server runtime");
        let client_runtime = tokio::runtime::Runtime::new().expect("client runtime");
        let (addr, tcp_listener_stream) = server_runtime.block_on(new_tcp_listener());

        // start a client
        let client = client_runtime.block_on(async {
            GrpcPubSubClient::connect(
                PersistPubSubClientConfig {
                    addr: format!("http://{}", addr.to_string()),
                    caller_id: "client".to_string(),
                },
                metrics,
            )
        });

        // we can subscribe before connecting to the pubsub server
        let _token = Arc::clone(&client.sender).subscribe(&SHARD_ID_0);
        // we can subscribe and unsubscribe before connecting to the pubsub server
        let _token_2 = Arc::clone(&client.sender).subscribe(&SHARD_ID_1);
        drop(_token_2);

        // create the server after the client is up
        let server_state = server_runtime.block_on(spawn_server(tcp_listener_stream));

        server_runtime.block_on(async {
            // client connects automatically once the server is up
            poll_until_true(Duration::from_secs(10), || {
                server_state.active_connections().len() == 1
            })
            .await;

            // client rehydrated its subscriptions. notably, only includes the shard that
            // still has an active token
            poll_until_true(Duration::from_secs(2), || {
                server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 1)])
            })
            .await;
        });

        // kill the server
        server_runtime.shutdown_timeout(Duration::from_secs(2));

        // client can still send requests without error
        let _token_2 = Arc::clone(&client.sender).subscribe(&SHARD_ID_1);

        // create a new server
        let server_runtime = tokio::runtime::Runtime::new().expect("server runtime");
        let tcp_listener_stream = server_runtime.block_on(async {
            TcpListenerStream::new(
                TcpListener::bind(addr)
                    .await
                    .expect("can bind to previous addr"),
            )
        });
        let server_state = server_runtime.block_on(spawn_server(tcp_listener_stream));

        server_runtime.block_on(async {
            // client automatically reconnects to new server
            poll_until_true(Duration::from_secs(5), || {
                server_state.active_connections().len() == 1
            })
            .await;

            // and rehydrates its subscriptions, including the new one that was sent
            // while the server was unavailable.
            poll_until_true(Duration::from_secs(3), || {
                server_state.shard_subscription_counts()
                    == HashMap::from([(SHARD_ID_0, 1), (SHARD_ID_1, 1)])
            })
            .await;
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_client_sender_subscription_tokens() {
        let metrics = Arc::new(Metrics::new(
            &PersistConfig::new_for_tests(),
            &MetricsRegistry::new(),
        ));

        let (addr, tcp_listener_stream) = new_tcp_listener().await;
        let server_state = spawn_server(tcp_listener_stream).await;

        let client = GrpcPubSubClient::connect(
            PersistPubSubClientConfig {
                addr: format!("http://{}", addr.to_string()),
                caller_id: "client".to_string(),
            },
            metrics,
        );

        // our client connects
        poll_until_true(Duration::from_secs(5), || {
            server_state.active_connections().len() == 1
        })
        .await;

        // we can subscribe to a shard, receiving back a token
        let token = Arc::clone(&client.sender).subscribe(&SHARD_ID_0);
        poll_until_true(Duration::from_secs(3), || {
            server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 1)])
        })
        .await;

        // dropping the token will unsubscribe our client
        drop(token);
        poll_until_true(Duration::from_secs(3), || {
            server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 0)])
        })
        .await;

        // we can resubscribe to a shard
        let token = Arc::clone(&client.sender).subscribe(&SHARD_ID_0);
        poll_until_true(Duration::from_secs(3), || {
            server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 1)])
        })
        .await;

        // we can subscribe many times idempotently, receiving back Arcs to the same token
        let token2 = Arc::clone(&client.sender).subscribe(&SHARD_ID_0);
        let token3 = Arc::clone(&client.sender).subscribe(&SHARD_ID_0);
        assert_eq!(Arc::strong_count(&token), 3);
        poll_until_true(Duration::from_secs(3), || {
            server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 1)])
        })
        .await;

        // dropping all of the tokens will unsubscribe the shard
        drop(token);
        drop(token2);
        drop(token3);
        poll_until_true(Duration::from_secs(3), || {
            server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 0)])
        })
        .await;

        // we can subscribe to many shards
        let _token0 = Arc::clone(&client.sender).subscribe(&SHARD_ID_0);
        let _token1 = Arc::clone(&client.sender).subscribe(&SHARD_ID_1);
        poll_until_true(Duration::from_secs(3), || {
            server_state.shard_subscription_counts()
                == HashMap::from([(SHARD_ID_0, 1), (SHARD_ID_1, 1)])
        })
        .await;
    }

    #[test]
    fn grpc_client_receiver() {
        let metrics = Arc::new(Metrics::new(
            &PersistConfig::new_for_tests(),
            &MetricsRegistry::new(),
        ));
        let server_runtime = tokio::runtime::Runtime::new().expect("server runtime");
        let client_runtime = tokio::runtime::Runtime::new().expect("client runtime");
        let (addr, tcp_listener_stream) = server_runtime.block_on(new_tcp_listener());

        // create two clients, so we can test that broadcast messages are received by the other
        let mut client_1 = client_runtime.block_on(async {
            GrpcPubSubClient::connect(
                PersistPubSubClientConfig {
                    addr: format!("http://{}", addr.to_string()),
                    caller_id: "client_1".to_string(),
                },
                Arc::clone(&metrics),
            )
        });
        let mut client_2 = client_runtime.block_on(async {
            GrpcPubSubClient::connect(
                PersistPubSubClientConfig {
                    addr: format!("http://{}", addr.to_string()),
                    caller_id: "client_2".to_string(),
                },
                metrics,
            )
        });

        // we can check our receiver output before connecting to the server.
        // these calls are race-y, since there's no guarantee on the time it
        // would take for a message to be received were one to have been sent,
        // but, better than nothing?
        assert!(client_1.receiver.next().now_or_never().is_none());
        assert!(client_2.receiver.next().now_or_never().is_none());

        // start the server
        let server_state = server_runtime.block_on(spawn_server(tcp_listener_stream));

        // wait until both clients are connected
        server_runtime.block_on(poll_until_true(Duration::from_secs(10), || {
            server_state.active_connections().len() == 2
        }));

        // no messages have been broadcast yet
        assert!(client_1.receiver.next().now_or_never().is_none());
        assert!(client_2.receiver.next().now_or_never().is_none());

        // subscribe and send a diff
        let _token_client_1 = Arc::clone(&client_1.sender).subscribe(&SHARD_ID_0);
        let _token_client_2 = Arc::clone(&client_2.sender).subscribe(&SHARD_ID_0);
        server_runtime.block_on(poll_until_true(Duration::from_secs(2), || {
            server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 2)])
        }));

        // the subscriber non-sender client receives the diff
        client_1.sender.push_diff(&SHARD_ID_0, &VERSIONED_DATA_1);
        assert!(client_1.receiver.next().now_or_never().is_none());
        client_runtime.block_on(async {
            assert_push(
                client_2.receiver.next().await.expect("has diff"),
                &SHARD_ID_0,
                &VERSIONED_DATA_1,
            )
        });

        // kill the server
        server_runtime.shutdown_timeout(Duration::from_secs(2));

        // receivers can still be polled without error
        assert!(client_1.receiver.next().now_or_never().is_none());
        assert!(client_2.receiver.next().now_or_never().is_none());

        // create a new server
        let server_runtime = tokio::runtime::Runtime::new().expect("server runtime");
        let tcp_listener_stream = server_runtime.block_on(async {
            TcpListenerStream::new(
                TcpListener::bind(addr)
                    .await
                    .expect("can bind to previous addr"),
            )
        });
        let server_state = server_runtime.block_on(spawn_server(tcp_listener_stream));

        // client automatically reconnects to new server and rehydrates subscriptions
        server_runtime.block_on(async {
            poll_until_true(Duration::from_secs(10), || {
                server_state.active_connections().len() == 2
            })
            .await;
            poll_until_true(Duration::from_secs(2), || {
                server_state.shard_subscription_counts() == HashMap::from([(SHARD_ID_0, 2)])
            })
            .await;
        });

        // pushing and receiving diffs works as expected.
        // this time we'll push from the other client.
        client_2.sender.push_diff(&SHARD_ID_0, &VERSIONED_DATA_0);
        client_runtime.block_on(async {
            assert_push(
                client_1.receiver.next().await.expect("has diff"),
                &SHARD_ID_0,
                &VERSIONED_DATA_0,
            )
        });
        assert!(client_2.receiver.next().now_or_never().is_none());
    }

    async fn new_tcp_listener() -> (SocketAddr, TcpListenerStream) {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
        let tcp_listener = TcpListener::bind(addr).await.expect("tcp listener");

        (
            tcp_listener.local_addr().expect("bound to local address"),
            TcpListenerStream::new(tcp_listener),
        )
    }

    async fn spawn_server(tcp_listener_stream: TcpListenerStream) -> Arc<PubSubState> {
        let server = PersistGrpcPubSubServer::new(&MetricsRegistry::new());
        let server_state = Arc::clone(&server.state);

        let _server_task =
            tokio::spawn(async move { server.serve_with_stream(tcp_listener_stream).await });
        server_state
    }

    async fn poll_until_true<F>(timeout: Duration, f: F)
    where
        F: Fn() -> bool,
    {
        let now = Instant::now();
        loop {
            if f() {
                return;
            }

            if now.elapsed() > timeout {
                panic!("timed out");
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn assert_push(message: ProtoPubSubMessage, shard: &ShardId, data: &VersionedData) {
        let message = message.message.expect("proto contains message");
        match message {
            Message::PushDiff(x) => {
                assert_eq!(x.shard_id, shard.into_proto());
                assert_eq!(x.seqno, data.seqno.into_proto());
                assert_eq!(x.diff, data.data);
            }
            Message::Subscribe(_) | Message::Unsubscribe(_) => panic!("unexpected message type"),
        };
    }
}
