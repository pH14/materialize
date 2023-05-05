// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime};

use futures::Stream;
use prost::Message;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream, UnboundedReceiverStream};
use tokio_stream::StreamExt;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::transport::Endpoint;
use tonic::{Extensions, Request, Response, Status, Streaming};
use tracing::{debug, error, info, info_span, warn};

use mz_ore::cast::CastFrom;
use mz_ore::collections::HashSet;
use mz_ore::metrics::MetricsRegistry;
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

/// The send-side client to Persist PubSub, responsible for issuing
/// RPCs to the PubSub service.
pub trait PubSubSender: std::fmt::Debug + Send + Sync {
    /// Push a diff to subscribers.
    fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData);

    /// Subscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    /// Returns a token that, when dropped, will unsubscribe the client from the
    /// shard.
    ///
    /// This call is idempotent and is a no-op for an already subscribed shard.
    fn subscribe(self: Arc<Self>, shard_id: &ShardId) -> Arc<ShardSubscriptionToken>;

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
#[derive(Debug)]
pub struct ShardSubscriptionToken {
    pub(crate) shard_id: ShardId,
    pub(crate) pubsub_sender: Arc<dyn PubSubSender>,
}

impl Drop for ShardSubscriptionToken {
    fn drop(&mut self) {
        self.pubsub_sender.unsubscribe(&self.shard_id)
    }
}

/// A gRPC-based implementation of a Persist PubSub server.
#[derive(Debug)]
pub struct PersistGrpcPubSubServer {
    service: PersistService,
}

impl PersistGrpcPubSubServer {
    /// Creates a new [PersistGrpcPubSubServer].
    pub fn new(metrics_registry: &MetricsRegistry) -> Self {
        let metrics = PubSubServerMetrics::new(metrics_registry);
        let service = PersistService::new(metrics);
        PersistGrpcPubSubServer { service }
    }

    /// Creates a client to [PersistGrpcPubSubServer] that is directly connected
    /// to the server, avoiding the need for network calls or message serde.
    pub fn new_direct_client(&self) -> PubSubClientConnection {
        self.service.new_direct_client()
    }

    /// Starts the gRPC server. Consumes `self` and runs until the task is cancelled.
    pub async fn serve(self, listen_addr: SocketAddr) -> Result<(), anyhow::Error> {
        tonic::transport::Server::builder()
            .add_service(ProtoPersistPubSubServer::new(self.service))
            .serve(listen_addr)
            .await?;
        Ok(())
    }
}

/// A gRPC metadata key to indicate the caller id of a client.
pub const PERSIST_PUBSUB_CALLER_KEY: &str = "persist-pubsub-caller-id";

/// Client configuration for connecting to a remote PubSub server.
#[derive(Debug)]
pub struct PersistPubSubClientConfig {
    pub addr: String,
    pub caller_id: String,
}

/// A [PersistPubSubClient] implementation backed by gRPC.
#[derive(Debug)]
pub struct GrpcPubSubClient;

impl PersistPubSubClient for GrpcPubSubClient {
    fn connect(config: PersistPubSubClientConfig, metrics: Arc<Metrics>) -> PubSubClientConnection {
        // WIP: we use a broadcast so the Sender does not need to be replaced if the underlying connection is swapped out
        // drop the initial receiver... we'll create receivers on-demand from our sender in our reconnection loop
        // we should only ever have 1 receiver open at a time
        let (rpc_requests, _) = tokio::sync::broadcast::channel(100);

        let pubsub_sender = Arc::new(GrpcPubSubSender {
            metrics: Arc::clone(&metrics),
            requests: rpc_requests.clone(),
            subscribes: Default::default(),
        });

        let (receiver_input, receiver_output) = tokio::sync::mpsc::channel(100);
        let sender = Arc::clone(&pubsub_sender);

        // WIP: return join handle so we can keep track of its existence a little more easily?
        mz_ore::task::spawn(|| format!("persist_pubsub_client_connection"), async move {
            let mut metadata = MetadataMap::new();
            metadata.insert(
                AsciiMetadataKey::from_static(PERSIST_PUBSUB_CALLER_KEY),
                AsciiMetadataValue::try_from(&config.caller_id)
                    .unwrap_or_else(|_| AsciiMetadataValue::from_static("unknown")),
            );

            loop {
                // WIP: count connections made
                // WIP: connect with retries and a timeout
                let client = mz_ore::retry::Retry::default()
                    .clamp_backoff(Duration::from_secs(60))
                    .retry_async(|_| async {
                        info!("connecting to pubsub");
                        let endpoint =
                            Endpoint::from_str(&config.addr)?.timeout(Duration::from_secs(5));
                        Ok::<_, anyhow::Error>(ProtoPersistPubSubClient::connect(endpoint).await?)
                    })
                    .await;

                let mut client = match client {
                    Ok(client) => client,
                    Err(err) => {
                        warn!("error connecting to persist pubsub: {:?}", err);
                        continue;
                    }
                };

                info!("Created pubsub client to: {:?}", config.addr);
                let mut broadcast = BroadcastStream::new(rpc_requests.subscribe());
                let pubsub_request = Request::from_parts(
                    metadata.clone(),
                    Extensions::default(),
                    async_stream::stream! {
                        while let Some(x) = broadcast.next().await {
                            match x {
                                Ok(x) => yield x,
                                // WIP: metrics for broadcast recv errors, # of messages lagged
                                Err(err) => {}
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

                sender.reconnect();
                info!("reconnected");

                loop {
                    info!("awaiting responses");
                    match responses.next().await {
                        None => break,
                        Some(Ok(x)) => {
                            // WIP
                            info!("got response: {:?}", x);
                            match receiver_input.send(x).await {
                                Ok(()) => {}
                                Err(err) => {
                                    warn!("pubsub client receiver input failure: {:?}", err);
                                }
                            }
                        }
                        Some(Err(err)) => {
                            // WIP count errors
                            warn!("pubsub client error: {:?}", err);
                            break;
                        }
                    }
                }

                info!("need to reconnect");
            }
        });

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
            let timestamp =
                u128::from_le_bytes(<[u8; 16]>::try_from(res.timestamp.as_slice()).expect("WIP"));
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
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("failed to get millis since epoch")
                        .as_micros();
                    let latency = now.saturating_sub(timestamp) as f64;
                    cache
                        .metrics
                        .pubsub_client
                        .receiver
                        .approx_diff_latency
                        .observe(latency);
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
#[derive(Debug)]
struct GrpcPubSubSender {
    metrics: Arc<Metrics>,
    requests: tokio::sync::broadcast::Sender<ProtoPubSubMessage>,
    subscribes: Arc<Mutex<BTreeMap<ShardId, Weak<ShardSubscriptionToken>>>>,
}

impl GrpcPubSubSender {
    fn reconnect(&self) {
        let mut subscribes = self.subscribes.lock().expect("lock");
        subscribes.retain(|shard_id, token| {
            if token.upgrade().is_none() {
                false
            } else {
                info!("reconnecting to: {}", shard_id);
                self.subscribe_locked(shard_id);
                true
            }
        })
    }

    fn subscribe_locked(&self, shard_id: &ShardId) {
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
}

impl PubSubSender for GrpcPubSubSender {
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
                info!("pushed to {} listeners", i);
                debug!("pushed ({}, {})", shard_id, seqno);
            }
            Err(err) => {
                metrics.failed.inc();
                error!("{}", err);
            }
        }
    }

    fn subscribe(self: Arc<Self>, shard_id: &ShardId) -> Arc<ShardSubscriptionToken> {
        info!("creating new subscribe token");
        let mut subscribes = self.subscribes.lock().expect("lock");
        if let Some(token) = subscribes.get(shard_id) {
            match token.upgrade() {
                None => assert!(subscribes.remove(shard_id).is_some()),
                Some(token) => {
                    return Arc::clone(&token);
                }
            }
        }
        let pubsub_sender = Arc::clone(&self);
        let pubsub_sender: Arc<dyn PubSubSender> = pubsub_sender;
        let token = Arc::new(ShardSubscriptionToken {
            shard_id: *shard_id,
            pubsub_sender,
        });
        assert!(subscribes
            .insert(*shard_id, Arc::downgrade(&token))
            .is_none());
        self.subscribe_locked(shard_id);
        token
    }

    fn unsubscribe(&self, shard: &ShardId) {
        let mut subscribes = self.subscribes.lock().expect("lock");
        let removed_token = subscribes.remove(shard);

        if let Some(removed_token) = removed_token {
            assert!(removed_token.upgrade().is_none());
        }

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get millis since epoch")
            .as_micros();
        let msg = ProtoPubSubMessage {
            timestamp: now.to_le_bytes().to_vec(),
            message: Some(proto_pub_sub_message::Message::Unsubscribe(
                ProtoUnsubscribe {
                    shard_id: shard.into_proto(),
                },
            )),
        };
        let size = msg.encoded_len();
        let metrics = &self.metrics.pubsub_client.sender.unsubscribe;
        match self.requests.send(msg) {
            Ok(_) => {
                metrics.succeeded.inc();
                metrics.bytes_sent.inc_by(u64::cast_from(size));
                debug!("unsubscribed from {}", shard);
            }
            Err(err) => {
                metrics.failed.inc();
                error!("error unsubscribing from {}: {}", shard, err);
            }
        }
    }
}

/// A wrapper intended to provide client-side metrics for a client
/// directly that communicates directly with the server state, such
/// as one created by [PersistService::new_direct_client].
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

    fn unsubscribe(&self, shard_id: &ShardId) {
        self.pubsub_sender.unsubscribe(shard_id);
        self.metrics
            .pubsub_client
            .sender
            .unsubscribe
            .succeeded
            .inc();
    }
}

/// Internal state of a PubSub server implementation.
#[derive(Debug)]
pub(crate) struct PubSubState {
    /// Assigns a unique ID to each incoming connection.
    connection_id_counter: AtomicUsize,
    /// Maintains a mapping of `ShardId --> [ConnectionId -> Tx]`.
    shard_subscribers: Arc<
        RwLock<
            BTreeMap<ShardId, BTreeMap<usize, UnboundedSender<Result<ProtoPubSubMessage, Status>>>>,
        >,
    >,
    /// Active connections.
    connections: Arc<RwLock<HashSet<usize>>>,
    /// Server-side metrics.
    metrics: Arc<PubSubServerMetrics>,
}

impl PubSubState {
    fn new_connection(
        self: Arc<Self>,
        notifier: UnboundedSender<Result<ProtoPubSubMessage, Status>>,
    ) -> PubSubConnection {
        let connection_id = self.connection_id_counter.fetch_add(1, Ordering::SeqCst);
        {
            let mut connections = self.connections.write().expect("lock");
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
                num_sent += 1;
                let _ = tx.send(Ok(req));
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
        notifier: UnboundedSender<Result<ProtoPubSubMessage, Status>>,
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
}

/// An active connection of [PubSubState].
///
/// When dropped, removes itself from [PubSubState], clearing all of its subscriptions.
#[derive(Debug)]
pub(crate) struct PubSubConnection {
    connection_id: usize,
    notifier: UnboundedSender<Result<ProtoPubSubMessage, Status>>,
    state: Arc<PubSubState>,
}

impl PubSubConnection {
    pub(crate) fn push_diff(&self, shard_id: &ShardId, data: &VersionedData) {
        self.state.push_diff(self.connection_id, shard_id, data)
    }

    pub(crate) fn subscribe(&self, shard_id: &ShardId) {
        self.state
            .subscribe(self.connection_id, self.notifier.clone(), shard_id)
    }

    pub(crate) fn unsubscribe(&self, shard_id: &ShardId) {
        self.state.unsubscribe(self.connection_id, shard_id)
    }
}

impl PubSubSender for PubSubConnection {
    fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData) {
        PubSubConnection::push_diff(self, shard_id, diff)
    }

    fn subscribe(self: Arc<Self>, shard_id: &ShardId) -> Arc<ShardSubscriptionToken> {
        PubSubConnection::subscribe(self.as_ref(), shard_id);

        let pubsub_sender = Arc::clone(&self);
        let pubsub_sender: Arc<dyn PubSubSender> = pubsub_sender;

        Arc::new(ShardSubscriptionToken {
            shard_id: shard_id.clone(),
            pubsub_sender,
        })
    }

    fn unsubscribe(&self, shard_id: &ShardId) {
        PubSubConnection::unsubscribe(self, shard_id);
    }
}

impl Drop for PubSubConnection {
    fn drop(&mut self) {
        self.state.remove_connection(self.connection_id)
    }
}

/// Entrypoint for running a Persist PubSub gRPC service.
#[derive(Debug)]
pub struct PersistService {
    state: Arc<PubSubState>,
}

impl PersistService {
    /// Creates a new [PersistService].
    pub fn new(metrics: PubSubServerMetrics) -> Self {
        PersistService {
            state: Arc::new(PubSubState {
                connection_id_counter: AtomicUsize::new(0),
                shard_subscribers: Default::default(),
                connections: Default::default(),
                metrics: Arc::new(metrics),
            }),
        }
    }

    /// Creates a direct [PubSubClientConnection] to this service.
    pub fn new_direct_client(&self) -> PubSubClientConnection {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let sender: Arc<dyn PubSubSender> = Arc::new(Arc::clone(&self.state).new_connection(tx));

        PubSubClientConnection {
            sender,
            receiver: Box::new(
                UnboundedReceiverStream::new(rx)
                    .filter_map(|x| Some(x.expect("cannot receive grpc errors locally"))),
            ),
        }
    }
}

#[async_trait]
impl proto_persist_pub_sub_server::ProtoPersistPubSub for PersistService {
    type PubSubStream = Pin<Box<dyn Stream<Item = Result<ProtoPubSubMessage, Status>> + Send>>;

    async fn pub_sub(
        &self,
        req: Request<Streaming<ProtoPubSubMessage>>,
    ) -> Result<Response<Self::PubSubStream>, Status> {
        let root_span = info_span!("persist::push::server");
        let _guard = root_span.enter();
        let caller_id = req
            .metadata()
            .get(AsciiMetadataKey::from_static(PERSIST_PUBSUB_CALLER_KEY))
            .map(|key| key.to_str().ok())
            .flatten()
            .map(|key| key.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        info!("incoming push from: {:?}", caller_id);

        let mut in_stream = req.into_inner();

        // WIP not unbounded
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // WIP: store the handles in a map somewhere? what would remove them
        let server_state = Arc::clone(&self.state);
        // this spawn here to cleanup after connection error / disconnect, otherwise the stream
        // would not be polled after the connection drops. in our case, we want to clear the
        // connection and its subscriptions from our shared state when it drops.
        tokio::spawn(async move {
            let root_span = info_span!("persist::push::server_conn");
            let _guard = root_span.enter();

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
                        info!("server received diff");
                        let shard_id = req.shard_id.parse().expect("valid shard id");
                        let diff = VersionedData {
                            seqno: req.seqno.into_rust().expect("WIP"),
                            data: req.diff.clone(),
                        };
                        connection.push_diff(&shard_id, &diff);
                    }
                    Some(proto_pub_sub_message::Message::Subscribe(diff)) => {
                        let shard_id = diff.shard_id.parse().expect("valid shard id");
                        connection.subscribe(&shard_id);
                    }
                    Some(proto_pub_sub_message::Message::Unsubscribe(diff)) => {
                        let shard_id = diff.shard_id.parse().expect("valid shard id");
                        connection.unsubscribe(&shard_id);
                    }
                }
            }
            info!("push stream to {} ended", caller_id);
        });

        let out_stream: Self::PubSubStream = Box::pin(UnboundedReceiverStream::new(rx));
        Ok(Response::new(out_stream))
    }
}

#[cfg(test)]
mod pubsub_state {
    use std::sync::Arc;

    use bytes::Bytes;
    use mz_ore::collections::HashSet;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tonic::Status;

    use mz_persist::location::{SeqNo, VersionedData};
    use mz_proto::RustType;

    use crate::internal::service::proto_pub_sub_message::Message;
    use crate::internal::service::ProtoPubSubMessage;
    use crate::rpc::PubSubState;
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
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
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

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
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

        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let conn1 = Arc::clone(&state).new_connection(tx1);

        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
        let conn2 = Arc::clone(&state).new_connection(tx2);

        let (tx3, mut rx3) = tokio::sync::mpsc::unbounded_channel();
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
        rx: &mut UnboundedReceiver<Result<ProtoPubSubMessage, Status>>,
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
