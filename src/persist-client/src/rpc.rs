// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use futures::Stream;
use futures_util::StreamExt;
use prost::Message;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::transport::Endpoint;
use tonic::{Extensions, Request};
use tracing::{debug, error, info, warn};

use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::location::VersionedData;
use mz_proto::{ProtoType, RustType};

use crate::cache::StateCache;
use crate::internal::metrics::PubSubServerMetrics;
use crate::internal::service::proto_persist_pub_sub_client::ProtoPersistPubSubClient;
use crate::internal::service::proto_persist_pub_sub_server::ProtoPersistPubSubServer;
use crate::internal::service::{
    proto_pub_sub_message, PersistService, ProtoPubSubMessage, ProtoPushDiff, ProtoSubscribe,
    ProtoUnsubscribe,
};
use crate::metrics::Metrics;
use crate::ShardId;

/// WIP: Persist PubSub
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
/// commands to the PubSub service.
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
        info!(
            "unsubscribing {} due to dropped pubsub token",
            self.shard_id
        );
        self.pubsub_sender.unsubscribe(&self.shard_id)
    }
}

/// A gRPC-based implementation of a Persist PubSub server.
#[derive(Debug)]
pub struct PersistGrpcPubSubServer {
    service: PersistService,
}

impl PersistGrpcPubSubServer {
    pub fn new(metrics_registry: &MetricsRegistry) -> Self {
        let metrics = PubSubServerMetrics::new(metrics_registry);
        let service = PersistService::new(metrics);
        PersistGrpcPubSubServer { service }
    }

    pub fn new_direct_client(&self) -> PubSubClientConnection {
        self.service.new_direct_client()
    }

    pub async fn serve(self, listen_addr: SocketAddr) -> Result<(), anyhow::Error> {
        tonic::transport::Server::builder()
            .add_service(ProtoPersistPubSubServer::new(self.service))
            .serve(listen_addr)
            .await?;
        Ok(())
    }
}

pub const PERSIST_PUBSUB_CALLER_KEY: &str = "persist-pubsub-caller-id";

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

/// WIP: Used to provide client statistics even for a local connection
#[derive(Debug)]
pub struct MetricsPubSubLocalSender {
    metrics: Arc<Metrics>,
    pubsub_sender: Arc<dyn PubSubSender>,
}

impl MetricsPubSubLocalSender {
    pub fn new(pubsub_sender: Arc<dyn PubSubSender>, metrics: Arc<Metrics>) -> Self {
        Self {
            pubsub_sender,
            metrics,
        }
    }
}

impl PubSubSender for MetricsPubSubLocalSender {
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

    fn subscribe(self: Arc<Self>, shard: &ShardId) -> Arc<ShardSubscriptionToken> {
        // WIP: shard vs shard_id
        let token = Arc::clone(&self.pubsub_sender).subscribe(shard);
        self.metrics.pubsub_client.sender.subscribe.succeeded.inc();
        token
    }

    fn unsubscribe(&self, shard: &ShardId) {
        self.pubsub_sender.unsubscribe(shard);
        self.metrics
            .pubsub_client
            .sender
            .unsubscribe
            .succeeded
            .inc();
    }
}
