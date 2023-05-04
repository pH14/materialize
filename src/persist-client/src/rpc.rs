// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, dead_code)] // WIP

use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::Stream;
use futures_util::StreamExt;
use prost::Message;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::transport::{Channel, Endpoint, Error};
use tonic::{Extensions, Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use crate::cache::StateCache;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::location::VersionedData;
use mz_proto::{ProtoType, RustType};

use crate::internal::metrics::PubSubServerMetrics;
use crate::internal::service::proto_persist_pub_sub_client::ProtoPersistPubSubClient;
use crate::internal::service::proto_persist_pub_sub_server::ProtoPersistPubSubServer;
use crate::internal::service::{
    proto_pub_sub_message, PersistService, ProtoPubSubMessage, ProtoPushDiff, ProtoSubscribe,
    ProtoUnsubscribe,
};
use crate::metrics::Metrics;
use crate::ShardId;

// WIP: make versions that wrap connection details so we can call into them, but connect later non-blocking like

/// WIP
#[async_trait]
pub trait PersistPubSub {
    /// Receive handles with which to push and subscribe to diffs.
    async fn connect(
        config: PersistPubSubClientConfig,
        metrics: Arc<Metrics>,
        // WIP: do we really want 3 traits here,
        // Arc<dyn PubSubPush>
        // Box<dyn PubSubSubscriber> - make subscribe/unsubscribe take &mut self to serialize
        // Box<dyn PubSubReceiver>
    ) -> (Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>);
}

/// The send-side client to Persist PubSub.
pub trait PubSubSender: std::fmt::Debug + Send + Sync {
    /// Push a diff to subscribers.
    fn push(&self, shard_id: &ShardId, diff: &VersionedData);

    /// Subscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    /// This call is idempotent and is a no-op for already subscribed shards.
    fn subscribe(&self, shard: &ShardId);

    /// Unsubscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    /// This call is idempotent and is a no-op for already unsubscribed shards.
    fn unsubscribe(&self, shard: &ShardId);
}

/// The receive-side client to Persist PubSub.
pub trait PubSubReceiver: Stream<Item = ProtoPubSubMessage> + Send + Unpin {}

impl<T> PubSubReceiver for T where T: Stream<Item = ProtoPubSubMessage> + Send + Unpin {}

#[derive(Debug)]
pub struct PersistPubSubServer {
    service: PersistService,
}

impl PersistPubSubServer {
    pub fn new(metrics_registry: &MetricsRegistry) -> Self {
        let metrics = PubSubServerMetrics::new(metrics_registry);
        let service = PersistService::new(metrics);
        PersistPubSubServer { service }
    }

    pub async fn serve(self, listen_addr: SocketAddr) -> Result<(), anyhow::Error> {
        tonic::transport::Server::builder()
            .add_service(ProtoPersistPubSubServer::new(self.service))
            .serve(listen_addr)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
struct PubSubSenderClient {
    metrics: Arc<Metrics>,
    requests: tokio::sync::broadcast::Sender<ProtoPubSubMessage>,
    subscribes: Arc<Mutex<BTreeSet<ShardId>>>,
}

impl PubSubSenderClient {
    fn reconnect(&self) {
        let subscribes = self.subscribes.lock().expect("lock");
        for shard_id in subscribes.iter() {
            info!("reconnecting to: {}", shard_id);
            self.subscribe_locked(shard_id);
        }
    }

    fn subscribe_locked(&self, shard: &ShardId) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get millis since epoch")
            .as_micros();
        let msg = ProtoPubSubMessage {
            timestamp: now.to_le_bytes().to_vec(),
            message: Some(proto_pub_sub_message::Message::Subscribe(ProtoSubscribe {
                shard: shard.to_string(),
            })),
        };
        let size = msg.encoded_len();
        let metrics = &self.metrics.pubsub_client.sender.subscribe;
        match self.requests.send(msg) {
            Ok(_) => {
                metrics.succeeded.inc();
                metrics.bytes_sent.inc_by(u64::cast_from(size));
                info!("subscribed to {}", shard);
            }
            Err(err) => {
                metrics.failed.inc();
                error!("error subscribing to {}: {}", shard, err);
            }
        }
    }
}

#[async_trait]
impl PubSubSender for PubSubSenderClient {
    fn push(&self, shard_id: &ShardId, diff: &VersionedData) {
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

    fn subscribe(&self, shard: &ShardId) {
        let mut subscribes = self.subscribes.lock().expect("lock");
        subscribes.insert(*shard);
        self.subscribe_locked(shard);
    }

    fn unsubscribe(&self, shard: &ShardId) {
        let mut subscribes = self.subscribes.lock().expect("lock");
        subscribes.remove(shard);

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("failed to get millis since epoch")
            .as_micros();
        let msg = ProtoPubSubMessage {
            timestamp: now.to_le_bytes().to_vec(),
            message: Some(proto_pub_sub_message::Message::Unsubscribe(
                ProtoUnsubscribe {
                    shard: shard.to_string(),
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

pub const PERSIST_PUBSUB_CALLER_KEY: &str = "persist-pubsub-caller-id";

#[derive(Debug)]
pub struct PersistPubSubClientConfig {
    pub addr: String,
    pub caller_id: String,
}

/// A [PersistPubSub] implementation backed by gRPC.
#[derive(Debug)]
pub struct PersistPubSubClient;

#[async_trait]
impl PersistPubSub for PersistPubSubClient {
    async fn connect(
        config: PersistPubSubClientConfig,
        metrics: Arc<Metrics>,
    ) -> (Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>) {
        // WIP: we use a broadcast so the Sender does not need to be replaced if the underlying connection is swapped out
        // drop the initial receiver... we'll create receivers on-demand from our sender in our reconnection loop
        // we should only ever have 1 receiver open at a time
        let (rpc_requests, _) = tokio::sync::broadcast::channel(100);

        let pubsub_sender = Arc::new(PubSubSenderClient {
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

        (
            pubsub_sender,
            Box::new(ReceiverStream::new(receiver_output)),
        )
    }
}

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
                    cache.metrics.pubsub_client.receiver.push_received.inc();
                    let shard_id = diff.shard_id.into_rust().expect("WIP");
                    let diff = VersionedData {
                        seqno: diff.seqno.into_rust().expect("WIP"),
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
