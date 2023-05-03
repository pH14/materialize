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
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::transport::{Channel, Endpoint, Error};
use tonic::{Extensions, Request};
use tracing::{debug, error, info, warn};

use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::location::VersionedData;
use mz_proto::RustType;

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
    ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error>;
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
            self.subscribe_inner(shard_id);
        }
    }

    fn subscribe_inner(&self, shard: &ShardId) {
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
        match self.requests.send(msg) {
            Ok(_) => {
                self.metrics.pubsub_client.sender.subscribe.succeeded.inc();
                self.metrics
                    .pubsub_client
                    .sender
                    .subscribe
                    .bytes_sent
                    .inc_by(u64::cast_from(size));
                info!("subscribed to {}", shard);
            }
            Err(err) => {
                self.metrics.pubsub_client.sender.subscribe.failed.inc();
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
        match self.requests.send(msg) {
            Ok(i) => {
                self.metrics.pubsub_client.sender.push.succeeded.inc();
                self.metrics
                    .pubsub_client
                    .sender
                    .push
                    .bytes_sent
                    .inc_by(u64::cast_from(size));
                info!("pushed to {} listeners", i);
                debug!("pushed ({}, {})", shard_id, seqno);
            }
            Err(err) => {
                self.metrics.pubsub_client.sender.push.failed.inc();
                error!("{}", err);
            }
        }
    }

    fn subscribe(&self, shard: &ShardId) {
        self.subscribes.lock().expect("lock").insert(*shard);
        self.subscribe_inner(shard);
    }

    fn unsubscribe(&self, shard: &ShardId) {
        self.subscribes.lock().expect("lock").remove(shard);

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
        match self.requests.send(msg) {
            Ok(_) => {
                self.metrics
                    .pubsub_client
                    .sender
                    .unsubscribe
                    .succeeded
                    .inc();
                self.metrics
                    .pubsub_client
                    .sender
                    .unsubscribe
                    .bytes_sent
                    .inc_by(u64::cast_from(size));
                debug!("unsubscribed from {}", shard);
            }
            Err(err) => {
                self.metrics.pubsub_client.sender.unsubscribe.failed.inc();
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
//
// #[derive(Clone, Debug)]
// pub struct PersistReconnectionSender {
//     sender: Arc<RwLock<Option<PubSubSenderClient>>>,
//     subscribes: Arc<Mutex<BTreeSet<ShardId>>>,
// }
//
// impl PubSubSender for PersistReconnectionSender {
//     fn push(&self, shard_id: &ShardId, diff: &VersionedData) {
//         if let Some(sender) = self.sender.read().expect("lock") {
//             sender.push(shard_id, diff);
//         }
//     }
//
//     fn subscribe(&self, shard_id: &ShardId) {
//         if let Some(sender) = self.sender.get() {
//             sender.subscribe(shard_id);
//         } else {
//             self.subscribes.lock().expect("lock").insert(*shard_id);
//         }
//     }
//
//     fn unsubscribe(&self, shard_id: &ShardId) {
//         if let Some(sender) = self.sender.get() {
//             sender.unsubscribe(shard_id);
//         } else {
//             self.subscribes.lock().expect("lock").remove(shard_id);
//         }
//     }
// }

#[async_trait]
impl PersistPubSub for PersistPubSubClient {
    async fn connect(
        config: PersistPubSubClientConfig,
        metrics: Arc<Metrics>,
    ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error> {
        // we use a broadcast so the Sender does not need to be replaced // if the underlying connection is swapped out
        let (rpc_requests, rpc_responses) = tokio::sync::broadcast::channel(100);
        drop(rpc_responses);

        let sender = Arc::new(PubSubSenderClient {
            metrics: Arc::clone(&metrics),
            requests: rpc_requests.clone(),
            subscribes: Default::default(),
        });

        let (receiver_input, receiver_output) = tokio::sync::mpsc::channel(100);
        let requestz = rpc_requests.clone();
        let senderz = Arc::clone(&sender);
        mz_ore::task::spawn(|| format!("persist_pubsub_client_connection"), async move {
            let mut metadata = MetadataMap::new();
            metadata.insert(
                AsciiMetadataKey::from_static(PERSIST_PUBSUB_CALLER_KEY),
                AsciiMetadataValue::try_from(&config.caller_id)
                    .unwrap_or_else(|_| AsciiMetadataValue::from_static("unknown")),
            );

            loop {
                let mut broadcast = BroadcastStream::new(requestz.subscribe());
                let pubsub_request = Request::from_parts(
                    metadata.clone(),
                    Extensions::default(),
                    async_stream::stream! {
                        while let Some(x) = broadcast.next().await {
                            match x {
                                Ok(x) => yield x,
                                // WIP: metrics for broadcast recv errors
                                Err(x) => {}
                            }
                        }
                    }, // BroadcastStream::new(rpc_responses.resubscribe()).filter_map(|x| async {
                       //     match x {
                       //         Ok(x) => Some(x),
                       //         // WIP: metrics for broadcast recv errors
                       //         Err(x) => None,
                       //     }
                       // }),
                );

                // WIP: count connections made
                // WIP: connect with retries and a timeout
                let client = mz_ore::retry::Retry::default()
                    .clamp_backoff(Duration::from_secs(60))
                    .retry_async(|_| async {
                        info!("connecting to pubsub");
                        let endpoint = Endpoint::from_str(&config.addr)
                            .expect("valid pubsub addr")
                            .timeout(Duration::from_secs(5));
                        ProtoPersistPubSubClient::connect(endpoint).await
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
                let mut responses = client
                    .pub_sub(pubsub_request)
                    .await
                    .expect("WIP")
                    .into_inner();

                senderz.reconnect();
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

        Ok((sender, Box::new(ReceiverStream::new(receiver_output))))
    }
}
