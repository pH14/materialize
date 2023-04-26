// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, dead_code)] // WIP

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use futures_util::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use prost::Message;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap};
use tonic::{Extensions, Request};
use tracing::{debug, error, info, warn};

use crate::internal::metrics::{PubSubClientCallMetrics, PubSubClientMetrics, PubSubServerMetrics};
use mz_persist::location::VersionedData;
use mz_proto::RustType;

use crate::internal::service::proto_persist_pub_sub_client::ProtoPersistPubSubClient;
use crate::internal::service::proto_persist_pub_sub_server::ProtoPersistPubSubServer;
use crate::internal::service::{
    proto_pub_sub_message, PersistService, ProtoPubSubMessage, ProtoPushDiff, ProtoSubscribe,
    ProtoUnsubscribe,
};
use crate::metrics::Metrics;
use crate::ShardId;

/// WIP
#[async_trait]
pub trait PersistPubSub {
    /// Receive handles with which to push and subscribe to diffs.
    async fn connect(
        addr: String,
        caller_id: String,
    ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error>;
}

/// The send-side client to Persist PubSub.
pub trait PubSubSender: std::fmt::Debug + Send + Sync {
    /// Push a diff to subscribers.
    fn push(&self, shard_id: &ShardId, diff: &VersionedData, metrics: &PubSubClientCallMetrics);

    /// Subscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    /// This call is idempotent and is a no-op for already subscribed shards.
    fn subscribe(&self, shard: &ShardId, metrics: &PubSubClientCallMetrics);

    /// Unsubscribe the corresponding [PubSubReceiver] to diffs for the given shard.
    /// This call is idempotent and is a no-op for already unsubscribed shards.
    fn unsubscribe(&self, shard: &ShardId, metrics: &PubSubClientCallMetrics);
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
    requests: tokio::sync::mpsc::UnboundedSender<ProtoPubSubMessage>,
}

#[async_trait]
impl PubSubSender for PubSubSenderClient {
    fn push(&self, shard_id: &ShardId, diff: &VersionedData, metrics: &PubSubClientCallMetrics) {
        let seqno = diff.seqno.clone();
        let diff = ProtoPushDiff {
            shard_id: shard_id.into_proto(),
            seqno: diff.seqno.into_proto(),
            diff: diff.data.clone(),
        };
        let msg = ProtoPubSubMessage {
            message: Some(proto_pub_sub_message::Message::PushDiff(diff)),
        };
        let size = msg.encoded_len();
        match self.requests.send(msg) {
            Ok(_) => {
                metrics.succeeded.inc();
                metrics.bytes_sent.inc_by(u64::cast_from(size));
                debug!("pushed ({}, {})", shard_id, seqno);
            }
            Err(err) => {
                metrics.failed.inc();
                error!("{}", err);
            }
        }
    }

    fn subscribe(&self, shard: &ShardId, metrics: &PubSubClientCallMetrics) {
        let msg = ProtoPubSubMessage {
            message: Some(proto_pub_sub_message::Message::Subscribe(ProtoSubscribe {
                shard: shard.to_string(),
            })),
        };
        let size = msg.encoded_len();
        match self.requests.send(msg) {
            Ok(_) => {
                metrics.succeeded.inc();
                metrics.bytes_sent.inc_by(u64::cast_from(size));
                debug!("subscribed to {}", shard);
            }
            Err(err) => {
                metrics.failed.inc();
                error!("error subscribing to {}: {}", shard, err);
            }
        }
    }

    fn unsubscribe(&self, shard: &ShardId, metrics: &PubSubClientCallMetrics) {
        let msg = ProtoPubSubMessage {
            message: Some(proto_pub_sub_message::Message::Unsubscribe(
                ProtoUnsubscribe {
                    shard: shard.to_string(),
                },
            )),
        };
        let size = msg.encoded_len();
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

/// A [PersistPubSub] implementation backed by gRPC.
#[derive(Debug)]
pub struct PersistPubSubClient;

#[async_trait]
impl PersistPubSub for PersistPubSubClient {
    async fn connect(
        addr: String,
        caller_id: String,
    ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error> {
        // WIP: connect with retries and a timeout
        let mut client = ProtoPersistPubSubClient::connect(addr.clone()).await?;
        info!("Created pubsub client to: {:?}", addr);
        // WIP not unbounded.
        let (requests, responses) = tokio::sync::mpsc::unbounded_channel();
        let mut metadata = MetadataMap::new();
        metadata.insert(
            AsciiMetadataKey::from_static(PERSIST_PUBSUB_CALLER_KEY),
            AsciiMetadataValue::try_from(caller_id)
                .unwrap_or_else(|_| AsciiMetadataValue::from_static("unknown")),
        );
        let pubsub_request = Request::from_parts(
            metadata,
            Extensions::default(),
            UnboundedReceiverStream::new(responses),
        );
        let responses = client.pub_sub(pubsub_request).await?;

        let sender = PubSubSenderClient { requests };
        let receiver = responses
            .into_inner()
            .filter_map(|res| async move {
                match res {
                    Ok(message) => Some(message),
                    Err(err) => {
                        // WIP: metric coverage here
                        warn!("pubsub client received err: {:?}", err);
                        None
                    }
                }
            })
            .boxed();

        Ok((Arc::new(sender), Box::new(receiver)))
    }
}
