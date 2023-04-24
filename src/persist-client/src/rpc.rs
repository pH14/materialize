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
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use futures_util::StreamExt;
use thiserror::Error;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tonic::{Response, Streaming};
use tracing::warn;

use mz_persist::location::VersionedData;
use mz_proto::RustType;

use crate::cache::PersistClientCache;
use crate::internal::service::proto_persist_pub_sub_client::ProtoPersistPubSubClient;
use crate::internal::service::proto_persist_pub_sub_server::ProtoPersistPubSubServer;
use crate::internal::service::proto_pub_sub_message::Message;
use crate::internal::service::{PersistService, ProtoPubSubMessage, ProtoPushDiff, ProtoSubscribe};
use crate::ShardId;

/// WIP
#[async_trait]
pub trait PersistPubSubClient {
    /// Receive handles with which to push and subscribe to diffs.
    async fn connect(
        addr: String,
    ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error>;
}

/// WIP
#[async_trait]
pub trait PubSubSender: std::fmt::Debug + Send + Sync {
    /// Push a diff to subscribers.
    /// WIP: fix error type
    async fn push(&self, shard_id: &ShardId, diff: &VersionedData) -> Result<(), Error>;

    /// Informs the server which shards this subscribed should receive diffs for.
    /// May be called at any time to update the set of subscribed shards.
    async fn subscribe(&self, shards: Vec<ShardId>) -> Result<(), Error>;
}

pub trait PubSubReceiver: Stream<Item = ProtoPubSubMessage> + Send + Unpin {}

impl<T> PubSubReceiver for T where T: Stream<Item = ProtoPubSubMessage> + Send + Unpin {}

#[derive(Copy, Clone, Debug, Error)]
pub enum Error {
    #[error("push request dropped")]
    PushDropped,
}

#[derive(Debug)]
pub struct PersistPubSubServer {
    service: PersistService,
}

impl PersistPubSubServer {
    pub fn new(cache: &PersistClientCache) -> Self {
        let service = PersistService::new(Arc::clone(&cache.state_cache));
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

pub(crate) struct PushedDiffFn(pub Box<dyn Fn(VersionedData) + Send + Sync>);

impl std::fmt::Debug for PushedDiffFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushedDiffFn").finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct PubSubSenderClient {
    reqs: tokio::sync::mpsc::UnboundedSender<ProtoPubSubMessage>,
}

#[async_trait]
impl PubSubSender for PubSubSenderClient {
    async fn push(&self, shard_id: &ShardId, diff: &VersionedData) -> Result<(), Error> {
        let diff = ProtoPushDiff {
            shard_id: shard_id.into_proto(),
            seqno: diff.seqno.into_proto(),
            diff: diff.data.clone(),
        };

        match self.reqs.send(ProtoPubSubMessage {
            message: Some(Message::PushDiff(diff)),
        }) {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::PushDropped),
        }
    }

    async fn subscribe(&self, shards: Vec<ShardId>) -> Result<(), Error> {
        match self.reqs.send(ProtoPubSubMessage {
            message: Some(Message::Subscribe(ProtoSubscribe {
                shards: shards.into_iter().map(|s| s.to_string()).collect(),
            })),
        }) {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::PushDropped),
        }
    }
}

#[derive(Debug)]
pub struct PersistPubSubClientImpl;

#[async_trait]
impl PersistPubSubClient for PersistPubSubClientImpl {
    async fn connect(
        addr: String,
    ) -> Result<(Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>), anyhow::Error> {
        let mut client = ProtoPersistPubSubClient::connect(addr).await?;
        // WIP not unbounded.
        let (requests, responses) = tokio::sync::mpsc::unbounded_channel();
        let responses = client
            .pub_sub(UnboundedReceiverStream::new(responses))
            .await?;

        let sender = PubSubSenderClient { reqs: requests };
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
