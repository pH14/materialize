// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, dead_code)] // WIP

use async_trait::async_trait;
use futures::Stream;
use std::net::SocketAddr;
use std::sync::Arc;

use mz_persist::location::VersionedData;
use mz_proto::RustType;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Response, Streaming};
use tracing::warn;

use crate::cache::PersistClientCache;
use crate::internal::service::proto_persist_pub_sub_client::ProtoPersistPubSubClient;
use crate::internal::service::proto_persist_pub_sub_server::ProtoPersistPubSubServer;
use crate::internal::service::proto_pub_sub_message::Message;
use crate::internal::service::{PersistService, ProtoPubSubMessage, ProtoPushDiff, ProtoSubscribe};
use crate::ShardId;

/// WIP
#[async_trait]
trait PersistPubSubClient {
    type Sender: PubSubSender;
    type Receiver: Stream<Item = ProtoPubSubMessage>;
    /// Receive handles with which to push and subscribe to diffs.
    async fn connect(addr: &str) -> (Self::Sender, Self::Receiver);
}

/// WIP
#[async_trait]
trait PubSubSender {
    /// Push a diff to subscribers.
    /// WIP: fix error type
    async fn push(&self, diff: ProtoPushDiff) -> Result<(), Error>;

    /// Informs the server which shards this subscribed should receive diffs for.
    /// May be called at any time to update the set of subscribed shards.
    async fn subscribe(&self, shards: Vec<ShardId>) -> Result<(), Error>;
}

#[derive(Copy, Clone, Debug, Error)]
enum Error {
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
pub struct PubSubSenderClient {
    reqs: tokio::sync::mpsc::UnboundedSender<ProtoPubSubMessage>,
}

#[async_trait]
impl PubSubSender for PubSubSenderClient {
    async fn push(&self, diff: ProtoPushDiff) -> Result<(), Error> {
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
pub struct PubSubReceiverClient {
    push_res: Response<Streaming<ProtoPubSubMessage>>,
}

#[derive(Debug)]
pub struct PushClient {
    client: ProtoPersistPubSubClient<Channel>,
    push_req: tokio::sync::mpsc::UnboundedSender<ProtoPubSubMessage>,
    push_res: Response<Streaming<ProtoPubSubMessage>>,
}

impl PushClient {
    pub async fn connect(addr: String) -> Result<Self, anyhow::Error> {
        let mut client = ProtoPersistPubSubClient::connect(addr).await?;
        // WIP not unbounded.
        let (push_req, push_req_rx) = tokio::sync::mpsc::unbounded_channel();
        // WIP don't do this call until we have something to hook up to the
        // responses
        let push_res = client
            .pub_sub(UnboundedReceiverStream::new(push_req_rx))
            .await?;
        Ok(PushClient {
            client,
            push_req,
            push_res,
        })
    }

    pub fn into_conn<F>(self, res_fn: F) -> PushClientConn
    where
        F: Fn(ProtoPushDiff) + Send + Sync + 'static,
    {
        let mut push_res = self.push_res.into_inner();
        let push_res = mz_ore::task::spawn(|| "persist::rpc::push_responses", async move {
            while let Some(res) = push_res.next().await {
                match res {
                    Ok(x) => match x.message {
                        None => {}
                        Some(Message::PushDiff(diff)) => res_fn(diff),
                        Some(Message::Subscribe(resp)) => {
                            warn!("pubsub client received stray subscribe: {:?}", resp);
                        }
                    },
                    Err(err) => {
                        warn!("push client received err: {:?}", err);
                    }
                }
            }
        });
        PushClientConn {
            client: self.client,
            push_req: self.push_req,
            push_res,
        }
    }
}

#[derive(Debug)]
pub struct PushClientConn {
    client: ProtoPersistPubSubClient<Channel>,
    push_req: tokio::sync::mpsc::UnboundedSender<ProtoPubSubMessage>,
    push_res: tokio::task::JoinHandle<()>,
}

impl PushClientConn {
    pub fn push_diff(&self, shard_id: &ShardId, diff: &VersionedData) {
        tracing::info!(
            "pushing diff {} {} {}",
            shard_id,
            diff.seqno,
            diff.data.len()
        );
        let req = ProtoPubSubMessage {
            message: Some(Message::PushDiff(ProtoPushDiff {
                shard_id: shard_id.into_proto(),
                seqno: diff.seqno.into_proto(),
                diff: diff.data.clone(),
            })),
        };
        match self.push_req.send(req) {
            Ok(()) => {}
            Err(_err) => {
                tracing::info!("push_req listener unexpectedly hung up")
            }
        }
    }

    pub async fn finish(self) -> Result<(), anyhow::Error> {
        drop(self.push_req);
        let () = self.push_res.await?;
        Ok(())
    }
}
