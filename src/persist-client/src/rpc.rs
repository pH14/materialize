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

use mz_persist::location::VersionedData;
use mz_proto::RustType;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Response, Streaming};
use tracing::warn;

use crate::cache::PersistClientCache;
use crate::internal::service::{
    proto_persist_client, proto_persist_server, PersistService, ProtoPushDiff,
};
use crate::ShardId;

#[derive(Debug)]
pub struct PushServer {
    service: PersistService,
}

impl PushServer {
    pub fn new(cache: &PersistClientCache) -> Self {
        let service = PersistService::new(Arc::clone(&cache.state_cache));
        PushServer { service }
    }

    pub async fn serve(self, listen_addr: SocketAddr) -> Result<(), anyhow::Error> {
        tonic::transport::Server::builder()
            .add_service(proto_persist_server::ProtoPersistServer::new(self.service))
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
pub struct PushClient {
    client: proto_persist_client::ProtoPersistClient<Channel>,
    push_req: tokio::sync::mpsc::UnboundedSender<ProtoPushDiff>,
    push_res: Response<Streaming<ProtoPushDiff>>,
}

impl PushClient {
    pub async fn connect(addr: String) -> Result<Self, anyhow::Error> {
        let mut client = proto_persist_client::ProtoPersistClient::connect(addr).await?;
        // WIP not unbounded.
        let (push_req, push_req_rx) = tokio::sync::mpsc::unbounded_channel();
        // WIP don't do this call until we have something to hook up to the
        // responses
        let push_res = client
            .push(UnboundedReceiverStream::new(push_req_rx))
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
                let res = match res {
                    Ok(x) => x,
                    Err(err) => {
                        warn!("push client received err: {:?}", err);
                        continue;
                    }
                };
                res_fn(res);
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
    client: proto_persist_client::ProtoPersistClient<Channel>,
    push_req: tokio::sync::mpsc::UnboundedSender<ProtoPushDiff>,
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
        let req = ProtoPushDiff {
            shard_id: shard_id.into_proto(),
            seqno: diff.seqno.into_proto(),
            diff: diff.data.clone(),
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
