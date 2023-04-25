// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, dead_code)] // WIP
#![allow(clippy::clone_on_ref_ptr, clippy::disallowed_methods)] // Generated code does this

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use futures::Stream;
use mz_ore::collections::{HashMap, HashSet};
use mz_persist::location::VersionedData;
use mz_proto::ProtoType;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::{IntoStreamingRequest, Request, Response, Status, Streaming};
use tracing::{info, info_span};

use crate::cache::StateCache;
use crate::internal::service::proto_pub_sub_message::Message;
use crate::ShardId;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.internal.service.rs"
));

#[derive(Debug)]
pub struct PersistService {
    /// Assigns a unique ID to each incoming connection.
    connection_id_counter: AtomicUsize,
    /// Maintains a mapping of `ShardId --> [ConnectionId]`.
    shard_subscribers: Arc<RwLock<BTreeMap<ShardId, BTreeSet<usize>>>>,
    /// Maintains a mapping of `ConnectionId --> Tx`.
    conns: Arc<RwLock<HashMap<usize, UnboundedSender<Result<ProtoPubSubMessage, Status>>>>>,
    state_cache: Arc<StateCache>,
}

impl PersistService {
    pub fn new(state_cache: Arc<StateCache>) -> Self {
        PersistService {
            conns: Arc::new(RwLock::new(HashMap::new())),
            shard_subscribers: Default::default(),
            connection_id_counter: AtomicUsize::new(0),
            state_cache,
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
        info!("incoming push");

        let mut in_stream = req.into_inner();

        // WIP not unbounded
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let connection_id = {
            let mut conns = self.conns.write().expect("lock poisoned");
            let connection_id = self.connection_id_counter.fetch_add(1, Ordering::SeqCst);
            conns.insert(connection_id, tx.clone());
            connection_id
        };

        let conns = Arc::clone(&self.conns);
        let subscribers = Arc::clone(&self.shard_subscribers);
        let state_cache = Arc::clone(&self.state_cache);
        // this spawn here is required if you want to handle connection error.
        // If we just map `in_stream` and write it back as `out_stream` the `out_stream`
        // will be drooped when connection error occurs and error will never be propagated
        // to mapped version of `in_stream`.
        tokio::spawn(async move {
            let root_span = info_span!("persist::push::server_conn");
            let _guard = root_span.enter();
            let mut current_subscriptions = BTreeSet::new();

            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => {
                        match req.message {
                            None => {}
                            Some(proto_pub_sub_message::Message::PushDiff(req)) => {
                                let shard_id = req.shard_id.parse().expect("WIP");
                                let diff = VersionedData {
                                    seqno: req.seqno.into_rust().expect("WIP"),
                                    data: req.diff.clone(),
                                };

                                {
                                    let subscribers = subscribers.read().expect("lock poisoned");
                                    match subscribers.get(&shard_id) {
                                        None => {}
                                        Some(subscribed_connections) => {
                                            let conns = conns.read().expect("lock poisoned");
                                            for connection in subscribed_connections {
                                                if *connection == connection_id {
                                                    continue;
                                                }

                                                if let Some(conn) = conns.get(connection) {
                                                    info!(
                                                        "server forwarding req to {} conns {} {} {}",
                                                        connection_id,
                                                        shard_id,
                                                        diff.seqno,
                                                        diff.data.len()
                                                    );
                                                    let req = ProtoPubSubMessage {
                                                        message: Some(Message::PushDiff(
                                                            req.clone(),
                                                        )),
                                                    };
                                                    let _ = conn.send(Ok(req));
                                                }
                                            }
                                        }
                                    }
                                }

                                // also apply it locally
                                state_cache.push_diff(&shard_id, diff);
                            }
                            Some(proto_pub_sub_message::Message::Subscribe(diff)) => {
                                info!("conn {} has subscriptions {:?}", connection_id, diff);

                                let mut subscribed_shards =
                                    subscribers.write().expect("lock poisoned");

                                let new_subscriptions: BTreeSet<ShardId> = diff
                                    .shards
                                    .into_iter()
                                    .map(|shard| shard.parse().expect("WIP"))
                                    .collect();

                                for retracted in
                                    current_subscriptions.difference(&new_subscriptions)
                                {
                                    subscribed_shards
                                        .entry(*retracted)
                                        .or_default()
                                        .remove(&connection_id);
                                }

                                for added in new_subscriptions.difference(&current_subscriptions) {
                                    subscribed_shards
                                        .entry(*added)
                                        .or_default()
                                        .insert(connection_id);
                                }

                                current_subscriptions = new_subscriptions;
                            }
                        }
                    }
                    Err(err) => {
                        if let Some(io_err) = match_for_io_error(&err) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                // here you can handle special case when client
                                // disconnected in unexpected way
                                info!("client disconnected: broken pipe");
                                break;
                            }
                        }

                        match tx.send(Err(err)) {
                            Ok(_) => (),
                            // response was dropped
                            Err(_err) => break,
                        }
                    }
                }
            }

            {
                let mut subscribers = subscribers.write().expect("lock poisoned");
                for shard_id in current_subscriptions {
                    subscribers
                        .entry(shard_id)
                        .or_default()
                        .remove(&connection_id);
                }
            }

            {
                let mut conns = conns.write().expect("lock poisoned");
                conns.remove(&connection_id);
            }

            info!("push stream ended");
        });

        let out_stream: Self::PubSubStream = Box::pin(UnboundedReceiverStream::new(rx));
        Ok(Response::new(out_stream))
    }
}

fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}
