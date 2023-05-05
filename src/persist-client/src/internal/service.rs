// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::clone_on_ref_ptr, clippy::disallowed_methods)] // Generated code does this

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use prost::Message;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::AsciiMetadataKey;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, info_span, warn};

use mz_ore::cast::CastFrom;
use mz_ore::collections::HashSet;
use mz_persist::location::VersionedData;
use mz_proto::{ProtoType, RustType};

use crate::internal::metrics::PubSubServerMetrics;
use crate::rpc::{PubSubReceiver, PubSubSender, ShardSubscriptionToken, PERSIST_PUBSUB_CALLER_KEY};
use crate::ShardId;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.internal.service.rs"
));

/// Internal state of a PubSub server implementation.
#[derive(Debug)]
pub(crate) struct PubSubState {
    /// Assigns a unique ID to each incoming connection.
    connection_id_counter: AtomicUsize,
    /// Maintains a mapping of `ShardId --> [ConnectionId -> Tx]`.
    shard_subscribers: Arc<
        RwLock<
            BTreeMap<String, BTreeMap<usize, UnboundedSender<Result<ProtoPubSubMessage, Status>>>>,
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

        info!("cleaning up state from: {}", connection_id);
        {
            let mut connections = self.connections.write().expect("lock");
            assert!(connections.remove(&connection_id));
        }

        {
            // WIP: is it OK to lock this to probe every shard?
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

    fn push_diff(&self, connection_id: usize, shard_id: String, data: &VersionedData) {
        let now = Instant::now();
        self.metrics.push_call_count.inc();

        assert!(self
            .connections
            .read()
            .expect("lock")
            .contains(&connection_id));

        let (num_sent, data_size) = {
            let subscribers = self.shard_subscribers.read().expect("lock poisoned");

            match subscribers.get(&shard_id) {
                None => (0, 0),
                Some(subscribed_connections) => {
                    let mut num_sent = 0;
                    let mut data_size = 0;
                    for (subscribed_conn_id, tx) in subscribed_connections {
                        // skip sending the diff back to the original sender
                        if *subscribed_conn_id == connection_id {
                            continue;
                        }
                        // debug!(
                        //     "server forwarding req to {} conns {} {} {}",
                        //     subscribed_conn_id,
                        //     &req.shard_id,
                        //     diff.seqno,
                        //     diff.data.len()
                        // );
                        let req = ProtoPubSubMessage {
                            timestamp: SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .expect("failed to get millis since epoch")
                                .as_micros()
                                .to_le_bytes()
                                .to_vec(),
                            message: Some(proto_pub_sub_message::Message::PushDiff(
                                ProtoPushDiff {
                                    seqno: data.seqno.into_proto(),
                                    shard_id: shard_id.to_string(),
                                    diff: Bytes::clone(&data.data),
                                },
                            )),
                        };
                        data_size = req.encoded_len();
                        num_sent += 1;
                        let _ = tx.send(Ok(req));
                    }
                    (num_sent, data_size)
                }
            }
        };

        self.metrics.broadcasted_diff_count.inc_by(num_sent);
        self.metrics
            .broadcasted_diff_bytes
            .inc_by(num_sent * u64::cast_from(data_size));
        self.metrics
            .push_seconds
            .inc_by(now.elapsed().as_secs_f64());
    }

    fn subscribe(
        &self,
        connection_id: usize,
        notifier: UnboundedSender<Result<ProtoPubSubMessage, Status>>,
        shard_id: String,
    ) {
        let now = Instant::now();
        self.metrics.subscribe_call_count.inc();

        assert!(self
            .connections
            .read()
            .expect("lock")
            .contains(&connection_id));

        // info!("conn {} adding subscription to {}", caller_id, diff.shard);
        {
            let mut subscribed_shards = self.shard_subscribers.write().expect("lock poisoned");
            subscribed_shards
                .entry(shard_id)
                .or_default()
                .insert(connection_id, notifier);
        }
        // current_subscriptions.insert(diff.shard);
        self.metrics
            .subscribe_seconds
            .inc_by(now.elapsed().as_secs_f64());
    }

    fn unsubscribe(&self, connection_id: usize, shard_id: &String) {
        let now = Instant::now();
        self.metrics.unsubscribe_call_count.inc();

        assert!(self
            .connections
            .read()
            .expect("lock")
            .contains(&connection_id));
        // info!(
        //                     "conn {} removing subscription from {}",
        //                     caller_id, diff.shard
        //                 );
        {
            let mut subscribed_shards = self.shard_subscribers.write().expect("lock poisoned");
            if let Some(subscribed_connections) = subscribed_shards.get_mut(shard_id) {
                subscribed_connections.remove(&connection_id);
            }
        }
        // current_subscriptions.remove(&diff.shard);
        self.metrics
            .unsubscribe_seconds
            .inc_by(now.elapsed().as_secs_f64());
    }
}

#[derive(Debug)]
pub(crate) struct PubSubConnection {
    connection_id: usize,
    notifier: UnboundedSender<Result<ProtoPubSubMessage, Status>>,
    state: Arc<PubSubState>,
}

impl PubSubConnection {
    pub(crate) fn push_diff(&self, shard_id: String, data: &VersionedData) {
        self.state.push_diff(self.connection_id, shard_id, data)
    }

    pub(crate) fn subscribe(&self, shard_id: String) {
        self.state
            .subscribe(self.connection_id, self.notifier.clone(), shard_id)
    }

    pub(crate) fn unsubscribe(&self, shard_id: &String) {
        self.state.unsubscribe(self.connection_id, shard_id)
    }
}

impl PubSubSender for PubSubConnection {
    fn push(&self, shard_id: &ShardId, diff: &VersionedData) {
        // WIP: fix up the String vs ShardId
        PubSubConnection::push_diff(self, shard_id.to_string(), diff)
    }

    fn subscribe(self: Arc<Self>, shard: &ShardId) -> Arc<ShardSubscriptionToken> {
        PubSubConnection::subscribe(self.as_ref(), shard.to_string());

        let pubsub_sender = Arc::clone(&self);
        let pubsub_sender: Arc<dyn PubSubSender> = pubsub_sender;

        Arc::new(ShardSubscriptionToken {
            shard_id: shard.clone(),
            pubsub_sender,
        })
    }

    fn unsubscribe(&self, shard: &ShardId) {
        PubSubConnection::unsubscribe(self, &shard.to_string());
    }
}

impl Drop for PubSubConnection {
    fn drop(&mut self) {
        self.state.remove_connection(self.connection_id)
    }
}

#[derive(Debug)]
pub struct PersistService {
    state: Arc<PubSubState>,
}

impl PersistService {
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

    pub fn new_direct_client(&self) -> (Arc<dyn PubSubSender>, Box<dyn PubSubReceiver>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let sender: Arc<dyn PubSubSender> = Arc::new(Arc::clone(&self.state).new_connection(tx));
        (
            sender,
            Box::new(
                UnboundedReceiverStream::new(rx)
                    .filter_map(|x| Some(x.expect("cannot receive grpc errors locally"))),
            ),
        )
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
                        let diff = VersionedData {
                            seqno: req.seqno.into_rust().expect("WIP"),
                            data: req.diff.clone(),
                        };
                        connection.push_diff(req.shard_id, &diff);
                    }
                    Some(proto_pub_sub_message::Message::Subscribe(diff)) => {
                        connection.subscribe(diff.shard);
                    }
                    Some(proto_pub_sub_message::Message::Unsubscribe(diff)) => {
                        connection.unsubscribe(&diff.shard);
                    }
                }
            }
            info!("push stream to {} ended", caller_id);
        });

        let out_stream: Self::PubSubStream = Box::pin(UnboundedReceiverStream::new(rx));
        Ok(Response::new(out_stream))
    }
}
