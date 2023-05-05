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
use tracing::{debug, info, info_span, warn};

use mz_ore::cast::CastFrom;
use mz_ore::collections::HashSet;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::location::VersionedData;
use mz_proto::{ProtoType, RustType};

use crate::internal::metrics::PubSubServerMetrics;
use crate::rpc::{
    PubSubClientConnection, PubSubSender, ShardSubscriptionToken, PERSIST_PUBSUB_CALLER_KEY,
};
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

        let (num_sent, data_size) = {
            let subscribers = self.shard_subscribers.read().expect("lock poisoned");

            match subscribers.get(shard_id) {
                None => (0, 0),
                Some(subscribed_connections) => {
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
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use mz_ore::collections::HashSet;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tonic::Status;

    use mz_persist::location::{SeqNo, VersionedData};
    use mz_proto::RustType;

    use crate::internal::service::proto_pub_sub_message::Message;
    use crate::internal::service::{ProtoPubSubMessage, PubSubState};
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
