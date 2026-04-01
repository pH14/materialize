// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Actor factory: abstracts how acceptors and learners are created.
//!
//! The metashard uses this trait during reconfiguration to create new actors.
//! The Router uses it to obtain handles when the partition map changes.
//!
//! In monolithic mode, the factory spawns in-process tokio tasks and caches
//! handles so repeated calls for the same shard return clones. In multi-process
//! mode, the factory connects to already-running processes via gRPC.

use std::collections::BTreeMap;
use std::sync::Mutex;

use timely::progress::Antichain;

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::{PersistClient, ShardId};

use crate::directory::ServiceDirectory;
use crate::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
use crate::rpc::{GrpcAcceptorHandle, GrpcLearnerHandle};
use crate::persist_log::router::ShardedRetractionSource;
use crate::{Acceptor, AcceptorConfig, Learner, RangeAssignment};

// ---------------------------------------------------------------------------
// ActorFactory trait
// ---------------------------------------------------------------------------

/// Factory for creating acceptor and learner handles.
///
/// Implementations must be idempotent: calling `create_acceptor` twice for the
/// same shard returns a handle to the same actor (not a duplicate).
///
/// The factory is responsible for wiring retraction sources: after creating
/// both the acceptor and learner for a shard, it connects the acceptor's
/// retraction polling to the learner.
#[async_trait::async_trait]
pub trait ActorFactory: Send + Sync + 'static {
    type A: Acceptor;
    type L: Learner;

    /// Create (or return an existing) acceptor for the given shard.
    async fn create_acceptor(
        &self,
        shard_id: ShardId,
        epoch: u64,
        predecessors: Vec<(ShardId, Antichain<u64>)>,
        range: RangeAssignment,
    ) -> Result<Self::A, String>;

    /// Create (or return an existing) learner for the given shard.
    async fn create_learner(&self, shard_id: ShardId) -> Result<Self::L, String>;

    /// Stop the acceptor and learner for a retired shard.
    ///
    /// Called after reconfiguration when a shard is sealed and no longer needed.
    /// The default implementation is a no-op (actors just keep running).
    async fn stop_shard(&self, _shard_id: ShardId) {}
}

/// Blanket impl: `Arc<F>` delegates to `F`.
#[async_trait::async_trait]
impl<F: ActorFactory> ActorFactory for std::sync::Arc<F> {
    type A = F::A;
    type L = F::L;

    async fn create_acceptor(
        &self,
        shard_id: ShardId,
        epoch: u64,
        predecessors: Vec<(ShardId, Antichain<u64>)>,
        range: RangeAssignment,
    ) -> Result<Self::A, String> {
        (**self).create_acceptor(shard_id, epoch, predecessors, range).await
    }

    async fn create_learner(&self, shard_id: ShardId) -> Result<Self::L, String> {
        (**self).create_learner(shard_id).await
    }

    async fn stop_shard(&self, shard_id: ShardId) {
        (**self).stop_shard(shard_id).await
    }
}

// ---------------------------------------------------------------------------
// InProcessActorFactory
// ---------------------------------------------------------------------------

/// Factory that spawns actors as in-process tokio tasks and caches handles.
///
/// Repeated calls for the same shard return a clone of the existing handle.
/// After both acceptor and learner are created for a shard, the factory wires
/// the acceptor's retraction source to query the learner directly.
pub struct InProcessActorFactory {
    persist_client: PersistClient,
    acceptors: Mutex<BTreeMap<ShardId, PersistAcceptorHandle>>,
    learners: Mutex<BTreeMap<ShardId, PersistLearnerHandle>>,
}

impl InProcessActorFactory {
    pub fn new(persist_client: PersistClient) -> Self {
        InProcessActorFactory {
            persist_client,
            acceptors: Mutex::new(BTreeMap::new()),
            learners: Mutex::new(BTreeMap::new()),
        }
    }

    /// Wire the acceptor's retraction source to query the learner for the
    /// given shard, if both exist in the cache.
    async fn maybe_wire_retractions(&self, shard_id: ShardId) {
        let (acceptor, learner) = {
            let acceptors = self.acceptors.lock().unwrap();
            let learners = self.learners.lock().unwrap();
            match (acceptors.get(&shard_id), learners.get(&shard_id)) {
                (Some(a), Some(l)) => (a.clone(), l.clone()),
                _ => return,
            }
        };
        let source: Box<dyn crate::RetractionSource> =
            Box::new(ShardedRetractionSource::new(vec![learner]));
        let _ = acceptor.set_retraction_source(source).await;
    }
}

#[async_trait::async_trait]
impl ActorFactory for InProcessActorFactory {
    type A = PersistAcceptorHandle;
    type L = PersistLearnerHandle;

    async fn create_acceptor(
        &self,
        shard_id: ShardId,
        epoch: u64,
        predecessors: Vec<(ShardId, Antichain<u64>)>,
        range: RangeAssignment,
    ) -> Result<PersistAcceptorHandle, String> {
        // Return cached handle if actor already exists.
        if let Some(handle) = self.acceptors.lock().unwrap().get(&shard_id) {
            return Ok(handle.clone());
        }

        let shard_registry = MetricsRegistry::new();
        let acceptor_metrics = AcceptorMetrics::register(&shard_registry);

        let (handle, _task) = PersistAcceptor::spawn(
            AcceptorConfig::default(),
            &self.persist_client,
            shard_id,
            acceptor_metrics,
            epoch,
            Box::new(crate::NoOpRetractionSource),
            predecessors,
            range,
        )
        .await;

        self.acceptors.lock().unwrap().insert(shard_id, handle.clone());
        // Wire retractions if the learner was already created.
        self.maybe_wire_retractions(shard_id).await;
        Ok(handle)
    }

    async fn create_learner(&self, shard_id: ShardId) -> Result<PersistLearnerHandle, String> {
        // Return cached handle if actor already exists.
        if let Some(handle) = self.learners.lock().unwrap().get(&shard_id) {
            return Ok(handle.clone());
        }

        let shard_registry = MetricsRegistry::new();
        let learner_metrics = LearnerMetrics::register(&shard_registry);

        let (handle, _task) = PersistLearner::spawn(
            PersistLearnerConfig::default(),
            &self.persist_client,
            shard_id,
            learner_metrics,
        )
        .await;

        self.learners.lock().unwrap().insert(shard_id, handle.clone());
        // Wire retractions if the acceptor was already created.
        self.maybe_wire_retractions(shard_id).await;
        Ok(handle)
    }
}

// ---------------------------------------------------------------------------
// GrpcActorFactory
// ---------------------------------------------------------------------------

/// Factory that connects to remote acceptor and learner processes via gRPC.
///
/// Used by the standalone router (Router) to create handles to actors
/// running in separate processes. The `ServiceDirectory` resolves shard IDs to
/// network addresses; this factory connects to those addresses with retry.
///
/// Handles are cached: repeated calls for the same shard return a clone of the
/// existing connection rather than opening a new one.
pub struct GrpcActorFactory<D: ServiceDirectory<Addr = String>> {
    directory: D,
    connect_timeout: std::time::Duration,
    acceptors: Mutex<BTreeMap<ShardId, GrpcAcceptorHandle>>,
    learners: Mutex<BTreeMap<ShardId, GrpcLearnerHandle>>,
}

impl<D: ServiceDirectory<Addr = String>> GrpcActorFactory<D> {
    pub fn new(directory: D, connect_timeout: std::time::Duration) -> Self {
        GrpcActorFactory {
            directory,
            connect_timeout,
            acceptors: Mutex::new(BTreeMap::new()),
            learners: Mutex::new(BTreeMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl<D: ServiceDirectory<Addr = String>> ActorFactory for GrpcActorFactory<D> {
    type A = GrpcAcceptorHandle;
    type L = GrpcLearnerHandle;

    async fn create_acceptor(
        &self,
        shard_id: ShardId,
        _epoch: u64,
        _predecessors: Vec<(ShardId, Antichain<u64>)>,
        _range: RangeAssignment,
    ) -> Result<GrpcAcceptorHandle, String> {
        if let Some(handle) = self.acceptors.lock().unwrap().get(&shard_id) {
            return Ok(handle.clone());
        }

        let addr = self.directory.acceptor_addr(shard_id);
        let handle = GrpcAcceptorHandle::connect_with_retry(addr, self.connect_timeout).await?;
        self.acceptors.lock().unwrap().insert(shard_id, handle.clone());
        Ok(handle)
    }

    async fn create_learner(&self, shard_id: ShardId) -> Result<GrpcLearnerHandle, String> {
        if let Some(handle) = self.learners.lock().unwrap().get(&shard_id) {
            return Ok(handle.clone());
        }

        let addrs = self.directory.learner_addrs(shard_id);
        let addr = addrs.into_iter().next().ok_or_else(|| {
            format!("no learner address for shard {shard_id}")
        })?;
        let handle = GrpcLearnerHandle::connect_with_retry(addr, self.connect_timeout).await?;
        self.learners.lock().unwrap().insert(shard_id, handle.clone());
        Ok(handle)
    }
}
