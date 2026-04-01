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
//! The ShardedService uses it to obtain handles when the partition map changes.
//!
//! In monolithic mode, the factory spawns in-process tokio tasks and caches
//! handles so repeated calls for the same shard return clones. In multi-process
//! mode, the factory connects to already-running processes via gRPC.

use std::collections::BTreeMap;
use std::sync::Mutex;

use timely::progress::Antichain;

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::{PersistClient, ShardId};

use crate::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
use crate::{Acceptor, AcceptorConfig, Learner, RangeAssignment};

// ---------------------------------------------------------------------------
// ActorFactory trait
// ---------------------------------------------------------------------------

/// Factory for creating acceptor and learner handles.
///
/// Implementations must be idempotent: calling `create_acceptor` twice for the
/// same shard returns a handle to the same actor (not a duplicate).
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
}

// ---------------------------------------------------------------------------
// InProcessActorFactory
// ---------------------------------------------------------------------------

/// Factory that spawns actors as in-process tokio tasks and caches handles.
///
/// Repeated calls for the same shard return a clone of the existing handle.
/// This allows both the metashard (which spawns actors during reconfiguration)
/// and the ShardedService (which needs handles for routing) to share the same
/// factory without spawning duplicate actors.
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
        Ok(handle)
    }
}
