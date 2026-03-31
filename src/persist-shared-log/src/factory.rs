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
//! In monolithic mode, actors are spawned as in-process tokio tasks. In
//! multi-process mode, actors are spawned as separate OS processes and
//! accessed via gRPC handles.


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
/// In the monolithic binary, the factory spawns in-process tokio tasks and
/// returns mpsc channel handles. In multi-process mode, the factory spawns
/// OS processes (or connects to already-running processes) and returns gRPC
/// client handles.
///
/// The associated types `A` and `L` must satisfy the `Acceptor` and `Learner`
/// trait bounds so that `RoutingState<F::A, F::L>` and `ShardedService<F::A,
/// F::L>` work generically.
#[async_trait::async_trait]
pub trait ActorFactory: Send + Sync + 'static {
    type A: Acceptor;
    type L: Learner;

    /// Create an acceptor for the given shard.
    async fn create_acceptor(
        &self,
        shard_id: ShardId,
        epoch: u64,
        predecessors: Vec<(ShardId, Antichain<u64>)>,
        range: RangeAssignment,
    ) -> Result<Self::A, String>;

    /// Create a learner for the given shard.
    async fn create_learner(&self, shard_id: ShardId) -> Result<Self::L, String>;
}

// ---------------------------------------------------------------------------
// InProcessActorFactory
// ---------------------------------------------------------------------------

/// Factory that spawns actors as in-process tokio tasks.
///
/// This is the current behavior of the monolithic binary, extracted behind
/// the `ActorFactory` trait so the metashard doesn't directly depend on
/// concrete actor types.
pub struct InProcessActorFactory {
    persist_client: PersistClient,
}

impl InProcessActorFactory {
    pub fn new(persist_client: PersistClient) -> Self {
        InProcessActorFactory { persist_client }
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
        let shard_registry = MetricsRegistry::new();
        let acceptor_metrics = AcceptorMetrics::register(&shard_registry);

        let (handle, _task) = PersistAcceptor::spawn(
            AcceptorConfig::default(),
            &self.persist_client,
            shard_id,
            acceptor_metrics,
            epoch,
            Box::new(crate::NoOpRetractionSource), // Wired to real source separately.
            predecessors,
            range,
        )
        .await;
        Ok(handle)
    }

    async fn create_learner(&self, shard_id: ShardId) -> Result<PersistLearnerHandle, String> {
        let shard_registry = MetricsRegistry::new();
        let learner_metrics = LearnerMetrics::register(&shard_registry);

        let (handle, _task) = PersistLearner::spawn(
            PersistLearnerConfig::default(),
            &self.persist_client,
            shard_id,
            learner_metrics,
        )
        .await;
        Ok(handle)
    }
}
