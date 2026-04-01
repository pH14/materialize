// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Service directory: maps shard IDs to actor addresses.
//!
//! Every actor in the shared log uses the directory to discover its peers:
//!
//! - **ShardedService** resolves acceptor and learner addresses for routing.
//! - **Acceptor** resolves learner addresses to query retractions directly.
//! - **Learner** resolves its acceptor to connect as a persist pubsub client.
//! - **Everyone** resolves the metashard to subscribe for partition map updates.
//!
//! The `ServiceDirectory` trait abstracts over deployment modes:
//!
//! - **In-process**: addresses are shard IDs — the `InProcessActorFactory`
//!   serves as the handle registry.
//! - **Multi-process**: addresses are Unix domain socket paths.
//! - **Kubernetes**: addresses are DNS names.

use mz_persist_client::ShardId;

/// Maps shard IDs to actor addresses. Metashard and acceptor are singletons
/// per shard; learners can have multiple replicas.
pub trait ServiceDirectory: Send + Sync + 'static {
    /// An address that can be used to connect to an actor.
    type Addr: Clone + Send + Sync + std::fmt::Debug;

    /// Address of the metashard actor (singleton).
    fn metashard_addr(&self) -> Self::Addr;

    /// Address of the acceptor for the given log shard (singleton).
    fn acceptor_addr(&self, shard_id: ShardId) -> Self::Addr;

    /// Addresses of all learner replicas for the given log shard.
    fn learner_addrs(&self, shard_id: ShardId) -> Vec<Self::Addr>;
}

// ---------------------------------------------------------------------------
// InProcessDirectory
// ---------------------------------------------------------------------------

/// In-process directory where addresses are shard IDs.
///
/// In the monolithic binary, every actor lives in the same process. The
/// `InProcessActorFactory` caches handles by shard ID, so the "address" is
/// just the shard ID itself — the factory resolves it to a channel handle.
///
/// The metashard shard ID is fixed at construction. Learner replica count
/// is always 1 for now.
pub struct InProcessDirectory {
    metashard_shard_id: ShardId,
}

impl InProcessDirectory {
    pub fn new(metashard_shard_id: ShardId) -> Self {
        InProcessDirectory { metashard_shard_id }
    }
}

impl ServiceDirectory for InProcessDirectory {
    type Addr = ShardId;

    fn metashard_addr(&self) -> ShardId {
        self.metashard_shard_id
    }

    fn acceptor_addr(&self, shard_id: ShardId) -> ShardId {
        shard_id
    }

    fn learner_addrs(&self, shard_id: ShardId) -> Vec<ShardId> {
        vec![shard_id]
    }
}
