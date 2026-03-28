// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metashard actor: maintains the partition map, coordinates reconfigurations,
//! and manages acceptor/learner lifecycle.
//!
//! The metashard is backed by a persist shard that stores the partition map and
//! reconfiguration intents durably. The actor materializes this into an
//! in-memory [`MetashardState`] and serves lookups from it.
//!
//! Follows the same actor pattern as the acceptor and learner: a passive state
//! machine driven by a command channel and a persist subscription, with a handle
//! type for sending commands.
//!
//! In Phase 1 (foundation), the metashard holds a static partition map — no
//! reconfiguration yet. The reconfiguration protocol (intent, pre-hydrate,
//! seal, commit, finalize) will be added in Phase 4.

use std::collections::BTreeMap;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use mz_persist_client::ShardId;

use crate::{MetashardError, PartitionMap, RangeAssignment};

// ---------------------------------------------------------------------------
// Metashard state
// ---------------------------------------------------------------------------

/// Log shard status in the metashard.
#[derive(Debug, Clone, PartialEq)]
pub enum LogShardStatus {
    /// Actively accepting writes.
    Active,
    /// Sealed (upper = []), no more writes. Data still readable.
    Sealed,
    /// Finalized: snapshot downstream covers this shard's state, data can be
    /// compacted away.
    Finalized,
}

/// Per-log-shard metadata tracked by the metashard.
#[derive(Debug, Clone)]
pub struct LogShardInfo {
    pub status: LogShardStatus,
    pub epoch_created: u64,
    pub epoch_sealed: Option<u64>,
    pub range: RangeAssignment,
    /// The log shard this one succeeded for overlapping ranges.
    pub predecessor: Option<ShardId>,
    /// Whether this shard contains T=0 snapshot entries from its predecessor.
    pub has_snapshot: bool,
}

/// The metashard actor's in-memory materialized state.
#[derive(Debug, Clone)]
pub struct MetashardState {
    /// Current configuration epoch.
    pub epoch: u64,
    /// The authoritative partition map.
    pub partition_map: PartitionMap,
    /// Per-log-shard metadata.
    pub log_shards: BTreeMap<ShardId, LogShardInfo>,
}

impl MetashardState {
    /// Create initial state with a single log shard covering the entire range.
    pub fn single(log_shard: ShardId) -> Self {
        let range = RangeAssignment {
            lo: 0x00,
            hi_exclusive: 0x100,
            log_shard,
        };
        let mut log_shards = BTreeMap::new();
        log_shards.insert(
            log_shard,
            LogShardInfo {
                status: LogShardStatus::Active,
                epoch_created: 0,
                epoch_sealed: None,
                range: range.clone(),
                predecessor: None,
                has_snapshot: false,
            },
        );
        MetashardState {
            epoch: 0,
            partition_map: PartitionMap::single(log_shard),
            log_shards,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands dispatched to the metashard actor.
pub enum MetashardCommand {
    /// Look up which log shard owns a client shard.
    Lookup {
        client_shard: String,
        reply: oneshot::Sender<Result<ShardId, MetashardError>>,
    },
    /// Return the current partition map.
    GetPartitionMap {
        reply: oneshot::Sender<Result<PartitionMap, MetashardError>>,
    },
    /// Return the current epoch.
    GetEpoch {
        reply: oneshot::Sender<Result<u64, MetashardError>>,
    },
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// A typed handle to the metashard actor's command channel.
#[derive(Debug, Clone)]
pub struct PersistMetashardHandle {
    tx: mpsc::Sender<MetashardCommand>,
}

impl PersistMetashardHandle {
    pub fn new(tx: mpsc::Sender<MetashardCommand>) -> Self {
        PersistMetashardHandle { tx }
    }
}

#[async_trait::async_trait]
impl crate::Metashard for PersistMetashardHandle {
    async fn lookup(&self, client_shard: &str) -> Result<ShardId, MetashardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetashardCommand::Lookup {
                client_shard: client_shard.to_string(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| MetashardError::Shutdown)?;
        reply_rx.await.map_err(|_| MetashardError::DroppedReply)?
    }

    async fn partition_map(&self) -> Result<PartitionMap, MetashardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetashardCommand::GetPartitionMap { reply: reply_tx })
            .await
            .map_err(|_| MetashardError::Shutdown)?;
        reply_rx.await.map_err(|_| MetashardError::DroppedReply)?
    }

    async fn current_epoch(&self) -> Result<u64, MetashardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(MetashardCommand::GetEpoch { reply: reply_tx })
            .await
            .map_err(|_| MetashardError::Shutdown)?;
        reply_rx.await.map_err(|_| MetashardError::DroppedReply)?
    }
}

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

/// The metashard actor.
///
/// Maintains an in-memory [`MetashardState`] and serves commands from the
/// handle. In Phase 1, the state is static (no persist shard backing, no
/// reconfiguration). Phase 4 will add persist shard subscription and the
/// full reconfiguration protocol.
pub struct PersistMetashardActor {
    state: MetashardState,
    rx: mpsc::Receiver<MetashardCommand>,
}

impl PersistMetashardActor {
    /// Create a new metashard actor with a static partition map.
    pub fn new(
        state: MetashardState,
        queue_depth: usize,
    ) -> (Self, PersistMetashardHandle) {
        let (tx, rx) = mpsc::channel(queue_depth);
        let actor = PersistMetashardActor { state, rx };
        let handle = PersistMetashardHandle::new(tx);
        (actor, handle)
    }

    /// Handle a single command.
    fn on_command(&self, cmd: MetashardCommand) {
        match cmd {
            MetashardCommand::Lookup {
                client_shard,
                reply,
            } => {
                let result = Ok(self.state.partition_map.route(&client_shard));
                let _ = reply.send(result);
            }
            MetashardCommand::GetPartitionMap { reply } => {
                let _ = reply.send(Ok(self.state.partition_map.clone()));
            }
            MetashardCommand::GetEpoch { reply } => {
                let _ = reply.send(Ok(self.state.epoch));
            }
        }
    }

    /// Run the actor loop until the command channel closes.
    pub async fn run(mut self) {
        info!(
            epoch = self.state.epoch,
            num_ranges = self.state.partition_map.ranges.len(),
            num_log_shards = self.state.log_shards.len(),
            "metashard actor starting"
        );

        loop {
            match self.rx.recv().await {
                Some(cmd) => {
                    debug!("metashard command received");
                    self.on_command(cmd);
                }
                None => {
                    info!("metashard actor shutting down (channel closed)");
                    break;
                }
            }
        }
    }

    /// Spawn the metashard actor as a tokio task.
    pub fn spawn(
        state: MetashardState,
        queue_depth: usize,
    ) -> (PersistMetashardHandle, mz_ore::task::JoinHandle<()>) {
        let (actor, handle) = Self::new(state, queue_depth);
        let task = mz_ore::task::spawn(|| "persist-metashard", actor.run());
        (handle, task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Metashard;

    fn test_shard(suffix: &str) -> ShardId {
        format!("s{:0>32}", suffix)
            .parse()
            .expect("valid shard id")
    }

    #[tokio::test]
    async fn metashard_lookup_routes_correctly() {
        let s1 = test_shard("1");
        let s2 = test_shard("2");
        let state = MetashardState {
            epoch: 1,
            partition_map: PartitionMap {
                epoch: 1,
                ranges: vec![
                    RangeAssignment {
                        lo: 0x00,
                        hi_exclusive: 0x80,
                        log_shard: s1,
                    },
                    RangeAssignment {
                        lo: 0x80,
                        hi_exclusive: 0x100,
                        log_shard: s2,
                    },
                ],
            },
            log_shards: BTreeMap::new(),
        };

        let (handle, _task) = PersistMetashardActor::spawn(state, 64);

        // "s0a..." → partition key 0x0a → first range → s1
        let result = handle
            .lookup("s0a000000-0000-0000-0000-000000000000")
            .await
            .unwrap();
        assert_eq!(result, s1);

        // "sff..." → partition key 0xff → second range → s2
        let result = handle
            .lookup("sff000000-0000-0000-0000-000000000000")
            .await
            .unwrap();
        assert_eq!(result, s2);

        // Verify epoch
        assert_eq!(handle.current_epoch().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn metashard_returns_partition_map() {
        let s1 = test_shard("1");
        let state = MetashardState::single(s1);

        let (handle, _task) = PersistMetashardActor::spawn(state.clone(), 64);

        let map = handle.partition_map().await.unwrap();
        assert_eq!(map, state.partition_map);
    }
}
