// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A shared log for Materialize persist consensus.
//!
//! Architecture follows Balakrishnan's shared log decomposition:
//!
//! - **Acceptor**: blind group commit. Receives proposals, batches them, flushes
//!   to a persist shard via `compare_and_append`. Returns receipts. Stateless
//!   w.r.t. shard data.
//! - **Learner**: state machine. Tails the persist shard, evaluates CAS during
//!   playback, maintains materialized state, serves reads and result queries.
//! - **Metashard actor**: maintains the range-based partition map, coordinates
//!   reconfigurations, manages acceptor/learner lifecycle.
//! - **Serving layer**: routes client requests to the correct acceptor/learner
//!   based on the cached partition map.
//!
//! Batches independent cross-shard proposals into a single durable persist
//! `compare_and_append` per flush, making cost O(1/batch_window) instead of
//! O(shards).
//!
//! For horizontal write scaling, client shards are range-partitioned across
//! multiple log shards (each with its own acceptor). See
//! `doc/reference/05_horizontal_sharding.md` for the full specification.

use mz_persist::generated::consensus_service::{
    ProtoAppendResponse, ProtoCompareAndSetResponse, ProtoHeadResponse, ProtoLogProposal,
    ProtoScanResponse, ProtoTruncateResponse,
};
use mz_persist_client::ShardId;

pub mod fault;
pub mod metrics;
pub mod persist_log;
pub mod service;
pub mod sharded_service;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the acceptor.
#[derive(Debug, Clone)]
pub struct AcceptorConfig {
    /// Depth of the command channel (mpsc queue).
    pub queue_depth: usize,
}

impl Default for AcceptorConfig {
    fn default() -> Self {
        AcceptorConfig { queue_depth: 4096 }
    }
}

// ---------------------------------------------------------------------------
// Partition map
// ---------------------------------------------------------------------------

/// Derive a partition byte from a client shard key.
///
/// Uses the first byte of the ShardId's hex UUID (characters 1-2 after the
/// `s` prefix). ShardIds are UUIDs so the distribution is uniform.
pub fn partition_key(client_shard: &str) -> u8 {
    if client_shard.len() >= 3 {
        u8::from_str_radix(&client_shard[1..3], 16).unwrap_or(0)
    } else {
        0
    }
}

/// A non-overlapping, covering partition of the [0x00, 0xFF] key space.
///
/// Each range maps to a log shard. The partition map is the single source of
/// truth for both write routing (client → acceptor) and read routing
/// (client → learner).
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionMap {
    /// Monotonically increasing configuration epoch.
    pub epoch: u64,
    /// Sorted, non-overlapping, covering ranges.
    pub ranges: Vec<RangeAssignment>,
}

/// A single range in the partition map.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeAssignment {
    /// Inclusive lower bound of the partition key range.
    pub lo: u8,
    /// Exclusive upper bound. 0x100 (256) for the last range, covering through 0xFF.
    pub hi_exclusive: u16,
    /// Log shard that accepts writes for this range.
    pub log_shard: ShardId,
}

impl PartitionMap {
    /// Create a single-range partition map covering the entire key space,
    /// pointing at `log_shard`. This is the genesis configuration.
    pub fn single(log_shard: ShardId) -> Self {
        PartitionMap {
            epoch: 0,
            ranges: vec![RangeAssignment {
                lo: 0x00,
                hi_exclusive: 0x100,
                log_shard,
            }],
        }
    }

    /// Route a client shard key to its log shard.
    pub fn route(&self, client_shard: &str) -> ShardId {
        let key = partition_key(client_shard);
        self.route_key(key)
    }

    /// Route a partition key byte to its log shard.
    pub fn route_key(&self, key: u8) -> ShardId {
        for r in &self.ranges {
            if key >= r.lo && u16::from(key) < r.hi_exclusive {
                return r.log_shard;
            }
        }
        // Invariant: the partition map is covering. If we get here, it's a bug.
        panic!(
            "partition map does not cover key 0x{:02x}: {:?}",
            key, self.ranges
        );
    }

    /// Validate partition map invariants: sorted, non-overlapping, covering.
    pub fn validate(&self) -> Result<(), String> {
        if self.ranges.is_empty() {
            return Err("partition map is empty".into());
        }
        if self.ranges[0].lo != 0x00 {
            return Err(format!(
                "first range starts at 0x{:02x}, expected 0x00",
                self.ranges[0].lo
            ));
        }
        let last = self.ranges.last().unwrap();
        if last.hi_exclusive != 0x100 {
            return Err(format!(
                "last range ends at 0x{:03x}, expected 0x100",
                last.hi_exclusive
            ));
        }
        for i in 1..self.ranges.len() {
            let prev = &self.ranges[i - 1];
            let curr = &self.ranges[i];
            if prev.hi_exclusive != u16::from(curr.lo) {
                return Err(format!(
                    "gap or overlap between ranges: prev.hi=0x{:03x}, curr.lo=0x{:02x}",
                    prev.hi_exclusive, curr.lo
                ));
            }
        }
        Ok(())
    }
}

/// Immutable identity for an acceptor or learner, assigned at creation.
#[derive(Debug, Clone, PartialEq)]
pub struct ActorIdentity {
    /// The range this actor serves.
    pub range: RangeAssignment,
    /// The log shard this actor reads/writes.
    pub log_shard: ShardId,
    /// The partition map epoch that created this actor.
    pub epoch: u64,
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Error returned by acceptor handle methods.
#[derive(Debug)]
pub enum AcceptorError {
    /// The acceptor's command channel was closed (acceptor shut down).
    Shutdown,
    /// The acceptor dropped the reply sender without responding.
    DroppedReply,
    /// The acceptor returned an application-level error.
    Command(String),
    /// The log shard has been sealed (frontier advanced to empty antichain).
    /// The serving layer should refresh its partition map and retry.
    Sealed,
}

impl std::fmt::Display for AcceptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptorError::Shutdown => write!(f, "acceptor shut down"),
            AcceptorError::DroppedReply => write!(f, "acceptor dropped reply"),
            AcceptorError::Command(msg) => write!(f, "{}", msg),
            AcceptorError::Sealed => write!(f, "log shard sealed"),
        }
    }
}

/// Error returned by learner handle methods.
#[derive(Debug)]
pub enum LearnerError {
    /// The learner's command channel was closed (learner shut down).
    Shutdown,
    /// The learner dropped the reply sender without responding.
    DroppedReply,
    /// The learner returned an application-level error.
    Command(String),
}

impl std::fmt::Display for LearnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LearnerError::Shutdown => write!(f, "learner shut down"),
            LearnerError::DroppedReply => write!(f, "learner dropped reply"),
            LearnerError::Command(msg) => write!(f, "{}", msg),
        }
    }
}

/// Error returned by metashard handle methods.
#[derive(Debug)]
pub enum MetashardError {
    /// The metashard actor shut down.
    Shutdown,
    /// The metashard actor dropped the reply sender.
    DroppedReply,
    /// Application-level error.
    Command(String),
    /// A reconfiguration was attempted but another is already in progress.
    ReconfigurationInProgress,
    /// The expected epoch did not match the current epoch.
    EpochMismatch { expected: u64, actual: u64 },
}

impl std::fmt::Display for MetashardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetashardError::Shutdown => write!(f, "metashard shut down"),
            MetashardError::DroppedReply => write!(f, "metashard dropped reply"),
            MetashardError::Command(msg) => write!(f, "{}", msg),
            MetashardError::ReconfigurationInProgress => {
                write!(f, "reconfiguration already in progress")
            }
            MetashardError::EpochMismatch { expected, actual } => {
                write!(f, "epoch mismatch: expected {}, actual {}", expected, actual)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait Acceptor: Clone + std::fmt::Debug + Send + Sync + 'static {
    async fn append(
        &self,
        proposal: ProtoLogProposal,
    ) -> Result<ProtoAppendResponse, AcceptorError>;
}

#[async_trait::async_trait]
pub trait Learner: Clone + std::fmt::Debug + Send + Sync + 'static {
    async fn head(&self, key: String) -> Result<ProtoHeadResponse, LearnerError>;
    async fn scan(
        &self,
        key: String,
        from: u64,
        limit: u64,
    ) -> Result<ProtoScanResponse, LearnerError>;
    async fn list_keys(&self) -> Result<Vec<String>, LearnerError>;
    async fn await_cas_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoCompareAndSetResponse, LearnerError>;
    async fn await_truncate_result(
        &self,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoTruncateResponse, LearnerError>;
}

/// The metashard maintains the partition map and coordinates reconfigurations.
///
/// In steady state, the metashard serves lookups from its in-memory partition
/// map. During reconfiguration, it coordinates the full lifecycle: intent →
/// pre-hydrate → seal → commit → finalize.
#[async_trait::async_trait]
pub trait Metashard: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Look up which log shard owns a client shard.
    async fn lookup(&self, client_shard: &str) -> Result<ShardId, MetashardError>;

    /// Return the current partition map.
    async fn partition_map(&self) -> Result<PartitionMap, MetashardError>;

    /// Current epoch.
    async fn current_epoch(&self) -> Result<u64, MetashardError>;

    /// Execute a reconfiguration: install a new partition map, spawning new
    /// actors and sealing old log shards. Returns the new epoch on success.
    async fn reconfigure(&self, plan: ReconfigurationPlan) -> Result<u64, MetashardError>;
}

/// A plan for a reconfiguration operation.
#[derive(Debug, Clone)]
pub struct ReconfigurationPlan {
    /// The epoch the caller believes is current (CaS-like guard).
    pub expected_epoch: u64,
    /// The new partition map to install.
    pub new_partition_map: PartitionMap,
}

// ---------------------------------------------------------------------------
// Partition map unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod partition_map_tests {
    use super::*;

    fn test_shard(suffix: &str) -> ShardId {
        // ShardId::new() generates random IDs; for deterministic tests, parse a known one.
        format!("s{:0>32}", suffix)
            .parse()
            .expect("valid shard id")
    }

    #[test]
    fn partition_key_extracts_first_byte() {
        // "s0a..." → 0x0a = 10
        assert_eq!(partition_key("s0a000000-0000-0000-0000-000000000000"), 0x0a);
        // "sff..." → 0xff = 255
        assert_eq!(partition_key("sff000000-0000-0000-0000-000000000000"), 0xff);
        // "s00..." → 0x00 = 0
        assert_eq!(partition_key("s00000000-0000-0000-0000-000000000000"), 0x00);
    }

    #[test]
    fn single_range_covers_all() {
        let shard = test_shard("1");
        let map = PartitionMap::single(shard);
        assert!(map.validate().is_ok());
        // Every key routes to the single shard.
        for key in 0..=255u8 {
            assert_eq!(map.route_key(key), shard);
        }
    }

    #[test]
    fn two_range_split() {
        let s1 = test_shard("1");
        let s2 = test_shard("2");
        let map = PartitionMap {
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
        };
        assert!(map.validate().is_ok());
        assert_eq!(map.route_key(0x00), s1);
        assert_eq!(map.route_key(0x7f), s1);
        assert_eq!(map.route_key(0x80), s2);
        assert_eq!(map.route_key(0xff), s2);
    }

    #[test]
    fn validate_catches_gap() {
        let s1 = test_shard("1");
        let s2 = test_shard("2");
        let map = PartitionMap {
            epoch: 0,
            ranges: vec![
                RangeAssignment {
                    lo: 0x00,
                    hi_exclusive: 0x40,
                    log_shard: s1,
                },
                // Gap: 0x40..0x80 unmapped
                RangeAssignment {
                    lo: 0x80,
                    hi_exclusive: 0x100,
                    log_shard: s2,
                },
            ],
        };
        assert!(map.validate().is_err());
    }

    #[test]
    fn validate_catches_incomplete_coverage() {
        let s1 = test_shard("1");
        let map = PartitionMap {
            epoch: 0,
            ranges: vec![RangeAssignment {
                lo: 0x00,
                hi_exclusive: 0x80,
                log_shard: s1,
            }],
        };
        assert!(map.validate().is_err());
    }

    /// PM1 + PM2 + PM3: multi-way split covers the full range with no gaps or
    /// overlaps, and epochs increase monotonically through a reconfiguration
    /// sequence.
    #[test]
    fn multi_way_split_and_reconfig_sequence() {
        let s1 = test_shard("1");
        let s2 = test_shard("2");
        let s3 = test_shard("3");
        let s4 = test_shard("4");

        // Epoch 0: single shard.
        let map0 = PartitionMap::single(s1);
        assert!(map0.validate().is_ok());
        assert_eq!(map0.epoch, 0);

        // Epoch 1: split into 4 ranges.
        let map1 = PartitionMap {
            epoch: 1,
            ranges: vec![
                RangeAssignment { lo: 0x00, hi_exclusive: 0x40, log_shard: s1 },
                RangeAssignment { lo: 0x40, hi_exclusive: 0x80, log_shard: s2 },
                RangeAssignment { lo: 0x80, hi_exclusive: 0xC0, log_shard: s3 },
                RangeAssignment { lo: 0xC0, hi_exclusive: 0x100, log_shard: s4 },
            ],
        };
        assert!(map1.validate().is_ok());
        assert!(map1.epoch > map0.epoch); // PM3: monotonic

        // Every key routes to exactly one shard.
        for key in 0..=255u8 {
            let shard = map1.route_key(key);
            let expected = match key {
                0x00..=0x3F => s1,
                0x40..=0x7F => s2,
                0x80..=0xBF => s3,
                0xC0..=0xFF => s4,
            };
            assert_eq!(shard, expected, "key 0x{key:02x} routed to wrong shard");
        }
    }

    /// Validate boundary keys route correctly at range edges.
    #[test]
    fn boundary_key_routing() {
        let s1 = test_shard("1");
        let s2 = test_shard("2");
        let s3 = test_shard("3");
        let map = PartitionMap {
            epoch: 0,
            ranges: vec![
                RangeAssignment { lo: 0x00, hi_exclusive: 0x55, log_shard: s1 },
                RangeAssignment { lo: 0x55, hi_exclusive: 0xAA, log_shard: s2 },
                RangeAssignment { lo: 0xAA, hi_exclusive: 0x100, log_shard: s3 },
            ],
        };
        assert!(map.validate().is_ok());

        // Boundary keys.
        assert_eq!(map.route_key(0x54), s1); // last key in range 1
        assert_eq!(map.route_key(0x55), s2); // first key in range 2
        assert_eq!(map.route_key(0xA9), s2); // last key in range 2
        assert_eq!(map.route_key(0xAA), s3); // first key in range 3
        assert_eq!(map.route_key(0xFF), s3); // last key in range 3
    }

    /// Validate that validate() catches an empty map.
    #[test]
    fn validate_catches_empty_map() {
        let map = PartitionMap {
            epoch: 0,
            ranges: vec![],
        };
        assert!(map.validate().is_err());
    }

    /// Validate that validate() catches a map that doesn't start at 0x00.
    #[test]
    fn validate_catches_wrong_start() {
        let s1 = test_shard("1");
        let map = PartitionMap {
            epoch: 0,
            ranges: vec![RangeAssignment {
                lo: 0x10,
                hi_exclusive: 0x100,
                log_shard: s1,
            }],
        };
        assert!(map.validate().is_err());
    }

    /// Test partition_key with short/empty inputs.
    #[test]
    fn partition_key_edge_cases() {
        assert_eq!(partition_key(""), 0);
        assert_eq!(partition_key("s"), 0);
        assert_eq!(partition_key("s0"), 0);
        assert_eq!(partition_key("sZZ"), 0); // invalid hex → 0
    }
}
