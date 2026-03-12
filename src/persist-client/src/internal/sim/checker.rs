// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Post-hoc correctness checking for DST simulation runs.
//!
//! Validates invariants over the recorded operation log:
//! - Write uniqueness: every committed write has a unique timestamp
//! - Write ordering: committed writes form a total order by timestamp
//! - Read consistency: snapshot reads return exactly the updates committed at or below `as_of`
//! - Liveness: at least some writes and reads completed

use std::collections::BTreeMap;

use tracing::info;

use super::workload::{CommittedWrite, CompletedRead, OperationLog};

/// Configuration for liveness assertions.
pub struct LivenessConfig {
    /// Minimum number of writes that must have been committed.
    pub min_writes: usize,
    /// Minimum number of reads that must have completed.
    pub min_reads: usize,
}

impl Default for LivenessConfig {
    fn default() -> Self {
        Self {
            min_writes: 1,
            min_reads: 0,
        }
    }
}

/// Check all invariants over the operation log from a simulation run.
///
/// Panics on invariant violations with a descriptive message.
pub fn check_invariants(log: &OperationLog, liveness: &LivenessConfig) {
    info!(
        "checking invariants: {} writes, {} reads",
        log.writes.len(),
        log.reads.len()
    );

    check_write_uniqueness(&log.writes);
    check_write_ordering(&log.writes);
    check_read_consistency(&log.writes, &log.reads);
    check_liveness(log, liveness);
}

/// Every committed write must have a unique `write_ts`. Two writers cannot
/// both successfully `compare_and_append` at the same timestamp because CaS
/// on the shard upper is serialized through consensus.
fn check_write_uniqueness(writes: &[CommittedWrite]) {
    let mut seen: BTreeMap<u64, &CommittedWrite> = BTreeMap::new();
    for w in writes {
        if let Some(prev) = seen.insert(w.write_ts, w) {
            panic!(
                "Write uniqueness violated: two writes committed at ts={}:\n  \
                 client-{}: ({}, {})\n  \
                 client-{}: ({}, {})",
                w.write_ts,
                prev.client_id,
                prev.key,
                prev.val,
                w.client_id,
                w.key,
                w.val,
            );
        }
    }
}

/// Committed writes, when sorted by timestamp, must form a strictly increasing
/// sequence (no gaps are required, but no duplicates).
fn check_write_ordering(writes: &[CommittedWrite]) {
    let mut sorted: Vec<u64> = writes.iter().map(|w| w.write_ts).collect();
    sorted.sort();
    for window in sorted.windows(2) {
        assert!(
            window[0] < window[1],
            "Write ordering violated: ts {} is not strictly less than ts {}",
            window[0],
            window[1],
        );
    }
}

/// For each read at `as_of = T`, the returned data (consolidated) must be
/// consistent with the set of committed writes at `ts <= T`.
///
/// Specifically, the consolidated state at time T is the multiset union of all
/// diffs from writes with `write_ts <= T`. Since our workload only appends
/// `diff = +1`, the expected state is a set of `(key, val)` pairs.
fn check_read_consistency(writes: &[CommittedWrite], reads: &[CompletedRead]) {
    for read in reads {
        // Build expected state: all writes with write_ts <= as_of.
        let mut expected: BTreeMap<(String, String), i64> = BTreeMap::new();
        for w in writes {
            if w.write_ts <= read.as_of {
                *expected.entry((w.key.clone(), w.val.clone())).or_default() += 1;
            }
        }
        // Remove zero entries.
        expected.retain(|_, d| *d != 0);

        // Build actual state from the read data.
        let mut actual: BTreeMap<(String, String), i64> = BTreeMap::new();
        for (k, v, _t, d) in &read.data {
            *actual.entry((k.clone(), v.clone())).or_default() += d;
        }
        actual.retain(|_, d| *d != 0);

        if expected != actual {
            // Find differences for a useful error message.
            let mut missing = Vec::new();
            let mut extra = Vec::new();

            for (kv, &exp_d) in &expected {
                let act_d = actual.get(kv).copied().unwrap_or(0);
                if exp_d != act_d {
                    missing.push(format!("  {:?}: expected diff={}, got diff={}", kv, exp_d, act_d));
                }
            }
            for (kv, &act_d) in &actual {
                if !expected.contains_key(kv) {
                    extra.push(format!("  {:?}: unexpected diff={}", kv, act_d));
                }
            }

            panic!(
                "Read consistency violated for client-{} at as_of={}:\n\
                 Missing or wrong:\n{}\n\
                 Extra:\n{}",
                read.client_id,
                read.as_of,
                missing.join("\n"),
                extra.join("\n"),
            );
        }
    }
}

/// Liveness: ensure the simulation actually did useful work.
fn check_liveness(log: &OperationLog, config: &LivenessConfig) {
    assert!(
        log.writes.len() >= config.min_writes,
        "Liveness: only {} writes committed, expected at least {}",
        log.writes.len(),
        config.min_writes,
    );
    assert!(
        log.reads.len() >= config.min_reads,
        "Liveness: only {} reads completed, expected at least {}",
        log.reads.len(),
        config.min_reads,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_write_uniqueness_ok() {
        let writes = vec![
            CommittedWrite { client_id: 0, key: "k0".into(), val: "v0".into(), write_ts: 0 },
            CommittedWrite { client_id: 1, key: "k1".into(), val: "v1".into(), write_ts: 1 },
        ];
        check_write_uniqueness(&writes);
    }

    #[test]
    #[should_panic(expected = "Write uniqueness violated")]
    fn test_check_write_uniqueness_dup() {
        let writes = vec![
            CommittedWrite { client_id: 0, key: "k0".into(), val: "v0".into(), write_ts: 0 },
            CommittedWrite { client_id: 1, key: "k1".into(), val: "v1".into(), write_ts: 0 },
        ];
        check_write_uniqueness(&writes);
    }

    #[test]
    fn test_check_read_consistency_ok() {
        let writes = vec![
            CommittedWrite { client_id: 0, key: "k0".into(), val: "v0".into(), write_ts: 0 },
            CommittedWrite { client_id: 0, key: "k0".into(), val: "v1".into(), write_ts: 1 },
            CommittedWrite { client_id: 1, key: "k1".into(), val: "v2".into(), write_ts: 2 },
        ];
        let reads = vec![CompletedRead {
            client_id: 0,
            as_of: 1,
            data: vec![
                ("k0".into(), "v0".into(), 0, 1),
                ("k0".into(), "v1".into(), 1, 1),
            ],
        }];
        check_read_consistency(&writes, &reads);
    }

    #[test]
    #[should_panic(expected = "Read consistency violated")]
    fn test_check_read_consistency_missing() {
        let writes = vec![
            CommittedWrite { client_id: 0, key: "k0".into(), val: "v0".into(), write_ts: 0 },
        ];
        // Read claims nothing exists, but a write at ts=0 should be visible.
        let reads = vec![CompletedRead {
            client_id: 0,
            as_of: 0,
            data: vec![],
        }];
        check_read_consistency(&writes, &reads);
    }
}
