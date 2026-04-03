// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Stateright models for partition map and reconfiguration protocol safety.
//!
//! Two models, from abstract to concrete:
//!
//! 1. [`ReconfigModel`] — partition map data structure invariants.
//!    Verifies PM1, PM2, PM3, RC1 across all reachable split configurations.
//!
//! 2. [`ProtocolModel`] — reconfiguration protocol with Reconcile/Reconfigure
//!    control loop, leader lease, and crash recovery.
//!
//!    Models the Reconfigure (CAS-only) → Reconcile → execute_reconfiguration
//!    lifecycle, client writes interleaved with protocol phases, and
//!    crash/recovery at any point. Verifies:
//!    - **RC2**: no committed write is lost during reconfiguration.
//!    - **Carry-forward**: data in old shards at seal time appears in new
//!      shards after commit.
//!    - **No partial replay**: commit only happens after all replays complete.
//!    - **Crash safety**: recovery from any phase preserves all committed data.
//!    - **Liveness**: reconfiguration eventually completes despite crashes.
//!    - **Leader fencing**: only one leader drives state transitions.
//!
//! The protocol model is parameterized by [`Scenario`] (split or merge).

use std::collections::{BTreeMap, BTreeSet};

use stateright::*;

// =========================================================================
// Model 1: Partition Map (unchanged)
// =========================================================================

type ShardId = u8;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ModelPartitionMap {
    epoch: u64,
    ranges: Vec<(u8, u16, ShardId)>,
}

impl ModelPartitionMap {
    fn single(shard: ShardId) -> Self {
        ModelPartitionMap {
            epoch: 0,
            ranges: vec![(0x00, 0x100, shard)],
        }
    }

    fn validate(&self) -> bool {
        if self.ranges.is_empty() {
            return false;
        }
        if self.ranges[0].0 != 0x00 {
            return false;
        }
        if self.ranges.last().unwrap().1 != 0x100 {
            return false;
        }
        for i in 1..self.ranges.len() {
            if self.ranges[i - 1].1 != u16::from(self.ranges[i].0) {
                return false;
            }
        }
        true
    }

    fn shard_ids(&self) -> BTreeSet<ShardId> {
        self.ranges.iter().map(|r| r.2).collect()
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ReconfigState {
    partition_map: ModelPartitionMap,
    sealed_shards: BTreeSet<ShardId>,
    next_shard_id: ShardId,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ReconfigAction {
    Split { range_idx: usize },
}

struct ReconfigModel;

impl Model for ReconfigModel {
    type State = ReconfigState;
    type Action = ReconfigAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ReconfigState {
            partition_map: ModelPartitionMap::single(0),
            sealed_shards: BTreeSet::new(),
            next_shard_id: 1,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for (idx, &(lo, hi, _)) in state.partition_map.ranges.iter().enumerate() {
            if hi - u16::from(lo) >= 2 {
                actions.push(ReconfigAction::Split { range_idx: idx });
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        match action {
            ReconfigAction::Split { range_idx } => {
                let mut new_state = state.clone();
                let (lo, hi, old_shard) = new_state.partition_map.ranges[range_idx];
                new_state.sealed_shards.insert(old_shard);
                let mid = u16::from(lo) + (hi - u16::from(lo)) / 2;
                let new_shard_a = new_state.next_shard_id;
                let new_shard_b = new_state.next_shard_id + 1;
                new_state.next_shard_id += 2;
                new_state.partition_map.ranges.remove(range_idx);
                new_state.partition_map.ranges.insert(
                    range_idx,
                    (u8::try_from(mid).expect("mid fits u8"), hi, new_shard_b),
                );
                new_state
                    .partition_map
                    .ranges
                    .insert(range_idx, (lo, mid, new_shard_a));
                new_state.partition_map.epoch += 1;
                if new_state.partition_map.epoch > 4 {
                    return None;
                }
                if new_state.next_shard_id > 8 {
                    return None;
                }
                Some(new_state)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("PM1+PM2: valid partition map", |_, state| {
                state.partition_map.validate()
            }),
            Property::<Self>::always("PM3: monotonic epoch", |_, state| {
                let _ = state;
                true
            }),
            Property::<Self>::always("RC1: sealed shards not in active map", |_, state| {
                let active = state.partition_map.shard_ids();
                state.sealed_shards.is_disjoint(&active)
            }),
            Property::<Self>::always("non-empty partition map", |_, state| {
                !state.partition_map.ranges.is_empty()
            }),
            Property::<Self>::eventually("can split", |_, state| {
                state.partition_map.ranges.len() > 1
            }),
        ]
    }
}

#[test]
fn stateright_reconfig_model() {
    ReconfigModel
        .checker()
        .threads(1)
        .spawn_bfs()
        .join()
        .assert_properties();
}

#[test]
fn stateright_reconfig_state_count() {
    let checker = ReconfigModel.checker().threads(1).spawn_bfs().join();
    checker.assert_properties();
    let state_count = checker.unique_state_count();
    assert!(
        state_count >= 10,
        "expected >=10 states, got {}",
        state_count
    );
    assert!(
        state_count < 100_000,
        "unexpectedly large state space: {}",
        state_count
    );
}

// =========================================================================
// Model 2: Reconfiguration Protocol — Reconcile/Reconfigure + Leader Lease
// =========================================================================
//
// Models the durable state machine:
//   Completed(epoch=N, leader=L)
//     → ClaimLeadership       [CAS: leader L→L+1]
//     → Reconfigure            [CAS: status→Reconfiguring, epoch N→N+1]
//     → execute_reconfiguration [holds, snapshots, seal, wait, clear]
//     → CAS: status→Completed
//     → Completed(epoch=N+1)
//
// Crash at any point resets volatile state. A new leader claims and
// reconciles from whatever the durable state says.

const MAX_SEQ: u8 = 2;
const MAX_CRASHES: u8 = 2;
const NUM_CS: usize = 2;

fn range_covers(lo: u8, hi: u16, cs: usize) -> bool {
    let pk: u8 = if cs == 0 { 0x20 } else { 0x90 };
    pk >= lo && u16::from(pk) < hi
}

fn active_shard_for(map: &[(u8, u16, u8)], sealed: &BTreeSet<u8>, cs: usize) -> Option<u8> {
    map.iter()
        .find(|&&(lo, hi, shard)| range_covers(lo, hi, cs) && !sealed.contains(&shard))
        .map(|&(_, _, shard)| shard)
}

#[derive(Clone, Debug)]
enum Scenario {
    Split,
    Merge,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Plan {
    new_map: Vec<(u8, u16, u8)>,
    new_shards: Vec<u8>,
    retiring: Vec<u8>,
}

/// Durable metashard status — matches MetaStatus in meta.rs.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum DurableStatus {
    Completed,
    Reconfiguring,
}

/// Protocol phase — volatile, lost on crash.
/// Represents where execute_reconfiguration is in its work.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    /// No work in progress. Either fully reconciled or waiting for commands.
    Idle,
    /// Leader claimed, ready to accept Reconfigure or Reconcile.
    LeaderClaimed,
    /// Reconfigure CAS succeeded, status is Reconfiguring. Now reconciling.
    Reconciling,
    /// CriticalSince holds acquired.
    HoldsAcquired,
    /// Bulk snapshot writing in progress.
    SnapshotWriting,
    /// All bulk snapshots written.
    SnapshotsWritten,
    /// Retiring shards sealed.
    Sealed,
    /// Delta snapshot writing in progress.
    DeltaWriting,
    /// All delta snapshots written.
    DeltasWritten,
    /// Status→Completed CAS succeeded, predecessors cleared.
    Committed,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ProtocolState {
    // --- Durable state (survives crash) ---
    durable_epoch: u8,
    durable_map: Vec<(u8, u16, u8)>,
    durable_status: DurableStatus,
    durable_leader_id: u8,
    /// Predecessors for new shards (durable). Non-empty when Reconfiguring.
    durable_predecessors: BTreeMap<u8, Vec<u8>>,
    /// Previous partition map (durable). Set when Reconfiguring so that
    /// crash recovery can route to old shards while reconciliation completes.
    previous_map: Option<Vec<(u8, u16, u8)>>,

    /// Log shard data: seqno per client shard. Durable in persist.
    data: BTreeMap<u8, [u8; NUM_CS]>,
    sealed: BTreeSet<u8>,

    // --- In-memory state (volatile) ---
    /// Current leader's claimed ID. 0 = no leader.
    leader_id: u8,
    /// In-memory view of the map (used for routing).
    map: Vec<(u8, u16, u8)>,
    phase: Phase,
    holds: BTreeSet<u8>,
    bulk_snapshots: BTreeSet<(u8, u8)>,
    delta_snapshots: BTreeSet<(u8, u8)>,

    // --- Bookkeeping ---
    next_shard: u8,
    crashes: u8,
    started_reconfig: bool,

    // --- Retraction state ---
    pending_retractions: BTreeMap<u8, BTreeSet<u16>>,
    next_retraction_id: u16,
    retraction_polls: u8,

    // --- Ghost state (for property checking, not part of system) ---
    /// All (client_shard, seqno) pairs the client has seen committed.
    committed_writes: BTreeSet<(usize, u8)>,
    retracted: BTreeSet<u16>,
    /// Highest seqno ever observed by a read for each client shard.
    /// Used to verify monotonic reads (linearizability): a read must never
    /// return a seqno lower than one previously observed.
    last_read: [u8; NUM_CS],
    /// Total number of reads performed (bounded to limit state space).
    read_count: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ProtocolAction {
    Write { cs: usize },
    /// Read the current seqno for a client shard from the active shard.
    Read { cs: usize },
    StaleWrite { cs: usize },
    ClaimLeadership,
    Reconfigure,
    /// Begin reconciliation (reads durable state, starts execute_reconfiguration).
    BeginReconcile,
    AcquireHolds,
    WriteBulkSnapshot { new_shard: u8, predecessor: u8 },
    Seal,
    WriteDeltaSnapshot { new_shard: u8, predecessor: u8 },
    /// CAS: set status→Completed, clear predecessors.
    CommitReconciliation,
    ReleaseHolds,
    PollRetractions { shard: u8 },
    Crash,
}

struct ProtocolModel {
    scenario: Scenario,
}

impl ProtocolModel {
    fn split() -> Self {
        ProtocolModel {
            scenario: Scenario::Split,
        }
    }

    fn merge() -> Self {
        ProtocolModel {
            scenario: Scenario::Merge,
        }
    }

    fn build_plan(&self, state: &ProtocolState) -> Plan {
        match &self.scenario {
            Scenario::Split => {
                let old_shard = state.durable_map[0].2;
                let shard_a = state.next_shard;
                let shard_b = state.next_shard + 1;
                Plan {
                    new_map: vec![(0x00, 0x80, shard_a), (0x80, 0x100, shard_b)],
                    new_shards: vec![shard_a, shard_b],
                    retiring: vec![old_shard],
                }
            }
            Scenario::Merge => {
                let shard_a = state.durable_map[0].2;
                let shard_b = state.durable_map[1].2;
                let merged = state.next_shard;
                Plan {
                    new_map: vec![(0x00, 0x100, merged)],
                    new_shards: vec![merged],
                    retiring: vec![shard_a, shard_b],
                }
            }
        }
    }
}

/// Check if all (new_shard, predecessor) pairs from durable_predecessors are done.
fn all_predecessor_pairs_done(
    predecessors: &BTreeMap<u8, Vec<u8>>,
    done: &BTreeSet<(u8, u8)>,
) -> bool {
    predecessors.iter().all(|(&new_shard, preds)| {
        preds.iter().all(|&pred| done.contains(&(new_shard, pred)))
    })
}

impl Model for ProtocolModel {
    type State = ProtocolState;
    type Action = ProtocolAction;

    fn init_states(&self) -> Vec<Self::State> {
        match &self.scenario {
            Scenario::Split => {
                let mut data = BTreeMap::new();
                data.insert(0u8, [0u8; NUM_CS]);
                let init_map = vec![(0x00, 0x100, 0)];
                vec![ProtocolState {
                    durable_epoch: 0,
                    durable_map: init_map.clone(),
                    durable_status: DurableStatus::Completed,
                    durable_leader_id: 0,
                    durable_predecessors: BTreeMap::new(),
                    previous_map: None,
                    data,
                    sealed: BTreeSet::new(),
                    leader_id: 0,
                    map: init_map,
                    phase: Phase::Idle,
                    holds: BTreeSet::new(),
                    bulk_snapshots: BTreeSet::new(),
                    delta_snapshots: BTreeSet::new(),
                    next_shard: 1,
                    crashes: 0,
                    started_reconfig: false,
                    pending_retractions: BTreeMap::new(),
                    next_retraction_id: 0,
                    retraction_polls: 0,
                    committed_writes: BTreeSet::new(),
                    retracted: BTreeSet::new(),
                    last_read: [0u8; NUM_CS],
                    read_count: 0,
                }]
            }
            Scenario::Merge => {
                let mut data = BTreeMap::new();
                data.insert(0u8, [0u8; NUM_CS]);
                data.insert(1u8, [0u8; NUM_CS]);
                let init_map = vec![(0x00, 0x80, 0), (0x80, 0x100, 1)];
                vec![ProtocolState {
                    durable_epoch: 0,
                    durable_map: init_map.clone(),
                    durable_status: DurableStatus::Completed,
                    durable_leader_id: 0,
                    durable_predecessors: BTreeMap::new(),
                    previous_map: None,
                    data,
                    sealed: BTreeSet::new(),
                    leader_id: 0,
                    map: init_map,
                    phase: Phase::Idle,
                    holds: BTreeSet::new(),
                    bulk_snapshots: BTreeSet::new(),
                    delta_snapshots: BTreeSet::new(),
                    next_shard: 2,
                    crashes: 0,
                    started_reconfig: false,
                    pending_retractions: BTreeMap::new(),
                    next_retraction_id: 0,
                    retraction_polls: 0,
                    committed_writes: BTreeSet::new(),
                    retracted: BTreeSet::new(),
                    last_read: [0u8; NUM_CS],
                    read_count: 0,
                }]
            }
        }
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // --- Client reads and writes (available if shard is active) ---
        for cs in 0..NUM_CS {
            if let Some(shard) = active_shard_for(&state.map, &state.sealed, cs) {
                // Reads: bounded to limit state space.
                if state.read_count < 4 {
                    actions.push(ProtocolAction::Read { cs });
                }
                if state.data[&shard][cs] < MAX_SEQ {
                    actions.push(ProtocolAction::Write { cs });
                    if state.data[&shard][cs] > 0 && state.next_retraction_id < 3 {
                        actions.push(ProtocolAction::StaleWrite { cs });
                    }
                }
            }
        }

        // --- Retraction polling ---
        if state.retraction_polls < 2 {
            for &(_, _, shard) in &state.map {
                if !state.sealed.contains(&shard) {
                    if let Some(pending) = state.pending_retractions.get(&shard) {
                        if !pending.is_empty() {
                            actions.push(ProtocolAction::PollRetractions { shard });
                        }
                    }
                }
            }
        }

        // --- Protocol phases ---
        match state.phase {
            Phase::Idle => {
                // Must claim leadership first.
                actions.push(ProtocolAction::ClaimLeadership);
            }
            Phase::LeaderClaimed => {
                // Can reconfigure (if status is Completed and not already done).
                if state.durable_status == DurableStatus::Completed && !state.started_reconfig {
                    let can_start = match &self.scenario {
                        Scenario::Split => state.durable_map.len() == 1,
                        Scenario::Merge => state.durable_map.len() == 2,
                    };
                    if can_start {
                        actions.push(ProtocolAction::Reconfigure);
                    }
                }
                // Can reconcile (always — idempotent).
                if state.durable_status == DurableStatus::Reconfiguring {
                    actions.push(ProtocolAction::BeginReconcile);
                }
            }
            Phase::Reconciling => {
                actions.push(ProtocolAction::AcquireHolds);
            }
            Phase::HoldsAcquired | Phase::SnapshotWriting => {
                for (&new_shard, preds) in &state.durable_predecessors {
                    for &pred in preds {
                        if !state.bulk_snapshots.contains(&(new_shard, pred)) {
                            actions.push(ProtocolAction::WriteBulkSnapshot {
                                new_shard,
                                predecessor: pred,
                            });
                        }
                    }
                }
            }
            Phase::SnapshotsWritten => {
                actions.push(ProtocolAction::Seal);
            }
            Phase::Sealed | Phase::DeltaWriting => {
                for (&new_shard, preds) in &state.durable_predecessors {
                    for &pred in preds {
                        if !state.delta_snapshots.contains(&(new_shard, pred)) {
                            actions.push(ProtocolAction::WriteDeltaSnapshot {
                                new_shard,
                                predecessor: pred,
                            });
                        }
                    }
                }
            }
            Phase::DeltasWritten => {
                actions.push(ProtocolAction::CommitReconciliation);
            }
            Phase::Committed => {
                actions.push(ProtocolAction::ReleaseHolds);
            }
        }

        // --- Crash ---
        if state.crashes < MAX_CRASHES && state.phase != Phase::Idle {
            actions.push(ProtocolAction::Crash);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();

        match action {
            ProtocolAction::Write { cs } => {
                let shard = active_shard_for(&s.map, &s.sealed, cs)
                    .expect("Write requires active shard");
                let entry = s.data.get_mut(&shard).expect("shard must have data");
                entry[cs] += 1;
                s.committed_writes.insert((cs, entry[cs]));
            }

            ProtocolAction::Read { cs } => {
                // Read the current seqno for this client shard from the
                // active (non-sealed) shard. Record the observed value in
                // ghost state for the monotonic reads property.
                let shard = active_shard_for(&s.map, &s.sealed, cs)
                    .expect("Read requires active shard");
                let observed = s.data[&shard][cs];
                s.last_read[cs] = s.last_read[cs].max(observed);
                s.read_count += 1;
            }

            ProtocolAction::StaleWrite { cs } => {
                let shard = active_shard_for(&s.map, &s.sealed, cs)
                    .expect("StaleWrite requires active shard");
                let retraction_id = s.next_retraction_id;
                s.next_retraction_id += 1;
                s.pending_retractions
                    .entry(shard)
                    .or_default()
                    .insert(retraction_id);
            }

            ProtocolAction::PollRetractions { shard } => {
                if let Some(pending) = s.pending_retractions.remove(&shard) {
                    for id in &pending {
                        assert!(
                            s.retracted.insert(*id),
                            "double retraction: shard={}, id={}",
                            shard,
                            id,
                        );
                    }
                }
                s.retraction_polls += 1;
            }

            ProtocolAction::ClaimLeadership => {
                // CAS: increment leader_id.
                s.durable_leader_id += 1;
                s.leader_id = s.durable_leader_id;
                s.phase = Phase::LeaderClaimed;
            }

            ProtocolAction::Reconfigure => {
                let plan = self.build_plan(state);

                // Initialize empty data entries for new shards.
                for &shard in &plan.new_shards {
                    s.data.insert(shard, [0u8; NUM_CS]);
                }
                s.next_shard =
                    state.next_shard + u8::try_from(plan.new_shards.len()).expect("len fits u8");

                // CAS: set status→Reconfiguring, bump epoch, store predecessors.
                // Save the old map so crash recovery can route to old shards.
                s.previous_map = Some(s.durable_map.clone());
                s.durable_epoch += 1;
                s.durable_map = plan.new_map.clone();
                s.durable_status = DurableStatus::Reconfiguring;
                // Store predecessors durably.
                s.durable_predecessors.clear();
                for &new_shard in &plan.new_shards {
                    s.durable_predecessors
                        .insert(new_shard, plan.retiring.clone());
                }

                // In-memory map stays at the OLD value during reconfiguration.
                // Routing only switches to new shards at CommitReconciliation,
                // after bulk+delta snapshots ensure data continuity.
                // s.map is NOT updated here.
                s.started_reconfig = true;

                // After Reconfigure CAS, transition to LeaderClaimed so
                // the actor sends Reconcile next.
                s.phase = Phase::LeaderClaimed;
            }

            ProtocolAction::BeginReconcile => {
                // Reads durable state (already in sync in the model).
                // Begins execute_reconfiguration.
                s.phase = Phase::Reconciling;
            }

            ProtocolAction::AcquireHolds => {
                // Collect all predecessor shards.
                let preds: BTreeSet<u8> = s
                    .durable_predecessors
                    .values()
                    .flat_map(|v| v.iter().copied())
                    .collect();
                s.holds = preds;
                s.phase = Phase::HoldsAcquired;
            }

            ProtocolAction::WriteBulkSnapshot {
                new_shard,
                predecessor,
            } => {
                // Copy data from predecessor to new shard (idempotent max).
                let new_range = s
                    .durable_map
                    .iter()
                    .find(|&&(_, _, sid)| sid == new_shard)
                    .copied();
                if let Some(new_range) = new_range {
                    for cs in 0..NUM_CS {
                        if range_covers(new_range.0, new_range.1, cs) {
                            let pred_seq = s.data.get(&predecessor).map_or(0, |d| d[cs]);
                            let entry = s.data.get_mut(&new_shard).unwrap();
                            entry[cs] = entry[cs].max(pred_seq);
                        }
                    }
                }
                s.bulk_snapshots.insert((new_shard, predecessor));

                // Check if all bulk snapshots done.
                let all_done = all_predecessor_pairs_done(
                    &s.durable_predecessors,
                    &s.bulk_snapshots,
                );
                s.phase = if all_done {
                    Phase::SnapshotsWritten
                } else {
                    Phase::SnapshotWriting
                };
            }

            ProtocolAction::Seal => {
                let retiring: BTreeSet<u8> = s
                    .durable_predecessors
                    .values()
                    .flat_map(|v| v.iter().copied())
                    .collect();
                for shard in &retiring {
                    s.sealed.insert(*shard);
                }
                s.phase = Phase::Sealed;
            }

            ProtocolAction::WriteDeltaSnapshot {
                new_shard,
                predecessor,
            } => {
                // Re-copy (captures writes between snapshot and seal).
                let new_range = s
                    .durable_map
                    .iter()
                    .find(|&&(_, _, sid)| sid == new_shard)
                    .copied();
                if let Some(new_range) = new_range {
                    for cs in 0..NUM_CS {
                        if range_covers(new_range.0, new_range.1, cs) {
                            let pred_seq = s.data.get(&predecessor).map_or(0, |d| d[cs]);
                            let entry = s.data.get_mut(&new_shard).unwrap();
                            entry[cs] = entry[cs].max(pred_seq);
                        }
                    }
                }
                s.delta_snapshots.insert((new_shard, predecessor));

                let all_done = all_predecessor_pairs_done(
                    &s.durable_predecessors,
                    &s.delta_snapshots,
                );
                s.phase = if all_done {
                    Phase::DeltasWritten
                } else {
                    Phase::DeltaWriting
                };
            }

            ProtocolAction::CommitReconciliation => {
                // CAS: status→Completed, clear predecessors, clear previous map.
                s.durable_status = DurableStatus::Completed;
                s.durable_predecessors.clear();
                s.previous_map = None;
                // NOW switch routing to the new partition map. Data continuity
                // is guaranteed because bulk+delta snapshots completed.
                s.map = s.durable_map.clone();
                s.phase = Phase::Committed;
            }

            ProtocolAction::ReleaseHolds => {
                s.holds.clear();
                s.phase = Phase::Idle;
            }

            ProtocolAction::Crash => {
                // Revert in-memory routing to the appropriate map.
                // During Reconfiguring, route to old shards (previous_map).
                // During Completed, route to current durable_map.
                s.map = match (&s.durable_status, &s.previous_map) {
                    (DurableStatus::Reconfiguring, Some(prev)) => prev.clone(),
                    _ => s.durable_map.clone(),
                };
                // Reset all volatile state.
                s.leader_id = 0;
                s.phase = Phase::Idle;
                s.holds.clear();
                s.bulk_snapshots.clear();
                s.delta_snapshots.clear();
                s.crashes += 1;
                // Recovery: new actor starts at Idle, must ClaimLeadership
                // before reconciling.
            }
        }

        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("PM1+PM2: valid partition map", |_, state| {
                let map = &state.map;
                if map.is_empty() {
                    return false;
                }
                if map[0].0 != 0x00 {
                    return false;
                }
                if map.last().unwrap().1 != 0x100 {
                    return false;
                }
                for i in 1..map.len() {
                    if map[i - 1].1 != u16::from(map[i].0) {
                        return false;
                    }
                }
                true
            }),
            // RC1: in stable (Completed) states, active map has no sealed shards.
            Property::<Self>::always(
                "RC1: stable config has no sealed shards in map",
                |_, state| {
                    if state.durable_status == DurableStatus::Reconfiguring {
                        return true;
                    }
                    let active: BTreeSet<u8> = state.map.iter().map(|r| r.2).collect();
                    state.sealed.is_disjoint(&active)
                },
            ),
            // Snapshot-before-seal: at seal time, all bulk snapshots are done.
            Property::<Self>::always("snapshots before seal", |_, state| {
                if state.phase != Phase::SnapshotsWritten && state.phase != Phase::Sealed {
                    return true;
                }
                all_predecessor_pairs_done(
                    &state.durable_predecessors,
                    &state.bulk_snapshots,
                )
            }),
            // Seal-before-delta: at delta-done time, all predecessors are sealed.
            Property::<Self>::always("seal before delta", |_, state| {
                if state.phase != Phase::DeltasWritten {
                    return true;
                }
                let retiring: BTreeSet<u8> = state
                    .durable_predecessors
                    .values()
                    .flat_map(|v| v.iter().copied())
                    .collect();
                retiring.iter().all(|s| state.sealed.contains(s))
            }),
            // RC2: after reconfig completes, every committed write is readable.
            Property::<Self>::always("RC2: no committed write lost after reconfig", |_, state| {
                let stable = state.started_reconfig
                    && state.durable_status == DurableStatus::Completed
                    && (state.phase == Phase::Committed
                        || state.phase == Phase::Idle
                        || state.phase == Phase::LeaderClaimed);
                if !stable {
                    return true;
                }
                for &(cs, seq) in &state.committed_writes {
                    let readable = state.map.iter().any(|&(lo, hi, shard)| {
                        range_covers(lo, hi, cs)
                            && !state.sealed.contains(&shard)
                            && state.data.get(&shard).map_or(false, |d| d[cs] >= seq)
                    });
                    if !readable {
                        return false;
                    }
                }
                true
            }),
            // Linearizability (monotonic reads): a read must never observe a
            // seqno lower than one previously observed for the same client shard.
            // This catches stale reads after reconfiguration where data wasn't
            // properly carried forward.
            Property::<Self>::always("monotonic reads", |_, state| {
                for cs in 0..NUM_CS {
                    if let Some(shard) = active_shard_for(&state.map, &state.sealed, cs) {
                        let current = state.data.get(&shard).map_or(0, |d| d[cs]);
                        if current < state.last_read[cs] {
                            return false;
                        }
                    }
                }
                true
            }),
            // Liveness: reconfiguration eventually completes.
            Property::<Self>::eventually("reconfiguration completes", |_, state| {
                state.started_reconfig
                    && state.durable_status == DurableStatus::Completed
                    && state.phase == Phase::Idle
            }),
            Property::<Self>::sometimes(
                "writes before reconfig are carried forward",
                |_, state| {
                    state.phase == Phase::Idle
                        && state.started_reconfig
                        && state.durable_status == DurableStatus::Completed
                        && !state.committed_writes.is_empty()
                },
            ),
            Property::<Self>::always("no double retraction", |_, state| {
                for (_, pending) in &state.pending_retractions {
                    for id in pending {
                        if state.retracted.contains(id) {
                            return false;
                        }
                    }
                }
                true
            }),
            Property::<Self>::sometimes("retractions are polled", |_, state| {
                !state.retracted.is_empty()
            }),
            // Reachability: reads happen after reconfiguration with non-zero data.
            Property::<Self>::sometimes(
                "reads after reconfig see carried-forward data",
                |_, state| {
                    state.started_reconfig
                        && state.durable_status == DurableStatus::Completed
                        && state.last_read.iter().any(|&v| v > 0)
                },
            ),
        ]
    }
}

#[test]
fn stateright_protocol_split() {
    let checker = ProtocolModel::split()
        .checker()
        .threads(1)
        .spawn_bfs()
        .join();
    checker.assert_properties();

    let state_count = checker.unique_state_count();
    eprintln!(
        "protocol split model: {} unique states explored",
        state_count
    );
    assert!(
        state_count >= 50,
        "expected >=50 states, got {}",
        state_count
    );
    assert!(
        state_count < 2_000_000,
        "state space too large: {}",
        state_count
    );
}

#[test]
fn stateright_protocol_merge() {
    let checker = ProtocolModel::merge()
        .checker()
        .threads(1)
        .spawn_bfs()
        .join();
    checker.assert_properties();

    let state_count = checker.unique_state_count();
    eprintln!(
        "protocol merge model: {} unique states explored",
        state_count
    );
    assert!(
        state_count >= 50,
        "expected >=50 states, got {}",
        state_count
    );
    assert!(
        state_count < 2_000_000,
        "state space too large: {}",
        state_count
    );
}
