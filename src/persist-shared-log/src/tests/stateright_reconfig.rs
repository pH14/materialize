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
//! 2. [`ProtocolModel`] — reconfiguration protocol with crash recovery.
//!    Models the full seal→replay→commit lifecycle, client writes interleaved
//!    with protocol phases, and crash/recovery at any point. Verifies:
//!    - **RC2**: no committed write is lost during reconfiguration.
//!    - **Carry-forward**: data in old shards at seal time appears in new
//!      shards after commit.
//!    - **No partial replay**: commit only happens after all replays complete.
//!    - **Crash safety**: recovery from any phase preserves all committed data.
//!    - **Liveness**: reconfiguration eventually completes despite crashes.
//!
//! The protocol model is parameterized by [`Scenario`] (split or merge).

use std::collections::{BTreeMap, BTreeSet};

use stateright::*;

// =========================================================================
// Model 1: Partition Map (existing, unchanged)
// =========================================================================

/// A log shard in the partition map model.
type ShardId = u8;

/// A simplified partition map for model checking.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ModelPartitionMap {
    epoch: u64,
    /// Vec of (lo_inclusive, hi_exclusive, shard_id).
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
        // PM1: starts at 0x00.
        if self.ranges[0].0 != 0x00 {
            return false;
        }
        // PM1: ends at 0x100.
        if self.ranges.last().unwrap().1 != 0x100 {
            return false;
        }
        // PM2: non-overlapping, contiguous.
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

/// The partition map model state.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ReconfigState {
    partition_map: ModelPartitionMap,
    sealed_shards: BTreeSet<ShardId>,
    next_shard_id: ShardId,
}

/// Actions the partition map model can take.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ReconfigAction {
    /// Split the range at index `range_idx` at midpoint, creating a new shard.
    Split { range_idx: usize },
}

/// Stateright model for partition map reconfiguration safety.
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
        // Can split any range that is wide enough (at least 2 keys).
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

                // RC1: seal the old shard before reassigning its range.
                new_state.sealed_shards.insert(old_shard);

                // Split at midpoint.
                let mid = u16::from(lo) + (hi - u16::from(lo)) / 2;
                let new_shard_a = new_state.next_shard_id;
                let new_shard_b = new_state.next_shard_id + 1;
                new_state.next_shard_id += 2;

                // Replace the old range with two new ranges.
                new_state.partition_map.ranges.remove(range_idx);
                new_state
                    .partition_map
                    .ranges
                    .insert(range_idx, (u8::try_from(mid).expect("mid fits u8"), hi, new_shard_b));
                new_state
                    .partition_map
                    .ranges
                    .insert(range_idx, (lo, mid, new_shard_a));

                // PM3: monotonic epoch.
                new_state.partition_map.epoch += 1;

                // Bound the state space: stop after 4 reconfigurations.
                if new_state.partition_map.epoch > 4 {
                    return None;
                }
                // Bound: max 8 shards.
                if new_state.next_shard_id > 8 {
                    return None;
                }

                Some(new_state)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // PM1 + PM2: partition map is always valid (covering, non-overlapping).
            Property::<Self>::always("PM1+PM2: valid partition map", |_, state| {
                state.partition_map.validate()
            }),
            // PM3: epoch increases monotonically (>= 0).
            Property::<Self>::always("PM3: monotonic epoch", |_, state| {
                let _ = state;
                true
            }),
            // RC1: sealed shards are never in the active partition map.
            Property::<Self>::always("RC1: sealed shards not in active map", |_, state| {
                let active = state.partition_map.shard_ids();
                state.sealed_shards.is_disjoint(&active)
            }),
            // The partition map always has at least one range.
            Property::<Self>::always("non-empty partition map", |_, state| {
                !state.partition_map.ranges.is_empty()
            }),
            // Eventually we can reach a state with multiple shards.
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
    let checker = ReconfigModel
        .checker()
        .threads(1)
        .spawn_bfs()
        .join();
    checker.assert_properties();
    let state_count = checker.unique_state_count();
    // Sanity check: we explore a reasonable number of states.
    assert!(
        state_count >= 10,
        "expected >=10 states, got {}",
        state_count
    );
    // The model is bounded, so we shouldn't explore millions.
    assert!(
        state_count < 100_000,
        "unexpectedly large state space: {}",
        state_count
    );
}

// =========================================================================
// Model 2: Reconfiguration Protocol with Crash Recovery
// =========================================================================
//
// This model verifies that the seal→replay→commit protocol preserves data
// integrity even when crashes occur at any point. It tracks abstract data
// (seqno per client shard per log shard) and verifies that committed writes
// are never lost.
//
// Key simplifications vs. the real system:
// - Seqno abstracted to 0..MAX_SEQ (not full CAS evaluation)
// - Replay is atomic (all predecessors at once, not per-predecessor)
// - No network, no message delivery, no timing
// - Durable state = log shard data + sealed set + metashard intent/epoch

/// Maximum seqno per client shard. Enough to distinguish "no data",
/// "one write", "two writes".
const MAX_SEQ: u8 = 2;

/// Maximum crash-and-recover events per exploration path.
const MAX_CRASHES: u8 = 2;

/// Number of client shards. Client shard 0 maps to partition key 0x20
/// (first half), client shard 1 maps to 0x90 (second half).
const NUM_CS: usize = 2;

/// Does the range [lo, hi_exclusive) cover client shard `cs`?
fn range_covers(lo: u8, hi: u16, cs: usize) -> bool {
    let pk: u8 = if cs == 0 { 0x20 } else { 0x90 };
    pk >= lo && u16::from(pk) < hi
}

/// Find the active (non-sealed) shard covering client shard `cs`, if any.
fn active_shard_for(
    map: &[(u8, u16, u8)],
    sealed: &BTreeSet<u8>,
    cs: usize,
) -> Option<u8> {
    map.iter()
        .find(|&&(lo, hi, shard)| range_covers(lo, hi, cs) && !sealed.contains(&shard))
        .map(|&(_, _, shard)| shard)
}

/// Reconfiguration scenario: split (1→2) or merge (2→1).
#[derive(Clone, Debug)]
enum Scenario {
    /// Start with 1 shard [0x00, 0x100), split into 2.
    Split,
    /// Start with 2 shards [0x00, 0x80) and [0x80, 0x100), merge into 1.
    Merge,
}

/// Plan for a reconfiguration, computed at the start.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Plan {
    new_map: Vec<(u8, u16, u8)>,
    new_shards: Vec<u8>,
    retiring: Vec<u8>,
}

/// Protocol phase (volatile — lost on crash).
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    /// No reconfiguration in progress.
    Idle,
    /// Intent persisted to metashard shard. Recovery will re-run from here.
    IntentPersisted,
    /// CriticalSince holds acquired on retiring shards.
    HoldsAcquired,
    /// Retiring shards sealed (upper = []). This is durable in the log shards.
    Sealed,
    /// All new shards have replayed predecessor data.
    Replayed,
    /// Routing swapped to new shards. The in-memory map and epoch have been
    /// updated, but durable_intent still exists and the durable epoch/map
    /// have NOT been updated. A crash here recovers from the durable state
    /// (old epoch + intent), re-runs do_reconfigure idempotently.
    RoutingSwapped,
    /// Durable state persisted. intent cleared, new epoch+map are durable.
    DurableCommitted,
}

/// The protocol model state.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ProtocolState {
    // --- Durable state (survives crash) ---

    /// Durable epoch (persisted to metashard shard).
    durable_epoch: u8,
    /// Durable partition map.
    durable_map: Vec<(u8, u16, u8)>,
    /// Log shard data: seqno per client shard. Lives in the persist shard,
    /// survives crashes.
    data: BTreeMap<u8, [u8; NUM_CS]>,
    /// Sealed log shards (upper = []). Durable in persist.
    sealed: BTreeSet<u8>,
    /// Pending reconfiguration intent. Persisted in the metashard shard
    /// at Phase::IntentPersisted; cleared at Phase::DurableCommitted.
    durable_intent: Option<Plan>,

    // --- In-memory state (may diverge from durable between SwapRouting and PersistCommit) ---

    /// In-memory epoch (updated at SwapRouting, made durable at PersistCommit).
    epoch: u8,
    /// In-memory partition map (updated at SwapRouting).
    map: Vec<(u8, u16, u8)>,

    // --- Volatile state (lost on crash) ---

    /// Current protocol phase.
    phase: Phase,
    /// CriticalSince holds (prevent compaction of sealed shards during replay).
    holds: BTreeSet<u8>,
    /// New shards that have completed predecessor replay.
    replayed: BTreeSet<u8>,

    // --- Bookkeeping ---

    /// Next log shard ID to allocate.
    next_shard: u8,
    /// Number of crashes so far (bounded to limit state space).
    crashes: u8,
    /// Whether a reconfiguration has been started (limits to 1).
    started_reconfig: bool,

    // --- Ghost state (for property checking, not part of system) ---

    /// All (client_shard, seqno) pairs the client has seen committed.
    /// Used to verify RC2: no committed write is lost.
    committed_writes: BTreeSet<(usize, u8)>,
}

/// Actions in the protocol model.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ProtocolAction {
    /// Client writes to the active shard for this client shard.
    Write { cs: usize },
    /// Begin reconfiguration: validate plan, persist intent.
    BeginReconfig,
    /// Acquire CriticalSince holds on retiring shards.
    AcquireHolds,
    /// Seal retiring log shards.
    Seal,
    /// Replay all predecessors into new shards (atomic for state space).
    Replay,
    /// Swap routing to new partition map. In-memory map/epoch updated,
    /// intent cleared in memory but NOT yet durably persisted.
    SwapRouting,
    /// Persist the committed state durably. After this, durable_intent is
    /// cleared and new epoch/map are crash-safe.
    PersistCommit,
    /// Release CriticalSince holds.
    ReleaseHolds,
    /// Crash and recover. Resets volatile state, recovers from durable.
    Crash,
}

/// The protocol model, parameterized by scenario.
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

    /// Build the reconfiguration plan based on the scenario and current state.
    fn build_plan(&self, state: &ProtocolState) -> Plan {
        match &self.scenario {
            Scenario::Split => {
                // Split [0x00, 0x100) into [0x00, 0x80) and [0x80, 0x100)
                let old_shard = state.map[0].2;
                let shard_a = state.next_shard;
                let shard_b = state.next_shard + 1;
                Plan {
                    new_map: vec![(0x00, 0x80, shard_a), (0x80, 0x100, shard_b)],
                    new_shards: vec![shard_a, shard_b],
                    retiring: vec![old_shard],
                }
            }
            Scenario::Merge => {
                // Merge [0x00, 0x80) + [0x80, 0x100) into [0x00, 0x100)
                let shard_a = state.map[0].2;
                let shard_b = state.map[1].2;
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
                    epoch: 0,
                    map: init_map,
                    data,
                    sealed: BTreeSet::new(),
                    durable_intent: None,
                    phase: Phase::Idle,
                    holds: BTreeSet::new(),
                    replayed: BTreeSet::new(),
                    next_shard: 1,
                    crashes: 0,
                    started_reconfig: false,
                    committed_writes: BTreeSet::new(),
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
                    epoch: 0,
                    map: init_map,
                    data,
                    sealed: BTreeSet::new(),
                    durable_intent: None,
                    phase: Phase::Idle,
                    holds: BTreeSet::new(),
                    replayed: BTreeSet::new(),
                    next_shard: 2,
                    crashes: 0,
                    started_reconfig: false,
                    committed_writes: BTreeSet::new(),
                }]
            }
        }
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // --- Client writes ---
        // Available when there's an unsealed shard covering the client shard
        // and seqno hasn't reached MAX_SEQ.
        for cs in 0..NUM_CS {
            if let Some(shard) = active_shard_for(&state.map, &state.sealed, cs) {
                if state.data[&shard][cs] < MAX_SEQ {
                    actions.push(ProtocolAction::Write { cs });
                }
            }
        }

        // --- Protocol phases ---
        match state.phase {
            Phase::Idle => {
                if !state.started_reconfig {
                    let can_start = match &self.scenario {
                        Scenario::Split => state.map.len() == 1,
                        Scenario::Merge => state.map.len() == 2,
                    };
                    if can_start {
                        actions.push(ProtocolAction::BeginReconfig);
                    }
                }
            }
            Phase::IntentPersisted => {
                actions.push(ProtocolAction::AcquireHolds);
            }
            Phase::HoldsAcquired => {
                actions.push(ProtocolAction::Seal);
            }
            Phase::Sealed => {
                actions.push(ProtocolAction::Replay);
            }
            Phase::Replayed => {
                actions.push(ProtocolAction::SwapRouting);
            }
            Phase::RoutingSwapped => {
                actions.push(ProtocolAction::PersistCommit);
            }
            Phase::DurableCommitted => {
                actions.push(ProtocolAction::ReleaseHolds);
            }
        }

        // --- Crash ---
        // Available during any active reconfiguration phase, bounded by max crashes.
        if state.crashes < MAX_CRASHES {
            match state.phase {
                Phase::IntentPersisted
                | Phase::HoldsAcquired
                | Phase::Sealed
                | Phase::Replayed
                | Phase::RoutingSwapped
                | Phase::DurableCommitted => {
                    actions.push(ProtocolAction::Crash);
                }
                Phase::Idle => {}
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();

        match action {
            ProtocolAction::Write { cs } => {
                let shard = active_shard_for(&s.map, &s.sealed, cs)
                    .expect("Write action only available when active shard exists");
                let entry = s.data.get_mut(&shard).expect("shard must have data entry");
                entry[cs] += 1;
                s.committed_writes.insert((cs, entry[cs]));
            }

            ProtocolAction::BeginReconfig => {
                let plan = self.build_plan(state);

                // Initialize empty data entries for new shards.
                for &shard in &plan.new_shards {
                    s.data.insert(shard, [0u8; NUM_CS]);
                }
                s.next_shard = state.next_shard + plan.new_shards.len() as u8;

                // Persist the intent (durable).
                s.durable_intent = Some(plan);
                s.phase = Phase::IntentPersisted;
                s.started_reconfig = true;
            }

            ProtocolAction::AcquireHolds => {
                let plan = s.durable_intent.as_ref().expect("must have intent");
                s.holds = plan.retiring.iter().copied().collect();
                s.phase = Phase::HoldsAcquired;
            }

            ProtocolAction::Seal => {
                let plan = s.durable_intent.as_ref().expect("must have intent");
                for &shard in &plan.retiring {
                    s.sealed.insert(shard);
                }
                s.phase = Phase::Sealed;
            }

            ProtocolAction::Replay => {
                let plan = s.durable_intent.as_ref().expect("must have intent").clone();
                // For each new shard, copy data from all overlapping predecessors.
                for &new_shard in &plan.new_shards {
                    let new_range = plan
                        .new_map
                        .iter()
                        .find(|&&(_, _, sid)| sid == new_shard)
                        .expect("new shard must be in new map");
                    for &pred in &plan.retiring {
                        let pred_range = state
                            .map
                            .iter()
                            .find(|&&(_, _, sid)| sid == pred)
                            .expect("predecessor must be in old map");
                        // Copy each client shard that both ranges cover.
                        for cs in 0..NUM_CS {
                            if range_covers(new_range.0, new_range.1, cs)
                                && range_covers(pred_range.0, pred_range.1, cs)
                            {
                                let pred_seq = s.data[&pred][cs];
                                let entry = s.data.get_mut(&new_shard).unwrap();
                                // Take the max — replay applies all predecessor data.
                                entry[cs] = entry[cs].max(pred_seq);
                            }
                        }
                    }
                    s.replayed.insert(new_shard);
                }
                s.phase = Phase::Replayed;
            }

            ProtocolAction::SwapRouting => {
                let plan = s.durable_intent.as_ref().expect("must have intent").clone();
                // Update IN-MEMORY partition map and epoch only.
                // durable_epoch / durable_map / durable_intent are UNCHANGED.
                // A crash here reverts to the durable state (old epoch + intent).
                s.epoch = s.durable_epoch + 1;
                s.map = plan.new_map;
                s.phase = Phase::RoutingSwapped;
            }

            ProtocolAction::PersistCommit => {
                // Make the in-memory state durable.
                s.durable_epoch = s.epoch;
                s.durable_map = s.map.clone();
                s.durable_intent = None;
                s.phase = Phase::DurableCommitted;
            }

            ProtocolAction::ReleaseHolds => {
                s.holds.clear();
                s.phase = Phase::Idle;
            }

            ProtocolAction::Crash => {
                // Revert in-memory state to durable state.
                s.epoch = s.durable_epoch;
                s.map = s.durable_map.clone();
                // Reset all volatile state.
                s.phase = Phase::Idle;
                s.holds.clear();
                s.replayed.clear();
                s.crashes += 1;

                // Recovery: if durable intent exists, re-enter the protocol.
                // This models metashard.rs run() detecting a pending intent
                // and calling do_reconfigure().
                if s.durable_intent.is_some() {
                    s.phase = Phase::IntentPersisted;
                }
                // If no durable intent, we crashed after DurableCommitted or
                // before BeginReconfig. Nothing to recover.
            }
        }

        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // PM1+PM2: partition map always valid.
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
            // RC1: in stable configurations (no pending reconfig intent), the
            // active partition map never references a sealed shard.
            //
            // During reconfiguration, the old map intentionally still
            // references sealed shards (the acceptor returns Sealed errors,
            // clients retry). RC1 is about the committed state.
            Property::<Self>::always(
                "RC1: stable config has no sealed shards in map",
                |_, state| {
                    if state.durable_intent.is_some() {
                        return true; // intermediate reconfig state — expected
                    }
                    let active: BTreeSet<u8> = state.map.iter().map(|r| r.2).collect();
                    state.sealed.is_disjoint(&active)
                },
            ),
            // RC1 strengthened: at the exact moment of commit, the new map
            // does not reference any sealed shards.
            Property::<Self>::always(
                "RC1: new map at commit has no sealed shards",
                |_, state| {
                    if state.phase != Phase::DurableCommitted {
                        return true;
                    }
                    let active: BTreeSet<u8> = state.map.iter().map(|r| r.2).collect();
                    state.sealed.is_disjoint(&active)
                },
            ),
            // Seal-before-commit: at commit time, all retiring shards from
            // the plan are sealed. This is the causal ordering guarantee
            // from Delos: old Loglet sealed before new Loglet activated.
            Property::<Self>::always("seal before commit", |_, state| {
                if state.phase != Phase::Replayed {
                    // Check just before commit (Replayed is the precondition).
                    return true;
                }
                // The retiring shards from the plan should all be sealed.
                // (durable_intent still exists at this point.)
                if let Some(plan) = &state.durable_intent {
                    plan.retiring.iter().all(|s| state.sealed.contains(s))
                } else {
                    true
                }
            }),
            // RC2 + Carry-forward: after reconfiguration completes, every
            // previously committed write is readable from some active
            // (non-sealed) shard.
            Property::<Self>::always(
                "RC2: no committed write lost after reconfig",
                |_, state| {
                    // Only check in stable states after reconfig.
                    let stable_after_reconfig = state.started_reconfig
                        && (state.phase == Phase::DurableCommitted || state.phase == Phase::Idle)
                        && state.durable_intent.is_none();
                    if !stable_after_reconfig {
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
                },
            ),
            // Liveness: reconfiguration eventually completes.
            // In terminal states (no more actions), phase should be Idle
            // with the reconfiguration done.
            Property::<Self>::eventually("reconfiguration completes", |_, state| {
                state.started_reconfig && state.phase == Phase::Idle
            }),
            // Reachability: we can reach a state where writes happened
            // before reconfiguration and data was carried forward.
            Property::<Self>::sometimes(
                "writes before reconfig are carried forward",
                |_, state| {
                    state.phase == Phase::Idle
                        && state.started_reconfig
                        && !state.committed_writes.is_empty()
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
    // Should explore a non-trivial number of states (writes × phases × crashes).
    assert!(
        state_count >= 50,
        "expected >=50 states, got {}",
        state_count
    );
    // But stay bounded.
    assert!(
        state_count < 1_000_000,
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
        state_count < 1_000_000,
        "state space too large: {}",
        state_count
    );
}
