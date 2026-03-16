mod scenario;

use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use scenario::{
    CompareAndAppendResult, ConcurrentScenarioProgram, ScenarioFault, ScenarioHistoryOp,
    ScenarioObservation, ScenarioOp, ScenarioThread, ShardOracle, SnapshotError, Update,
    linearizability_smoke_program,
};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};
use stateright::{Checker, Model, Property};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct SequentialShardSpec {
    upper: u64,
    since: u64,
    updates: Vec<Update>,
}

impl SequentialShardSpec {
    fn snapshot(&self, as_of: u64) -> Vec<Update> {
        let mut diffs = BTreeMap::<String, i64>::new();
        for update in &self.updates {
            if update.time <= as_of {
                *diffs.entry(update.key.clone()).or_insert(0) += update.diff;
            }
        }
        diffs
            .into_iter()
            .filter_map(|(key, diff)| (diff != 0).then(|| Update::new(key, as_of, diff)))
            .collect()
    }
}

impl SequentialSpec for SequentialShardSpec {
    type Op = ScenarioHistoryOp;
    type Ret = ScenarioObservation;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            ScenarioHistoryOp::CompareAndAppend {
                writer: _writer,
                updates,
                expected_upper,
                new_upper,
            } => {
                assert!(
                    expected_upper <= new_upper,
                    "invalid append bounds [{expected_upper}, {new_upper})"
                );
                for update in updates {
                    assert!(
                        *expected_upper <= update.time && update.time < *new_upper,
                        "update {:?} is outside [{expected_upper}, {new_upper})",
                        update
                    );
                }
                if *expected_upper != self.upper {
                    return ScenarioObservation::CompareAndAppend(
                        CompareAndAppendResult::UpperMismatch {
                            expected: *expected_upper,
                            current: self.upper,
                        },
                    );
                }
                self.updates.extend(updates.iter().cloned());
                self.upper = *new_upper;
                ScenarioObservation::CompareAndAppend(CompareAndAppendResult::Committed)
            }
            ScenarioHistoryOp::DowngradeSince {
                reader: _reader,
                new_since,
            } => {
                assert!(
                    self.since <= *new_since,
                    "since cannot regress from {} to {new_since}",
                    self.since
                );
                self.since = *new_since;
                ScenarioObservation::SinceDowngraded { since: self.since }
            }
            ScenarioHistoryOp::Snapshot {
                reader: _reader,
                as_of,
            } => {
                if *as_of < self.since {
                    return ScenarioObservation::Snapshot(Err(SnapshotError::Since(self.since)));
                }
                assert!(
                    *as_of < self.upper,
                    "completed snapshot at as_of={} must have upper={}>as_of",
                    as_of,
                    self.upper
                );
                ScenarioObservation::Snapshot(Ok(self.snapshot(*as_of)))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PendingCall {
    op: ScenarioOp,
    ret: Option<ScenarioObservation>,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct ThreadState {
    pc: usize,
    pending: Option<PendingCall>,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct ThreadStates {
    writer0: ThreadState,
    writer1: ThreadState,
    reader0: ThreadState,
}

impl ThreadStates {
    fn thread(&self, thread: ScenarioThread) -> &ThreadState {
        match thread {
            ScenarioThread::Writer0 => &self.writer0,
            ScenarioThread::Writer1 => &self.writer1,
            ScenarioThread::Reader0 => &self.reader0,
        }
    }

    fn thread_mut(&mut self, thread: ScenarioThread) -> &mut ThreadState {
        match thread {
            ScenarioThread::Writer0 => &mut self.writer0,
            ScenarioThread::Writer1 => &mut self.writer1,
            ScenarioThread::Reader0 => &mut self.reader0,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PersistShardModelState {
    shard: ShardOracle,
    history: LinearizabilityTester<ScenarioThread, SequentialShardSpec>,
    threads: ThreadStates,
    storage_available: bool,
    overlap_seen: bool,
    faulted_pending_seen: bool,
}

impl Default for PersistShardModelState {
    fn default() -> Self {
        Self {
            shard: ShardOracle::default(),
            history: LinearizabilityTester::new(SequentialShardSpec::default()),
            threads: ThreadStates::default(),
            storage_available: true,
            overlap_seen: false,
            faulted_pending_seen: false,
        }
    }
}

impl PersistShardModelState {
    fn thread(&self, thread: ScenarioThread) -> &ThreadState {
        self.threads.thread(thread)
    }

    fn thread_mut(&mut self, thread: ScenarioThread) -> &mut ThreadState {
        self.threads.thread_mut(thread)
    }

    fn all_ops_complete(&self, model: &PersistShardModel) -> bool {
        ScenarioThread::ALL.iter().all(|thread| {
            let state = self.thread(*thread);
            state.pending.is_none() && state.pc == model.script(*thread).len()
        })
    }

    fn any_pending(&self) -> bool {
        ScenarioThread::ALL
            .iter()
            .copied()
            .any(|thread| self.thread(thread).pending.is_some())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum PersistAction {
    Invoke(ScenarioThread),
    Linearize(ScenarioThread),
    Return(ScenarioThread),
    Fault(ScenarioFault),
}

#[derive(Clone, Debug)]
struct PersistShardModel {
    writer0: Vec<ScenarioOp>,
    writer1: Vec<ScenarioOp>,
    reader0: Vec<ScenarioOp>,
}

impl PersistShardModel {
    fn new() -> Self {
        let ConcurrentScenarioProgram {
            writer0,
            writer1,
            reader0,
        } = linearizability_smoke_program();
        Self {
            writer0,
            writer1,
            reader0,
        }
    }

    fn script(&self, thread: ScenarioThread) -> &[ScenarioOp] {
        match thread {
            ScenarioThread::Writer0 => &self.writer0,
            ScenarioThread::Writer1 => &self.writer1,
            ScenarioThread::Reader0 => &self.reader0,
        }
    }

    fn next_op<'a>(
        &'a self,
        state: &'a PersistShardModelState,
        thread: ScenarioThread,
    ) -> Option<&'a ScenarioOp> {
        self.script(thread).get(state.thread(thread).pc)
    }

    fn op_is_ready(&self, shard: &ShardOracle, op: &ScenarioOp) -> bool {
        match op {
            ScenarioOp::OpenWriter { writer: _writer } => true,
            ScenarioOp::OpenReader { reader: _reader } => true,
            ScenarioOp::CompareAndAppend {
                writer: _writer,
                updates: _updates,
                expected_upper: _expected_upper,
                new_upper: _new_upper,
            } => true,
            ScenarioOp::DowngradeSince {
                reader: _reader,
                new_since: _new_since,
            } => true,
            ScenarioOp::Snapshot {
                reader: _reader,
                as_of,
            } => *as_of < shard.upper(),
        }
    }

    fn apply_linearization(
        &self,
        shard: &mut ShardOracle,
        op: &ScenarioOp,
    ) -> Result<ScenarioObservation> {
        match op {
            ScenarioOp::Snapshot {
                reader: _reader,
                as_of,
            } => {
                ensure!(
                    *as_of < shard.upper(),
                    "snapshot at as_of={} is not yet complete because upper is {}",
                    as_of,
                    shard.upper()
                );
            }
            ScenarioOp::OpenWriter { writer: _writer }
            | ScenarioOp::OpenReader { reader: _writer }
            | ScenarioOp::CompareAndAppend {
                writer: _writer,
                updates: _,
                expected_upper: _,
                new_upper: _,
            }
            | ScenarioOp::DowngradeSince {
                reader: _writer,
                new_since: _,
            } => {}
        }
        shard.apply(op)
    }
}

impl Model for PersistShardModel {
    type State = PersistShardModelState;
    type Action = PersistAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![PersistShardModelState::default()]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.storage_available {
            actions.push(PersistAction::Fault(ScenarioFault::PartitionStorage));
        } else {
            actions.push(PersistAction::Fault(ScenarioFault::RepairStorage));
        }
        for thread in ScenarioThread::ALL {
            let thread_state = state.thread(thread);
            match &thread_state.pending {
                Some(PendingCall { op, ret: None }) => {
                    if state.storage_available && self.op_is_ready(&state.shard, op) {
                        actions.push(PersistAction::Linearize(thread));
                    }
                }
                Some(PendingCall { ret: Some(_), .. }) => {
                    actions.push(PersistAction::Return(thread));
                }
                None => {
                    if let Some(op) = self.next_op(state, thread) {
                        if self.op_is_ready(&state.shard, op) {
                            actions.push(PersistAction::Invoke(thread));
                        }
                    }
                }
            }
        }
    }

    fn next_state(&self, last_state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last_state.clone();
        match action {
            PersistAction::Invoke(thread) => {
                let op = self.next_op(&state, thread)?.clone();
                if !self.op_is_ready(&state.shard, &op) {
                    return None;
                }
                if let Some(history_op) = op.history_op() {
                    state.history.on_invoke(thread, history_op).ok()?;
                }
                let overlap = ScenarioThread::ALL
                    .iter()
                    .copied()
                    .filter(|other| *other != thread)
                    .any(|other| state.thread(other).pending.is_some());
                state.overlap_seen |= overlap;
                state.faulted_pending_seen |= !state.storage_available;
                state.thread_mut(thread).pending = Some(PendingCall { op, ret: None });
            }
            PersistAction::Linearize(thread) => {
                let op = state.thread(thread).pending.as_ref()?.op.clone();
                if !state.storage_available || !self.op_is_ready(&state.shard, &op) {
                    return None;
                }
                let ret = self.apply_linearization(&mut state.shard, &op).ok()?;
                state.thread_mut(thread).pending.as_mut()?.ret = Some(ret);
            }
            PersistAction::Return(thread) => {
                let pending = state.thread_mut(thread).pending.take()?;
                let ret = pending.ret?;
                if pending.op.history_op().is_some() {
                    state.history.on_return(thread, ret).ok()?;
                }
                state.thread_mut(thread).pc += 1;
            }
            PersistAction::Fault(fault) => match fault {
                ScenarioFault::PartitionStorage => {
                    if !state.storage_available {
                        return None;
                    }
                    state.faulted_pending_seen |= state.any_pending();
                    state.storage_available = false;
                }
                ScenarioFault::RepairStorage => {
                    if state.storage_available {
                        return None;
                    }
                    state.storage_available = true;
                }
            },
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("history remains linearizable", |_model: &Self, state: &Self::State| {
                state.history.is_consistent()
            }),
            Property::always("frontiers stay ordered", |_model: &Self, state: &Self::State| {
                state.shard.since() <= state.shard.upper()
            }),
            Property::sometimes("writer and reader overlap", |_model: &Self, state: &Self::State| {
                state.overlap_seen
            }),
            Property::sometimes("storage faults delay an operation", |_model: &Self, state: &Self::State| {
                state.faulted_pending_seen
            }),
            Property::sometimes("entire scripted scenario completes", |model: &Self, state: &Self::State| {
                state.all_ops_complete(model)
            }),
        ]
    }
}

#[test]
fn first_persist_model_is_linearizable() {
    let checker = PersistShardModel::new()
        .checker()
        .threads(num_cpus::get())
        .spawn_dfs()
        .join();
    checker.assert_properties();
}
