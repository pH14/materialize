# Simulation Tests

## CRITICAL: There is no such thing as a flaky test

**A flaky test IS a bug. Every test failure must be investigated and
root-caused. No exceptions.** Do not dismiss, skip, or retry a failing test
hoping it goes away. Do not mark a test as `#[ignore]` to unblock other work.
Do not say "it passed when I re-ran it" and move on. If a test fails even once,
something is wrong and it must be understood.

This applies to all tests in this crate — deterministic simulation tests,
integration tests, and unit tests alike. A non-reproducible failure is *worse*
than a reproducible one because it means something subtle (nondeterminism, race
condition, leaked state) is hiding. Treat it with more urgency, not less.

### Deterministic simulation failures

The simulation tests in `persist_sim.rs` are seeded and deterministic. A given
seed must always produce the same trace and the same outcome. If a test fails
for a seed on one run but passes on a subsequent run, **that is not a flaky
test — it is a determinism bug**.

The entire value of DST depends on reproducibility. A "flaky" failure means
nondeterminism has leaked into the simulation: HashMap iteration order, tokio
scheduling across iterations, `Instant::now()` affecting control flow, shared
global state between seeds, or something similar. Every such failure must be
investigated and root-caused, not dismissed.

When investigating:

1. **Reproduce first**: `SEED=<n> cargo test -p mz-persist-shared-log persist_sim_single -- --nocapture`
2. **Read the trace**: the full operation history is printed on failure. Look at
   the last few operations before the mismatch.
3. **Check for inter-seed contamination**: does the failure depend on which seeds
   ran before it? Try `SEED=<n> SIM_SEEDS=1` vs running as part of a larger
   batch. If it only fails in the batch, something is leaking between iterations.
4. **Check for tokio nondeterminism**: the tests run on a `current_thread`
   runtime and persist uses a disabled `IsolatedRuntime` (via
   `PersistClientCache::new_for_turmoil()`), so all async work — acceptor,
   learner, and persist internals — runs on one thread. This eliminates
   scheduling nondeterminism. The `persist_sim_deterministic` test verifies
   both observational and scheduling determinism.
5. **Never dismiss**: if you cannot reproduce a failure, add the seed to a
   tracked list and increase the `persist_sim_deterministic` iteration count
   around that seed range.

## Architecture

- `scenario.rs` — shared vocabulary (`SharedLogOp`, `SharedLogObservation`) and
  independent oracle (`SharedLogOracle`). The oracle implements Stateright's
  `SequentialSpec` trait so it can be used for both DST linearizability checking
  and (future) Stateright model checking.
- `trace.rs` — structured operation trace (`SimTrace`) printed on failure.
- `persist_sim.rs` — the simulation harness. Every operation is checked against
  the oracle and fed through Stateright's `LinearizabilityTester`.

## Coverage

- **CAS rejection**: `OpGenerator::gen_cas()` generates ~15% stale expected
  seqnos, exercising the rejected CAS → garbage → retraction pipeline.
- **Multi-writer contention**: `persist_sim_multi_writer` runs two acceptors
  against the same shard pool. Each writer maintains its own stale snapshot,
  so one writer's commit causes the other's next CAS to be rejected.

## TODOs

- **Ground-truth validation from persist shards**: Read the raw persist shard
  data directly and validate that the history is as-intended and per-shard
  linearizable. Currently we only check the system through the client API
  (acceptor/learner). Reading the persist shard directly would catch bugs
  in the acceptor's write path (e.g., the diff-dropping bug in bulk/delta
  snapshots) that are invisible through the client API due to consolidation.

- **Retractions during reconfiguration**: No test currently generates
  retractions in the window between CriticalSince and seal. Add a test
  that produces rejected CAS writes (which generate -1 retraction diffs)
  during an active reconfiguration and verifies they're properly carried
  forward to the new shard.

- **TODO: Invariants as a first-class struct**: The protocol invariants checked
  in `stateright_reconfig.rs` `properties()` are inlined closures. Factor them
  out into a dedicated `Invariants` struct with named methods. Eventually this
  struct should have multiple impls or backends: one for Stateright model
  checking, one for the DST harness (checked after each simulated operation),
  and potentially one that compiles to debug asserts in the production actors.

## Running

```bash
# Single seed
SEED=42 cargo test -p mz-persist-shared-log persist_sim_single -- --nocapture

# Default (100 seeds)
cargo test -p mz-persist-shared-log persist_sim

# Extended
SIM_SEEDS=1000 cargo test -p mz-persist-shared-log persist_sim_single

# Infinite fuzzing
SEED=0 cargo test -p mz-persist-shared-log persist_sim_fuzz -- --ignored
```
