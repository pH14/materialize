# Persist Shared Log: Testing & Verification Strategy

## Overview

The shared log is verified through three complementary approaches, ordered
from most abstract to most concrete:

1. **Semi-formal methods (Stateright)**: exhaustive model checking of the
   protocol's state space. Finds protocol-level bugs.
2. **Deterministic simulation testing (DST)**: exercises the real Rust code
   under simulated faults with deterministic scheduling. Finds
   implementation-level bugs.
3. **Stress testing (open-loop)**: exercises the real system under realistic
   production load. Finds performance and scalability issues.

Each layer catches a different class of bugs. Together, they provide high
confidence that the system is correct, performant, and resilient.

## Architectural Choices for Testability

The shared log uses an actor-based architecture specifically to make the
system easier to test deterministically. Each component (acceptor, learner,
metashard) is a passive state machine driven by a command channel:

- **Acceptor**: receives `Append` and `Flush` commands, produces
  `compare_and_append` writes. No timers, no autonomous behavior — the
  flush policy is in the run loop, but the mechanism (buffering + flushing)
  is exposed directly for tests to drive step-by-step.
- **Learner**: receives commands and events from its persist subscribe.
  Deterministic evaluation: same events in the same order always produce the
  same state (C1).
- **Metashard**: receives commands via its handle. Reconfiguration is a
  deterministic state machine (phase transitions driven by external events).

This decomposition has several testing benefits:

1. **Controlled state transitions**: Tests can drive each actor one command
   at a time, inspecting intermediate state. No need to coordinate timers
   or background threads.
2. **Deterministic replay**: Given the same persist shard contents (same
   events in the same order), any actor produces identical output. This is
   what makes DST work — seed the RNG, control the schedule, get
   reproducible traces.
3. **Independent unit testing**: Each actor can be tested in isolation with
   a mock persist backend. Integration tests compose real actors with
   in-memory persist.
4. **Fault injection at actor boundaries**: BUGGIFY points sit at phase
   transitions in the metashard's reconfiguration protocol. Because each
   phase is an explicit state, injecting faults between phases is natural.
5. **Stateright modeling**: The actor decomposition maps directly to the
   Stateright model's actions and state. `WriteBulkSnapshot`,
   `WriteDeltaSnapshot`, `Seal`, `SwapRouting` are both protocol phases in
   the code and actions in the model.

The alternative — a monolithic service with interleaved concerns — would
make deterministic simulation much harder because internal state transitions
would be implicit in control flow rather than explicit in command handling.

## Layer 1: Semi-Formal Methods (Stateright)

### Purpose

Exhaustively verify that the protocol's safety and liveness properties hold
across all reachable states, including adversarial interleavings that are
unlikely to occur in practice.

### What Is Modeled

Two models exist, from abstract to concrete:

**Partition map model** (`stateright_reconfig.rs`, `ReconfigModel`): verifies
that range-based partition map transitions (split) maintain the covering
invariant. Properties: PM1+PM2 (valid map), PM3 (monotonic epoch), RC1
(seal-before-reassign). Bounded to 4 reconfigurations, 8 shards.

**Protocol model** (`stateright_reconfig.rs`, `ProtocolModel`): verifies the
full reconfiguration lifecycle with acceptor-owned predecessor state, including
crash recovery. The protocol phases are: `WriteBulkSnapshot` (before seal) →
`Seal` → `WriteDeltaSnapshot` (after seal) → `SwapRouting` → `PersistCommit`.
Models both split and merge scenarios with client writes interleaved with
protocol phases, and crash/recovery at every intermediate phase. Properties:
PM1+PM2, RC1, RC2 (no committed write lost after reconfig), snapshots-before-seal,
seal-before-delta, reconfiguration liveness, reachability of carried-forward
state, no double retraction. Bounded to 2 client shards, seqno cap 2, max 2
crashes.

### What Is Not Modeled (Planned)

The following are candidates for future Stateright models:

- Single-shard CAS operations with seqno-based preconditions (C1-C5)
- Multiple concurrent writers and readers with indeterminate CAS outcomes
- Batch ordering and within-batch position ordering (L1-L5)
- Truncate operations (T1-T3)
- Linearizable reads (R1)

### Properties Verified

See [02_invariants.md](02_invariants.md) for the verification matrix. The
Stateright models verify partition map invariants (PM1-PM3), reconfiguration
safety (RC1, RC2), protocol liveness (RC5), and crash recovery correctness
across split and merge scenarios.

### State Space

The partition map model explores ~100-1000 states. The protocol models
explore ~500 states each (split and merge), verifying all properties across
all reachable states including crash/recovery paths.

### Relationship to Implementation

The Stateright models are abstract: they verify the protocol design, not the
Rust implementation. If the protocol changes, the models must be updated.
The models and implementation are connected through shared invariant
definitions ([02_invariants.md](02_invariants.md)) and through DST/integration
tests that check the same properties on real code.

### Running

```bash
cargo test -p mz-persist-shared-log stateright
```

## Layer 2: Deterministic Simulation Testing (DST)

### Purpose

Exercise the real Rust implementation under controlled, reproducible
conditions with fault injection. DST catches bugs where the implementation
diverges from the protocol: off-by-one errors, missed edge cases in async
code, race conditions in the listen/evaluate pipeline.

### Framework

Uses in-memory persist (via `PersistClientCache::new_for_turmoil()`) with a
`current_thread` tokio runtime for deterministic scheduling. All randomness
is seeded, so any failing test can be reproduced by re-running with the same
seed. See `tests/CLAUDE.md` for the determinism policy.

### Architecture

```
persist_sim harness
├── persist infrastructure (in-memory blob + consensus)
├── shared log acceptor (real PersistAcceptor code)
├── shared log learner (real PersistLearner code)
├── seeded OpGenerator (CAS, Head, Scan, Truncate)
├── independent oracle (SharedLogOracle, implements SequentialSpec)
├── LinearizabilityTester (Stateright, sequential operations)
└── SimTrace (structured operation log for debugging)
```

Operations are submitted sequentially from a single thread. The
`LinearizabilityTester` is wired up but is currently tautological for
sequential histories.

### What Is Tested

- **Oracle consistency**: Every operation's result is checked against the
  independent `SharedLogOracle` reference implementation.
- **CAS rejection pipeline**: ~15% of generated CAS operations have stale
  expected seqnos, exercising rejection → garbage → retraction.
- **Crash recovery**: Learner drop/reopen cycles with post-recovery state
  verification against the oracle.
- **Multi-writer contention**: Two acceptors on the same shard pool with
  stale snapshot handling (`persist_sim_multi_writer`).
- **Determinism**: Same seed always produces identical traces
  (`persist_sim_deterministic`).

### What Is Not Yet Tested (Planned)

- **Network partitions**: No turmoil hosts, no network-level faults.
- **Concurrent linearizability**: Operations are sequential; concurrent
  history checking requires multiple client tasks with overlapping
  invoke/return windows.
- **Sharded reconfiguration under faults**: The DST covers single-shard
  operations only. Reconfiguration is tested via integration tests in
  `sharded.rs`.
- **BUGGIFY-style cooperative fault injection**: No injection points inside
  protocol phase boundaries.

### Fault Injection

Currently limited to:
- **CAS rejection** (stale expected seqnos, ~15% of operations)
- **Crash/recovery** (learner drop and reopen, ~5% of actions)

Planned additions:
- Network partitions and message delays (via turmoil)
- BUGGIFY injection points at protocol phase boundaries
- Acceptor restarts with in-flight proposals

### Seed Exploration

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

### Invariant Checking

_Inline_: Every operation is checked against the oracle immediately after
execution. Mismatches print the full `SimTrace` for debugging.

_Linearizability_: Stateright's `LinearizabilityTester` is invoked on every
operation. Currently sequential; concurrent checking is planned.

## Layer 3: Stress Testing (Open-Loop)

### Purpose

Validate that the system meets performance targets under realistic
production load. Unlike DST (which tests correctness under faults), stress
testing measures throughput, latency, and resource utilization under
sustained load.

### Methodology: Open-Loop

Stress tests use an _open-loop_ workload generator: clients submit proposals
at a fixed rate regardless of whether previous proposals have completed.

Closed-loop generators (wait for response before sending next request)
automatically throttle when the system is slow, masking backpressure issues
and queuing effects. Open-loop generators expose these problems: if the
system cannot keep up with the offered load, latency grows unbounded and the
test fails, which is the desired signal.

### Target Workload

| Parameter        | Value                    |
|------------------|--------------------------|
| Writers          | 10,000 concurrent        |
| Write rate       | 10 Hz per writer         |
| Payload size     | 4 KiB per proposal       |
| Aggregate rate   | 100,000 proposals/s      |
| Flush interval   | 20ms                     |
| Duration         | Sustained (minutes+)     |

This represents the target production workload: 10,000 client shards each
ticking at 10Hz with moderately-sized state updates.

### Metrics

**Throughput:**
- Proposals accepted per second (acceptor)
- Proposals evaluated per second (learner)
- Batches flushed per second (acceptor)
- Proposals per batch (batch efficiency)

**Latency (histogram):**
- End-to-end CAS latency (client submit to result received)
- Acceptor flush latency (pending to `compare_and_append` complete)
- Learner lag (acceptor upper minus learner listen frontier, in batches and
  wall time)
- Read linearization latency (read issued to read served)

**Resources:**
- CPU utilization (acceptor, learner, client)
- Memory utilization (learner StateMachine, result cache)
- Network bandwidth (transport, persist pubsub)
- Object storage API calls (writes from persist, reads on rehydration)

### Acceptance Criteria

From [02_invariants.md](02_invariants.md) performance properties:

| Metric                     | Target                 |
|----------------------------|------------------------|
| Aggregate throughput       | 100,000 proposals/s    |
| CAS p50 latency            | < 25ms                 |
| CAS p99 latency            | < 50ms                 |
| Read p50 latency           | < 5ms                  |
| Read p99 latency           | < 15ms                 |
| Learner rehydration        | < 10s                  |
| Batch efficiency           | ~2,000 proposals/batch |

### Known Scaling Considerations

- **Connection pooling**: A single HTTP/2 connection bottlenecks at ~10K
  concurrent streams due to h2 mutex contention. Multiple connections with
  shard-based distribution address this.
- **Learner memory**: The StateMachine holds all client shard state in
  memory. At ~300 entries/shard with 4KiB data, 10K client shards is roughly
  12 GiB.
- **Result cache sizing**: At 100K proposals/s with 10K batch retention, the
  cache holds roughly 2s of results. Clients must retrieve results within
  this window.

## How the Layers Compose

```
Stateright ──── verifies protocol design ────────── finds design bugs
     │
     │ shared invariant definitions
     │ (02_invariants.md)
     ▼
   DST ──────── verifies implementation ─────────── finds code bugs
     │           under faults
     │
     │ same code, realistic load
     │
     ▼
Stress test ─── verifies performance at scale ───── finds scalability bugs
```

A property failure at any layer is a bug:
- Stateright failure: protocol design bug. Fix the protocol.
- DST failure: implementation bug (protocol is sound). Fix the code.
- Stress test failure: scalability bug. Fix the implementation or revise
  targets.

## Ongoing Verification

These are designed for continuous use:

- **Stateright**: Runs in CI on every change to the protocol model. Fast
  (seconds).
- **DST with targeted seeds**: Runs in CI as a regular test. Fast (seconds
  per seed).
- **DST fuzzing**: Runs continuously in the background, exploring new seeds.
  Failures are captured as regression seeds.
- **Stress tests**: Run on demand or in nightly CI against a deployed
  cluster. Reports metrics for trend analysis.
