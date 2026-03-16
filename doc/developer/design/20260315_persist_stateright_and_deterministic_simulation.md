# Persist Stateright Modeling and Deterministic Simulation

Status: Draft

## Context

Materialize's formalism defines the user-visible contract in terms of pTVCs,
`since`, `upper`, and correctness between those frontiers. Persist is the
durable implementation of that contract, and `persist-client` contains the
current semantic state machine for shard metadata and maintenance.

Today, the repo already has several useful pieces:

- The intended semantics in the top-level formalism:
  `doc/developer/platform/formalism.md`
- The persist design background and its explicit "deterministic state machine"
  framing:
  `doc/developer/design/20220330_persist.md`
- The current semantic core:
  `src/persist-client/src/internal/{machine,state,trace,state_versions}.rs`
- Existing deterministic-ish unit coverage:
  datadriven machine tests, proptests, and fixed-seed tests in
  `src/persist-client`
- A deterministic network/runtime substrate for simulation:
  `src/persist/src/turmoil.rs` and
  `PersistClientCache::new_for_turmoil` in `src/persist-client/src/cache.rs`

This makes the problem less "how do we invent formal and simulation testing"
and more "how do we add one small formal model and one simulation harness that
reuse the existing seams and stay aligned with the code over time."

## Goals

- Check the safety properties that matter for shard correctness with an
  explicit model checker.
- Check linearizability of the externally visible single-shard API at the
  semantic boundary we choose to model.
- Add deterministic simulation tests (DSTs) that exercise the real code under
  crashes, partitions, retries, and maintenance interleavings.
- Keep the model and DSTs aligned with the implementation as persist evolves.
- Keep the initial scope small enough to land and run in CI.

## Non-goals

- Modeling S3/Postgres/Aurora byte-for-byte.
- Proving the entire implementation correct in one model.
- Replacing the existing datadriven, unit, or proptest coverage.
- Modeling the full Materialize system; the unit of reasoning is a single
  persist shard.

## Key Observation

There are two distinct verification targets:

1. `persist-client` owns the shard semantics.
   This is where `compare_and_append`, `downgrade_since`,
   `compare_and_downgrade_since`, leases, maintenance requests, and trace
   updates are decided.

2. `persist` owns the durability substrate and failure surface.
   This is where blob/consensus contracts, rollup/diff reconstruction, and
   deterministic failure injection live.

That suggests two complementary efforts instead of one oversized model.

## Proposed Verification Stack

### 1. `persist-client`: Stateright semantic model

The primary Stateright model should live with `persist-client` and target the
single-shard protocol exposed by the machine/state layers.

The model should treat the shard as a deterministic state machine plus a small
set of explicitly scheduled side effects:

- `compare_and_append`
- leased reader registration / expiry / `downgrade_since`
- critical reader registration / `compare_and_downgrade_since`
- maintenance creation and completion:
  rollup, compaction, gc
- finalization / tombstone behavior

This is the right level because it is close to the formalism and also close to
the code that currently decides semantics.

### 2. `persist`: deterministic simulation tests for the real substrate

The primary real-system simulations should use the actual client/machine code
against the existing `turmoil` blob and consensus servers.

These tests should verify that the real implementation preserves the modeled
semantics across:

- host crashes and restarts
- network partitions
- delayed or timed-out RPCs
- retried requests after indeterminate outcomes
- maintenance running concurrently with reads and writes

### 3. Shared scenario vocabulary

To keep the model and DSTs aligned, both should execute the same abstract
operation language.

Concretely, we should introduce a small test-only `ScenarioOp` enum and reuse
it in:

- the Stateright model driver
- the deterministic simulation harness
- optionally, adapters from existing datadriven regressions

If a new shard operation matters semantically, it should first appear in
`ScenarioOp`, then get:

- model behavior
- real-system harness behavior
- property/assertion coverage

This is the main mechanism that keeps the verification artifacts in lockstep
with the code.

The same applies to environment actions like partitions, repairs, and restarts:
they should live in shared test-only enums instead of being encoded ad hoc in
individual tests.

We should reinforce this with code-level patterns that make drift fail at
compile time:

- keep shared actor identities and shared test operations in common enums
- use a separate history-op enum for the linearizability boundary instead of a
  boolean flag
- destructure shared structs and enums exhaustively instead of indexing
  anonymous vectors or relying on `..` patterns

That way, adding a new externally visible operation or changing the shape of an
existing one forces updates in the model, the deterministic simulation harness,
and the shared scenario adapters.

### 4. Linearizability as a first-class property

In addition to state invariants, the model should check that the externally
visible shard behavior is linearizable against an abstract single-shard
specification.

This matters because frontier monotonicity and reconstruction invariants alone
are not enough to catch all of the failures we care about. In particular,
persist has subtle cases around:

- idempotent retries after indeterminate outcomes
- contended writers
- reads that race with writes and maintenance
- internal maintenance work that must be observationally silent

Those are exactly the sorts of bugs that linearizability checks are good at
surfacing.

## Model Boundaries

### `persist-client` model boundary

The model should represent:

- shard-global `upper`
- shard-global `since`
- shard-global `seqno_since`
- registered writers and their last successful write token/upper
- leased readers and critical readers
- an abstract representation of shard contents
- pending maintenance work and claimed work
- tombstone/finalized state

The model should not represent:

- encoded batch bytes
- blob keys
- rollup serialization
- actual task spawning
- detailed metrics

Those details are important for implementation testing, but not for the first
semantic model.

The model should, however, define an abstract sequential specification for the
operations that are part of the modeled external history. Internal maintenance
steps may be represented in the model state, but they should not themselves be
part of the linearizability history unless we intentionally expose them.

### `persist` model boundary

For `persist` itself, we should not build a second full shard semantics model.
Instead, the `persist`-focused verification should center on:

- the `Blob` and `Consensus` contracts in `src/persist/src/location.rs`
- `StateVersions` reconstruction invariants in
  `src/persist-client/src/internal/state_versions.rs`
- failure classification:
  determinate vs indeterminate
- retry/idempotence expectations at the storage boundary

This avoids duplicating the `persist-client` model while still giving `src/persist`
its own explicit verification surface.

## First Model

The first Stateright model should be intentionally narrow:

- one shard
- total-order timestamps only
- a tiny key domain, e.g. `a`, `b`
- tiny diffs, e.g. `-1`, `+1`
- small numbers of actors:
  1-2 writers, 1 leased reader, 1 critical reader, 1 maintenance worker
- abstract batches represented as logical update sets, not parts or blob keys

The model state should include an abstract TVC oracle:

- a set of all logically appended updates
- the currently visible pTVC interval `[since, upper)`
- read results derived from that abstract state

This lets us assert correctness in the language of the formalism, not only in
the language of implementation metadata.

## Initial Action Set

The first action set should be:

- `RegisterWriter`
- `CompareAndAppend { expected_upper, new_upper, updates, token }`
- `RegisterLeasedReader`
- `DowngradeSince`
- `RegisterCriticalReader`
- `CompareAndDowngradeSince`
- `ExpireWriter`
- `ExpireLeasedReader`
- `StartRoutineMaintenance`
- `CompleteRollup`
- `CompleteGc`
- `StartCompaction`
- `CompleteCompaction`

We should defer schema evolution, restore, rewrite, and multi-shard scenarios
until the first model is stable.

For linearizability, we should distinguish between:

- externally visible operations that enter the history
- internal maintenance operations that do not

The initial linearizability history should likely include:

- `CompareAndAppend { expected_upper, new_upper, updates, token }`
- `RegisterLeasedReader`
- `DowngradeSince`
- `RegisterCriticalReader`
- `CompareAndDowngradeSince`
- point reads over the abstract shard state, e.g. modeled `Snapshot(as_of)`
  operations

The initial history should not include:

- rollup creation
- gc
- compaction

Instead, those should be required to preserve observational equivalence of the
abstract shard object.

## Properties to Check

### Safety properties

- `upper` never regresses.
- `since` never regresses.
- `seqno` never regresses.
- `seqno_since <= seqno`.
- Reads only succeed for times in the readable interval implied by the shard
  state.
- Successful appends extend the logical shard contents exactly once.
- Replaying the same idempotency token does not duplicate logical data.
- Compaction preserves logical shard contents between `since` and `upper`.
- GC never deletes metadata still needed to reconstruct any state at or after
  `seqno_since`.
- Finalized shards stay tombstoned and reject future writes.

### Linearizability properties

- The externally visible shard operations are linearizable against an abstract
  single-shard specification.
- Successful writes appear exactly once in the linearized history.
- Reads observe a state consistent with some point between invocation and
  response.
- Internal maintenance does not change the externally visible sequential
  behavior of the shard.

For the first version, the sequential spec can be intentionally small:

- a logical shard contents oracle
- current `upper`
- current `since`
- readable `snapshot(as_of)` behavior

This is enough to make retries, stale reads, and maintenance interference
meaningfully testable.

### Observational equivalence

For every reachable state, the metadata state should agree with an abstract
pTVC oracle:

- for all `t` such that `since <= t < upper`, `snapshot(t)` is correct
- no successful write can create a gap in the shard upper progression
- no maintenance action can change the abstract shard contents

This is the main formalism-to-code bridge.

Together, the two checks serve different purposes:

- linearizability catches externally visible ordering bugs
- invariants and observational equivalence catch internal metadata bugs

## State-Space Control

To keep Stateright practical:

- Start with a single total-order timestamp lattice.
- Represent frontiers as single integers plus the empty antichain.
- Model compaction/gc results abstractly instead of enumerating blobs/parts.
- Separate the work into multiple models rather than one giant one:
  core append/read model, lease model, maintenance model.
- Prefer explicit actor roles over modeling every internal task as a node.

If necessary, maintenance can initially be modeled as environment actions that
produce valid or stale results, rather than full background workflows.

## Deterministic Simulation Plan

The DST harness should use the real code with the existing turmoil-friendly
adapters:

- `mz_persist::turmoil::{serve_blob, serve_consensus}`
- `PersistClientCache::new_for_turmoil`
- real `PersistClient`, `WriteHandle`, `ReadHandle`, and `SinceHandle`

### Topology

Each simulation should run:

- one `blob` host
- one `consensus` host
- 1-N client hosts

Client hosts can play one of:

- writer
- leased reader
- critical reader
- observer/assertion client

### Faults

The first DST suite should cover:

- bouncing the blob host during append
- bouncing the consensus host during append
- network partitions between a client and blob
- network partitions between a client and consensus
- client crash and restart with a new writer lease
- maintenance running while a reader snapshots or listens

### Assertions

Each scenario should end by checking via the real client:

- final `upper`
- final `since`
- snapshot contents at selected times
- ability or inability to read outside the readable interval
- no duplicate logical writes after retries

We should prefer end-state assertions in terms of logical contents and
frontiers, not internal logs or metrics.

## Where the Shared Scenario Layer Lives

The simplest initial layout is:

- `src/persist-client/tests/stateright/`
  model definitions and Stateright property tests
- `src/persist-client/tests/turmoil/`
  deterministic simulation scenarios using the real client
- `src/persist-client/tests/scenario/`
  shared `ScenarioOp`, generators, and end-state assertions

This keeps the semantic model, real-system DSTs, and shared scenario language
in one crate with direct access to the current shard API.

For `src/persist`, lower-level turmoil-specific tests can continue to live near
the crate, but the shard-centric scenarios should stay in `persist-client`.

## How We Keep This Aligned With the Code

### 1. Tie the model to the existing semantic seam

The model should track the concepts implemented in:

- `src/persist-client/src/internal/state.rs`
- `src/persist-client/src/internal/machine.rs`
- `src/persist-client/src/internal/trace.rs`
- `src/persist-client/src/internal/state_versions.rs`

If semantics move elsewhere, the model boundary should move with them.

### 2. Use one scenario language

Every semantically meaningful operation should be expressed once in
`ScenarioOp`. That becomes the checklist for keeping model and DST coverage in
sync.

### 3. Reuse existing regressions

High-value existing datadriven cases in `src/persist-client/tests/machine/`
should be migrated or mirrored into scenario tests over time. This avoids
building a second completely separate regression corpus.

### 4. Add a lightweight maintenance checklist

Whenever a PR changes:

- `StateCollections`
- `Machine`
- public handle semantics in `read.rs`, `write.rs`, or `critical.rs`
- state reconstruction in `state_versions.rs`

the author should update one or more of:

- the Stateright model
- the scenario enum / runner
- the deterministic simulation suite
- this design note if the verification boundary changed

### 5. Split CI by cost

- Per-PR:
  small bounded Stateright runs plus a small fixed-seed DST suite
- Nightly or opt-in:
  larger Stateright exploration and multi-seed turmoil runs

That keeps verification present in normal development without making it
prohibitively expensive.

## Proposed Delivery Order

### Phase 0: shared test scaffolding

- Add `ScenarioOp`
- Add a tiny abstract shard oracle for expected contents/frontiers
- Add one fixed-seed real-system scenario runner with turmoil

### Phase 1: core Stateright model

- Model writer registration, append, leased reader registration, and
  `downgrade_since`
- Check frontier monotonicity, idempotency, readable-interval correctness, and
  linearizability of the modeled external history

### Phase 2: real DST coverage for retries and crashes

- Indeterminate append outcome followed by retry
- client crash/restart
- blob/consensus bounce around append and snapshot

### Phase 3: maintenance semantics

- compaction preserve-contents property
- GC preserve-reconstructability property
- rollup/recovery property

### Phase 4: advanced features

- critical since handles
- finalization/tombstones
- schema registration/evolution

## Recommended First Slice

The best first slice is:

1. shared `ScenarioOp`
2. one Stateright model for:
   writer register, append, leased reader, downgrade since, and snapshot
   linearizability
3. one turmoil DST for:
   append, restart after indeterminate failure, and snapshot validation

That is small enough to land, directly exercises the formalism's
`since`/`upper` contract, and creates the scaffolding needed to grow into
compaction/gc later.

## Open Questions

- Whether compaction should initially be modeled as an atomic semantic action or
  as a two-step request/result workflow.
- Whether the scenario runner should adapt existing datadriven tests directly,
  or whether we should hand-port only the highest-value cases.
- Whether the `persist`-specific verification surface should also include a
  tiny model for `StateVersions` rollup/diff reconstruction, or whether DSTs
  plus the `persist-client` model are enough initially.

## Recommendation

Start with `persist-client` as the semantic source of truth, and treat
`src/persist` primarily as the deterministic failure substrate for real-system
simulation. Build one shared scenario layer, one small Stateright model with a
linearizability check over the external shard API, and one small turmoil suite
before expanding scope.

That gives us the highest confidence per line of new test code, and it creates
the right social/process hooks to keep the model and DSTs aligned with the code
instead of letting them drift into separate worlds.
