# Actors

The shared log service is built from four message-driven actors. Each actor
is an async task that owns private state and communicates through typed
channels. This directory contains the implementations.

## Why actors?

- No shared mutable state. Each actor owns its state machine directly.
- Deterministic simulation testing. Message passing and persist APIs compose
  cleanly with turmoil and the DST harness.
- Clear boundaries. Control-plane concerns stay in the metashard and router;
  steady-state data-plane concerns stay in the acceptor and learner.

## The actors

### Meta

The metashard is the control-plane authority.

Responsibilities:

- persist the current `MetaState` in the meta shard
- claim leadership by CAS-bumping `leader_id`
- reject overlapping reconfigurations when `start_state.is_some()`
- create new acceptors and learners
- seal predecessor shards and commit the new routing

The metashard is the only actor that coordinates reconfiguration.

### Router

The router is the client-facing entry point.

Responsibilities:

- serve the `PersistSharedLog` RPC surface
- subscribe to the meta shard in the background
- cache a stable `PartitionMap`
- route writes to the correct acceptor
- route reads and await-result requests to the correct learner
- retry requests when a shard is sealed or routing changes

Routers ignore meta-shard updates while `start_state.is_some()`. Traffic only
moves after the metashard commits a stable state.

### Acceptor

The acceptor is the single writer for one log shard.

Responsibilities:

- accept `ProtoLogProposal` appends
- batch and flush proposals with `compare_and_append`
- return `ProtoAppendResponse`
- during reconfiguration, write batch 1 bulk snapshot and batch 2 delta
  snapshot before regular traffic starts
- poll a `RetractionSource` and flush returned entries as `-1` diffs

The acceptor never evaluates CAS preconditions. It treats `Proposal` as
opaque bytes.

### Learner

The learner is the replicated state machine for one log shard.

Responsibilities:

- subscribe to one log shard
- decode proposals and apply CAS or truncate semantics
- maintain materialized client-shard state in memory
- serve `head`, `scan`, and `list_keys`
- answer await-result queries
- identify pending retractions for dead proposals

Multiple learner replicas may follow the same log shard and converge to the
same state.

## Actor relationships

```text
meta shard
  ^                       router(s)
  |                           ^
  |                           |
metashard --------------------+
  |
  | creates actors / seals predecessors
  v
log shard N <----- acceptor N
    |
    +-----> learner N replica 0
    +-----> learner N replica 1

router
  -> acceptor N for CompareAndSet / Truncate
  -> learner N for Head / Scan / ListKeys / await-result
```

The acceptor and learner know nothing about the metashard or the partition
map. They operate on a single persist shard identified by `ShardId`. Only the
metashard and router deal with partition maps, epochs, or multi-shard
coordination.

## Persist pubsub groups

Pubsub is an optimization for notification latency.

- `Meta shard pubsub`: routers subscribe so they notice committed routing
  updates quickly.
- `Per-log-shard pubsub`: learners subscribe so a freshly flushed batch
  becomes visible without waiting on consensus polling.

Pubsub does not change the ownership model:

- metashard writes the meta shard
- acceptor writes the log shard
- learner remains read-only

## Data model

### Log shards

Each log shard stores proposals in differential form:

- `K`: `OrderedKey` = `(batch_id, position, shard)`
- `V`: `Proposal` = serialized `ProtoLogProposal`
- `T`: `u64` append timestamp
- `D`: `i64`, where `+1` adds a proposal and `-1` retracts one

The acceptor is the only writer. Learners discover retractions, but the
acceptor is the actor that flushes them.

### Meta shard

The meta shard stores:

- `K`: `MetaState`
- `V`: `()`
- `T`: `u64`
- `D`: `i64`, where `+1` adds the new state and `-1` retracts the old one

Each durable update keeps exactly one live `MetaState` row.

`MetaState` contains:

- `epoch`: monotonically increasing configuration version
- `leader_id`: durable fencing token for the current metashard leader
- `target_state`: the current or desired shard set
- `start_state`: `None` when stable, `Some(old_state)` while a
  reconfiguration is in progress

## Why this split matters

This layout keeps the steady-state actors simple:

- one acceptor writes one shard
- one learner tails one shard

And it keeps the complex coordination in one place:

- the metashard owns leader fencing and reconfiguration
- the router owns request routing and retry

That separation is what lets the implementation scale horizontally without
teaching the acceptor or learner about global shard topology.
