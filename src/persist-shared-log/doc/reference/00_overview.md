# Persist Shared Log: Architecture Overview

Persist shared log is a `Consensus` implementation built out of persist
itself. Instead of every client shard independently doing durable CAS against
a root consensus store, many client-shard operations are batched into a
smaller number of writes to one or more persist log shards. Learners then
deterministically replay those log shards to decide which operations
committed.

If you are new to the crate, read the docs in this order:

1. This overview
2. [01_protocol.md](01_protocol.md)
3. [05_horizontal_sharding.md](05_horizontal_sharding.md)
4. [02_invariants.md](02_invariants.md)
5. [03_testing.md](03_testing.md)
6. [04_virtual_log.md](04_virtual_log.md)

## Glossary

- `client shard`: A persist shard from the rest of Materialize. This is the
  thing whose `Consensus` operations we are serving.
- `log shard`: A persist shard used as the shared log for some set of client
  shards.
- `meta shard`: A dedicated persist shard that stores routing and
  reconfiguration state for the whole service.
- `epoch`: The configuration version of the partition map.
- `batch`: One atomic `compare_and_append` into a log shard. Regular traffic
  starts at batch 3. Batches 1 and 2 are reserved for reconfiguration setup.
- `receipt`: The `ProtoAppendResponse` returned by the acceptor after a
  proposal is durably appended.
- `upper`: Persist's write frontier. Reads linearize against a fetched upper.
- `since`: Persist's compaction frontier. Reconfiguration uses CriticalSince
  holds to keep predecessor data readable.

## The Four Actors

The system is split into four actor roles:

- `Router`: The client-facing entry point. It serves the `PersistSharedLog`
  API, caches the current `PartitionMap`, routes writes to acceptors, routes
  reads and result waits to learners, and retries when routing changes.
- `Metashard`: The control-plane actor. It durably stores the current
  `MetaState`, fences competing leaders, and coordinates reconfiguration.
- `Acceptor`: The single writer for one log shard. It blindly appends
  proposals, writes setup batches during reconfiguration, and periodically
  flushes learner-sourced retractions.
- `Learner`: A replica of the state machine for one log shard. It tails that
  shard, evaluates proposals, serves reads, and identifies entries that
  should later be retracted.

Acceptors and learners are metashard-blind. They do not read the meta shard,
do not understand partition maps, and do not coordinate reconfiguration
themselves. Only the router and metashard know about routing or shard
movement.

That keeps the steady-state data path simple:

- acceptor: one log shard, one write handle
- learner: one log shard, one subscribe stream
- router: routing logic
- metashard: control plane only

## Network and Dependency Graph

At a high level:

```text
client
  -> router
     -> acceptor for CompareAndSet / Truncate
     -> learner for Head / Scan / ListKeys / await-result

router background task
  -> subscribes to the meta shard

metashard actor
  -> writes the meta shard

acceptor
  -> compare_and_append to exactly one log shard
  -> polls a RetractionSource for -1 diffs

learner
  -> subscribes to exactly one log shard
  -> fetches that shard's upper for read linearization
```

A few operational consequences:

- Routers talk directly to acceptors and learners once they have a routing
  snapshot. The metashard is not on the synchronous steady-state request
  path.
- A learner depends only on its own log shard plus the router's request
  stream. It has no dependency on the meta shard.
- An acceptor depends only on its own log shard plus the router's append
  stream and retraction source. It has no dependency on the meta shard.
- Retraction polling exists for garbage collection, not commit correctness.
  If learner-backed retractions stop flowing, committed outcomes stay
  correct, but dead proposals can accumulate in the log.

## What Persist Stores

There are two distinct persist-backed collections in this design.

### Log shard

Each log shard stores proposal rows as a differential collection:

```text
K = OrderedKey(batch_id, position, shard)
V = Proposal(bytes)
T = u64
D = i64
```

- `+1` rows add proposals.
- `-1` rows retract previously added proposals.
- The acceptor is the only writer. Learners discover retractions, but the
  acceptor is the actor that writes the `-1` rows.

### Meta shard

The meta shard stores routing state as:

```text
K = MetaState
V = ()
T = u64
D = i64
```

Each metashard update appends the new `MetaState` with `+1` and retracts the
previous one with `-1` in the same CAS batch. That keeps the shard bounded
to a single live row.

## Scaling and Failure Boundaries

The split gives clear horizontal-scaling boundaries:

- Routers scale horizontally. They are stateless apart from their cached
  routing snapshot.
- Learners scale horizontally per log shard. Any caught-up replica can serve
  reads for that shard.
- Write throughput scales by adding more log shards.
- A single log shard's useful write path is still single-acceptor. Multiple
  writers can technically contend on the same shard, but that does not
  produce true write scale-out.

It also gives clean failure isolation:

- A failure in one log shard affects only the client shards routed to that
  log shard.
- Metashard failure blocks new reconfigurations and actor reconciliation,
  but already-running routers, acceptors, and learners keep serving
  steady-state traffic.
- If an acceptor is sealed during reconfiguration, it returns
  `AcceptorError::Sealed`. The router waits for the next committed routing
  snapshot and retries.
- If a learner replica fails, another replica for that shard can serve
  reads.

## Why the "Virtual Log" Works

A client shard only needs a total order over its own proposals. Client
shards are independent, so the system does not need one global order across
every physical log shard. The router and metashard partition client shards
across multiple log shards without teaching the acceptor or learner about
global sharding.

That is the main design payoff: we scale writes by adding log shards while
keeping the steady-state actors simple and single-shard.

See [01_protocol.md](01_protocol.md) for the exact request paths and
[05_horizontal_sharding.md](05_horizontal_sharding.md) for the
reconfiguration protocol.
