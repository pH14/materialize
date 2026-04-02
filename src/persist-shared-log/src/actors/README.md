# Actors

The shared log service is built from four message-driven actors. Each is an
async task that owns private state and communicates exclusively through typed
command channels (mpsc). This directory contains their implementations.

## Why actors?

**No shared mutable state.** All state is owned by a single task — no mutexes,
no lock ordering concerns. Each actor processes one command at a time, making
the state machine straightforward to reason about.

**Deterministic simulation testing.** Because actors interact only through
message channels and persist APIs (which go through turmoil's simulated
network), the entire system can run under turmoil with a fixed seed and produce
identical traces. This is the foundation of the DST suite in `tests/`.

## The actors

### Metashard (`metashard.rs`)

Partition map authority. Manages the mapping from key ranges to log shards.
Persists its state to a dedicated persist shard (the "meta shard") for crash
recovery. Drives reconfiguration (split/merge) and creates actors for new
shards via the `ActorFactory`.

### Acceptor (`acceptor.rs`)

Blind group commit. Receives CAS and truncate proposals, batches them, and
flushes to a persist shard. Returns receipts (batch number + position) but
does NOT evaluate CAS — proposals are appended unconditionally. The learner
evaluates them during playback.

### Learner (`learner.rs`)

Replicated state machine. Each learner subscribes to the acceptor's persist
shard and deterministically replays the same ordered log of proposals. Because
every replica processes the same log in the same order, they all converge to
identical state — any replica can serve reads. During playback, the learner
evaluates CAS preconditions, materializes state, and serves reads and result
queries.

### Router (`router.rs`)

Client-facing entry point. Clients connect to the router, which routes each
request to the correct acceptor or learner based on the partition map.
Subscribes to the meta shard for partition map updates.

## Actor relationships

```
  ┌─ meta shard ──────────────────────────────────────────────┐
  │                                                           │
  │  ┌─────────────┐       ┌─────────────────────────────┐    │
  │  │  Metashard   │       │     Meta Persist Shard     │    │
  │  │  (authority) │──────▶│     (partition map)        │    │
  │  └─────────────┘       └──────────────┬──────────────┘    │
  │                                       │                   │
  └───────────────────────────────────────│───────────────────┘
                                          │ subscribes to
                                          ▼
                                   ┌─────────────┐
                                   │   Router(s) │
                                   └──┬───────┬──┘
                       writes / reads │       │ to each shard range
                       ┌──────────────┘       └──────────────┐
                       ▼                                     ▼
  ┌─ log shard 0 ──────────────────┐  ┌─ log shard 1 ──────────────────┐
  │                                │  │                                │
  │  ┌────────────────────┐        │  │  ┌────────────────────┐        │
  │  │     Acceptor 0     │        │  │  │     Acceptor 1     │        │
  │  │  (blind commit)    │        │  │  │  (blind commit)    │        │
  │  └────────┬───────────┘        │  │  └────────┬───────────┘        │
  │           │ writes to          │  │           │ writes to          │
  │           ▼                    │  │           ▼                    │
  │  ┌────────────────────┐        │  │  ┌────────────────────┐        │
  │  │  Log Persist Shard │        │  │  │  Log Persist Shard │        │
  │  └──┬─────────────┬───┘        │  │  └──┬─────────────┬───┘        │
  │     │ subscribes  │ subscribes │  │     │ subscribes  │ subscribes │
  │     ▼             ▼            │  │     ▼             ▼            │
  │  ┌──────────┐ ┌──────────┐     │  │  ┌──────────┐ ┌──────────┐     │
  │  │Learner 0 │ │Learner 1 │     │  │  │Learner 0 │ │Learner 1 │     │
  │  │(replica) │ │(replica) │     │  │  │(replica) │ │(replica) │     │
  │  └──────────┘ └──────────┘     │  │  └──────────┘ └──────────┘     │
  │                                │  │                                │
  └────────────────────────────────┘  └────────────────────────────────┘
```

## Persist pubsub groups

Pubsub provides instant write notifications so that subscribers don't have to
poll consensus (Postgres/CRDB). Two groups:

- **Meta shard pubsub** — The metashard hosts a pubsub server. Routers connect
  as clients. When the metashard persists a new partition map, each router's
  routing task sees it instantly via `Subscribe::fetch_next()`.

- **Per-shard pubsub** — Each acceptor hosts a pubsub server. Its learner(s)
  connect as clients. When the acceptor flushes a batch, the learner's
  `Subscribe::fetch_next()` returns instantly.

## Boundaries

The acceptor and learner know nothing about the metashard, partition maps, or
multi-shard coordination. They operate on a single persist shard identified by
`ShardId`. The metashard and router are the only actors that deal with partition
maps and routing.

## Data model

### Log shards

Each log persist shard stores proposals in differential format:

- **K**: `OrderedKey` — `(batch_id, position, shard)` StructArray giving a
  stable total order through compaction
- **V**: `Proposal` — serialized protobuf bytes
- **T**: `u64` — incremented by 1 per batch, in lock-step with persist upper
- **D**: `i64` — `+1` for proposals, `-1` for learner retractions

### Meta shard

The meta persist shard stores a single key (`__metashard`) whose value is a
serialized `ProtoMetashardState` protobuf containing:

- **epoch** — monotonically increasing configuration version
- **ranges** — the partition map (`[lo, hi_exclusive) -> log_shard_id`)
- **intent** — in-flight reconfiguration state for crash recovery
