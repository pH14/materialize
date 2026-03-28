# Persist Shared Log: Horizontal Write Sharding

## Context

A single log shard + single acceptor tops out around 100K proposals/s. We want
1M client CaS ops/sec at <100ms p99. The approach: partition client shards
across multiple independent log shards (each with its own acceptor), tracked by
a metashard. This follows the Delos VirtualLog architecture, adapted to our
system where client shard independence (C4) means we don't need a total order
across log shards.

[04_virtual_log.md](04_virtual_log.md) sketches the idea at a high level. This
document fills in the concrete system specification: data models, actor
interfaces, protocols, failure handling, and the precise learner reconfiguration
sequence.

---

## 1. System Architecture

```
Client (persist Consensus trait)
  └─ Serving Layer (horizontally scalable, routes by client shard)
       ├─ Metashard Actor (partition map, reconfig coordinator, lifecycle manager)
       │    └─ Meta log shard (persist shard storing partition map + intents)
       ├─ Acceptor pool (one per log shard, managed by metashard actor)
       │    ├─ Log shard 0 (persist shard)
       │    ├─ Log shard 1 (persist shard)
       │    └─ ...
       └─ Learner pool (partitioned by log shard, N replicas each)
            ├─ Learner 0a, 0b (log shard 0's range)
            ├─ Learner 1a, 1b (log shard 1's range)
            └─ ...
```

**Four actor roles:**
1. **Acceptor** — one per log shard, unchanged except for a new `Sealed` error variant
2. **Learner** — partitioned by range. 1..N replicas per log shard for fault tolerance. Each replica subscribes to the log shards in its range, materializes those client shards' state. All replicas for the same range produce identical state (C1). On reconfiguration, learners carry forward state and transition subscriptions.
3. **Metashard Actor** — new role. Maintains the range-based partition map, coordinates reconfigurations (including pre-hydration), manages acceptor/learner lifecycle (spawn, teardown).
4. **Serving Layer** — new role. Routes client requests to the correct acceptor/learner based on the cached partition map.

---

## 2. Range-Based Partition Map

### 2.1 Partition key

The partition key is derived from the raw ShardId bytes. ShardIds are UUIDs (`s` prefix + hex), so distribution is uniform.

```rust
/// Derive a partition byte from a client shard key.
/// Uses the first byte of the ShardId's hex UUID (characters 1-2 after 's' prefix).
fn partition_key(client_shard: &str) -> u8 {
    u8::from_str_radix(&client_shard[1..3], 16).unwrap_or(0)
}
```

### 2.2 Partition map structure

A non-overlapping, covering partition of the [0x00, 0xFF] key space:

```rust
struct PartitionMap {
    epoch: u64,
    ranges: Vec<RangeAssignment>,
}

struct RangeAssignment {
    /// Inclusive lower bound of the partition key range.
    lo: u8,
    /// Exclusive upper bound. 0x100 (256) for the last range, covering through 0xFF.
    hi_exclusive: u16,
    /// Log shard that accepts writes for this range.
    log_shard: ShardId,
}
```

**Invariants on `PartitionMap`:**
- `ranges` is sorted by `lo`
- Non-overlapping: `ranges[i].hi_exclusive == ranges[i+1].lo`
- Covering: `ranges[0].lo == 0x00`, `ranges[last].hi_exclusive == 0x100`
- Every point in [0x00, 0xFF] maps to exactly one log shard

### 2.3 Routing

```rust
fn route(client_shard: &str, map: &PartitionMap) -> ShardId {
    let key = partition_key(client_shard);
    map.ranges.iter()
        .find(|r| key >= r.lo && (key as u16) < r.hi_exclusive)
        .unwrap()
        .log_shard
}
```

The same partition map is used for both write routing (client → acceptor) and read routing (client → learner). Multiple learner replicas can be assigned to the same range for fault tolerance; the serving layer picks any caught-up replica.

### 2.4 Reconfiguration as range operations

**Split (scale out):**
```
epoch 0: [([0x00, 0x100) → L1)]
epoch 1: [([0x00, 0x80) → L1), ([0x80, 0x100) → L2)]
```

**Split again:**
```
epoch 2: [([0x00, 0x40) → L1), ([0x40, 0x80) → L3), ([0x80, 0x100) → L2)]
```

**Merge (scale in):**
```
epoch 3: [([0x00, 0x80) → L4), ([0x80, 0x100) → L2)]
// L1 and L3 sealed, L4 absorbs their ranges
```

### 2.5 Learner state at creation (no runtime range changes)

Since all learners are created net-new on reconfiguration, there is no runtime gain/lose logic. A learner's range is immutable from birth. The only time a learner needs to read multiple log shards is during **initial hydration** (pre-hydration during reconfiguration, or chain replay on cold start).

At creation, the learner:
1. Knows its range and epoch (immutable identity)
2. Reads the metashard for the replay chain (predecessor links)
3. Builds state from the chain: load snapshot (if available) + replay sealed shards + subscribe to active shard, filtering by `partition_key(client_shard)` against its range boundaries
4. Once caught up, signals "ready"

At runtime, the learner processes one active log shard. That's it.

---

## 3. Metashard

### 3.1 What it is

A persist shard that stores the partition map and reconfiguration intents. Analogous to the Delos MetaStore. The metashard cannot use the shared-log service itself (circular) — it is bootstrapped independently with its own persist shard and a well-known `ShardId` passed via CLI.

### 3.2 Data model

The metashard persist shard schema:

| Dimension | Type | Meaning |
|-----------|------|---------|
| K | `MetashardKey` | Discriminated union, see below |
| V | `MetashardValue` | Entry-type-specific payload |
| T | `u64` | Configuration version, incremented per reconfiguration |
| D | `i64` | +1 insert, -1 retract |

**Entry types:**

```
PartitionMap entry:
  key:   ("partition_map")
  value: PartitionMap (the full range → log_shard mapping)

LogShard entry:
  key:   ("log_shard", log_shard_id: ShardId)
  value: (status: Active|Sealed|Finalized, epoch_created: u64, epoch_sealed: Option<u64>)

ReconfigurationIntent entry:
  key:   ("intent", intent_id: u64)
  value: (plan: ReconfigurationPlan, status: Preparing|Hydrating|Sealed|Committed)
```

The metashard actor materializes this into an in-memory state:

```rust
struct MetashardState {
    epoch: u64,
    partition_map: PartitionMap,
    log_shards: BTreeMap<ShardId, LogShardInfo>,
    pending_intent: Option<ReconfigurationIntent>,
}
```

### 3.3 Metashard trait

```rust
#[async_trait]
pub trait Metashard: Clone + Debug + Send + Sync + 'static {
    /// Look up the partition map to find which log shard owns a client shard.
    async fn lookup(&self, client_shard: String) -> Result<Assignment, MetashardError>;

    /// Return the current partition map.
    async fn partition_map(&self) -> Result<PartitionMap, MetashardError>;

    /// Subscribe to partition map changes. Returns a receiver yielding deltas.
    async fn subscribe(&self) -> Result<mpsc::Receiver<ReconfigurationDelta>, MetashardError>;

    /// Execute a reconfiguration (full lifecycle: intent → pre-hydrate → seal → commit).
    async fn reconfigure(&self, plan: ReconfigurationPlan) -> Result<u64, MetashardError>;

    /// Current epoch.
    async fn current_epoch(&self) -> Result<u64, MetashardError>;
}
```

Supporting types:

```rust
struct Assignment { log_shard: ShardId, epoch: u64 }

struct ReconfigurationPlan {
    expected_epoch: u64,
    /// New partition map to install.
    new_partition_map: PartitionMap,
    /// Log shards to seal.
    seal_log_shards: Vec<ShardId>,
    /// New log shards to create.
    new_log_shards: Vec<ShardId>,
}

struct ReconfigurationDelta {
    new_epoch: u64,
    old_map: PartitionMap,
    new_map: PartitionMap,
    sealed: Vec<ShardId>,
    added: Vec<ShardId>,
}
```

### 3.4 Metashard actor implementation

`PersistMetashardActor` follows the same actor pattern as `PersistAcceptor` and `PersistLearner`:

- **State**: `MetashardState` (materialized from its persist shard subscription)
- **Inputs**: command channel (`mpsc::Receiver<MetashardCommand>`), event source (persist `Subscribe` on metashard shard)
- **Outputs**: handle (`PersistMetashardHandle`), subscriber push channels
- **Run loop**: `select!` over event source and command channel
- **Lifecycle**: manages acceptor + learner spawn/teardown during reconfiguration

The metashard actor subscribes to its own persist shard. When it writes a reconfiguration, it observes the write via subscription, updates the in-memory state, and pushes deltas to subscribers. Multiple replicas can run for availability — they all converge to the same state.

**Shard count changes** are driven by an external CLI command that tells the metashard actor to reconfigure (e.g., `mz-persist-shared-log reconfigure --num-shards 10`).

---

## 4. Reconfiguration Protocol

Reconfiguration moves client shards from one log shard to another. The metashard actor coordinates the full lifecycle.

### 4.1 Step-by-step protocol (with pre-hydration)

```
reconfigure(plan: ReconfigurationPlan):

  Phase 0: Intent
    1. Validate: plan.expected_epoch == current epoch
    2. Write ReconfigurationIntent to metashard:
       { plan, status: Preparing }
    3. Intent is durable. On crash, restart resumes from here.

  Phase 1: Prepare infrastructure
    4. Create new log shard persist shards
    5. Spawn new acceptors for new log shards (NOT routing traffic yet)
    6. Spawn new learners for new log shards

  Phase 2: Pre-hydrate + snapshot
    7. New learners subscribe to OLD log shards (read-only) for ranges they're gaining
    8. New learners build up state, catch up to old log shard upper U
    9. Acquire CriticalSince on old log shards at frontier U
       Deterministic reader ID: "reconfig-epoch{N}-range{lo:02x}"
       Durable hold preventing compaction past U.
   10. Write snapshot to new log shards at T=0 via compare_and_append
       (CaS proposals establishing initial state — the BIG write, non-blocking)
   11. Continue pre-hydrating from old shards (catching up from U onward)
   12. Update intent: status = Hydrated

  Phase 3: Seal
   13. New learners signal "ready" (caught up)
   14. Seal old log shards: advance upper to Antichain::new() (idempotent)
   15. New learners drain remaining events from sealed shards (small window)
   16. Update intent: status = Sealed

  Phase 3.5: Write delta (on critical path, tiny)
   17. Write delta to new log shards at T=1: proposals between U and seal point
       ~100-1000 proposals. Milliseconds. CriticalSince guarantees diffs exist.

  Phase 4: Commit
   18. Write new partition map to metashard
   19. Update log shard entries: old → Sealed, new → Active + has_snapshot = true
   20. Update intent: status = Committed
   21. Push ReconfigurationDelta to all subscribers
   22. Serving layer switches routing
   23. FIRST proposals in new shards arrive NOW (after snapshot is already durable)

  Phase 5: Finalize (background, not blocking)
   24. Release CriticalSince on old shards: downgrade to empty antichain []
   25. Old shards → Finalized (new shards contain snapshot + delta)
   26. Advance old shard `since` to [], allowing persist to compact
   27. Tear down old acceptors (already sealed, no-op)
   28. Old learners drain result waiters, then shut down
```

### 4.2 Key property: pre-hydration eliminates cold-start

By Phase 3, the new learners already have the full state from the old log shards. When the seal lands, they drain the small remaining window of events and are immediately ready to serve traffic. The unavailability window shrinks to just the seal + metashard write (tens of milliseconds).

### 4.3 Ordering invariant: seal before partition map update

The seal MUST complete before the new partition map is committed. This prevents split-brain scenarios where proposals for the same client shard could commit on both old and new log shards.

### 4.4 Partial failure

**Crash during any phase:** The intent is durable. On restart, the metashard actor reads its persist shard, finds a pending intent, and resumes from the last completed phase:
- `Preparing`: re-spawn infrastructure
- `Hydrating`/`Hydrated`: re-check learner readiness, proceed to seal
- `Sealed`: proceed to commit
- `Committed`: proceed to finalize

**Intent is the lynchpin.** Without it, a crash between seal and commit leaves the system stuck (sealed shard, no new routing). With it, any replica can pick up and complete the reconfiguration.

### 4.5 Log shard chains and finalization

When reconfigurations happen faster than finalization, intermediate log shards accumulate. A learner reconstituting state must replay the chain of log shards that contributed to its range.

**Example: rapid reconfigurations, nothing finalized**
```
Epoch 0: [0x00, 0x100) → L1
Epoch 1: [0x00, 0x80) → L2, [0x80, 0x100) → L3.  L1 sealed, not finalized.
Epoch 2: [0x00, 0x40) → L4, [0x40, 0x80) → L5.  L2 sealed, not finalized.

Chain for range [0x00, 0x40): L1 → L2 → L4
```

**Predecessor tracking:** Each `LogShard` entry in the metashard records:
```
LogShard entry: {
    status: Active|Sealed|Finalized,
    epoch_created: u64,
    epoch_sealed: Option<u64>,
    range: RangeAssignment,
    predecessor: Option<ShardId>,   // shard this succeeded for overlapping ranges
    has_snapshot: bool,             // whether T=0 snapshot entries exist in this shard
}
```

**Chain reconstruction:** Follow `predecessor` links backward from the current shard until you reach a shard with `has_snapshot: true` (snapshot captures all prior state) or a genesis shard (no predecessor).

```rust
fn build_replay_chain(current: ShardId, shards: &BTreeMap<ShardId, LogShardInfo>) -> Vec<ShardId> {
    let mut chain = vec![current];
    let mut cursor = current;
    loop {
        let info = &shards[&cursor];
        if info.has_snapshot { break; }   // snapshot covers all prior state
        match info.predecessor {
            Some(pred) => { chain.push(pred); cursor = pred; }
            None => break,                // genesis
        }
    }
    chain.reverse() // oldest first
}
```

**Replay:** Subscribe to each shard in the chain in order. Sealed shards are finite — process to completion, then move to the next. The active shard (last in chain) continues indefinitely. Filter events by partition_key for the learner's range.

**Snapshots short-circuit the chain.** If L2 wrote a snapshot, the chain for [0x00, 0x40) collapses to `[L2 (load snapshot), L4]` — L1 is skipped.

**Finalization rule:** A sealed shard can be finalized only when a snapshot downstream in its chain covers its state. Specifically: shard X can be finalized when some descendant shard Y has `has_snapshot: true` and Y's snapshot was written after Y consumed X completely.

```
L1 can be finalized when: L2 has a snapshot (covers L1's state)
L2 can be finalized when: L4 or L5 has a snapshot (covers L1+L2's state)
```

If no downstream snapshot exists, the sealed shard MUST remain readable. Snapshots are an optimization for recovery speed, but the chain is always sufficient for correctness. Never finalize a shard that might still be needed for chain replay.

---

## 5. Receipt Evolution

The receipt must now identify which log shard a proposal landed on.

```
Current:  (batch_number: u64, position: u32)
New:      (log_shard: ShardId, batch_number: u64, position: u32)
```

Proto change:
```protobuf
message ProtoAppendResponse {
  uint64 batch_number = 1;
  uint32 position = 2;
  string log_shard = 3;  // NEW: ShardId of the log shard
  uint64 epoch = 4;      // NEW: partition map epoch (for stale-routing detection)
}
```

---

## 6. Acceptor Changes

Minimal changes to the existing `PersistAcceptor`:

1. **New error variant `AcceptorError::Sealed`**: Detected when `compare_and_append` returns `UpperMismatch` with `current = []` (empty antichain). Instead of retrying, the acceptor returns `Sealed`.

2. **Log shard identity in receipt**: The acceptor stores its `log_shard_id: ShardId` (passed at construction) and includes it in `ProtoAppendResponse`.

The acceptor's flush logic, batching, and retry behavior are unchanged.

---

## 7. Learner Reconfiguration

### 7.1 Partitioning model

Learners are partitioned by range. Each log shard has 1..N learner replicas for fault tolerance. All replicas for the same range subscribe to the same log shard(s) and produce identical state (C1).

The serving layer can route reads to any caught-up replica. Write result queries (`await_cas_result`) go to any replica for the relevant log shard.

### 7.2 Per-log-shard state

```rust
struct PersistLearner {
    // Global: client shard state, accumulated across all log shards
    state: StateMachine,

    // Per-log-shard
    log_shards: BTreeMap<ShardId, LogShardState>,

    // Range this learner is responsible for
    my_range: RangeAssignment,

    // Metashard subscription
    metashard: PersistMetashardHandle,

    // Command channel, config, metrics (unchanged)
    ...
}

struct LogShardState {
    event_source: ChannelEventSource,
    retraction_write: WriteHandle<OrderedKey, Proposal, u64, i64>,
    listen_frontier: Antichain<u64>,
    results: BTreeMap<u64, Vec<ProposalResult>>,
    result_waiters: BTreeMap<u64, Vec<ResultWaiter>>,
    pending_retractions: BTreeMap<OrderedKey, Proposal>,
    sealed: bool,
}
```

`StateMachine` stays global — `shards: BTreeMap<String, ShardState>` accumulates state regardless of which log shard the proposals came from. `pending_retractions` moves from `StateMachine` to `LogShardState` (retractions go to the log shard that sourced them).

### 7.3 Pre-hydration (before seal)

During Phase 2 of reconfiguration, new learners subscribe to OLD log shards to build state:

```
1. Metashard actor tells new learner: "pre-hydrate ranges [0x80, 0xFF] from L1"
2. New learner subscribes to L1 (read-only)
3. For each event from L1: if partition_key(client_shard) is in [0x80, 0xFF],
   apply to StateMachine. Otherwise, skip.
4. When listen_frontier catches up to L1's upper: signal "ready"
```

This happens while L1 is still active and serving traffic. The pre-hydration is invisible to clients.

### 7.4 Transition sequence (after seal)

```
1. L1 is sealed. New learner drains remaining events from L1.
   Listen frontier for L1 reaches [].

2. All pending result waiters for L1 are resolved.

3. Metashard commits new partition map.

4. New learner starts processing events from L2 (its new log shard).
   Proposals for [0x80, 0xFF] client shards arrive on L2.
   Evaluated against StateMachine state carried from L1.

5. L1's LogShardState is dropped from the learner.

Note: The snapshot (capturing L1's state) was already written to L2 at T=0
during Phase 2, and the delta at T=1 during Phase 3.5, before any regular
proposals landed in L2.
```

### 7.5 Ordering across reconfiguration

All proposals for client shard A on L1 must be processed before any proposals for A on L2. Guaranteed structurally:
- L1 is sealed before the partition map update
- Serving layer routes A's proposals to L2 only after seeing the new map
- Learner processes L1 fully (frontier = []) before processing L2 for the same client shard
- C4 ensures proposals for different client shards across log shards don't interact

### 7.6 Multi-source run loop

```rust
loop {
    select! {
        events = next_from_any_active_source(&mut self.log_shards) => {
            let (log_shard_id, events) = events;
            self.process_events(log_shard_id, events);
            if self.log_shards[&log_shard_id].listen_frontier.is_empty() {
                self.handle_seal(log_shard_id).await;
            }
        }
        delta = metashard_rx.recv() => {
            self.handle_reconfiguration(delta).await;
        }
        cmd = cmd_rx.recv() => {
            self.on_command(cmd);
        }
        // Retraction timer, upper fetch — unchanged
    }
}
```

### 7.7 Read linearization during transition

Per-log-shard linearization is sufficient (C4). During transition:
- Sealed shard fully consumed → serve from materialized state immediately
- Active shard → bus-stop pattern against its upper (unchanged)
- `list_keys()` fans out to all learners, merges results

### 7.8 Retraction handling for sealed shards

Once sealed, retractions can't be written. Accept bounded garbage in sealed shards — un-retracted proposals are inert, bounded by the last retraction interval. The snapshot entry in the new shard supersedes the sealed shard entirely once finalized.

### 7.9 Rehydration after crash (chain replay)

A fresh learner reconstitutes state by replaying its log shard chain:

1. Read metashard for current partition map and log shard history
2. Build replay chain via predecessor links (Section 4.5)
   - If a snapshot exists partway through the chain, start from there
   - Otherwise, start from the genesis shard
3. For each sealed shard in the chain: subscribe, filter events by range, process to completion
4. For the active shard (last in chain): subscribe and continue
5. C1 guarantees identical state regardless of restart timing

**Example with rapid reconfigurations (L1→L2→L4, no snapshots):**
```
1. Chain = [L1, L2, L4]
2. Subscribe to L1 (sealed). Filter for [0x00, 0x40). Process to completion.
3. Subscribe to L2 (sealed). Filter for [0x00, 0x40). Process to completion.
4. Subscribe to L4 (active). Process events. Start serving.
```

**Example where L2 has snapshot entries (common case):**
```
1. Chain = [L2 (has snapshot at T=0 + delta at T=1), L4]
2. Subscribe to L2 at since. T=0 snapshot entries establish initial state.
   T=1 delta entries fill the gap. Process to completion.
3. Subscribe to L4 (active). Process events. Start serving.
   (L1 skipped entirely — L2's T=0/T=1 contain its state.)
```

---

## 8. Serving Layer

### 8.1 Structure

```rust
pub struct ShardedService<M: Metashard> {
    metashard: M,
    partition_map: Arc<RwLock<PartitionMap>>,
    acceptors: Arc<RwLock<BTreeMap<ShardId, AcceptorHandle>>>,
    /// Multiple learner replicas per log shard.
    learners: Arc<RwLock<BTreeMap<ShardId, Vec<LearnerHandle>>>>,
}
```

Background task subscribes to metashard for push updates to partition map.

### 8.2 Write path

```
1. key = partition_key(client_shard)
2. log_shard = route(key, partition_map)
3. receipt = acceptors[log_shard].append(proposal)
4. If AcceptorError::Sealed: refresh partition_map, retry (bounded)
5. result = learners[log_shard].any_replica().await_cas_result(receipt)
6. Return result
```

### 8.3 Read path

```
1. key = partition_key(client_shard)
2. log_shard = route(key, partition_map)
3. learner = learners[log_shard].pick_replica()  // any caught-up replica
4. Return learner.head(key) / .scan(key, from, limit)
```

### 8.4 list_keys

Fan-out to one replica per log shard, merge, deduplicate.

---

## 9. Learner Trait Evolution

```rust
#[async_trait]
pub trait Learner: Clone + Debug + Send + Sync + 'static {
    // Reads — unchanged
    async fn head(&self, key: String) -> Result<ProtoHeadResponse, LearnerError>;
    async fn scan(&self, key: String, from: u64, limit: u64) -> Result<ProtoScanResponse, LearnerError>;
    async fn list_keys(&self) -> Result<Vec<String>, LearnerError>;

    // Result queries — gain log_shard parameter
    async fn await_cas_result(
        &self,
        log_shard: ShardId,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoCompareAndSetResponse, LearnerError>;
    async fn await_truncate_result(
        &self,
        log_shard: ShardId,
        batch_number: u64,
        position: u32,
    ) -> Result<ProtoTruncateResponse, LearnerError>;
}
```

---

## 10. Performance Model

| Component | Throughput | Scaling axis |
|-----------|-----------|--------------|
| Serving layer | ~200K req/s per instance | Add instances (stateless) |
| Acceptor | ~100K proposals/s per log shard | Add log shards (10 → 1M/s) |
| Learner (replay) | ~100K proposals/s per range | Partitioned by range, scales linearly |
| Learner (reads) | High (in-memory) | N replicas per range for availability |
| Metashard | Low write volume | Not a bottleneck |

**Latency budget (<100ms p99):**
- Serving layer routing: ~1ms (in-memory partition map lookup)
- Acceptor batching + flush: ~20ms
- Learner catch-up: ~20ms
- Network: ~5-10ms per hop
- Total: ~50ms typical, <100ms p99

---

## 11. New Invariants

### Reconfiguration Safety

**RC1. Seal-before-reassign.** A log shard is sealed (upper = []) before the partition map update that reassigns its ranges is committed.

**RC2. No proposal loss.** Every proposal either: (a) commits on the old shard, (b) fails with `Sealed` and client retries on new shard, or (c) times out and client retries. No silent loss.

**RC3. Learner ordering.** For any client shard A that moves from L1 to L2, the learner processes all of L1's proposals for A before any of L2's.

**RC4. Metashard convergence.** All metashard actor replicas processing the same persist shard arrive at identical partition maps.

**RC5. Reconfiguration liveness.** If a reconfiguration intent is written and the metashard is available, the reconfiguration eventually completes.

**RC6. Chain completeness.** For any range, the replay chain from the current active shard back through predecessor links to either a snapshot or genesis shard is complete — every shard in the chain is readable (not yet finalized). A sealed shard is finalized only when a downstream snapshot covers its state.

**RC7. Snapshot correctness.** A snapshot entry in shard Y captures the exact accumulated state from all predecessor shards through Y at the point the snapshot was written. Loading the snapshot + replaying Y from that point produces the same state as replaying the full chain.

### Partition Map Safety

**PM1. Covering.** The partition map covers [0x00, 0xFF] with no gaps.

**PM2. Non-overlapping.** No two ranges overlap.

**PM3. Monotonic epochs.** Each reconfiguration increments the epoch.

---

## 12. Resolved Design Decisions

All open questions have been resolved:

### Snapshot: pre-loaded into L2's persist shard + CriticalSince

The snapshot is written as entries directly into L2's persist shard during pre-hydration (Phase 2), keeping it off the critical path. A CriticalSince hold on L1 protects the diffs between the snapshot point and the seal, ensuring crash safety.

**Why not an out-of-band blob?** Keeping the snapshot in L2's persist shard means L2 is self-contained — persist compaction manages the snapshot data naturally alongside regular proposals. No external blob lifecycle management.

**Why not write the snapshot after the seal?** That would put a large write (40-400MB) on the critical path between seal and commit, inflating the unavailability window to seconds.

**The approach: big write during pre-hydration, tiny delta on the critical path.**

```
Phase 2b: Snapshot write (during pre-hydration, non-blocking)
  1. Learner catches up to L1's upper U
  2. Acquire CriticalSince on L1 at frontier U
     Reader ID: deterministic, e.g. "reconfig-epoch{N}-range{lo:02x}"
     Durable across crashes. Prevents L1's since from advancing past U.
  3. Write snapshot to L2 at T=0 via compare_and_append:
     For each client shard in the range, write a CaS proposal
     (expected_seqno=None → succeeds in empty shard, establishing initial state).
     This is the BIG write, but it happens during pre-hydration.
     Old acceptor/learner still serving traffic normally.
  4. Continue pre-hydrating from L1 (catching up from U onward)

Phase 3.5: Delta write (on critical path, tiny)
  5. After seal + drain: write delta to L2 at T=1
     Only the proposals between U (snapshot point) and S (seal point).
     ~100-1000 proposals. Milliseconds to write.
     CriticalSince on L1 guarantees these diffs still exist.

Phase 5: Release CriticalSince (background)
  6. Downgrade CriticalSince on L1 to empty antichain []
  7. L1's since can advance freely → compaction proceeds → finalization
```

**Fresh learner replaying L2:**
```
T=0: Snapshot entries (CaS proposals establishing initial state from L1)
T=1: Delta entries (L1 tail between snapshot and seal)
T=2+: Regular proposals (new traffic)
```
All just CaS proposals — the learner's existing replay logic handles them naturally.

**Crash safety via CriticalSince:**

| Crash point | Recovery |
|-------------|----------|
| Before CriticalSince acquired | No snapshot written. Restart pre-hydration from scratch. |
| After CriticalSince, before snapshot | Hold preserves L1 at U. Restart, write snapshot. |
| After snapshot, before delta | Hold preserves L1 at U. Replay L1 from U→S, write delta. |
| After delta written | L2 complete. Release CriticalSince, proceed to commit. |
| After commit | Release CriticalSince in background. |

The deterministic reader ID (derived from epoch + range) means the handle is always recoverable after crash.

**CriticalSince lifecycle:** The hold is acquired at the snapshot point and released after the delta is confirmed written. Duration: minutes at most. The held-back data is small (timestamps ≥ U, which is close to L1's upper).

**Chain replay fallback:** If L2 has no snapshot (e.g., snapshot write failed), fresh learners fall back to replaying the full chain of sealed predecessors. The metashard's predecessor links provide the chain. Correctness is always guaranteed; the snapshot is an optimization.

### Actor identity (net-new, immutable)

On reconfiguration, **create net-new actors for all ranges whose log shard assignment changes.** Unchanged ranges keep their existing actors.

Every actor (acceptor and learner) has an immutable identity at creation:

```rust
struct ActorIdentity {
    range: RangeAssignment,
    log_shard: ShardId,
    epoch: u64,
}
```

The reconfiguration is a blue-green deployment:
```
Epoch 0: [Acceptor_e0(L1, [0x00,0x100)), Learner_e0(L1, [0x00,0x100))]

Split:
  Phase 1: Spawn new actors (not yet routing)
    Acceptor_e1(L2, [0x00,0x80)), Learner_e1(L2, [0x00,0x80))
    Acceptor_e1(L3, [0x80,0x100)), Learner_e1(L3, [0x80,0x100))

  Phase 2: Pre-hydrate
    Learner_e1(L2) subscribes to L1, filters for [0x00,0x80), catches up
    Learner_e1(L3) subscribes to L1, filters for [0x80,0x100), catches up

  Phase 3: Seal L1
  Phase 4: Commit — route traffic to epoch 1 actors
  Phase 5: Epoch 0 actors drain and shut down
```

No mutable identity. No "following" client shards. Each actor knows its range, shard, and epoch from birth.

### No concurrent reconfigurations

One reconfiguration at a time. The intent mechanism enforces this: `reconfigure()` is rejected when `pending_intent.is_some()`.

### Summary of all decisions
- **Partition model**: Range-based, raw ShardId first byte as partition key
- **Partition maps**: Same map for acceptors and learners, with N replicas per range
- **Lifecycle**: Metashard actor manages acceptor/learner spawn/teardown
- **Shard count changes**: External CLI tells metashard actor to reconfigure
- **ReconfigurationIntent**: Required — enables pre-hydration and crash recovery
- **Pre-hydration**: New learners hydrate from old shards BEFORE seal
- **Snapshot format**: Pre-loaded into L2's persist shard at T=0 during pre-hydration. L2 is self-contained. CriticalSince on L1 protects diffs for crash safety. Delta (U→S) written at T=1 on the critical path (tiny, milliseconds).
- **Log shard finalization**: Safe after CriticalSince released and new shard contains snapshot + delta. Chain replay is the always-correct fallback.
- **Actor identity**: Immutable (range, log_shard, epoch), net-new on reconfiguration
- **Concurrent reconfigs**: Not allowed — one at a time via intent lock

---

## 13. Implementation Phasing

**Phase 0: Design doc** ← **current step**
Write the full specification as reference docs:
- Extend `04_virtual_log.md` with range partition map and concrete protocol
- Add `05_reconfiguration.md` for the reconfiguration protocol (intent, pre-hydrate, seal, commit, finalize)
- Update `02_invariants.md` with RC1-RC6, PM1-PM3

**Phase 1: Foundation** — Metashard data model, codec, actor, trait. Static partition map.

**Phase 2: Multi-shard serving** — ShardedService routing, multi-acceptor pool, receipt evolution. Static N log shards.

**Phase 3: Learner partitioning** — LogShardState extraction, per-log-shard result cache, partitioned read routing, N replicas.

**Phase 4: Reconfiguration** — Intent protocol, pre-hydration, seal, commit, finalize. Snapshot entries. Dynamic reconfiguration.

**Phase 5: Testing** — DST with reconfiguration scenarios. Stateright model for RC1-RC6. Stress tests for 1M CaS/s.

---

## 14. Verification Plan

| Property | DST | Stateright | Stress Test |
|----------|-----|------------|-------------|
| RC1 (seal-before-reassign) | Yes | Yes | |
| RC2 (no proposal loss) | Yes | | Yes |
| RC3 (learner ordering) | Yes | Yes | |
| RC4 (metashard convergence) | Yes | | |
| RC5 (reconfig liveness) | Yes | Yes* | Yes |
| RC6 (chain completeness) | Yes | | Yes |
| RC7 (snapshot correctness) | Yes | | |
| PM1-PM3 (partition map) | Yes | Yes | |
| 1M CaS/s target | | | Yes |
| <100ms p99 | | | Yes |

---

## 15. File Changes

### New files
- `doc/reference/05_reconfiguration.md` — reconfiguration protocol spec (intent, pre-hydrate, seal, commit, finalize)
- `src/persist_log/metashard.rs` — `PersistMetashardActor`, `PersistMetashardHandle`, `MetashardState`, `PartitionMap`
- `src/persist_log/metashard_codec.rs` — `MetashardKey`, `MetashardValue`, range types, columnar codecs

### Modified files
- `src/lib.rs` — add `Metashard` trait, `MetashardError`, `PartitionMap` types, update `Learner` trait
- `src/service.rs` — replace `PersistSharedLogGrpcService` with `ShardedService`
- `src/persist_log/acceptor.rs` — add `AcceptorError::Sealed`, log_shard in receipt
- `src/persist_log/learner.rs` — extract `LogShardState`, multi-source run loop, range filtering, snapshot entries
- `src/main.rs` — multi-shard wiring, `--metashard-id` CLI arg
- `src/metrics.rs` — add `MetashardMetrics`
- `doc/reference/04_virtual_log.md` — concrete range partition map, resolve open questions
- `doc/reference/02_invariants.md` — add RC1-RC6, PM1-PM3
