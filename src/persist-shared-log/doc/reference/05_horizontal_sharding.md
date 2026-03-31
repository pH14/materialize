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

### 4.1 Step-by-step protocol (acceptor-owned predecessor state)

```
reconfigure(plan: ReconfigurationPlan):

  Phase 0: Intent + CriticalSince
    1. Validate: plan.expected_epoch == current epoch
    2. Write ReconfigurationIntent to metashard:
       { plan, status: Preparing }
    3. Intent is durable. On crash, restart resumes from here.
    4. Acquire CriticalSince on retiring log shards.
       Deterministic reader ID: "reconfig-epoch{N}-range{lo:02x}"
       Durable hold preventing compaction.

  Phase 1: Spawn actors
    5. Create new log shard persist shards.
    6. Spawn new acceptors with predecessor list + range assignment.
       Each acceptor writes two setup batches before entering its
       normal flush loop (see Section 6 for details):
         batch_id=1: bulk snapshot (predecessor entries at CriticalSince)
         batch_id=2: delta snapshot (predecessor entries between snapshot and seal)
       Without predecessors, both are empty writes advancing upper.
    7. Spawn new learners (no predecessor awareness — they subscribe to
       their shard and process events from batch_id=1 onward).

  Phase 2: Bulk snapshot + seal
    8. Metashard subscribes to each new shard's frontier.
    9. When all new shard frontiers ≥ 2: bulk snapshots done.
   10. Seal old log shards: advance upper to Antichain::new() (idempotent).
   11. Update intent: status = Sealed.

  Phase 3: Delta snapshot
   12. Acceptors detect predecessor seal via subscribe (frontier = []).
   13. Acceptors write delta to new shards at batch_id=2: proposals
       between CriticalSince and seal. ~100-1000 proposals, milliseconds.
   14. Metashard watches new shard frontiers ≥ 3: delta done.

  Phase 4: Commit
   15. Write new partition map to metashard.
   16. Update log shard entries: old → Sealed, new → Active + has_snapshot = true.
   17. Update intent: status = Committed.
   18. Swap routing to new shards.
   19. FIRST regular proposals in new shards arrive NOW (at batch_id=3+).

  Phase 5: Finalize (background, not blocking)
   20. Release CriticalSince on old shards: downgrade to empty antichain [].
   21. Old shards → Finalized (new shards contain snapshot + delta).
   22. Advance old shard `since` to [], allowing persist to compact.
   23. Tear down old acceptors (already sealed, no-op).
   24. Old learners drain result waiters, then shut down.
```

### 4.2 Key property: acceptor setup before seal minimizes downtime

The acceptor's bulk snapshot (batch_id=1) is written while the predecessor shard is still live, keeping the large data copy off the critical path. The learner subscribes to the new shard and processes these entries as they arrive, building state automatically. By the time the seal lands, the learner already has the predecessor's state — only the small delta (batch_id=2) remains. The unavailability window is just the seal + delta write + routing swap (tens of milliseconds).

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

### 6.1 Sealed detection

**New error variant `AcceptorError::Sealed`**: Detected when `compare_and_append` returns `UpperMismatch` with `current = []` (empty antichain). Instead of retrying, the acceptor returns `Sealed`.

### 6.2 Receipt identity

**Log shard identity in receipt**: The acceptor stores its `log_shard_id: ShardId` (passed at construction) and includes it in `ProtoAppendResponse`.

### 6.3 Predecessor state writing (setup batches)

The acceptor is responsible for writing predecessor state into its shard. Every acceptor writes two setup batches before entering its normal flush loop:

| batch_id | Name | With predecessors | Without predecessors |
|----------|------|--------------------|----------------------|
| 1 | Bulk snapshot | Predecessor entries at CriticalSince (blind copy) | Empty (advance upper) |
| 2 | Delta snapshot | Predecessor entries between CriticalSince and seal | Empty (advance upper) |
| 3+ | Regular traffic | Normal proposals from clients | Normal proposals |

Same codepath every time — the learner sees the same event structure regardless.

**Bulk snapshot (batch_id=1):** The acceptor subscribes to each predecessor shard at its CriticalSince `since`. For each event with diff=+1 (live data), it filters by the acceptor's range assignment, re-keys the `OrderedKey` to `(batch_id=1, position, shard)`, and writes all entries via `compare_and_append`. The `Proposal` bytes are opaque — the acceptor doesn't decode them.

**Delta snapshot (batch_id=2):** After the bulk snapshot, the acceptor continues reading from the predecessor subscribe. When the predecessor's frontier hits `[]` (sealed by the metashard), the delta is complete. Events between the snapshot point and the seal are re-keyed to `(batch_id=2, position, shard)` and written. Only +1 diffs are copied — predecessor retractions (-1 diffs) are discarded because the new shard's learner generates its own.

**Multi-acceptor coordination:** All competing acceptors for the same shard independently build identical batches (deterministic from same predecessor at same CriticalSince). `compare_and_append` CAS on upper resolves races — first writer wins, losers detect `UpperMismatch`, check upper, and skip.

**Idempotency on crash recovery:** The acceptor checks the shard's upper to determine which phase to resume: upper < 2 → write bulk snapshot; upper < 3 → write delta; upper ≥ 3 → skip to regular traffic.

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

### 7.3 Learner hydration (from its own shard)

The learner subscribes only to its own shard. It never reads predecessor shards directly — the acceptor handles that (Section 6.3). The learner processes events as they arrive:

```
1. Learner subscribes to L2 at since=0.
2. Acceptor writes bulk snapshot at batch_id=1 → learner sees CaS
   proposals establishing initial state from predecessors. Applies them
   with normal CaS semantics (expected=None chain builds up state).
3. Acceptor writes delta at batch_id=2 → learner sees proposals from
   the predecessor's tail (between snapshot and seal). Applies them
   against the snapshot state.
4. Acceptor enters normal flush loop at batch_id=3+ → learner sees
   regular client traffic.
```

The learner doesn't know or care about reconfiguration. It just processes events from its shard in timestamp order.

### 7.4 Transition sequence

```
1. Old shard L1 is sealed. Old learner drains remaining L1 events.
   Listen frontier for L1 reaches [].

2. All pending result waiters for L1 are resolved.

3. Metashard commits new partition map, swaps routing.

4. New learner is already processing events from L2 (its shard).
   By now it has processed batch_id=1 (snapshot) and batch_id=2 (delta),
   so it has the full state from L1. Regular proposals (batch_id=3+)
   are evaluated against this carried-forward state.

5. L1's actors are torn down.

Note: The snapshot (batch_id=1) and delta (batch_id=2) were written by the
new acceptor before regular traffic started. The learner processed them
automatically via its subscribe.
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

### Snapshot: acceptor-written into L2's persist shard + CriticalSince

The snapshot is written by the **new acceptor** as entries directly into L2's persist shard at batch_id=1, keeping the large data copy off the critical path. The acceptor blindly copies persist entries from predecessor shards (re-keying OrderedKeys, preserving Proposal bytes untouched). A CriticalSince hold on L1 protects the diffs between the snapshot point and the seal, ensuring crash safety.

**Why the acceptor, not the learner?** The acceptor is the single writer to the shard. Having the acceptor write the snapshot preserves this invariant and keeps the learner purely read-only — it subscribes to one shard and processes events. No cross-shard awareness, no write handles, no special reconfiguration logic in the learner.

**Why not an out-of-band blob?** Keeping the snapshot in L2's persist shard means L2 is self-contained — persist compaction manages the snapshot data naturally alongside regular proposals. No external blob lifecycle management.

**Why not write the snapshot after the seal?** That would put a large write (40-400MB) on the critical path between seal and commit, inflating the unavailability window to seconds.

**The approach: big write before seal, tiny delta after seal.**

```
Phase 1: Acceptor spawned with predecessors
  1. Acquire CriticalSince on L1.
     Reader ID: deterministic, e.g. "reconfig-epoch{N}-range{lo:02x}"
     Durable across crashes. Prevents L1's since from advancing.
  2. Acceptor subscribes to L1 at CriticalSince since.
  3. Reads snapshot events (consolidated state at since).
     Filters by range. Re-keys OrderedKeys to (batch_id=1, position, shard).
  4. compare_and_append all entries at batch_id=1.
     This is the BIG write, but it happens while L1 still serves traffic.
  5. Meanwhile, new learner subscribes to L2 and processes batch_id=1
     entries automatically (CaS chain builds state).

Phase 2: Metashard seals L1 (after observing L2's upper ≥ 2)

Phase 3: Delta write (on critical path, tiny)
  6. Acceptor's predecessor subscribe detects L1 frontier = [] (sealed).
  7. Acceptor writes delta to L2 at batch_id=2:
     Only +1 diffs between CriticalSince and seal.
     ~100-1000 proposals. Milliseconds to write.

Phase 4: Metashard swaps routing (after observing L2's upper ≥ 3)

Phase 5: Release CriticalSince (background)
  8. Downgrade CriticalSince on L1 to empty antichain []
  9. L1's since can advance freely → compaction proceeds → finalization
```

**Fresh learner replaying L2:**
```
batch_id=1: Snapshot entries (blind copy of predecessor state at CriticalSince)
batch_id=2: Delta entries (predecessor tail between snapshot and seal)
batch_id=3+: Regular proposals (new traffic)
```
All just proposals in the persist shard — the learner's existing event processing handles them naturally with CaS semantics.

**Crash safety via CriticalSince + idempotent writes:**

| Crash point | Recovery |
|-------------|----------|
| Before CriticalSince acquired | No snapshot written. Restart from intent. |
| After CriticalSince, before snapshot | Hold preserves L1. Acceptor checks upper < 2, writes snapshot. |
| After snapshot, before delta | Hold preserves L1. Acceptor checks upper < 3, writes delta. |
| After delta written | L2 complete (upper ≥ 3). Release CriticalSince, proceed to commit. |
| After commit | Release CriticalSince in background. |

The deterministic reader ID (derived from epoch + range) means the CriticalSince handle is always recoverable after crash. The acceptor's setup batches are idempotent — it checks the shard's upper to determine which phase to resume from.

**CriticalSince lifecycle:** The hold is acquired before spawning actors and released after the delta is confirmed written. Duration: minutes at most.

**Multi-acceptor coordination:** All competing acceptors deterministically compute the same batches from the same predecessor at the same CriticalSince. `compare_and_append` CAS on upper resolves races — first writer wins, losers skip.

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
- **ReconfigurationIntent**: Required — enables crash recovery
- **Predecessor state**: Acceptor writes bulk snapshot at batch_id=1 (before seal) and delta at batch_id=2 (after seal). Same codepath with or without predecessors (empty writes when none). Learner is read-only — subscribes to its shard and processes events.
- **Snapshot format**: Blind copy of predecessor persist entries into L2's persist shard. L2 is self-contained. CriticalSince on L1 protects diffs for crash safety. Proposals are opaque bytes; only OrderedKeys are re-keyed.
- **Multi-acceptor coordination**: Deterministic batches, CAS on upper resolves races.
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
