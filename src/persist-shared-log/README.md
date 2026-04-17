# persist-shared-log

A group commit consensus service for Materialize persist. Batches independent
cross-shard CAS proposals into a single durable persist write per flush
interval, then a learner evaluates them deterministically during playback.

## Architecture

The service is split into four actor types that can run in a single process
(monolith mode) or as separate OS processes communicating over Unix domain
sockets (distributed mode):

- `Metashard`: control-plane authority. Persists `MetaState`, fences
  competing leaders, and coordinates reconfiguration.
- `Router`: client-facing entry point. Subscribes to the meta shard, caches
  the current `PartitionMap`, and routes each request to the correct acceptor
  or learner.
- `Acceptor`: single writer for one log shard. Batches proposals, flushes
  them to persist, and writes setup batches during reconfiguration.
- `Learner`: state machine for one log shard. Tails the shard, evaluates CAS
  during playback, serves reads, and identifies proposals that need to be
  retracted. A log shard can have one or more learner replicas.

Acceptors and learners do not read the meta shard. Only the metashard and
router know about partition maps or reconfiguration.

## Running

### Monolith mode (all-in-one)

Runs all actors in a single process with in-memory storage:

```bash
cargo run -p mz-persist-shared-log -- monolith \
  --metashard-id s00000000-0000-0000-0000-000000000000
```

With external storage (Postgres consensus + file blob):

```bash
cargo run -p mz-persist-shared-log -- monolith \
  --metashard-id s00000000-0000-0000-0000-000000000000 \
  --blob-url file:///tmp/persist/blob \
  --consensus-url 'postgres://$(whoami)@localhost:5432/consensus'
```

### Distributed mode (separate processes)

The metashard automatically spawns acceptor and learner child processes when
the partition map is created. You only need to start the metashard and the
router.

```bash
export RUN_DIR=/tmp/shared-log
export METASHARD_ID=s00000000-0000-0000-0000-000000000000
export PERSIST_BLOB_URL=file:///tmp/persist/blob
export PERSIST_CONSENSUS_URL='postgres://phemberger@localhost:5432/consensus'
```

**Terminal 1, metashard** (spawns acceptor + learner automatically):

```bash
cargo run -p mz-persist-shared-log -- metashard \
  --run-dir $RUN_DIR \
  --metashard-id $METASHARD_ID \
  --blob-url $PERSIST_BLOB_URL \
  --consensus-url $PERSIST_CONSENSUS_URL
```

**Terminal 2, router:**

```bash
cargo run -p mz-persist-shared-log -- router \
  --run-dir $RUN_DIR \
  --metashard-id $METASHARD_ID \
  --listen-addr 0.0.0.0:6890 \
  --blob-url $PERSIST_BLOB_URL \
  --consensus-url $PERSIST_CONSENSUS_URL
```

The router listens on TCP `:6890` for client requests and connects to actors
via Unix sockets under `$RUN_DIR`.

**Test a write:**

```bash
grpcurl -plaintext \
  -proto src/persist/src/consensus_service.proto \
  -d '{"key":"test-key","new":{"seqno":1,"data":"aGVsbG8="}}' \
  localhost:6890 \
  mz_persist.gen.consensus_service.PersistSharedLog/CompareAndSet
```

**Inspect the partition map:**

```bash
grpcurl -plaintext \
  -proto src/persist/src/consensus_service.proto \
  unix:///$RUN_DIR/metashard-$METASHARD_ID/grpc.sock \
  mz_persist.gen.consensus_service.ConsensusMetashard/GetPartitionMap
```

**Reconfigure (split to 2 shards):**

```bash
grpcurl -plaintext \
  -proto src/persist/src/consensus_service.proto \
  -d '{"num_shards": 2}' \
  unix:///$RUN_DIR/metashard-$METASHARD_ID/grpc.sock \
  mz_persist.gen.consensus_service.ConsensusMetashard/Reconfigure
```

### Socket path layout

In distributed mode, each actor listens on a deterministic Unix socket path:

```
$RUN_DIR/
  metashard-<metashard_id>/grpc.sock      # metashard gRPC (grpcurl-able)
  metashard-<metashard_id>/pubsub.sock    # metashard persist pubsub
  acceptor-<shard_id>/grpc.sock           # acceptor gRPC
  pubsub-<shard_id>/grpc.sock             # acceptor-hosted persist pubsub
  learner-<shard_id>-0/grpc.sock          # learner replica 0
  learner-<shard_id>-1/grpc.sock          # learner replica 1 (optional)
```

The router discovers learner replicas by globbing
`learner-<shard_id>-*/grpc.sock`.

## Running the benchmarks

```bash
cargo run --release --example spec -- --num-keys 100 --ops-per-key 100
```

## Running the tests

```bash
cargo test -p mz-persist-shared-log --lib
```
