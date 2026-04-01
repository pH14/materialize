# persist-shared-log

A group commit consensus service for Materialize persist. Batches independent
cross-shard CAS proposals into a single durable persist write per flush
interval, then a learner evaluates them deterministically during playback.

## Architecture

The service is split into four actor types that can run in a single process
(monolith mode) or as separate OS processes communicating over Unix domain
sockets (distributed mode):

- **Metashard** -- partition map authority. Manages the mapping from key ranges
  to log shards. Persists reconfiguration state for crash recovery.
- **Acceptor** -- blind group commit. Receives proposals, batches them, flushes
  to persist. One per log shard.
- **Learner** -- state machine that tails the log, evaluates CAS during
  playback, serves reads. N replicas per log shard.
- **Router** (ShardedService) -- routes client gRPC requests to the correct
  acceptor/learner based on the partition map.

## Running

### Monolith mode (all-in-one)

Simplest way to run -- all actors in a single process with in-memory storage:

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

Each actor runs as a separate process. They find each other via Unix domain
sockets under a shared `--run-dir` directory.

```bash
export RUN_DIR=/tmp/shared-log
export METASHARD_ID=s00000000-0000-0000-0000-000000000000
export PERSIST_BLOB_URL=file:///tmp/persist/blob
export PERSIST_CONSENSUS_URL='postgres://phemberger@localhost:5432/consensus'
```

**Terminal 1 -- Metashard:**

```bash
cargo run -p mz-persist-shared-log -- metashard \
  --run-dir $RUN_DIR \
  --metashard-id $METASHARD_ID \
  --blob-url $PERSIST_BLOB_URL \
  --consensus-url $PERSIST_CONSENSUS_URL
```

**Terminal 2 -- Discover shard IDs:**

```bash
grpcurl -plaintext -unix \
  $RUN_DIR/metashard-$METASHARD_ID/grpc.sock \
  mz_persist.gen.consensus_service.ConsensusMetashard/GetPartitionMap
```

This returns the partition map with the log shard IDs the metashard generated.
Note the `log_shard` value for the next steps (e.g.
`s11111111-1111-1111-1111-111111111111`).

**Terminal 3 -- Acceptor** (using the shard ID from step 2):

```bash
export PERSIST_SHARD_ID=<shard-id-from-step-2>

cargo run -p mz-persist-shared-log -- acceptor \
  --run-dir $RUN_DIR \
  --shard-id $PERSIST_SHARD_ID \
  --blob-url $PERSIST_BLOB_URL \
  --consensus-url $PERSIST_CONSENSUS_URL
```

**Terminal 4 -- Learner** (same shard ID):

```bash
cargo run -p mz-persist-shared-log -- learner \
  --run-dir $RUN_DIR \
  --shard-id $PERSIST_SHARD_ID \
  --replica-id 0 \
  --blob-url $PERSIST_BLOB_URL \
  --consensus-url $PERSIST_CONSENSUS_URL
```

**Terminal 5 -- Router:**

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

**Terminal 6 -- Test a write:**

```bash
grpcurl -plaintext -d '{
  "key": "test-key",
  "new": {"seqno": 1, "data": "aGVsbG8="}
}' localhost:6890 \
  mz_persist.gen.consensus_service.PersistSharedLog/CompareAndSet
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
