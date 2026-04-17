# Persist Shared Log: Virtual Log

The current implementation details live in
[05_horizontal_sharding.md](05_horizontal_sharding.md). This page exists
only to explain the idea behind the term "virtual log."

## What "Virtual Log" Means Here

A single physical log shard can only scale so far. To increase write
throughput, the system partitions client shards across multiple independent
log shards.

From the perspective of any one client shard, there is still a single
logical log: all proposals for that client shard are routed to exactly one
physical log shard at a time, and that shard provides the total order that
determines CAS outcomes.

The collection of physical log shards is the virtual log.

## Why This Works

The system does not need one global order across every client shard. It
only needs a total order per client shard, and client shards are
independent.

That lets us scale writes by partitioning:

- shard A can be ordered on log shard L1
- shard B can be ordered on log shard L2
- neither shard needs to know about the other's log

## Who Knows About the Partitioning

Only the control plane and routing layer know about the virtual log:

- the `Metashard` stores the partition map and coordinates movement between
  log shards
- the `Router` caches that map and routes requests

The steady-state data-plane actors stay simple:

- the `Acceptor` knows one log shard
- the `Learner` knows one log shard

Neither actor reads the meta shard, follows replay chains, or subscribes to
multiple log shards.

## What Reconfiguration Achieves

Reconfiguration changes which physical log shard owns a key range. The
metashard coordinates that move so the new shard is self-contained before
routers switch traffic to it.

A new learner can recover by replaying its own shard alone. It does not
need to chase historical predecessor shards.

For the concrete protocol, see
[05_horizontal_sharding.md](05_horizontal_sharding.md).
