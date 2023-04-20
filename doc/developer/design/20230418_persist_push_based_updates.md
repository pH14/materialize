- Feature name: Persist Push-Based Updates
- Associated: https://github.com/MaterializeInc/materialize/issues/18661

# Summary
[summary]: #summary

Replacing Persist's polling-based discovery of new state with push-based updates.

# Motivation
[motivation]: #motivation

tl;dr
* Reduces CockroachDB read traffic to near zero, reducing current CPU usage by 20-30% (relative to total usage).
  This will allow us to [scale down our clusters further](https://github.com/MaterializeInc/materialize/issues/18665).
* Reduces the median latency of observing new data written to a shard from ~300ms to single digit millis

---

Full context:

Persist stores the state related to a particular shard in `Consensus`, which today is implemented on CockroachDB (CRDB).
When any changes to state occur, such a frontier advancement or new data being written, a diff with the relevant change 
is appended into `Consensus`. Any handles to the shard can read these diffs, and apply them to their state to observe
the latest updates.

However, today, we lack a way to promptly discover newly appended diffs beyond continuous polling of `Consensus`/CRDB.
This means that ongoing read handles to a shard, such as those powering Materialized Views or `Subscribe` queries, must 
poll `Consensus` to learn about progress. This polling today contributes ~30% of our total CRDB CPU usage, and introduces
~300ms worth of latency (and ~500ms in the worst case) between when a diff is appended into `Consensus` and when the read
handles are actually able to observe the change and progress their dataflows.

If Persist had a sidechannel to communicate appended diffs between handles, we could eliminate nearly the full read load
we put on CRDB, in addition to vastly reducing the latency between committing new data and observing it. This design doc
will explore the interface for this communication, as well as a proposed initial implementation based on RPCs that flow 
through `environmentd`.

# Explanation
[explanation]: #explanation

## PubSub Trait

We abstract the details of communicating push-based updates through a Trait that uses a publisher-subscriber-like model.
We additionally keep the abstraction simple: callers are given an address that they can dial to begin pushing and receiving
state diffs, and that is all that is needed. This API is modeled in a way to set us up for future changes in implementation
-- as we scale, we may wish to back the sidechannel with an external pubsub/message broker system, such as Kafka, and this
API should be sufficient to do so.

One of the goals of the Trait is to encapsulate all of the logic needed to communicate diffs to the right parties within
Persist. The sidechannel used to communicate updates can be thought of as an internal detail to Persist: somehow, without
needing to know exactly how, Persist shards are able to update themselves in response to changes made in other processes,
and it does not need to be tightly bound to the coordination logic elsewhere in Materialize.

Below is a strawman proposal for this interface:

```rust
pub struct ProtoPushDiff {
  pub shard_id: String,
  pub seqno: u64,
  pub diff: Bytes,
}

trait PersistPubSubClient {
  type Push: PersistPush;
  type Sub: PersistSub;
  
  /// Receive handles with which to push and subscribe to diffs.
  async fn connect(addr: &str) -> (Self::Push, Self::Sub);
}

trait PersistPush {
  /// Push a diff to subscribers.
  async fn push(&self, diff: ProtoPushDiff) -> Result<(), Error>;
}

trait PersistSub {
  type S: Stream<ProtoPushDiff>;
  
  /// Informs the server which shards this subscribed should receive diffs for.
  /// May be called at any time to update the set of subscribed shards.
  async fn subscribe(&self, shards: Vec<ShardId>) -> Result<(), Error>;
  
  /// Returns a Stream of diffs to the subscribed shards.
  async fn stream(&mut self) -> S;
}
```

## Publishing Updates

When any handle to a persist shard successfully compares-and-sets a new state, it will publish the change to `PersistPush`.

## Applying Received Updates

When a process receives a diff, it needs to apply it to the local copy of state and notify any readers that new data /
progress may be visible. The mechanisms to do this have largely been built:

* [#18488](https://github.com/MaterializeInc/materialize/pull/18488) gives each process a single, shared copy of persist
  state per shard. This gives us a natural entrypoint to applying the diff.
* [#18778](https://github.com/MaterializeInc/materialize/pull/18778) introduces the ability to `await` a change to state,
  rather relying on polling.

It is not assumed for push-based updates delivery to be perfectly reliable nor ordered. We will continue to use polling
as a fallback for discovering state updates, though with a more generous retry policy than we currently have. If a process 
receives a diff that does not apply cleanly (e.g. out-of-order delivery), the process will fetch the latest state from
Cockroach directly instead. While we will continue to have these fallbacks, we anticipate push-based updates being effective
enough to eliminate the need for nearly all Cockroach reads.

# Reference Explanation

As a first implementation of the Persist sidechannel, we propose building a new RPC service that flows through `environmentd`.
The service would be backed by gRPC with a minimal surface area -- to start, we would use a simple bidirection stream that 
allows each end to push updates back and forth, with `environmentd` broadcasting out any changes it receives.

## RPC Service

```protobuf
message ProtoPushDiff {
    string shard_id = 1;
    uint64 seqno = 2;
    bytes diff = 3;
}

message ProtoPushSubscribe {
  repeated string shards = 1;
}

message ProtoPushMessage {
  oneof message {
    ProtoPushDiff push_diff = 1;
    ProtoPushSubscribe subscribe = 2;
  }
}

service ProtoPersist {
    rpc Push (stream ProtoPushMessage) returns (stream ProtoPushMessage);
}
```

The proposed topology would have each `clusterd` connect to `environmentd`'s RPC service. `clusterd` would subscribe to
updates to any shards in its `PersistClientCache` (a singleton in both `environmentd` and `clusterd`). When any `clusterd` 
commits a change to a shard, it publishes the diff to `environmentd`, which tracks which connections are subscribed to which
shards, and broadcasts out the diff to the interested subscribers. From there, each subscriber would [apply the diff](#applying-received-updates).

`environmentd` is chosen to host the RPC service as it necessarily holds a handle to each shard (and therefore is always 
interested in all diffs), and to align with the existing topology of the Controller, rather than introduce the complexity
of point-to-point connections between unrelated `clusterd` processes.

# Rollout
[rollout]: #rollout

We will roll out this change in phases behind a feature flag. The change is seen purely as an optimization opportunity,
and should be safe to shut off at any time. We will aim ensure that any errors handled due to push-based updates cannot
crash or halt the process.

We anticipate message volume to be on the order of 1-1.5 per second per shard, with each message in the 100 bytes-1KiB
range. Given the scale of our usage today, we would expect `environmentd` to be able to broadcast this data and message volume
comfortably with minimal impact on performance. 

That said, we'll want to keep an eye on is how much time `environmentd` spends decoding, applying, and broadcasting diffs.
`environmentd` is the only process that must own a copy of state to each shard, and it will likely be applying more diffs
than before, in addition to the new workload of broadcasting. We anticipate `clusterd` to be negligibly impacted by this
added RPC traffic, as each `clusterd` would only need to push/receive updates for the shards it is reading and writing.

Our existing Persist metrics dashboard, in addition to the new metrics outlined below, should be sufficient to monitor
the change and understand its impact.

## Testing and observability
[testing-and-observability]: #testing-and-observability

We will introduce several metrics to understand the behavior of the change:

* # of updates pushed
* # of updates received that apply cleanly & uncleanly
* How frequently `Listen`s need to fallback to polling
* End-to-end latency of appending data to observing it
* Timings of `environmentd`'s broadcasting / load

CI will be used to ensure we do not introduce novel crashes or halts, but we anticipate the most interesting data to
come from opted-in environments in staging and prod, rather than what we can synthetically observe.

## Lifecycle
[lifecycle]: #lifecycle

We will use a feature flag that can quickly toggle on the publication / subscription to updates dynamically. Our metrics
dashboards will inform our rollout process.

# Drawbacks
[drawbacks]: #drawbacks

Looking forward, we know our polling-based discovery is not long-term sustainable and will need replacement. Given that,
we think the main drawbacks here are around broader prioritization (should our time be spent on something else for now),
and on the specific merits of the proposed idea (could we design it better).

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

## Architectural

As Materialize is currently architected, Persist serves as a type of communication layer between processes. A sidechannel
would allow for quicker, more efficient communication between processes access the same shard.

An alternative to this architecture, that would obsolete the need for a sidechannel entirely, would be to consolidate
interactions with Persist purely into `environmentd`, and allow it to instruct `clusterd` what work to perform explicitly.
Some ideas have been floating that [move in this direction](https://materializeinc.slack.com/archives/C04LEA9D8BU/p16813495590089590).

After much thought on this one, our determination is that, regardless of whether we make broader architectural changes,
a Persist sidechannel is worth pursuing now, even if it is obsoleted and ripped out later. It would meaningfully benefit
the product and our infra costs sooner rather than later, its scope is well contained, and it would not introduce any new
complexity that would impede any larger reworkings.

## Sidechannel Implementations

While we have proposed an RPC-based implementation that flows through `environmentd`, these are the alternatives we've
considered:

### Controller RPCs

We could push the RPCs into the Controller, and use the existing Adapter<->Compute|Storage interfaces to communicate diffs.
This would avoid any complexity involved in setting up new connections and request handling, and allow the Controller to
precisely specify which shards are needed by which processes, which is information that it already knows.

To this point, we've rejected this option for several reasons: Persist sidechannel traffic could become a noisy neighbor
(or by affected by noisy neighbors) in the existing request handling; pushing diff updates is purely an optimization while
other messages in the Controller RPC stream are critical for correctness and overall operations; Persist internal details
leak into the Controller in a way that weakens its abstraction.

### Point-to-Point RPCs / Mesh

We could alternatively set up the proposed gRPC service to connect `environmentd` and each `clusterd` to each other
directly, creating a mesh. In this model, `environmentd` would no longer need to broadcast out changes -- each process
would send diffs directly to the processes that need them.

This approach is additionally interesting in that it would set Persist up to communicate other types of data as well,
not just pub-sub of diffs. One hypothetical -- shard writers could cache recently-written blobs to S3 in memory or on
disk, and other processes could request those blobs from the writer directly, rather than paying the costs of going to
S3, which is likely to have much longer tail latencies.

The downsides of this approach include the complexity overhead, as it introduces the novel pattern of intra-`clusterd`
communication in a way that does not exist today, and the scaling limitations imposed by all-to-all communication.

### External PubSub/Message Bus

The [PubSub Trait](#pubsub-trait) would be a good fit for an external message bus, like SQS, Kafka, RedPanda, etc, and
has been designed to accommodate such a choice long-term. These systems could easily support the workload we've outlined.
However, since we do not currently operate any of these systems, and because one of the primary impetuses for the work is
to cut down on infrastructure cost, it would both be a significant lift and added cost to introduce one of them. This
option may be worth exploring in the future as our usage grows, or if other needs for a bonafide message bus elsewhere in
the product crop up.

### CRDB Changefeeds

CRDB contains a CDC feature, Changefeeds, that would allow us to subscribe to any changes to our `Consensus` table.
While appealing on the surface, we do not believe Changefeeds provide the right tradeoffs -- one of our goals is to
reduce CRDB load; low latency delivery of updates [is not guaranteed or expected](https://github.com/cockroachdb/cockroach/issues/36289);
and the ability to scale to 100s or 1000s of changefeeds per CRDB cluster is unknown.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

TBD, no obviously unresolved questions yet

# Future work
[future-work]: #future-work

Once we have the groundwork in place to push diffs, there will likely be other applications and optimization opportunities
we can explore, e.g. we could push data blobs (provided it is small) between processes to eliminate many S3 reads.
