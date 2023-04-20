- Feature name: Persist Push-Based Updates
- Associated: https://github.com/MaterializeInc/materialize/issues/18661

# Summary
[summary]: #summary

Replacing Persist's polling-based discovery with push-based updates.

# Motivation
[motivation]: #motivation

tl;dr
* Reduces CockroachDB read traffic to near zero, reducing current CPU usage by 20-30% (relative to total usage). This will allow us to [scale down
  our clusters further](https://github.com/MaterializeInc/materialize/issues/18665).
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

When any handle to a persist shard successfully compares-and-sets a new state, it will publish the change to `PersistPushClient`.

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
shards, and broadcasts out the diff to the interested subscribers.

<mermaid diagram>

From there, each subscriber would [apply the diff](#applying-received-updates).


Explain the design as if it were part of Materialize and you were teaching the team about it.
This can mean:

- Introduce new named concepts.
- Explain the feature using examples that demonstrate product-level changes.
- Explain how it builds on the current architecture.
- Explain how engineers and users should think about this change, and how it influences how everyone uses the product.
- If needed, talk though errors, backwards-compatibility, or migration strategies.
- Discuss how this affects maintainability, or whether it introduces concepts that might be hard to change in the future.

# Rollout
[rollout]: #rollout

optimization, ok if it doesn't work. can hold behind feature flag

We anticipate message volume to be on the order of 1-1.5 per second per shard, with each message in the 100 bytes-1KiB
range. Given the scale of our usage today, we would expect `environmentd` to be able to broadcast this data and message volume
comfortably with minimal impact on performance. One metric we'll want to keep an eye on is how much time `environmentd` spends
decoding and applying diffs, as it necessarily owns a copy of state for all shards in the environment, and will be updating
state more frequently than before. We anticipate `clusterd` to be negligibly impacted by this added RPC traffic, as each
`clusterd` would only need to push/receive updates for the shards it is reading and writing.

Using push-based updates is seen as an optimization opportunity, and can be safely put behind a feature flag.

[//]: # (Describe what steps are necessary to enable this feature for users.)

[//]: # (How do we validate that the feature performs as expected? What monitoring and observability does it require?)

## Testing and observability
[testing-and-observability]: #testing-and-observability

We will introduce several metrics to understand the behavior of the change:

* # of updates pushed
* # of updates received that apply cleanly & uncleanly
* Latency of update push to receiving
* Timings of `environmentd`'s broadcasting


* Lots of metrics, existing persist/testing should cover cases of disconnections etc
* 


Testability and explainability are top-tier design concerns!
Describe how you will test and roll out the implementation.
When the deliverable is a refactoring, the existing tests may be sufficient.
When the deliverable is a new feature, new tests are imperative.

Describe what metrics can be used to monitor and observe the feature.
What information do we need to expose internally, and what information is interesting to the user?
How do we expose the information?

Basic guidelines:

* Nearly every feature requires either Rust unit tests, sqllogictest tests, or testdrive tests.
* Features that interact with Kubernetes additionally need a cloudtest test.
* Features that interact with external systems additionally should be tested manually in a staging environment.
* Features or changes to performance-critical parts of the system should be load tested.

## Lifecycle
[lifecycle]: #lifecycle

This feature will be wrapped in a feature flag.

If the design is risky or has the potential to be destabilizing, you should plan to roll the implementation out behind a feature flag.
List all feature flags, their behavior and when it is safe to change their value.
Describe the [lifecycle of the feature](https://www.notion.so/Feature-lifecycle-2fb13301803b4b7e9ba0868238bd4cfb).
Will it start as an alpha feature behind a feature flag?
What level of testing will be required to promote to beta?
To stable?

# Drawbacks
[drawbacks]: #drawbacks

The main drawback here is how the work fits within broader prioritization, and whether the time should be spent elsewhere.
Looking forward, we know our polling-based discovery is not long-term sustainable and will need replacement.

[//]: # (Why should we *not* do this?)

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

- Why is the design the best to solve the problem?
- What other designs have been considered, and what were the reasons to not pick any other?
- What is the impact of not implementing this design?

## Architectural

A Persist sidechannel is valuable as Persist is used as a communication layer between processes, and the sidechannel allows
for quicker and more efficient communication between them.

An alternative to this architecture, that out obsolete the need for a sidechannel entirely, would be to consolidate
interactions with Persist purely into `environmentd`, and allow it to instruct `clusterd` what work to perform explicitly.
Some ideas have been floating that [move more in this direction](https://materializeinc.slack.com/archives/C04LEA9D8BU/p16813495590089590).

After much thought on this one, our determination is that, regardless of whether we make broader architectural changes,
a Persist sidechannel is worth pursuing now, even if it is obsoleted later. It would benefit the product sooner rather
than later, its scope is well contained, and it would not introduce any new complexity that would impede any larger redesigns.

## Sidechannel Implementations

While we have proposed an RPC-based implementation that flows through `environmentd`, these are the alternatives we've
considered:

### CRDB Changefeeds



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
S3 which has long tail latencies.

The downsides of this approach include the complexity overhead, as it introduces the novel pattern of intra-`clusterd`
communication in a way that does not exist today, and the scaling limitations imposed by all-to-all communication.

### External PubSub/Message Bus

The [PubSub Trait](#pubsub-trait) would be a good fit for an external message bus, like SQS, Kafka, RedPanda, etc, and
has been designed to accommodate such a choice long-term. These systems could easily support the workload we've outlined.
However, since we do not currently operate any of these systems, and because one of the primary impetuses for the work is
to cut down on infrastructure cost, it would both be a significant lift and added cost to introduce one of them. This
option may be worth exploring in the future as our usage grows, or if other needs for a message bus elsewhere in the
product crop up.

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- What questions need to be resolved to finalize the design?
- What questions will need to be resolved during the implementation of the design?
- What questions does this design raise that we should address separately?

# Future work
[future-work]: #future-work

Pushing data (!)

Describe what work should follow from this design, which new aspects it enables, and how it might affect individual parts of Materialize.
Think in larger terms.
This section can also serve as a place to dump ideas that are related but not part of the design.

If you can't think of any, please note this down.
