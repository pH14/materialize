// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Concurrent-history linearizability tests for the sharded service.
//!
//! Multiple client tasks submit overlapping CAS and Head operations through
//! the `ShardedService`. Every operation is checked against the
//! `SharedLogOracle` via Stateright's `LinearizabilityTester`, which verifies
//! that the combined history of overlapping operations can be linearized.
//!
//! Unlike `persist_sim.rs` (sequential oracle checking), these tests have
//! genuine concurrency: between an invoke (send to acceptor) and a return
//! (receive from learner), other tasks can execute. The interleaving creates
//! histories that are not trivially sequential, exercising the linearizability
//! checker for real.
//!
//! ## Why this matters
//!
//! The single-shard DST uses `LinearizabilityTester` but submits operations
//! sequentially (one thread), making the check tautological. Here, N client
//! tasks submit concurrently, creating overlapping invoke/return windows that
//! the checker must resolve.
//!
//! ## Running
//!
//! ```text
//! cargo test -p mz-persist-shared-log sharded_sim
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use tokio::sync::Mutex;

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLog;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoHeadRequest, ProtoVersionedData,
};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistClient, PersistLocation, ShardId};

use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
use crate::persist_log::metashard::{MetashardState, PersistMetashardActor};
use crate::persist_log::{OrderedKeySchema, ProposalSchema};
use crate::sharded_service::ShardedService;
use crate::{AcceptorConfig, Metashard, PartitionMap, RangeAssignment, ReconfigurationPlan};

use super::scenario::{SharedLogObservation, SharedLogOp, SharedLogOracle, VersionedData};
use super::trace::SimThread;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn new_client() -> PersistClient {
    tokio::time::pause();
    let cache = PersistClientCache::new_for_turmoil();
    cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem persist client")
}

async fn spawn_shard(
    client: &PersistClient,
    shard_id: ShardId,
) -> (PersistAcceptorHandle, PersistLearnerHandle) {
    let registry = MetricsRegistry::new();
    let key_schema = Arc::new(OrderedKeySchema);
    let val_schema = Arc::new(ProposalSchema);

    let mut write = client
        .open_writer::<crate::persist_log::OrderedKey, crate::persist_log::Proposal, u64, i64>(
            shard_id,
            Arc::clone(&key_schema),
            Arc::clone(&val_schema),
            mz_persist_client::Diagnostics::from_purpose("sharded-sim-acceptor"),
        )
        .await
        .expect("open writer");

    if write.upper().as_option() == Some(&0) {
        write
            .advance_upper(&timely::progress::Antichain::from_elem(1))
            .await;
    }

    let acceptor_metrics = crate::metrics::AcceptorMetrics::register(&registry);
    let learner_metrics = crate::metrics::LearnerMetrics::register(&registry);

    let (acceptor, write, handle_a) =
        PersistAcceptor::new(AcceptorConfig::default(), write, acceptor_metrics, shard_id, 0);
    let _atask =
        mz_ore::task::spawn(|| "sharded-sim-acceptor", acceptor.run(write)).abort_on_drop();

    let (handle_l, _ltask, _rx) = PersistLearner::spawn(
        PersistLearnerConfig::default(),
        client,
        shard_id,
        Vec::new(),
        learner_metrics,
    )
    .await;

    std::mem::forget(_atask);
    std::mem::forget(_ltask);

    (handle_a, handle_l)
}

fn cas_request(
    key: &str,
    expected: Option<u64>,
    new_seqno: u64,
    data: &[u8],
) -> tonic::Request<ProtoCompareAndSetRequest> {
    tonic::Request::new(ProtoCompareAndSetRequest {
        key: key.to_string(),
        expected,
        new: Some(ProtoVersionedData {
            seqno: new_seqno,
            data: data.to_vec(),
        }),
    })
}

fn cas_observation(
    resp: &mz_persist::generated::consensus_service::ProtoCompareAndSetResponse,
) -> SharedLogObservation {
    SharedLogObservation::Cas {
        committed: resp.committed,
    }
}

fn head_observation(
    resp: &mz_persist::generated::consensus_service::ProtoHeadResponse,
) -> SharedLogObservation {
    SharedLogObservation::Head {
        data: resp.data.as_ref().map(|d| VersionedData {
            seqno: d.seqno,
            data: d.data.clone(),
        }),
    }
}

/// Shared state for the concurrent linearizability harness.
struct ConcurrentHarness {
    checker: LinearizabilityTester<SimThread, SharedLogOracle>,
    /// Step counter for trace (monotonically increasing across all threads).
    step: usize,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Concurrent-history linearizability: N client tasks submit overlapping CAS
/// and Head operations through a ShardedService. The combined history is
/// verified against the SharedLogOracle via Stateright's LinearizabilityTester.
///
/// Key difference from persist_sim: operations genuinely overlap. Between
/// `on_invoke` and `on_return`, the current task yields to tokio, allowing
/// other tasks to invoke and return their own operations. This creates
/// non-trivial concurrent histories.
#[mz_ore::test(tokio::test)]
async fn sharded_sim_concurrent_linearizability() {
    let client = new_client().await;

    // Single shard for simplicity — the linearizability property is about
    // CAS evaluation ordering, not routing.
    let shard = ShardId::new();
    let (acc, lrn) = spawn_shard(&client, shard).await;

    let partition_map = PartitionMap::single(shard);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard, acc);
    let mut learners = BTreeMap::new();
    learners.insert(shard, lrn);

    let service = Arc::new(ShardedService::new(partition_map, acceptors, learners));

    let oracle = SharedLogOracle::new();
    let harness = Arc::new(Mutex::new(ConcurrentHarness {
        checker: LinearizabilityTester::new(oracle),
        step: 0,
    }));

    // Client keys — all route to the same shard, creating contention.
    let keys = [
        "s10000000-0000-0000-0000-000000000000",
        "s20000000-0000-0000-0000-000000000000",
    ];

    let num_clients = 4;
    let ops_per_client = 10;

    let mut tasks = Vec::new();

    for client_id in 0..num_clients {
        let service = Arc::clone(&service);
        let harness = Arc::clone(&harness);
        let thread = SimThread::Client(client_id);
        // Each client alternates between two keys to create contention.
        let key = keys[client_id % keys.len()].to_string();

        tasks.push(mz_ore::task::spawn(
            || format!("client-{}", client_id),
            async move {
                let mut my_seqno: Option<u64> = None;

                for i in 0..ops_per_client {
                    // Alternate between CAS and Head operations.
                    if i % 3 != 2 {
                        // CAS operation
                        let expected = my_seqno;
                        let new_seqno = my_seqno.unwrap_or(0) + 1;
                        let data = format!("c{}_{}", client_id, new_seqno).into_bytes();

                        let op = SharedLogOp::Cas {
                            shard: key.clone(),
                            expected,
                            seqno: new_seqno,
                            data: data.clone(),
                        };

                        // INVOKE: register with linearizability checker BEFORE executing.
                        {
                            let mut h = harness.lock().await;
                            h.checker
                                .on_invoke(thread, op.clone())
                                .expect("on_invoke should succeed");
                            h.step += 1;
                        }
                        // Mutex released — other tasks can interleave here.

                        // Execute the operation.
                        let result = service
                            .compare_and_set(cas_request(&key, expected, new_seqno, &data))
                            .await;

                        let observation = match result {
                            Ok(resp) => {
                                let inner = resp.into_inner();
                                if inner.committed {
                                    my_seqno = Some(new_seqno);
                                }
                                cas_observation(&inner)
                            }
                            Err(status) => {
                                // Transport errors should not occur in in-memory
                                // tests. Panic rather than silently misclassifying
                                // an unknown outcome as a CAS rejection — that
                                // would let the linearizability checker accept
                                // histories it shouldn't.
                                panic!(
                                    "unexpected transport error in linearizability test: {}",
                                    status
                                );
                            }
                        };

                        // RETURN: register result with linearizability checker.
                        {
                            let mut h = harness.lock().await;
                            h.checker
                                .on_return(thread, observation)
                                .expect("on_return should succeed");
                            h.step += 1;
                        }
                    } else {
                        // Head operation
                        let op = SharedLogOp::Head {
                            shard: key.clone(),
                        };

                        {
                            let mut h = harness.lock().await;
                            h.checker
                                .on_invoke(thread, op.clone())
                                .expect("on_invoke should succeed");
                            h.step += 1;
                        }

                        let resp = service
                            .head(tonic::Request::new(ProtoHeadRequest {
                                key: key.clone(),
                            }))
                            .await
                            .unwrap();

                        let observation = head_observation(&resp.into_inner());

                        // Update our snapshot from the read result.
                        if let SharedLogObservation::Head {
                            data: Some(ref vd), ..
                        } = observation
                        {
                            my_seqno = Some(vd.seqno);
                        }

                        {
                            let mut h = harness.lock().await;
                            h.checker
                                .on_return(thread, observation)
                                .expect("on_return should succeed");
                            h.step += 1;
                        }
                    }

                    // Yield to allow interleaving with other tasks.
                    tokio::task::yield_now().await;
                }
            },
        ));
    }

    // Wait for all client tasks.
    for task in tasks {
        task.await;
    }

    // Verify linearizability of the combined concurrent history.
    let h = harness.lock().await;
    assert!(
        h.checker.is_consistent(),
        "concurrent history is NOT linearizable! \
         {} operations across {} clients produced an illegal history",
        h.step,
        num_clients,
    );
}

/// Multi-seed variant: runs the concurrent linearizability test across
/// multiple seeds for broader coverage. Each seed produces a different
/// interleaving of client operations.
#[mz_ore::test(tokio::test)]
async fn sharded_sim_concurrent_linearizability_multi_seed() {
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};

    let client = new_client().await;

    let seed_count: u64 = std::env::var("SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    for seed in 0..seed_count {
        let shard = ShardId::new();
        let (acc, lrn) = spawn_shard(&client, shard).await;

        let partition_map = PartitionMap::single(shard);
        let mut acceptors = BTreeMap::new();
        acceptors.insert(shard, acc);
        let mut learners = BTreeMap::new();
        learners.insert(shard, lrn);

        let service = Arc::new(ShardedService::new(partition_map, acceptors, learners));

        let oracle = SharedLogOracle::new();
        let harness = Arc::new(Mutex::new(ConcurrentHarness {
            checker: LinearizabilityTester::new(oracle),
            step: 0,
        }));

        let keys = [
            "s10000000-0000-0000-0000-000000000000",
            "s20000000-0000-0000-0000-000000000000",
        ];

        let num_clients = 3;
        let ops_per_client = 8;
        let mut tasks = Vec::new();

        for client_id in 0..num_clients {
            let service = Arc::clone(&service);
            let harness = Arc::clone(&harness);
            let thread = SimThread::Client(client_id);
            let key = keys[client_id % keys.len()].to_string();
            let mut rng = SmallRng::seed_from_u64(seed * 1000 + u64::try_from(client_id).expect("client_id fits u64"));

            tasks.push(mz_ore::task::spawn(
                || format!("seed{}-client{}", seed, client_id),
                async move {
                    let mut my_seqno: Option<u64> = None;

                    for _ in 0..ops_per_client {
                        let do_read = rng.r#gen::<f64>() < 0.3;

                        if do_read {
                            let op = SharedLogOp::Head { shard: key.clone() };
                            {
                                let mut h = harness.lock().await;
                                h.checker.on_invoke(thread, op.clone()).expect("invoke");
                                h.step += 1;
                            }

                            let resp = service
                                .head(tonic::Request::new(ProtoHeadRequest {
                                    key: key.clone(),
                                }))
                                .await
                                .unwrap();

                            let observation = head_observation(&resp.into_inner());
                            if let SharedLogObservation::Head {
                                data: Some(ref vd), ..
                            } = observation
                            {
                                my_seqno = Some(vd.seqno);
                            }

                            {
                                let mut h = harness.lock().await;
                                h.checker.on_return(thread, observation).expect("return");
                                h.step += 1;
                            }
                        } else {
                            let expected = my_seqno;
                            let new_seqno = my_seqno.unwrap_or(0) + 1;
                            let data =
                                format!("s{}c{}_{}", seed, client_id, new_seqno).into_bytes();

                            let op = SharedLogOp::Cas {
                                shard: key.clone(),
                                expected,
                                seqno: new_seqno,
                                data: data.clone(),
                            };

                            // Register invoke BEFORE execution to preserve
                            // the real overlap window for the checker. No
                            // reconfig in this test, so transport errors are
                            // not expected.
                            {
                                let mut h = harness.lock().await;
                                h.checker.on_invoke(thread, op.clone()).expect("invoke");
                                h.step += 1;
                            }

                            let result = service
                                .compare_and_set(cas_request(&key, expected, new_seqno, &data))
                                .await;

                            let observation = match result {
                                Ok(resp) => {
                                    let inner = resp.into_inner();
                                    if inner.committed {
                                        my_seqno = Some(new_seqno);
                                    }
                                    cas_observation(&inner)
                                }
                                Err(status) => {
                                    panic!(
                                        "seed={}: unexpected transport error: {}",
                                        seed, status
                                    );
                                }
                            };

                            {
                                let mut h = harness.lock().await;
                                h.checker.on_return(thread, observation).expect("return");
                                h.step += 1;
                            }
                        }

                        tokio::task::yield_now().await;
                    }
                },
            ));
        }

        for task in tasks {
            task.await;
        }

        let h = harness.lock().await;
        assert!(
            h.checker.is_consistent(),
            "seed={}: concurrent history is NOT linearizable! {} ops across {} clients",
            seed,
            h.step,
            num_clients,
        );
    }
}

/// Same as above but with a reconfiguration mid-flight: operations overlap
/// with a split reconfiguration, exercising the Sealed→retry path and
/// verifying linearizability is preserved across the reconfiguration boundary.
#[mz_ore::test(tokio::test)]
async fn sharded_sim_linearizability_across_reconfig() {
    let client = new_client().await;
    let registry = MetricsRegistry::new();

    let shard_old = ShardId::new();
    let (acc, lrn) = spawn_shard(&client, shard_old).await;

    let partition_map = PartitionMap::single(shard_old);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_old, acc);
    let mut learners = BTreeMap::new();
    learners.insert(shard_old, lrn);

    let service = Arc::new(ShardedService::new(partition_map, acceptors, learners));
    let routing_handle = service.routing_handle();

    let metashard_state = MetashardState::single(shard_old);
    let (metashard_handle, _metashard_task) = PersistMetashardActor::spawn(
        metashard_state,
        256,
        client.clone(),
        registry,
        routing_handle,
        ShardId::new(),
    )
    .await;

    let oracle = SharedLogOracle::new();
    let harness = Arc::new(Mutex::new(ConcurrentHarness {
        checker: LinearizabilityTester::new(oracle),
        step: 0,
    }));

    // Two keys: one in each half of the partition space.
    let key_lo = "s10000000-0000-0000-0000-000000000000"; // [0x00, 0x80)
    let key_hi = "s90000000-0000-0000-0000-000000000000"; // [0x80, 0x100)
    let keys = [key_lo, key_hi];

    let num_clients = 4;
    let ops_per_client = 8;

    let mut tasks = Vec::new();

    for client_id in 0..num_clients {
        let service = Arc::clone(&service);
        let harness = Arc::clone(&harness);
        let thread = SimThread::Client(client_id);
        let key = keys[client_id % keys.len()].to_string();

        tasks.push(mz_ore::task::spawn(
            || format!("reconfig-client-{}", client_id),
            async move {
                let mut my_seqno: Option<u64> = None;

                for i in 0..ops_per_client {
                    if i % 3 != 2 {
                        let expected = my_seqno;
                        let new_seqno = my_seqno.unwrap_or(0) + 1;
                        let data = format!("c{}_{}", client_id, new_seqno).into_bytes();

                        let op = SharedLogOp::Cas {
                            shard: key.clone(),
                            expected,
                            seqno: new_seqno,
                            data: data.clone(),
                        };

                        // Register invoke BEFORE execution to preserve
                        // the real overlap window for the checker.
                        {
                            let mut h = harness.lock().await;
                            h.checker.on_invoke(thread, op.clone()).expect("invoke");
                            h.step += 1;
                        }

                        let result = service
                            .compare_and_set(cas_request(&key, expected, new_seqno, &data))
                            .await;

                        match result {
                            Ok(resp) => {
                                let inner = resp.into_inner();
                                if inner.committed {
                                    my_seqno = Some(new_seqno);
                                }
                                let observation = cas_observation(&inner);
                                let mut h = harness.lock().await;
                                h.checker.on_return(thread, observation).expect("return");
                                h.step += 1;
                            }
                            Err(_) => {
                                // Transport error during reconfig. Leave the
                                // invoke pending — the LinearizabilityTester
                                // treats pending invokes as operations that
                                // can complete with any valid outcome.
                                //
                                // This is conservative: some errors (e.g.
                                // "acceptor sealed after 3 retry attempts")
                                // are definite non-commits that could be
                                // modeled as `Cas { committed: false }`. By
                                // leaving them pending we accept more
                                // histories than strictly necessary, making
                                // the check sound but weaker than full
                                // client-visible linearizability.
                                //
                                // This thread can't issue another op (the
                                // checker rejects double-invoke on the same
                                // thread), so break out of the op loop.
                                break;
                            }
                        }
                    } else {
                        let op = SharedLogOp::Head {
                            shard: key.clone(),
                        };

                        {
                            let mut h = harness.lock().await;
                            h.checker.on_invoke(thread, op.clone()).expect("invoke");
                            h.step += 1;
                        }

                        let resp = service
                            .head(tonic::Request::new(ProtoHeadRequest {
                                key: key.clone(),
                            }))
                            .await
                            .unwrap();

                        let observation = head_observation(&resp.into_inner());

                        if let SharedLogObservation::Head {
                            data: Some(ref vd), ..
                        } = observation
                        {
                            my_seqno = Some(vd.seqno);
                        }

                        {
                            let mut h = harness.lock().await;
                            h.checker.on_return(thread, observation).expect("return");
                            h.step += 1;
                        }
                    }

                    tokio::task::yield_now().await;
                }
            },
        ));
    }

    // After the first few operations land, trigger a split reconfiguration.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let shard_a = ShardId::new();
    let shard_b = ShardId::new();
    let reconfig_task = mz_ore::task::spawn(|| "reconfig", async move {
        metashard_handle
            .reconfigure(ReconfigurationPlan {
                expected_epoch: 0,
                new_partition_map: PartitionMap {
                    epoch: 1,
                    ranges: vec![
                        RangeAssignment {
                            lo: 0x00,
                            hi_exclusive: 0x80,
                            log_shard: shard_a,
                        },
                        RangeAssignment {
                            lo: 0x80,
                            hi_exclusive: 0x100,
                            log_shard: shard_b,
                        },
                    ],
                },
            })
            .await
    });

    // Wait for all client tasks.
    for task in tasks {
        task.await;
    }

    // Wait for reconfiguration.
    let _ = reconfig_task.await;

    // Verify linearizability.
    let h = harness.lock().await;
    assert!(
        h.checker.is_consistent(),
        "concurrent history across reconfiguration is NOT linearizable! \
         {} operations across {} clients",
        h.step,
        num_clients,
    );
}
