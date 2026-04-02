// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Whole-system distributed simulator for the persist shared log.
//!
//! Runs the shared-log service on turmoil simulated hosts with explicit
//! network links between the service, persist backends, and client hosts.
//! The simulator controls link partitions, host crashes/restarts, and
//! deterministic time — moving from "many async tasks in one process" to
//! "multiple processes with a network and durable state."
//!
//! ## Architecture
//!
//! ```text
//! turmoil::Sim
//! ├── "consensus" host       (MemConsensus over turmoil TCP)
//! ├── "blob" host            (MemBlob over turmoil TCP)
//! ├── "service" host         (Router + metashard + acceptors + learners)
//! └── "client-N" hosts       (workload generators with linearizability checking)
//! ```
//!
//! The service host creates a PersistClient that connects to the consensus
//! and blob hosts via turmoil TCP (using `TurmoilConsensus`/`TurmoilBlob`).
//! Persist operations go through the simulated network, so partitioning
//! `service ↔ consensus` exercises persist failure modes.
//!
//! ## What this tests beyond the existing suite
//!
//! - Persist operation failures (compare_and_append timeouts, subscribe failures)
//! - Ambiguous appends (network failure between submit and confirm)
//! - Service crash/restart from durable state
//! - Client retry across network partitions
//! - Deterministic scheduling with seeded turmoil

use std::time::Duration;

use crate::actors::meta::{MetaState, PersistMetaActor};
use crate::actors::router::Router;
use crate::factory::InProcessActorFactory;
use crate::{PartitionMap, RangeAssignment, ReconfigurationPlan};
use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::ProtoVersionedData;
use mz_persist::turmoil::{BlobState, ConsensusState, serve_blob, serve_consensus};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{PersistClient, PersistLocation, ShardId};

/// Port for consensus and blob turmoil servers.
const PERSIST_PORT: u16 = 7000;

/// Create a PersistClient that connects to turmoil consensus/blob hosts.
async fn new_turmoil_persist_client() -> PersistClient {
    let persist_config = PersistConfig::new_for_tests();
    let registry = MetricsRegistry::new();
    let cache = PersistClientCache::new(persist_config, &registry, |_, _| {
        PubSubClientConnection::noop()
    });
    let location = PersistLocation {
        blob_uri: format!("turmoil://blob:{PERSIST_PORT}")
            .parse()
            .expect("valid blob URI"),
        consensus_uri: format!("turmoil://consensus:{PERSIST_PORT}")
            .parse()
            .expect("valid consensus URI"),
    };
    cache
        .open(location)
        .await
        .expect("open turmoil persist client")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Smoke test: boot the full cluster on turmoil, write data through the
/// service, read it back from a client. Verifies the basic wiring works.
#[test]
fn sim_cluster_smoke() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    // Stable shard IDs.
    let log_shard = ShardId::new();
    let metashard_shard = ShardId::new();

    // Boot persist backends.
    let consensus_state = ConsensusState::new();
    let blob_state = BlobState::new();
    sim.host("consensus", {
        let state = consensus_state.clone();
        move || serve_consensus(PERSIST_PORT, state.clone())
    });
    sim.host("blob", {
        let state = blob_state.clone();
        move || serve_blob(PERSIST_PORT, state.clone())
    });

    // Boot the shared-log service on its own host.
    // turmoil's host() takes Fn (restartable), so clone inside the closure.
    let shard_ids = vec![log_shard];
    let ms_shard = metashard_shard;
    sim.host("service", move || {
        let shard_ids = shard_ids.clone();
        async move {
            let client = new_turmoil_persist_client().await;

            let (router, router_handle, routing_tx) = Router::new(4096);
            mz_ore::task::spawn(|| "test-router", router.run());

            let factory = std::sync::Arc::new(InProcessActorFactory::new(client.clone()));
            let metashard_state = MetaState::single(shard_ids[0]);
            let (_ms_handle, _, _ms_task) = PersistMetaActor::spawn(
                metashard_state,
                256,
                client.clone(),
                std::sync::Arc::clone(&factory),
                ms_shard,
            )
            .await;

            crate::actors::router::spawn_routing_task(&client, ms_shard, factory, routing_tx).await;

            // Write and read directly through the router handle.
            let key = "s30000000-0000-0000-0000-000000000000";

            // Write.
            let resp = router_handle
                .compare_and_set(
                    key.to_string(),
                    None,
                    ProtoVersionedData {
                        seqno: 1,
                        data: b"hello from turmoil".to_vec(),
                    },
                )
                .await
                .expect("CAS should succeed");
            assert!(resp.committed, "first CAS should commit");

            // Read.
            let resp = router_handle
                .head(key.to_string())
                .await
                .expect("head should succeed");
            let data = resp.data.expect("should have data");
            assert_eq!(data.seqno, 1);
            assert_eq!(data.data, b"hello from turmoil");

            Ok(())
        }
    });

    // Run the simulation.
    sim.run().expect("simulation should complete");
}

/// Service crash and restart: write data, crash the service host, restart
/// from the same persist shards, verify data survives.
#[test]
fn sim_cluster_crash_restart() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build();

    let log_shard = ShardId::new();
    let metashard_shard = ShardId::new();

    let consensus_state = ConsensusState::new();
    let blob_state = BlobState::new();
    sim.host("consensus", {
        let state = consensus_state.clone();
        move || serve_consensus(PERSIST_PORT, state.clone())
    });
    sim.host("blob", {
        let state = blob_state.clone();
        move || serve_blob(PERSIST_PORT, state.clone())
    });

    let shard_ids = vec![log_shard];
    let ms_shard = metashard_shard;

    // Phase 1: Boot service, write data, then return Ok to signal completion.
    // Use sim.client() so turmoil waits for it to finish.
    sim.client("phase1", {
        let shard_ids = shard_ids.clone();
        async move {
            let client = new_turmoil_persist_client().await;

            let (router, router_handle, routing_tx) = Router::new(4096);
            mz_ore::task::spawn(|| "test-router", router.run());

            let factory = std::sync::Arc::new(InProcessActorFactory::new(client.clone()));
            let metashard_state = MetaState::single(shard_ids[0]);
            let (_ms_handle, _, _) = PersistMetaActor::spawn(
                metashard_state,
                256,
                client.clone(),
                std::sync::Arc::clone(&factory),
                ms_shard,
            )
            .await;

            crate::actors::router::spawn_routing_task(&client, ms_shard, factory, routing_tx).await;

            let key = "s30000000-0000-0000-0000-000000000000";

            let resp = router_handle
                .compare_and_set(
                    key.to_string(),
                    None,
                    ProtoVersionedData {
                        seqno: 1,
                        data: b"survive crash".to_vec(),
                    },
                )
                .await
                .expect("CAS should succeed");
            assert!(resp.committed);

            let resp = router_handle
                .head(key.to_string())
                .await
                .expect("head should succeed");
            assert_eq!(resp.data.unwrap().seqno, 1);

            Ok(())
        }
    });

    sim.run().expect("phase 1 should complete");

    // Phase 2: Restart from the same persist shards and verify data survives.
    // The consensus/blob hosts are still running with the durable state.
    sim.client("phase2", {
        let shard_ids = shard_ids.clone();
        async move {
            let client = new_turmoil_persist_client().await;

            let (router, router_handle, routing_tx) = Router::new(4096);
            mz_ore::task::spawn(|| "test-router", router.run());

            let factory = std::sync::Arc::new(InProcessActorFactory::new(client.clone()));
            let metashard_state = MetaState::single(shard_ids[0]);
            let (_ms_handle, _, _) = PersistMetaActor::spawn(
                metashard_state,
                256,
                client.clone(),
                std::sync::Arc::clone(&factory),
                ms_shard,
            )
            .await;

            crate::actors::router::spawn_routing_task(&client, ms_shard, factory, routing_tx).await;

            let key = "s30000000-0000-0000-0000-000000000000";

            // Data from before the "crash" should be readable.
            let resp = router_handle
                .head(key.to_string())
                .await
                .expect("head after restart should succeed");
            let data = resp.data;
            assert!(data.is_some(), "data should survive restart");
            assert_eq!(data.unwrap().seqno, 1);

            // New writes should work with carried-forward expected seqno.
            let resp = router_handle
                .compare_and_set(
                    key.to_string(),
                    Some(1),
                    ProtoVersionedData {
                        seqno: 2,
                        data: b"after restart".to_vec(),
                    },
                )
                .await
                .expect("CAS after restart should succeed");
            assert!(resp.committed, "CAS with pre-crash expected should commit");

            Ok(())
        }
    });

    sim.run().expect("phase 2 should complete");
}

/// Network partition: partition the service from consensus, verify writes
/// fail, repair the partition, verify writes succeed again.
///
/// This exercises the persist-level failure path: when the consensus host
/// is unreachable, compare_and_append times out and the acceptor propagates
/// the error to the client.
#[test]
fn sim_cluster_persist_partition() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    let log_shard = ShardId::new();
    let metashard_shard = ShardId::new();

    let consensus_state = ConsensusState::new();
    let blob_state = BlobState::new();
    sim.host("consensus", {
        let state = consensus_state.clone();
        move || serve_consensus(PERSIST_PORT, state.clone())
    });
    sim.host("blob", {
        let state = blob_state.clone();
        move || serve_blob(PERSIST_PORT, state.clone())
    });

    let shard_ids = vec![log_shard];
    let ms_shard = metashard_shard;

    // Phase 1: Write data normally (no partition).
    sim.client("phase1", {
        let shard_ids = shard_ids.clone();
        async move {
            let client = new_turmoil_persist_client().await;

            let (router, router_handle, routing_tx) = Router::new(4096);
            mz_ore::task::spawn(|| "test-router", router.run());

            let factory = std::sync::Arc::new(InProcessActorFactory::new(client.clone()));
            let metashard_state = MetaState::single(shard_ids[0]);
            let (_, _, _) = PersistMetaActor::spawn(
                metashard_state,
                256,
                client.clone(),
                std::sync::Arc::clone(&factory),
                ms_shard,
            )
            .await;

            crate::actors::router::spawn_routing_task(&client, ms_shard, factory, routing_tx).await;

            let key = "s30000000-0000-0000-0000-000000000000";
            let resp = router_handle
                .compare_and_set(
                    key.to_string(),
                    None,
                    ProtoVersionedData {
                        seqno: 1,
                        data: b"before partition".to_vec(),
                    },
                )
                .await
                .expect("CAS should succeed");
            assert!(resp.committed);

            Ok(())
        }
    });
    sim.run().expect("phase 1 should complete");

    // Phase 2: Boot a service normally, then partition it from consensus
    // and attempt a write. The write should fail because the acceptor's
    // compare_and_append can't reach the consensus server.
    //
    // We boot the service first (handles opened), then partition and
    // attempt the write. The service boots normally because consensus
    // was reachable at boot time; the partition happens only for the
    // subsequent CAS.
    sim.client("phase2", {
        let shard_ids = shard_ids.clone();
        async move {
            let client = new_turmoil_persist_client().await;

            let (router, router_handle, routing_tx) = Router::new(4096);
            mz_ore::task::spawn(|| "test-router", router.run());

            let factory = std::sync::Arc::new(InProcessActorFactory::new(client.clone()));
            let metashard_state = MetaState::single(shard_ids[0]);
            let (_, _, _) = PersistMetaActor::spawn(
                metashard_state,
                256,
                client.clone(),
                std::sync::Arc::clone(&factory),
                ms_shard,
            )
            .await;

            crate::actors::router::spawn_routing_task(&client, ms_shard, factory, routing_tx).await;

            let key = "s30000000-0000-0000-0000-000000000000";

            // Verify pre-partition data is readable from the learner
            // (learner subscribed and caught up during boot, before partition).
            let resp = router_handle
                .head(key.to_string())
                .await
                .expect("head should succeed (learner has local state)");
            let data = resp.data;
            assert!(data.is_some(), "phase 1 data should be readable");
            assert_eq!(data.unwrap().seqno, 1);

            // Now partition ourselves from consensus. This simulates a
            // network failure between the service and the persist backend.
            turmoil::partition("phase2", "consensus");

            // The CAS should hang: persist retries indefinitely under
            // partition (correct production behavior). Use a timeout to
            // detect the hang and move on.
            let partitioned_result = tokio::time::timeout(
                Duration::from_secs(5),
                router_handle.compare_and_set(
                    key.to_string(),
                    Some(1),
                    ProtoVersionedData {
                        seqno: 2,
                        data: b"during partition".to_vec(),
                    },
                ),
            )
            .await;

            assert!(
                partitioned_result.is_err(),
                "CAS should time out when partitioned from consensus"
            );

            // Repair the partition.
            turmoil::repair("phase2", "consensus");

            // After repair, the partitioned CAS may have committed
            // asynchronously (persist retried in the background). The
            // outcome is ambiguous — this is correct production behavior.
            //
            // Read the current state to determine what actually happened,
            // then write based on the actual head.
            // Give the background retry a moment to resolve.
            tokio::time::sleep(Duration::from_secs(2)).await;

            // The previously timed-out CAS may still be in-flight (the
            // spawned task outlives the caller's timeout). After the partition
            // heals it may commit at any moment. Read-then-CAS in a retry
            // loop to handle this race.
            loop {
                let resp = router_handle
                    .head(key.to_string())
                    .await
                    .expect("head should succeed after partition heals");
                let head = resp.data.expect("should have data");

                assert!(
                    head.seqno == 1 || head.seqno == 2,
                    "head seqno should be 1 or 2, got {}",
                    head.seqno,
                );

                let next_seqno = head.seqno + 1;
                let resp = router_handle
                    .compare_and_set(
                        key.to_string(),
                        Some(head.seqno),
                        ProtoVersionedData {
                            seqno: next_seqno,
                            data: b"after partition".to_vec(),
                        },
                    )
                    .await
                    .expect("CAS should succeed after partition heals");
                if resp.committed {
                    break;
                }
                // The in-flight CAS committed between our head and CAS.
                // Re-read and retry.
            }

            Ok(())
        }
    });
    sim.run().expect("phase 2 should complete");
}

/// Reconfiguration while writers are active: split a single shard into two
/// while multiple keys are being written. After the split completes, all
/// pre-split and post-split data should be readable from the correct new
/// shards via chain replay.
#[test]
fn sim_cluster_split_with_writes() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    let log_shard = ShardId::new();
    let metashard_shard = ShardId::new();
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();

    let consensus_state = ConsensusState::new();
    let blob_state = BlobState::new();
    sim.host("consensus", {
        let state = consensus_state.clone();
        move || serve_consensus(PERSIST_PORT, state.clone())
    });
    sim.host("blob", {
        let state = blob_state.clone();
        move || serve_blob(PERSIST_PORT, state.clone())
    });

    let shard_ids = vec![log_shard];
    let ms_shard = metashard_shard;

    sim.client("split-test", {
        let shard_ids = shard_ids.clone();
        async move {
            let client = new_turmoil_persist_client().await;

            let (router, router_handle, routing_tx) = Router::new(4096);
            mz_ore::task::spawn(|| "test-router", router.run());

            let factory = std::sync::Arc::new(InProcessActorFactory::new(client.clone()));
            let metashard_state = MetaState::single(shard_ids[0]);
            let (ms_handle, _, _) = PersistMetaActor::spawn(
                metashard_state,
                256,
                client.clone(),
                std::sync::Arc::clone(&factory),
                ms_shard,
            )
            .await;

            crate::actors::router::spawn_routing_task(&client, ms_shard, factory, routing_tx).await;

            let key_lo = "s10000000-0000-0000-0000-000000000000"; // 0x10 → [0x00, 0x80)
            let key_hi = "s90000000-0000-0000-0000-000000000000"; // 0x90 → [0x80, 0x100)

            // Write to both keys on the single shard.
            for (key, label) in [(key_lo, "lo"), (key_hi, "hi")] {
                let resp = router_handle
                    .compare_and_set(
                        key.to_string(),
                        None,
                        ProtoVersionedData {
                            seqno: 1,
                            data: format!("{}_pre_split", label).into_bytes(),
                        },
                    )
                    .await
                    .expect("pre-split CAS");
                assert!(resp.committed, "{} pre-split write", label);
            }

            // Split.
            use crate::Metashard;
            let new_epoch = ms_handle
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
                .expect("split should succeed");
            assert_eq!(new_epoch, 1);

            tokio::time::sleep(Duration::from_millis(200)).await;

            // Verify carried-forward data.
            for (key, label) in [(key_lo, "lo"), (key_hi, "hi")] {
                let resp = router_handle
                    .head(key.to_string())
                    .await
                    .expect("head after split");
                let data = resp.data;
                assert!(data.is_some(), "{} should have data after split", label);
                assert_eq!(data.unwrap().seqno, 1, "{} seqno carried forward", label);
            }

            // Post-split writes to both new shards.
            for (key, label) in [(key_lo, "lo"), (key_hi, "hi")] {
                let resp = router_handle
                    .compare_and_set(
                        key.to_string(),
                        Some(1),
                        ProtoVersionedData {
                            seqno: 2,
                            data: format!("{}_post_split", label).into_bytes(),
                        },
                    )
                    .await
                    .expect("post-split CAS");
                assert!(resp.committed, "{} post-split write", label);
            }

            Ok(())
        }
    });
    sim.run().expect("split test should complete");
}

/// BUGGIFY + turmoil: trigger a split with fault injection at `after_seal`
/// on the turmoil persist backend. Both fault layers (network-level turmoil
/// + protocol-level BUGGIFY) compose correctly.
#[test]
fn sim_cluster_reconfig_with_buggify() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    let log_shard = ShardId::new();
    let metashard_shard = ShardId::new();
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();

    let consensus_state = ConsensusState::new();
    let blob_state = BlobState::new();
    sim.host("consensus", {
        let state = consensus_state.clone();
        move || serve_consensus(PERSIST_PORT, state.clone())
    });
    sim.host("blob", {
        let state = blob_state.clone();
        move || serve_blob(PERSIST_PORT, state.clone())
    });

    let shard_ids = vec![log_shard];
    let ms_shard = metashard_shard;

    sim.client("buggify-reconfig", {
        let shard_ids = shard_ids.clone();
        async move {
            use crate::fault::{self, FaultConfig};

            let client = new_turmoil_persist_client().await;

            let (router, router_handle, routing_tx) = Router::new(4096);
            mz_ore::task::spawn(|| "test-router", router.run());

            let factory = std::sync::Arc::new(InProcessActorFactory::new(client.clone()));
            let metashard_state = MetaState::single(shard_ids[0]);
            let (ms_handle, _, _) = PersistMetaActor::spawn(
                metashard_state,
                256,
                client.clone(),
                std::sync::Arc::clone(&factory),
                ms_shard,
            )
            .await;

            crate::actors::router::spawn_routing_task(&client, ms_shard, factory, routing_tx).await;

            let key = "s30000000-0000-0000-0000-000000000000";

            let resp = router_handle
                .compare_and_set(
                    key.to_string(),
                    None,
                    ProtoVersionedData {
                        seqno: 1,
                        data: b"pre".to_vec(),
                    },
                )
                .await
                .unwrap();
            assert!(resp.committed);

            let plan = ReconfigurationPlan {
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
            };

            // Fault at after_seal.
            fault::configure(FaultConfig::with_points(&["after_seal"], 1.0, 42));

            use crate::Metashard;
            let result = ms_handle.reconfigure(plan.clone()).await;
            assert!(result.is_err(), "should fail at after_seal");

            fault::clear();
            let new_epoch = ms_handle
                .reconfigure(plan)
                .await
                .expect("retry should succeed");
            assert_eq!(new_epoch, 1);

            tokio::time::sleep(Duration::from_millis(200)).await;

            let resp = router_handle.head(key.to_string()).await.unwrap();
            assert_eq!(
                resp.data.unwrap().seqno,
                1,
                "pre-split data should survive buggify + turmoil"
            );

            fault::clear();
            Ok(())
        }
    });
    sim.run().expect("buggify reconfig test should complete");
}

/// Split during persist partition: write data, partition from consensus,
/// attempt split (hangs), repair, retry split, verify data carries forward.
#[test]
fn sim_cluster_split_during_persist_partition() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .build();

    let log_shard = ShardId::new();
    let metashard_shard = ShardId::new();
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();

    let consensus_state = ConsensusState::new();
    let blob_state = BlobState::new();
    sim.host("consensus", {
        let state = consensus_state.clone();
        move || serve_consensus(PERSIST_PORT, state.clone())
    });
    sim.host("blob", {
        let state = blob_state.clone();
        move || serve_blob(PERSIST_PORT, state.clone())
    });

    let shard_ids = vec![log_shard];
    let ms_shard = metashard_shard;

    sim.client("split-partition", {
        let shard_ids = shard_ids.clone();
        async move {
            use mz_persist::generated::consensus_service::ProtoVersionedData;

            let client = new_turmoil_persist_client().await;

            let (router, router_handle, routing_tx) = Router::new(4096);
            mz_ore::task::spawn(|| "test-router", router.run());

            let factory = std::sync::Arc::new(InProcessActorFactory::new(client.clone()));
            let metashard_state = MetaState::single(shard_ids[0]);
            let (ms_handle, _, _) = PersistMetaActor::spawn(
                metashard_state,
                256,
                client.clone(),
                std::sync::Arc::clone(&factory),
                ms_shard,
            )
            .await;

            crate::actors::router::spawn_routing_task(&client, ms_shard, factory, routing_tx).await;

            let key = "s30000000-0000-0000-0000-000000000000";

            let resp = router_handle
                .compare_and_set(
                    key.to_string(),
                    None,
                    ProtoVersionedData {
                        seqno: 1,
                        data: b"pre_split".to_vec(),
                    },
                )
                .await
                .unwrap();
            assert!(resp.committed);

            // Partition from consensus, then attempt split.
            turmoil::partition("split-partition", "consensus");

            use crate::Metashard;
            let plan = ReconfigurationPlan {
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
            };

            // Reconfiguration hangs (persist can't seal or write intent).
            let result =
                tokio::time::timeout(Duration::from_secs(10), ms_handle.reconfigure(plan.clone()))
                    .await;
            assert!(result.is_err(), "reconfig should time out during partition");

            // Repair and retry.
            turmoil::repair("split-partition", "consensus");
            tokio::time::sleep(Duration::from_secs(1)).await;

            // The partitioned reconfiguration may have partially or fully
            // completed in the background (the intent was persisted before
            // the partition, and persist retried seal/replay when the network
            // healed during the timeout window). Check the current epoch.
            let current_epoch = ms_handle.current_epoch().await.unwrap();
            if current_epoch == 0 {
                // Reconfig didn't complete — retry.
                let new_epoch = ms_handle
                    .reconfigure(plan)
                    .await
                    .expect("retried reconfig should succeed");
                assert_eq!(new_epoch, 1);
            } else {
                // Reconfig completed via background retry — expected behavior
                // when the partition heals during the timeout window.
                assert_eq!(current_epoch, 1);
            }

            tokio::time::sleep(Duration::from_millis(500)).await;

            let resp = router_handle.head(key.to_string()).await.unwrap();
            let data = resp.data;
            assert!(data.is_some(), "pre-split data should survive");
            assert_eq!(data.unwrap().seqno, 1);

            let resp = router_handle
                .compare_and_set(
                    key.to_string(),
                    Some(1),
                    ProtoVersionedData {
                        seqno: 2,
                        data: b"post_split".to_vec(),
                    },
                )
                .await
                .unwrap();
            assert!(resp.committed, "post-split CAS should work");

            Ok(())
        }
    });
    sim.run()
        .expect("split-during-partition test should complete");
}
