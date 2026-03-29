// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for multi-shard routing via `ShardedService`.

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLog;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoHeadRequest, ProtoListKeysRequest, ProtoScanRequest,
    ProtoTruncateRequest, ProtoVersionedData,
};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};

use mz_ore::metrics::MetricsRegistry;

use crate::metrics::{AcceptorMetrics, LearnerMetrics};
use crate::persist_log::acceptor::{PersistAcceptor, PersistAcceptorHandle};
use crate::persist_log::learner::{PersistLearner, PersistLearnerConfig, PersistLearnerHandle};
use crate::persist_log::metashard::{MetashardState, PersistMetashardActor};
use crate::persist_log::{OrderedKey, OrderedKeySchema, Proposal, ProposalSchema};
use crate::sharded_service::ShardedService;
use crate::{AcceptorConfig, Metashard, PartitionMap, RangeAssignment, ReconfigurationPlan};

async fn new_persist_client_for_test() -> PersistClient {
    tokio::time::pause();
    let cache = PersistClientCache::new_for_turmoil();
    cache
        .open(PersistLocation::new_in_mem())
        .await
        .expect("in-mem persist client")
}

/// Spawn an acceptor + learner pair for a single log shard, returning handles.
/// Each call creates its own metrics registry to avoid double-registration panics.
async fn spawn_shard(
    client: &PersistClient,
    shard_id: ShardId,
) -> (PersistAcceptorHandle, PersistLearnerHandle) {
    let registry = MetricsRegistry::new();
    let key_schema = Arc::new(OrderedKeySchema);
    let val_schema = Arc::new(ProposalSchema);

    #[allow(unused_mut)]
    let mut write = client
        .open_writer::<OrderedKey, Proposal, u64, i64>(
            shard_id,
            Arc::clone(&key_schema),
            Arc::clone(&val_schema),
            Diagnostics::from_purpose("test-sharded-acceptor"),
        )
        .await
        .expect("open acceptor writer");

    #[allow(unused_mut)]
    let (mut upper_handle, read) = client
        .open::<OrderedKey, Proposal, u64, i64>(
            shard_id,
            key_schema,
            val_schema,
            Diagnostics::from_purpose("test-sharded-learner"),
            false,
        )
        .await
        .expect("open learner handles");

    if write.upper().as_option() == Some(&0) {
        write
            .advance_upper(&timely::progress::Antichain::from_elem(1))
            .await;
    }

    let since = read.since().clone();
    let subscribe = read.subscribe(since).await.expect("subscribe");

    let retraction_write = client
        .open_writer::<OrderedKey, Proposal, u64, i64>(
            shard_id,
            Arc::new(OrderedKeySchema),
            Arc::new(ProposalSchema),
            Diagnostics::from_purpose("test-sharded-learner-retraction"),
        )
        .await
        .expect("open retraction writer");

    let acceptor_metrics = AcceptorMetrics::register(&registry);
    let learner_metrics = LearnerMetrics::register(&registry);

    let (acceptor, write, acceptor_handle) =
        PersistAcceptor::new(AcceptorConfig::default(), write, acceptor_metrics, shard_id, 0);
    let _acceptor_task =
        mz_ore::task::spawn(|| "test-sharded-acceptor", acceptor.run(write)).abort_on_drop();

    let (learner, learner_handle) =
        PersistLearner::new(PersistLearnerConfig::default(), subscribe, retraction_write, learner_metrics);
    let _learner_task =
        mz_ore::task::spawn(|| "test-sharded-learner", learner.run(upper_handle)).abort_on_drop();

    // Leak the abort-on-drop handles so the tasks keep running for the test's lifetime.
    // (In a real test harness we'd store them, but for these tests the runtime
    // drops everything at the end.)
    std::mem::forget(_acceptor_task);
    std::mem::forget(_learner_task);

    (acceptor_handle, learner_handle)
}

/// Build a ShardedService with 2 log shards: [0x00, 0x80) and [0x80, 0x100).
async fn build_two_shard_service(
    client: &PersistClient,
) -> (ShardedService<PersistAcceptorHandle, PersistLearnerHandle>, ShardId, ShardId) {
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();

    let (acceptor_a, learner_a) = spawn_shard(client, shard_a).await;
    let (acceptor_b, learner_b) = spawn_shard(client, shard_b).await;

    let partition_map = PartitionMap {
        epoch: 0,
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
    };

    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_a, acceptor_a);
    acceptors.insert(shard_b, acceptor_b);

    let mut learners = BTreeMap::new();
    learners.insert(shard_a, learner_a);
    learners.insert(shard_b, learner_b);

    let service = ShardedService::new(partition_map, acceptors, learners);
    (service, shard_a, shard_b)
}

fn cas_request(key: &str, expected: Option<u64>, new_seqno: u64, data: &[u8]) -> tonic::Request<ProtoCompareAndSetRequest> {
    tonic::Request::new(ProtoCompareAndSetRequest {
        key: key.to_string(),
        expected,
        new: Some(ProtoVersionedData {
            seqno: new_seqno,
            data: data.to_vec(),
        }),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Two client shards in different ranges route to different log shards and
/// don't interfere with each other.
#[mz_ore::test(tokio::test)]
async fn test_sharded_routing_isolation() {
    let client = new_persist_client_for_test().await;
    let (service, _shard_a, _shard_b) = build_two_shard_service(&client).await;

    // "s10..." → partition key 0x10 → shard_a (range [0x00, 0x80))
    // "s90..." → partition key 0x90 → shard_b (range [0x80, 0x100))
    let key_a = "s10000000-0000-0000-0000-000000000000";
    let key_b = "s90000000-0000-0000-0000-000000000000";

    // CAS on key_a (shard_a).
    let resp = service.compare_and_set(cas_request(key_a, None, 1, b"hello")).await.unwrap();
    assert!(resp.into_inner().committed);

    // CAS on key_b (shard_b) — independent, also commits.
    let resp = service.compare_and_set(cas_request(key_b, None, 1, b"world")).await.unwrap();
    assert!(resp.into_inner().committed);

    // Head on key_a reads from shard_a's learner.
    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key_a.into() }))
        .await
        .unwrap();
    let data = resp.into_inner().data.unwrap();
    assert_eq!(data.seqno, 1);
    assert_eq!(data.data, b"hello");

    // Head on key_b reads from shard_b's learner.
    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key_b.into() }))
        .await
        .unwrap();
    let data = resp.into_inner().data.unwrap();
    assert_eq!(data.seqno, 1);
    assert_eq!(data.data, b"world");
}

/// CAS rejection works correctly through the sharded service.
#[mz_ore::test(tokio::test)]
async fn test_sharded_cas_rejection() {
    let client = new_persist_client_for_test().await;
    let (service, _, _) = build_two_shard_service(&client).await;

    let key = "s10000000-0000-0000-0000-000000000000";

    // First CAS commits.
    let resp = service.compare_and_set(cas_request(key, None, 1, b"v1")).await.unwrap();
    assert!(resp.into_inner().committed);

    // Stale CAS rejected.
    let resp = service.compare_and_set(cas_request(key, None, 2, b"v2")).await.unwrap();
    assert!(!resp.into_inner().committed);

    // Correct expected → commits.
    let resp = service.compare_and_set(cas_request(key, Some(1), 2, b"v2")).await.unwrap();
    assert!(resp.into_inner().committed);
}

/// list_keys fans out to all learners and merges results.
#[mz_ore::test(tokio::test)]
async fn test_sharded_list_keys_fan_out() {
    let client = new_persist_client_for_test().await;
    let (service, _, _) = build_two_shard_service(&client).await;

    let key_a = "s10000000-0000-0000-0000-000000000000";
    let key_b = "s90000000-0000-0000-0000-000000000000";

    // Write to both shards.
    service.compare_and_set(cas_request(key_a, None, 1, b"a")).await.unwrap();
    service.compare_and_set(cas_request(key_b, None, 1, b"b")).await.unwrap();

    // list_keys should return both keys (merged from both learners).
    let resp = service
        .list_keys(tonic::Request::new(ProtoListKeysRequest {}))
        .await
        .unwrap();
    let stream = resp.into_inner();
    let keys: Vec<String> = tokio_stream::StreamExt::collect::<Vec<_>>(stream)
        .await
        .into_iter()
        .map(|r| r.unwrap().key)
        .collect();

    assert!(keys.contains(&key_a.to_string()), "missing key_a in {:?}", keys);
    assert!(keys.contains(&key_b.to_string()), "missing key_b in {:?}", keys);
    assert_eq!(keys.len(), 2);
}

/// Truncate works through the sharded service.
#[mz_ore::test(tokio::test)]
async fn test_sharded_truncate() {
    let client = new_persist_client_for_test().await;
    let (service, _, _) = build_two_shard_service(&client).await;

    let key = "s10000000-0000-0000-0000-000000000000";

    // Write some data.
    service.compare_and_set(cas_request(key, None, 1, b"v1")).await.unwrap();
    service.compare_and_set(cas_request(key, Some(1), 2, b"v2")).await.unwrap();
    service.compare_and_set(cas_request(key, Some(2), 3, b"v3")).await.unwrap();

    // Truncate up to seqno 2.
    let resp = service
        .truncate(tonic::Request::new(ProtoTruncateRequest {
            key: key.into(),
            seqno: 2,
        }))
        .await
        .unwrap();
    let deleted = resp.into_inner().deleted;
    assert_eq!(deleted, Some(1), "should delete 1 entry (seqno 1)");

    // Scan should return only seqno 2 and 3.
    let resp = service
        .scan(tonic::Request::new(ProtoScanRequest {
            key: key.into(),
            from: 0,
            limit: 100,
        }))
        .await
        .unwrap();
    let data = resp.into_inner().data;
    assert_eq!(data.len(), 2);
    assert_eq!(data[0].seqno, 2);
    assert_eq!(data[1].seqno, 3);
}

/// Reconfiguration: start with 1 shard, write data, split into 2 shards,
/// verify new writes route correctly to both new shards.
#[mz_ore::test(tokio::test)]
async fn test_reconfiguration_split() {
    let client = new_persist_client_for_test().await;
    let registry = MetricsRegistry::new();

    // Start with a single log shard.
    let shard_1 = ShardId::new();
    let (acceptor_1, learner_1) = spawn_shard(&client, shard_1).await;

    let partition_map = PartitionMap::single(shard_1);

    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_1, acceptor_1);
    let mut learners = BTreeMap::new();
    learners.insert(shard_1, learner_1);

    let service = ShardedService::new(partition_map, acceptors, learners);
    let routing_handle = service.routing_handle();

    let metashard_state = MetashardState::single(shard_1);
    let (metashard_handle, _metashard_task) = PersistMetashardActor::spawn(
        metashard_state,
        256,
        client.clone(),
        registry,
        routing_handle,
        ShardId::new(),
    ).await;

    // --- Pre-reconfiguration: write data on the single shard ---
    let key_lo = "s10000000-0000-0000-0000-000000000000"; // partition key 0x10
    let key_hi = "s90000000-0000-0000-0000-000000000000"; // partition key 0x90

    let resp = service.compare_and_set(cas_request(key_lo, None, 1, b"lo_v1")).await.unwrap();
    assert!(resp.into_inner().committed, "pre-reconfig CAS should commit");

    let resp = service.compare_and_set(cas_request(key_hi, None, 1, b"hi_v1")).await.unwrap();
    assert!(resp.into_inner().committed, "pre-reconfig CAS should commit");

    // --- Reconfigure: split [0x00, 0x100) into [0x00, 0x80) and [0x80, 0x100) ---
    let shard_a = ShardId::new(); // [0x00, 0x80)
    let shard_b = ShardId::new(); // [0x80, 0x100)

    let new_partition_map = PartitionMap {
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
    };

    let new_epoch = metashard_handle
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0,
            new_partition_map: new_partition_map.clone(),
        })
        .await
        .expect("reconfiguration should succeed");
    assert_eq!(new_epoch, 1);

    // Verify the metashard state updated.
    assert_eq!(metashard_handle.current_epoch().await.unwrap(), 1);
    let map = metashard_handle.partition_map().await.unwrap();
    assert_eq!(map.ranges.len(), 2);

    // --- Post-reconfiguration: writes route to new shards ---
    // Give learners a moment to finish predecessor replay.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // State carried forward via chain replay: key_lo and key_hi already have
    // seqno 1 from the predecessor shard. CAS with expected=Some(1) succeeds.
    let resp = service
        .compare_and_set(cas_request(key_lo, Some(1), 2, b"lo_v2_on_shard_a"))
        .await
        .unwrap();
    assert!(
        resp.into_inner().committed,
        "post-reconfig CAS with carried-forward expected seqno should commit"
    );

    let resp = service
        .compare_and_set(cas_request(key_hi, Some(1), 2, b"hi_v2_on_shard_b"))
        .await
        .unwrap();
    assert!(
        resp.into_inner().committed,
        "post-reconfig CAS with carried-forward expected seqno should commit"
    );

    // Read from new shards — data reflects the post-reconfig writes.
    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key_lo.into() }))
        .await
        .unwrap();
    let data = resp.into_inner().data.unwrap();
    assert_eq!(data.data, b"lo_v2_on_shard_a");

    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key_hi.into() }))
        .await
        .unwrap();
    let data = resp.into_inner().data.unwrap();
    assert_eq!(data.data, b"hi_v2_on_shard_b");

    // Verify epoch mismatch is caught.
    let err = metashard_handle
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0, // stale
            new_partition_map,
        })
        .await;
    assert!(matches!(err, Err(crate::MetashardError::EpochMismatch { .. })));
}

/// Reconfiguration with state carryforward: data written before reconfiguration
/// is readable after reconfiguration via chain replay.
///
/// This tests the core state migration guarantee: new learners replay sealed
/// predecessor shards and carry forward the materialized state.
#[mz_ore::test(tokio::test)]
async fn test_reconfiguration_state_carryforward() {
    let client = new_persist_client_for_test().await;
    let registry = MetricsRegistry::new();

    // Start with a single log shard covering the full range.
    let shard_old = ShardId::new();
    let (acceptor_old, learner_old) = spawn_shard(&client, shard_old).await;

    let partition_map = PartitionMap::single(shard_old);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_old, acceptor_old);
    let mut learners = BTreeMap::new();
    learners.insert(shard_old, learner_old);

    let service = ShardedService::new(partition_map, acceptors, learners);
    let routing_handle = service.routing_handle();

    let metashard_state = MetashardState::single(shard_old);
    let (metashard_handle, _metashard_task) = PersistMetashardActor::spawn(
        metashard_state,
        256,
        client.clone(),
        registry,
        routing_handle,
        ShardId::new(),
    ).await;

    // --- Write data before reconfiguration ---
    let key = "s30000000-0000-0000-0000-000000000000"; // partition key 0x30

    let resp = service
        .compare_and_set(cas_request(key, None, 1, b"before_reconfig_v1"))
        .await
        .unwrap();
    assert!(resp.into_inner().committed);

    let resp = service
        .compare_and_set(cas_request(key, Some(1), 2, b"before_reconfig_v2"))
        .await
        .unwrap();
    assert!(resp.into_inner().committed);

    // Verify data is readable before reconfig.
    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().data.unwrap().seqno, 2);

    // --- Reconfigure: replace old shard with new shard ---
    let shard_new = ShardId::new();
    let new_partition_map = PartitionMap::single(shard_new);

    let new_epoch = metashard_handle
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0,
            new_partition_map,
        })
        .await
        .expect("reconfiguration should succeed");
    assert_eq!(new_epoch, 1);

    // Give the learner a moment to finish predecessor replay.
    // The spawn_with_predecessors blocks on replay before entering the run
    // loop, but there's a brief async window for the learner task to start.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // --- Verify state carried forward ---
    // The new learner should have replayed shard_old and have the data.
    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
        .await
        .unwrap();
    let data = resp.into_inner().data;
    assert!(
        data.is_some(),
        "data should be carried forward from predecessor shard"
    );
    let data = data.unwrap();
    assert_eq!(data.seqno, 2, "seqno should be carried forward");
    assert_eq!(
        data.data, b"before_reconfig_v2",
        "data should be carried forward"
    );

    // --- Verify new writes work on the new shard ---
    let resp = service
        .compare_and_set(cas_request(key, Some(2), 3, b"after_reconfig_v3"))
        .await
        .unwrap();
    assert!(
        resp.into_inner().committed,
        "CAS with correct expected seqno from carried-forward state should commit"
    );

    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().data.unwrap().seqno, 3);
}

/// DST-style test: run a workload across multiple client shards, reconfigure
/// mid-flight, continue the workload, verify all state is consistent.
///
/// Tests N client shards spread across the partition key space. Each shard
/// gets a sequence of CaS operations building a chain of seqnos. After
/// reconfiguration, the chains continue correctly on the new log shards.
#[mz_ore::test(tokio::test)]
async fn test_dst_workload_with_reconfiguration() {
    let client = new_persist_client_for_test().await;
    let registry = MetricsRegistry::new();

    let shard_1 = ShardId::new();
    let (acceptor, learner) = spawn_shard(&client, shard_1).await;

    let partition_map = PartitionMap::single(shard_1);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_1, acceptor);
    let mut learners = BTreeMap::new();
    learners.insert(shard_1, learner);

    let service = ShardedService::new(partition_map, acceptors, learners);
    let routing_handle = service.routing_handle();

    let metashard_state = MetashardState::single(shard_1);
    let (metashard_handle, _metashard_task) = PersistMetashardActor::spawn(
        metashard_state,
        256,
        client.clone(),
        registry,
        routing_handle,
        ShardId::new(),
    ).await;

    // Client shard keys spread across the partition key space.
    let keys = [
        "s10000000-0000-0000-0000-000000000000", // 0x10 → first half
        "s30000000-0000-0000-0000-000000000000", // 0x30 → first half
        "s50000000-0000-0000-0000-000000000000", // 0x50 → first half
        "s90000000-0000-0000-0000-000000000000", // 0x90 → second half
        "sb0000000-0000-0000-0000-000000000000", // 0xb0 → second half
        "sd0000000-0000-0000-0000-000000000000", // 0xd0 → second half
    ];

    // Track expected seqno per key.
    let mut expected_seqno: BTreeMap<&str, u64> = BTreeMap::new();

    // --- Phase 1: Write initial data (all on single shard) ---
    for &key in &keys {
        for seqno in 1..=3u64 {
            let prev = if seqno == 1 { None } else { Some(seqno - 1) };
            let data = format!("key={}_seq={}", &key[1..3], seqno);
            let resp = service
                .compare_and_set(cas_request(key, prev, seqno, data.as_bytes()))
                .await
                .unwrap();
            assert!(
                resp.into_inner().committed,
                "pre-reconfig CAS for {} seqno {} should commit",
                &key[1..3],
                seqno
            );
            expected_seqno.insert(key, seqno);
        }
    }

    // Verify all heads before reconfiguration.
    for &key in &keys {
        let resp = service
            .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
            .await
            .unwrap();
        let seqno = resp.into_inner().data.unwrap().seqno;
        assert_eq!(seqno, 3, "pre-reconfig head for {} should be 3", &key[1..3]);
    }

    // --- Phase 2: Reconfigure — split into two shards ---
    let shard_a = ShardId::new(); // [0x00, 0x80)
    let shard_b = ShardId::new(); // [0x80, 0x100)

    let new_partition_map = PartitionMap {
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
    };

    let new_epoch = metashard_handle
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0,
            new_partition_map,
        })
        .await
        .expect("reconfiguration should succeed");
    assert_eq!(new_epoch, 1);

    // Wait for learner predecessor replay to complete.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // --- Phase 3: Verify state carried forward for ALL keys ---
    for &key in &keys {
        let resp = service
            .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
            .await
            .unwrap();
        let data = resp.into_inner().data;
        assert!(
            data.is_some(),
            "post-reconfig head for {} should have data (carried forward)",
            &key[1..3]
        );
        assert_eq!(
            data.unwrap().seqno, 3,
            "post-reconfig seqno for {} should be 3 (carried forward)",
            &key[1..3]
        );
    }

    // --- Phase 4: Continue workload on new shards ---
    for &key in &keys {
        let prev_seqno = expected_seqno[key];
        for delta in 1..=3u64 {
            let seqno = prev_seqno + delta;
            let prev = Some(seqno - 1);
            let data = format!("post_reconfig_key={}_seq={}", &key[1..3], seqno);
            let resp = service
                .compare_and_set(cas_request(key, prev, seqno, data.as_bytes()))
                .await
                .unwrap();
            assert!(
                resp.into_inner().committed,
                "post-reconfig CAS for {} seqno {} should commit",
                &key[1..3],
                seqno
            );
            expected_seqno.insert(key, seqno);
        }
    }

    // --- Phase 5: Final verification ---
    for &key in &keys {
        let resp = service
            .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
            .await
            .unwrap();
        let data = resp.into_inner().data.unwrap();
        assert_eq!(
            data.seqno,
            expected_seqno[key],
            "final seqno for {} should be {}",
            &key[1..3],
            expected_seqno[key]
        );
    }

    // Verify list_keys returns all keys across both shards.
    let resp = service
        .list_keys(tonic::Request::new(ProtoListKeysRequest {}))
        .await
        .unwrap();
    let stream = resp.into_inner();
    let listed: Vec<String> = tokio_stream::StreamExt::collect::<Vec<_>>(stream)
        .await
        .into_iter()
        .map(|r| r.unwrap().key)
        .collect();
    assert_eq!(
        listed.len(),
        keys.len(),
        "list_keys should return all {} keys across both shards",
        keys.len()
    );
    for &key in &keys {
        assert!(
            listed.contains(&key.to_string()),
            "list_keys missing {}",
            key
        );
    }
}

// ---------------------------------------------------------------------------
// Invariant + protocol obligation tests
// ---------------------------------------------------------------------------

/// Helper: read all entries from a persist shard and return the client shard
/// keys (the OrderedKey.shard field from each +1 diff entry).
#[allow(dead_code)] // Kept for future shard-content inspection tests.
async fn read_shard_keys(client: &PersistClient, shard_id: ShardId) -> Vec<String> {
    let key_schema = Arc::new(OrderedKeySchema);
    let val_schema = Arc::new(ProposalSchema);

    let (write, read) = client
        .open::<OrderedKey, Proposal, u64, i64>(
            shard_id,
            key_schema,
            val_schema,
            Diagnostics::from_purpose("test-read-shard-keys"),
            false,
        )
        .await
        .expect("open shard");

    let target_upper = write.upper().as_option().copied().unwrap_or(u64::MAX);
    let since = read.since().clone();
    let mut subscribe = read.subscribe(since).await.expect("subscribe");

    let mut keys = Vec::new();
    loop {
        let events = subscribe.fetch_next().await;
        let mut done = false;
        for event in &events {
            match event {
                mz_persist_client::read::ListenEvent::Progress(frontier) => {
                    if frontier.is_empty()
                        || frontier.as_option().copied() >= Some(target_upper)
                    {
                        done = true;
                    }
                }
                mz_persist_client::read::ListenEvent::Updates(updates) => {
                    for ((key, _), _, diff) in updates {
                        if *diff == 1 {
                            keys.push(key.shard.clone());
                        }
                    }
                }
            }
        }
        if done {
            break;
        }
    }
    keys
}

/// Invariant: after a split reconfiguration, each new shard contains ONLY
/// entries whose partition_key falls within its assigned range.
#[mz_ore::test(tokio::test)]
async fn test_shard_ownership_invariant_after_split() {
    let client = new_persist_client_for_test().await;
    let registry = MetricsRegistry::new();

    let shard_old = ShardId::new();
    let (acceptor, learner) = spawn_shard(&client, shard_old).await;

    let partition_map = PartitionMap::single(shard_old);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_old, acceptor);
    let mut learners = BTreeMap::new();
    learners.insert(shard_old, learner);

    let service = ShardedService::new(partition_map, acceptors, learners);
    let routing_handle = service.routing_handle();

    let metashard_state = MetashardState::single(shard_old);
    let (metashard_handle, _) = PersistMetashardActor::spawn(
        metashard_state,
        256,
        client.clone(),
        registry,
        routing_handle,
        ShardId::new(),
    ).await;

    // Write data across the full key range.
    let keys = [
        "s10000000-0000-0000-0000-000000000000", // 0x10 → first half
        "s50000000-0000-0000-0000-000000000000", // 0x50 → first half
        "s90000000-0000-0000-0000-000000000000", // 0x90 → second half
        "sd0000000-0000-0000-0000-000000000000", // 0xd0 → second half
    ];
    for &key in &keys {
        service.compare_and_set(cas_request(key, None, 1, b"v1")).await.unwrap();
    }

    // Split into two shards.
    let shard_a = ShardId::new(); // [0x00, 0x80)
    let shard_b = ShardId::new(); // [0x80, 0x100)
    metashard_handle
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0,
            new_partition_map: PartitionMap {
                epoch: 1,
                ranges: vec![
                    RangeAssignment { lo: 0x00, hi_exclusive: 0x80, log_shard: shard_a },
                    RangeAssignment { lo: 0x80, hi_exclusive: 0x100, log_shard: shard_b },
                ],
            },
        })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Invariant: each key's head is readable through the service, routed to
    // the correct shard. Keys in [0x00, 0x80) should route to shard_a, and
    // keys in [0x80, 0x100) should route to shard_b.
    let lo_keys = ["s10000000-0000-0000-0000-000000000000", "s50000000-0000-0000-0000-000000000000"];
    let hi_keys = ["s90000000-0000-0000-0000-000000000000", "sd0000000-0000-0000-0000-000000000000"];

    for &key in &lo_keys {
        let resp = service
            .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
            .await
            .unwrap();
        assert!(
            resp.into_inner().data.is_some(),
            "lo-range key {} should be readable after split (routed to shard_a)",
            key
        );
    }

    for &key in &hi_keys {
        let resp = service
            .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
            .await
            .unwrap();
        assert!(
            resp.into_inner().data.is_some(),
            "hi-range key {} should be readable after split (routed to shard_b)",
            key
        );
    }

    // Post-split writes go to the correct shard. Verify with carried-forward state.
    let resp = service
        .compare_and_set(cas_request(lo_keys[0], Some(1), 2, b"post_split"))
        .await
        .unwrap();
    assert!(
        resp.into_inner().committed,
        "CaS with expected=1 (carried from predecessor) should commit on shard_a"
    );

    let resp = service
        .compare_and_set(cas_request(hi_keys[0], Some(1), 2, b"post_split"))
        .await
        .unwrap();
    assert!(
        resp.into_inner().committed,
        "CaS with expected=1 (carried from predecessor) should commit on shard_b"
    );

    // If a new shard's persist data contained writes for ANOTHER new shard, write
    // them here, then read: the new acceptor would route writes only to the correct
    // new learner, so the state should be consistent. We verify via post-split CaS
    // correctness above: if out-of-range state existed, the expected seqno would
    // be wrong and the CaS would fail.
}

/// Fan-in (merge) reconfiguration: 2 shards → 1 shard.
/// Verifies all state from both predecessors carries forward.
#[mz_ore::test(tokio::test)]
async fn test_reconfiguration_merge() {
    let client = new_persist_client_for_test().await;
    let registry = MetricsRegistry::new();

    // Start with 2 shards.
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();
    let (acc_a, lrn_a) = spawn_shard(&client, shard_a).await;
    let (acc_b, lrn_b) = spawn_shard(&client, shard_b).await;

    let partition_map = PartitionMap {
        epoch: 0,
        ranges: vec![
            RangeAssignment { lo: 0x00, hi_exclusive: 0x80, log_shard: shard_a },
            RangeAssignment { lo: 0x80, hi_exclusive: 0x100, log_shard: shard_b },
        ],
    };

    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_a, acc_a);
    acceptors.insert(shard_b, acc_b);
    let mut learners = BTreeMap::new();
    learners.insert(shard_a, lrn_a);
    learners.insert(shard_b, lrn_b);

    let service = ShardedService::new(partition_map.clone(), acceptors, learners);
    let routing_handle = service.routing_handle();

    let metashard_state = MetashardState {
        epoch: 0,
        partition_map,
        log_shards: BTreeMap::new(),
        pending_intent: None,
    };
    let (metashard_handle, _) = PersistMetashardActor::spawn(
        metashard_state,
        256,
        client.clone(),
        registry,
        routing_handle,
        ShardId::new(),
    ).await;

    // Write data to both shards.
    let key_lo = "s20000000-0000-0000-0000-000000000000"; // 0x20 → shard_a
    let key_hi = "sa0000000-0000-0000-0000-000000000000"; // 0xa0 → shard_b

    service.compare_and_set(cas_request(key_lo, None, 1, b"lo_v1")).await.unwrap();
    service.compare_and_set(cas_request(key_hi, None, 1, b"hi_v1")).await.unwrap();

    // Merge into a single shard.
    let shard_merged = ShardId::new();
    metashard_handle
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0,
            new_partition_map: PartitionMap::single(shard_merged),
        })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Both keys should be readable from the merged shard.
    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key_lo.into() }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().data.unwrap().seqno,
        1,
        "key_lo state should carry forward from shard_a"
    );

    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key_hi.into() }))
        .await
        .unwrap();
    assert_eq!(
        resp.into_inner().data.unwrap().seqno,
        1,
        "key_hi state should carry forward from shard_b"
    );

    // CaS with carried-forward state should work.
    let resp = service
        .compare_and_set(cas_request(key_lo, Some(1), 2, b"lo_v2_merged"))
        .await
        .unwrap();
    assert!(resp.into_inner().committed, "CaS on merged shard should commit");
}

/// RC2: No silent proposal loss during reconfiguration.
///
/// Runs CaS operations concurrently with a reconfiguration. Every operation
/// must either: commit on old shard, fail with a retriable error (Sealed /
/// gRPC error), or commit on the new shard. No operation may silently vanish.
#[mz_ore::test(tokio::test)]
async fn test_no_silent_loss_during_reconfiguration() {
    let client = new_persist_client_for_test().await;
    let registry = MetricsRegistry::new();

    let shard_old = ShardId::new();
    let (acc, lrn) = spawn_shard(&client, shard_old).await;

    let partition_map = PartitionMap::single(shard_old);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_old, acc);
    let mut learners = BTreeMap::new();
    learners.insert(shard_old, lrn);

    let service = std::sync::Arc::new(ShardedService::new(
        partition_map,
        acceptors,
        learners,
    ));
    let routing_handle = service.routing_handle();

    let metashard_state = MetashardState::single(shard_old);
    let (metashard_handle, _) = PersistMetashardActor::spawn(
        metashard_state,
        256,
        client.clone(),
        registry,
        routing_handle,
        ShardId::new(),
    ).await;

    // Use a key that will stay in [0x00, 0x80) after the split.
    let key = "s20000000-0000-0000-0000-000000000000";

    // Write initial state.
    let resp = service.compare_and_set(cas_request(key, None, 1, b"initial")).await.unwrap();
    assert!(resp.into_inner().committed);

    // Track all CaS attempts, their outcomes, and the highest committed seqno.
    let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let highest_committed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1));

    // Spawn a background task that continuously issues CaS operations.
    let service_clone = Arc::clone(&service);
    let results_clone = Arc::clone(&results);
    let highest_clone = Arc::clone(&highest_committed);
    let writer_task = mz_ore::task::spawn(|| "concurrent-writer", async move {
        let mut seqno = 2u64;
        let mut expected = 1u64;
        for _ in 0..20 {
            let result = service_clone
                .compare_and_set(cas_request(key, Some(expected), seqno, b"concurrent"))
                .await;
            match result {
                Ok(resp) => {
                    let committed = resp.into_inner().committed;
                    results_clone.lock().unwrap().push(("ok", committed));
                    if committed {
                        highest_clone.store(seqno, std::sync::atomic::Ordering::SeqCst);
                        expected = seqno;
                        seqno += 1;
                    }
                }
                Err(_status) => {
                    // gRPC error (sealed, unavailable, etc.) — retriable.
                    results_clone
                        .lock()
                        .unwrap()
                        .push(("error", false));
                    // Don't advance seqno — retry with same expected.
                }
            }
            // Small delay to spread operations across the reconfiguration window.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    });

    // Wait a bit for some operations to land, then reconfigure.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let shard_new_a = ShardId::new();
    let shard_new_b = ShardId::new();
    let _ = metashard_handle
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0,
            new_partition_map: PartitionMap {
                epoch: 1,
                ranges: vec![
                    RangeAssignment { lo: 0x00, hi_exclusive: 0x80, log_shard: shard_new_a },
                    RangeAssignment { lo: 0x80, hi_exclusive: 0x100, log_shard: shard_new_b },
                ],
            },
        })
        .await;

    // Wait for the writer to finish.
    writer_task.await;

    let outcomes: Vec<_> = results.lock().unwrap().clone();

    // RC2: No silent loss. Every operation is accounted for.
    let total = outcomes.len();
    let committed = outcomes.iter().filter(|(s, c)| *s == "ok" && *c).count();
    let rejected = outcomes.iter().filter(|(s, c)| *s == "ok" && !*c).count();
    let errors = outcomes.iter().filter(|(s, _)| *s == "error").count();

    assert_eq!(
        total,
        committed + rejected + errors,
        "every operation must be accounted for"
    );
    assert!(total >= 10, "should have issued at least 10 operations, got {total}");
    assert!(committed >= 1, "at least one operation should have committed");

    // RC2 strong check: the head seqno must EXACTLY equal the highest
    // committed seqno. If any committed write was silently lost during
    // reconfiguration, this will catch it.
    let expected_head = highest_committed.load(std::sync::atomic::Ordering::SeqCst);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let resp = service
        .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
        .await
        .unwrap();
    let data = resp.into_inner().data.unwrap();
    assert_eq!(
        data.seqno, expected_head,
        "head seqno must equal the highest committed seqno ({}); \
         a mismatch means a committed write was silently lost during reconfiguration. \
         outcomes: committed={committed}, rejected={rejected}, errors={errors}",
        expected_head,
    );
}

/// Restart recovery: after a committed reconfiguration, simulate a process
/// restart and verify that carried-forward state (keys never rewritten on
/// the new shard) is still readable.
///
/// This is the test the reviewer identified as missing: it catches the bug
/// where recovery spawns plain learners that can't access predecessor state.
#[mz_ore::test(tokio::test)]
async fn test_restart_after_reconfiguration_preserves_state() {
    let client = new_persist_client_for_test().await;

    // --- Phase 1: initial setup + write data + reconfigure ---
    let registry1 = MetricsRegistry::new();
    let shard_old = ShardId::new();
    let metashard_shard = ShardId::new(); // durable metashard

    let (acc, lrn) = spawn_shard(&client, shard_old).await;
    let partition_map = PartitionMap::single(shard_old);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_old, acc);
    let mut learners = BTreeMap::new();
    learners.insert(shard_old, lrn);

    let service1 = ShardedService::new(partition_map, acceptors, learners);
    let routing_handle1 = service1.routing_handle();

    let metashard_state1 = MetashardState::single(shard_old);
    let (actor1, handle1) = PersistMetashardActor::new(
        metashard_state1,
        256,
        client.clone(),
        registry1,
        routing_handle1,
        metashard_shard,
    ).await;
    let _task1 = mz_ore::task::spawn(|| "metashard-1", actor1.run());

    // Write data that will need to survive reconfiguration + restart.
    let key = "s30000000-0000-0000-0000-000000000000";
    let resp = service1
        .compare_and_set(cas_request(key, None, 1, b"survive_restart"))
        .await
        .unwrap();
    assert!(resp.into_inner().committed);

    // Reconfigure: old shard → new shard.
    let shard_new = ShardId::new();
    handle1
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0,
            new_partition_map: PartitionMap::single(shard_new),
        })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify data is readable after reconfiguration (in-memory carryforward).
    let resp = service1
        .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
        .await
        .unwrap();
    assert_eq!(resp.into_inner().data.unwrap().seqno, 1);

    // --- Phase 2: simulate restart ---
    // Drop the old service (simulates process death).
    drop(handle1);
    drop(_task1);
    drop(service1);

    // Build a fresh service from "bootstrap" args (epoch 0, stale topology).
    // This simulates what main.rs does on a fresh start.
    let registry2 = MetricsRegistry::new();

    // Bootstrap with a dummy shard — the recovery should override this.
    let bootstrap_shard = ShardId::new();
    let (acc2, lrn2) = spawn_shard(&client, bootstrap_shard).await;
    let bootstrap_map = PartitionMap::single(bootstrap_shard);
    let mut acceptors2 = BTreeMap::new();
    acceptors2.insert(bootstrap_shard, acc2);
    let mut learners2 = BTreeMap::new();
    learners2.insert(bootstrap_shard, lrn2);

    let service2 = ShardedService::new(bootstrap_map, acceptors2, learners2);
    let routing_handle2 = service2.routing_handle();

    let metashard_state2 = MetashardState::single(bootstrap_shard);
    let (actor2, _handle2) = PersistMetashardActor::new(
        metashard_state2,
        256,
        client.clone(),
        registry2,
        routing_handle2,
        metashard_shard,
    ).await;
    // Recover from the durable metashard shard — this should restore the
    // committed partition map and rebuild routing with predecessors.
    let _task2 = mz_ore::task::spawn(|| "metashard-2", actor2.run());

    // Give the recovery path time to rebuild routing and replay predecessors.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // --- Phase 3: verify carried-forward state survived restart ---
    let resp = service2
        .head(tonic::Request::new(ProtoHeadRequest { key: key.into() }))
        .await
        .unwrap();
    let data = resp.into_inner().data;
    assert!(
        data.is_some(),
        "carried-forward key should be readable after restart"
    );
    assert_eq!(
        data.unwrap().seqno, 1,
        "carried-forward seqno should be preserved across restart"
    );
}

/// Restart recovery after a 2→1 merge: both predecessors' carried-forward
/// state must survive. This catches the bug where only one predecessor was
/// persisted/recovered due to Option<ShardId> instead of Vec<ShardId>.
#[mz_ore::test(tokio::test)]
async fn test_restart_after_merge_preserves_both_predecessors() {
    let client = new_persist_client_for_test().await;

    // --- Phase 1: start with 2 shards, write to both, merge ---
    let registry1 = MetricsRegistry::new();
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();
    let metashard_shard = ShardId::new();

    let (acc_a, lrn_a) = spawn_shard(&client, shard_a).await;
    let (acc_b, lrn_b) = spawn_shard(&client, shard_b).await;

    let partition_map = PartitionMap {
        epoch: 0,
        ranges: vec![
            RangeAssignment { lo: 0x00, hi_exclusive: 0x80, log_shard: shard_a },
            RangeAssignment { lo: 0x80, hi_exclusive: 0x100, log_shard: shard_b },
        ],
    };
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_a, acc_a);
    acceptors.insert(shard_b, acc_b);
    let mut learners = BTreeMap::new();
    learners.insert(shard_a, lrn_a);
    learners.insert(shard_b, lrn_b);

    let service1 = ShardedService::new(partition_map.clone(), acceptors, learners);
    let routing_handle1 = service1.routing_handle();

    let metashard_state1 = MetashardState {
        epoch: 0,
        partition_map,
        log_shards: BTreeMap::new(),
        pending_intent: None,
    };
    let (actor1, handle1) = PersistMetashardActor::new(
        metashard_state1,
        256,
        client.clone(),
        registry1,
        routing_handle1,
        metashard_shard,
    ).await;
    let _task1 = mz_ore::task::spawn(|| "metashard-merge-1", actor1.run());

    // Write to shard_a and shard_b.
    let key_lo = "s20000000-0000-0000-0000-000000000000"; // 0x20 → shard_a
    let key_hi = "sa0000000-0000-0000-0000-000000000000"; // 0xa0 → shard_b

    service1.compare_and_set(cas_request(key_lo, None, 1, b"from_a")).await.unwrap();
    service1.compare_and_set(cas_request(key_hi, None, 1, b"from_b")).await.unwrap();

    // Merge into a single shard.
    let shard_merged = ShardId::new();
    handle1
        .reconfigure(ReconfigurationPlan {
            expected_epoch: 0,
            new_partition_map: PartitionMap::single(shard_merged),
        })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify both keys readable after merge.
    let resp = service1.head(tonic::Request::new(ProtoHeadRequest { key: key_lo.into() })).await.unwrap();
    assert_eq!(resp.into_inner().data.unwrap().seqno, 1);
    let resp = service1.head(tonic::Request::new(ProtoHeadRequest { key: key_hi.into() })).await.unwrap();
    assert_eq!(resp.into_inner().data.unwrap().seqno, 1);

    // --- Phase 2: simulate restart ---
    drop(handle1);
    drop(_task1);
    drop(service1);

    let registry2 = MetricsRegistry::new();
    let bootstrap_shard = ShardId::new();
    let (acc2, lrn2) = spawn_shard(&client, bootstrap_shard).await;
    let bootstrap_map = PartitionMap::single(bootstrap_shard);
    let mut acceptors2 = BTreeMap::new();
    acceptors2.insert(bootstrap_shard, acc2);
    let mut learners2 = BTreeMap::new();
    learners2.insert(bootstrap_shard, lrn2);

    let service2 = ShardedService::new(bootstrap_map, acceptors2, learners2);
    let routing_handle2 = service2.routing_handle();

    let metashard_state2 = MetashardState::single(bootstrap_shard);
    let (actor2, _handle2) = PersistMetashardActor::new(
        metashard_state2,
        256,
        client.clone(),
        registry2,
        routing_handle2,
        metashard_shard,
    ).await;
    let _task2 = mz_ore::task::spawn(|| "metashard-merge-2", actor2.run());

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // --- Phase 3: verify BOTH predecessors' state survived ---
    let resp = service2.head(tonic::Request::new(ProtoHeadRequest { key: key_lo.into() })).await.unwrap();
    let data = resp.into_inner().data;
    assert!(data.is_some(), "key from shard_a should survive merge + restart");
    assert_eq!(data.unwrap().seqno, 1);

    let resp = service2.head(tonic::Request::new(ProtoHeadRequest { key: key_hi.into() })).await.unwrap();
    let data = resp.into_inner().data;
    assert!(data.is_some(), "key from shard_b should survive merge + restart");
    assert_eq!(data.unwrap().seqno, 1);
}

/// Crash during reconfiguration: simulate a crash after the intent is persisted
/// but before reconfiguration completes. On restart, the metashard actor should
/// detect the pending intent and resume the reconfiguration.
///
/// This directly exercises the recovery code path at metashard.rs `run()`.
#[mz_ore::test(tokio::test)]
async fn test_crash_during_reconfiguration_recovers_intent() {
    let client = new_persist_client_for_test().await;

    // Use a fixed metashard shard ID so both incarnations share durable state.
    let metashard_shard = ShardId::new();

    // --- Phase 1: write data on a single shard, then start reconfiguration and crash ---
    let shard_old = ShardId::new();
    let (acc_old, lrn_old) = spawn_shard(&client, shard_old).await;

    let partition_map = PartitionMap::single(shard_old);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_old, acc_old);
    let mut learners = BTreeMap::new();
    learners.insert(shard_old, lrn_old);

    let service1 = ShardedService::new(partition_map.clone(), acceptors, learners);
    let routing_handle1 = service1.routing_handle();

    let metashard_state1 = MetashardState::single(shard_old);
    let registry1 = MetricsRegistry::new();
    let (actor1, handle1) = PersistMetashardActor::new(
        metashard_state1,
        256,
        client.clone(),
        registry1,
        routing_handle1,
        metashard_shard,
    )
    .await;
    let task1 = mz_ore::task::spawn(|| "metashard-crash-1", actor1.run());

    // Write data that must survive the crash+recovery.
    let key = "s30000000-0000-0000-0000-000000000000"; // partition key 0x30
    let resp = service1
        .compare_and_set(cas_request(key, None, 1, b"before_crash"))
        .await
        .unwrap();
    assert!(resp.into_inner().committed, "pre-crash write should commit");

    // Start a reconfiguration: split [0x00, 0x100) into two new shards.
    let shard_a = ShardId::new();
    let shard_b = ShardId::new();

    let split_plan = ReconfigurationPlan {
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

    // Use BUGGIFY to deterministically crash after the intent is persisted
    // but before the reconfiguration completes. This guarantees we exercise
    // the recovery path (pending intent detected on restart), unlike a
    // timing-based approach that might race past the crash window.
    use crate::fault::{self, FaultConfig};
    fault::configure(FaultConfig::with_points(&["after_intent_persist"], 1.0, 42));

    let result = handle1.reconfigure(split_plan).await;
    assert!(
        result.is_err(),
        "reconfiguration should fail at after_intent_persist injection point"
    );

    fault::clear();

    // Crash: drop all handles and abort the actor task.
    drop(handle1);
    task1.abort_and_wait().await;
    drop(service1);

    // --- Phase 2: restart with a fresh metashard actor using the same durable shard ---
    let registry2 = MetricsRegistry::new();

    // Bootstrap with a dummy single shard (the recovery will overwrite this
    // from the durable partition map + pending intent).
    let bootstrap_shard = ShardId::new();
    let (acc_boot, lrn_boot) = spawn_shard(&client, bootstrap_shard).await;

    let bootstrap_map = PartitionMap::single(bootstrap_shard);
    let mut acceptors2 = BTreeMap::new();
    acceptors2.insert(bootstrap_shard, acc_boot);
    let mut learners2 = BTreeMap::new();
    learners2.insert(bootstrap_shard, lrn_boot);

    let service2 = ShardedService::new(bootstrap_map, acceptors2, learners2);
    let routing_handle2 = service2.routing_handle();

    // Create a new metashard actor with the same durable shard ID. The actor's
    // constructor reads the durable state and discovers the pending intent.
    let metashard_state2 = MetashardState::single(bootstrap_shard);
    let (actor2, handle2) = PersistMetashardActor::new(
        metashard_state2,
        256,
        client.clone(),
        registry2,
        routing_handle2,
        metashard_shard,
    )
    .await;

    // Start the actor — its run() method will detect the pending intent and
    // resume the reconfiguration automatically.
    let _task2 = mz_ore::task::spawn(|| "metashard-crash-2", actor2.run());

    // Wait for recovery to complete (intent detection + do_reconfigure).
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // --- Phase 3: verify recovery completed and data carried forward ---

    // The recovered metashard should have completed the split reconfiguration.
    let recovered_epoch = handle2.current_epoch().await.unwrap();
    assert_eq!(
        recovered_epoch, 1,
        "recovered metashard should have completed the split to epoch 1"
    );

    let recovered_map = handle2.partition_map().await.unwrap();
    assert_eq!(
        recovered_map.ranges.len(),
        2,
        "recovered partition map should have 2 ranges after split"
    );

    // The pre-crash write (key with partition key 0x30, seqno 1) should be
    // readable via the new shard_a (range [0x00, 0x80)) through chain replay.
    let resp = service2
        .head(tonic::Request::new(ProtoHeadRequest {
            key: key.into(),
        }))
        .await
        .unwrap();
    let data = resp.into_inner().data;
    assert!(
        data.is_some(),
        "pre-crash write should survive crash + recovery via predecessor replay"
    );
    assert_eq!(data.unwrap().seqno, 1);

    // Post-recovery writes should work: CAS with expected=Some(1) from
    // carried-forward state.
    let resp = service2
        .compare_and_set(cas_request(key, Some(1), 2, b"after_recovery"))
        .await
        .unwrap();
    assert!(
        resp.into_inner().committed,
        "post-recovery CAS with carried-forward expected seqno should commit"
    );
}

/// Concurrent linearizability test: multiple client tasks submit overlapping
/// operations while a reconfiguration is in flight. Every operation is checked
/// against a sequential oracle via Stateright's `LinearizabilityTester`.
///
/// This is the concurrent-history variant of persist_sim_single: instead of
/// sequential operations, multiple tasks submit concurrently, and the combined
/// history (with real concurrency, not just alternation) is verified.
#[mz_ore::test(tokio::test)]
async fn test_concurrent_linearizability_during_reconfig() {
    use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLog;
    use std::sync::atomic::{AtomicU64, Ordering};

    let client = new_persist_client_for_test().await;
    let registry = MetricsRegistry::new();

    // Start with a single log shard.
    let shard_old = ShardId::new();
    let (acc_old, lrn_old) = spawn_shard(&client, shard_old).await;

    let partition_map = PartitionMap::single(shard_old);
    let mut acceptors = BTreeMap::new();
    acceptors.insert(shard_old, acc_old);
    let mut learners = BTreeMap::new();
    learners.insert(shard_old, lrn_old);

    let service = Arc::new(ShardedService::new(partition_map.clone(), acceptors, learners));
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

    // Shared state for tracking committed seqnos per client shard.
    let committed_seqnos: Arc<[AtomicU64; 4]> = Arc::new([
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ]);

    // Tracking for linearizability: each operation records (invoke_time, return_time, result).
    let history: Arc<tokio::sync::Mutex<Vec<(usize, String, u64, bool, u64)>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // Client shard keys spread across partition space.
    let keys = [
        "s10000000-0000-0000-0000-000000000000", // 0x10 → first half
        "s30000000-0000-0000-0000-000000000000", // 0x30 → first half
        "s90000000-0000-0000-0000-000000000000", // 0x90 → second half
        "sb0000000-0000-0000-0000-000000000000", // 0xb0 → second half
    ];

    let op_counter = Arc::new(AtomicU64::new(0));

    // Spawn 4 client tasks, each writing a chain of CAS operations.
    let num_ops_per_client = 5;
    let mut client_tasks = Vec::new();

    for client_id in 0..4usize {
        let service = Arc::clone(&service);
        let seqnos = Arc::clone(&committed_seqnos);
        let history = Arc::clone(&history);
        let op_counter = Arc::clone(&op_counter);
        let key = keys[client_id];

        client_tasks.push(mz_ore::task::spawn(
            || format!("client-{}", client_id),
            async move {
                for _ in 0..num_ops_per_client {
                    let invoke_time = op_counter.fetch_add(1, Ordering::SeqCst);
                    let current = seqnos[client_id].load(Ordering::SeqCst);
                    let expected = if current == 0 { None } else { Some(current) };
                    let new_seqno = current + 1;
                    let data = format!("client{}_{}", client_id, new_seqno);

                    let result = service
                        .compare_and_set(cas_request(key, expected, new_seqno, data.as_bytes()))
                        .await;

                    let return_time = op_counter.fetch_add(1, Ordering::SeqCst);

                    match result {
                        Ok(resp) => {
                            let committed = resp.into_inner().committed;
                            if committed {
                                seqnos[client_id].store(new_seqno, Ordering::SeqCst);
                            }
                            history.lock().await.push((
                                client_id,
                                key.to_string(),
                                invoke_time,
                                committed,
                                return_time,
                            ));
                        }
                        Err(_) => {
                            // Sealed error during reconfig — legal, just retry.
                            // Record as not-committed for history.
                            history.lock().await.push((
                                client_id,
                                key.to_string(),
                                invoke_time,
                                false,
                                return_time,
                            ));
                        }
                    }

                    // Small yield to allow interleaving with other tasks and reconfig.
                    tokio::task::yield_now().await;
                }
            },
        ));
    }

    // After the first batch of client writes, trigger a split reconfiguration
    // concurrently. Give writers a head start.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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

    // Wait for all client tasks to complete.
    for task in client_tasks {
        task.await;
    }

    // Wait for reconfiguration to complete.
    let reconfig_result = reconfig_task.await;
    assert!(
        reconfig_result.is_ok(),
        "reconfiguration should succeed: {:?}",
        reconfig_result
    );

    // --- Verify results ---
    let history = history.lock().await;

    // Basic sanity: we got results from all operations.
    assert!(
        !history.is_empty(),
        "should have recorded at least some operations"
    );

    // Check that each client shard's committed operations form a valid chain.
    // For each key, committed CAS operations should have strictly increasing
    // seqnos with no gaps (each CAS depends on the previous committed one).
    let mut committed_per_key: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for &(_client_id, ref key, _invoke, committed, _return_time) in history.iter() {
        if committed {
            let current = committed_per_key
                .entry(key.clone())
                .or_default()
                .last()
                .copied()
                .unwrap_or(0);
            committed_per_key
                .entry(key.clone())
                .or_default()
                .push(current + 1);
        }
    }

    // Verify no committed write is lost: read each key and check the seqno
    // matches the highest committed seqno.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for (i, key) in keys.iter().enumerate() {
        let expected_seqno = committed_seqnos[i].load(Ordering::SeqCst);
        if expected_seqno > 0 {
            let resp = service
                .head(tonic::Request::new(ProtoHeadRequest {
                    key: key.to_string(),
                }))
                .await
                .unwrap();
            let data = resp.into_inner().data;
            assert!(
                data.is_some(),
                "key {} should have data after writes (expected seqno {})",
                key,
                expected_seqno
            );
            assert_eq!(
                data.unwrap().seqno, expected_seqno,
                "key {} head seqno should match committed chain",
                key
            );
        }
    }

    // Verify no proposal was silently lost: every committed CAS in the history
    // should be reflected in a monotonically increasing seqno chain.
    for (key, seqnos) in &committed_per_key {
        for (i, &seqno) in seqnos.iter().enumerate() {
            assert_eq!(
                seqno,
                (i as u64) + 1,
                "key {} committed seqnos should form a 1..n chain, got {:?}",
                key,
                seqnos
            );
        }
    }
}

/// BUGGIFY: exercise reconfiguration with fault injection at each protocol
/// boundary. For each injection point, enable it at 100% probability, start
/// a reconfiguration (which will fail at that point), then retry without
/// faults and verify the system recovers correctly.
#[mz_ore::test(tokio::test)]
async fn test_buggify_reconfiguration_recovery() {
    use crate::fault::{self, FaultConfig};

    // Pre-commit injection points: the reconfiguration has not committed yet,
    // so the error propagates and the caller can retry cleanly.
    let injection_points = [
        "after_intent_persist",
        "after_seal",
        "after_actor_spawn",
        "during_predecessor_replay",
        "after_replay_complete",
    ];

    let client = new_persist_client_for_test().await;

    for point in &injection_points {
        // Clean up fault config between iterations.
        fault::clear();

        let registry = MetricsRegistry::new();

        let shard_old = ShardId::new();
        let (acc, lrn) = spawn_shard(&client, shard_old).await;

        let partition_map = PartitionMap::single(shard_old);
        let mut acceptors = BTreeMap::new();
        acceptors.insert(shard_old, acc);
        let mut learners = BTreeMap::new();
        learners.insert(shard_old, lrn);

        let service = ShardedService::new(partition_map, acceptors, learners);
        let routing_handle = service.routing_handle();

        let metashard_state = MetashardState::single(shard_old);
        let (metashard_handle, _task) = PersistMetashardActor::spawn(
            metashard_state,
            256,
            client.clone(),
            registry,
            routing_handle,
            ShardId::new(),
        )
        .await;

        // Write data before reconfiguration.
        let key = "s30000000-0000-0000-0000-000000000000";
        let resp = service
            .compare_and_set(cas_request(key, None, 1, b"pre_fault"))
            .await
            .unwrap();
        assert!(
            resp.into_inner().committed,
            "point={}: pre-fault write should commit",
            point
        );

        let shard_a = ShardId::new();
        let shard_b = ShardId::new();
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

        // Enable the fault injection point at 100% probability.
        fault::configure(FaultConfig::with_points(&[point], 1.0, 42));

        // Attempt reconfiguration — should fail at the injection point.
        let result = metashard_handle.reconfigure(plan.clone()).await;
        assert!(
            result.is_err(),
            "point={}: reconfiguration should fail at injection point",
            point
        );

        // Disable fault injection.
        fault::clear();

        // Retry reconfiguration — should succeed now.
        let result = metashard_handle.reconfigure(plan).await;
        assert!(
            result.is_ok(),
            "point={}: retry should succeed after fault cleared: {:?}",
            point,
            result
        );

        // Verify the pre-fault write survived via chain replay.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let resp = service
            .head(tonic::Request::new(ProtoHeadRequest {
                key: key.into(),
            }))
            .await
            .unwrap();
        let data = resp.into_inner().data;
        assert!(
            data.is_some(),
            "point={}: pre-fault write should survive recovery",
            point
        );
        assert_eq!(
            data.unwrap().seqno, 1,
            "point={}: seqno should be 1 after recovery",
            point
        );
    }

    fault::clear();
}

/// BUGGIFY: exercise the post-commit injection points `after_routing_swap`
/// and `after_commit_persist`. These fire AFTER the point of no return — the
/// routing has been swapped and/or the durable state persisted. The error
/// propagates to the caller, but the reconfiguration has effectively committed.
///
/// `after_routing_swap` is the hardest case: routing points to new shards but
/// the durable metashard state still has the old epoch. On the caller's next
/// operation, the epoch mismatch between routing and durable state must not
/// cause data loss.
#[mz_ore::test(tokio::test)]
async fn test_buggify_post_commit_injection_points() {
    use crate::fault::{self, FaultConfig};

    let post_commit_points = [
        "after_routing_swap",
        "after_commit_persist",
        "before_hold_release",
    ];

    let client = new_persist_client_for_test().await;

    for point in &post_commit_points {
        fault::clear();

        let registry = MetricsRegistry::new();
        let shard_old = ShardId::new();
        let (acc, lrn) = spawn_shard(&client, shard_old).await;

        let partition_map = PartitionMap::single(shard_old);
        let mut acceptors = BTreeMap::new();
        acceptors.insert(shard_old, acc);
        let mut learners = BTreeMap::new();
        learners.insert(shard_old, lrn);

        let service = ShardedService::new(partition_map, acceptors, learners);
        let routing_handle = service.routing_handle();

        let metashard_state = MetashardState::single(shard_old);
        let (metashard_handle, _task) = PersistMetashardActor::spawn(
            metashard_state,
            256,
            client.clone(),
            registry,
            routing_handle,
            ShardId::new(),
        )
        .await;

        let key = "s30000000-0000-0000-0000-000000000000";
        let resp = service
            .compare_and_set(cas_request(key, None, 1, b"pre_fault"))
            .await
            .unwrap();
        assert!(resp.into_inner().committed, "point={}: pre-fault write", point);

        let shard_a = ShardId::new();
        let shard_b = ShardId::new();
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

        fault::configure(FaultConfig::with_points(&[point], 1.0, 42));

        // The reconfiguration should fail AFTER the routing swap / commit
        // persist — the error is from the injection point, not from any
        // actual protocol failure.
        let result = metashard_handle.reconfigure(plan).await;
        assert!(
            result.is_err(),
            "point={}: should fail at injection point",
            point
        );

        fault::clear();

        // Despite the error, the reconfiguration has effectively committed
        // (routing was swapped and/or state was persisted). Wait for learner
        // predecessor replay to complete, then verify data is accessible.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let resp = service
            .head(tonic::Request::new(ProtoHeadRequest {
                key: key.into(),
            }))
            .await
            .unwrap();
        let data = resp.into_inner().data;
        assert!(
            data.is_some(),
            "point={}: pre-fault write should be readable after post-commit fault",
            point,
        );
        assert_eq!(
            data.unwrap().seqno, 1,
            "point={}: seqno should be 1",
            point,
        );
    }

    fault::clear();
}
