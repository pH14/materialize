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
    );

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
    );

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
    );

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
