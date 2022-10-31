// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wow!

use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::consolidation::consolidate_updates;
use timely::progress::Antichain;
use tokio::sync::Mutex;

use crate::compaction::UpsertArgs;
use mz_build_info::DUMMY_BUILD_INFO;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistConfig, PersistLocation, ShardId};

pub(crate) async fn run(args: UpsertArgs) -> Result<(), anyhow::Error> {
    let num_rounds = args.num_rounds;
    let num_keys = args.num_keys;
    let num_keys_last_round = args.num_keys;

    let mut cfg = PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
    cfg.blob_target_size = args.blob_target_size;

    let persist_cache = Arc::new(Mutex::new(PersistClientCache::new(
        cfg,
        &MetricsRegistry::new(),
    )));

    let persist_location = PersistLocation {
        blob_uri: "mem://".to_owned(),
        consensus_uri: "mem://".to_owned(),
    };

    let mut persist_clients = persist_cache.lock().await;
    let persist_client = persist_clients
        .open(persist_location)
        .await
        .expect("error creating persist client");

    let shard_id = ShardId::new();
    let (mut write, mut read) = persist_client
        .open::<String, String, u64, i64>(shard_id)
        .await
        .expect("invalid usage");

    for round in 0..(num_rounds) {
        println!("round {round}");

        // Last round can emit one order of magnitude more keys
        let (num_retractions, num_additions) = if round == 0 {
            // First round has no retractions.
            (0, num_keys)
        } else if round == num_rounds - 1 {
            (num_keys, num_keys_last_round)
        } else {
            (num_keys, num_keys)
        };

        // Retract updates from previous round.
        let value = vec![0; args.value_size];
        let retractions =
            (0..num_retractions).map(|key| ((key.to_string(), value.to_string()), round, -1));

        // Add updates for new round.
        let additions =
            (0..num_additions).map(|key| ((key.to_string(), value.to_string()), round, 1));

        let combined = retractions.chain(additions);

        write
            .compare_and_append(
                combined,
                Antichain::from_elem(round),
                Antichain::from_elem(round + 1),
            )
            .await
            .expect("invalid usage")
            .expect("indeterminate")
            .expect("invalid expected upper");

        // compaction is async, give it time to perform
        tokio::time::sleep(args.write_delay_ms).await;

        let (spine_batches, metrics) = &write.internal_stats();
        let mut num_batches = 0;
        let mut num_batch_parts = 0;
        let mut num_entries = 0;
        let mut num_fueled_merges = 0;
        for (batches, updates, parts, _fuel_needed) in spine_batches {
            num_batches += batches;
            num_batch_parts += parts;
            num_entries += updates;
            if *batches == 2 {
                num_fueled_merges += 1;
            }
        }
        println!("{round} spine   stats: num_batches: {num_batches}, num_batch_parts: {num_batch_parts}, num_entries: {num_entries}, num_fueled_merges: {num_fueled_merges} ");
        println!("{round} compact stats: requested: {}, started: {}, skipped: {}, dropped: {}, failed: {}, noop: {}, applied_exact: {}, applied_subset: {}",
                 metrics.requested.get(),
                 metrics.started.get(),
                 metrics.skipped.get(),
                 metrics.dropped.get(),
                 metrics.failed.get(),
                 metrics.noop.get(),
                 metrics.applied_exact_match.get(),
                 metrics.applied_subset_match.get());

        read.downgrade_since(&Antichain::from_elem(round.saturating_sub(args.since_lag)))
            .await;
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let recent_upper = write.fetch_recent_upper().await;
    println!(
        "recent_upper: {:?}, trying to snapshot at {}",
        recent_upper,
        num_rounds - 1
    );
    let snap = read
        .snapshot_and_fetch(Antichain::from_elem(num_rounds - 1))
        .await
        .expect("incorrect since");

    let mut updates = snap
        .into_iter()
        .map(|((key, value), ts, diff)| ((key.unwrap(), value.unwrap()), ts, diff))
        .collect::<Vec<_>>();

    let num_entries_from_snapshot = updates.len();

    consolidate_updates(&mut updates);

    let num_consolidated_entries = updates.len();

    println!("num_entries_from_snapshot: {num_entries_from_snapshot}");
    println!("consolidated updates.len: {}", num_consolidated_entries);

    Ok(())
}
