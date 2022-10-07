// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An example that shows how a highly-available (HA) persistent source could be implemented on top
//! of the new persist API, along with a way to do compare-and-set updates of a collection of
//! timestamp bindings. In our case, what we need to make the source HA is that multiple instances
//! can run concurrently and produce identical data. We achieve this by making the minting of new
//! timestamp bindings "go through" consensus. In our case, some system that supports a
//! compare-and-set operation.
//!
//! The example consists of five parts, an [`api`] module, a [`types`] module, an [`impls`] module,
//! a [`render`] module, and a [`reader`] module. The `api` module has traits for defining sources
//! and a timestamper, `impls` has example implementations of these traits, and [`render`] can
//! spawn (or "render") the machinery to read from a source and write it into persistence. Module
//! [`reader`] has code for a persist consumer, it reads and prints the data that the source
//! pipeline writes to persistence.

// TODO(aljoscha): Figure out which of the Send + Sync shenanigans are needed and/or if the
// situation can be improved.

use std::sync::Arc;
use std::time::Duration;

use timely::progress::Antichain;
use tokio::sync::Mutex;

use mz_build_info::DUMMY_BUILD_INFO;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::internal::trace::MergeState;
use mz_persist_client::internal::trace::MergeVariant;
use mz_persist_client::{PersistConfig, PersistLocation, ShardId};

#[tokio::main]
async fn main() {
    let num_rounds = 10;
    let num_keys = 1_000;
    let num_keys_last_round = 1_000;

    let persistcfg = PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());

    let persist_cache = Arc::new(Mutex::new(PersistClientCache::new(
        persistcfg,
        &MetricsRegistry::new(),
    )));

    // Also manually assert the since of the remap shard.
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

    // Immediately allow compaction to "the max", so we don't block compactions.
    read.downgrade_since(&Antichain::from_elem(num_rounds + 1))
        .await;

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
        let retractions =
            (0..num_retractions).map(|key| ((key.to_string(), (round - 1).to_string()), round, -1));

        // Add updates for new round.
        let additions =
            (0..num_additions).map(|key| ((key.to_string(), (round).to_string()), round, 1));

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

        // let persist do some work if it has to
        tokio::time::sleep(Duration::from_millis(100)).await;

        let spine = &write.machine.state.collections.trace.spine;

        println!(
            "spine.upper: {:?}, spine.since: {:?}",
            spine.upper, spine.since
        );

        let mut num_batches = 0;
        let mut num_entries = 0;

        for batch in spine.merging.iter().rev() {
            match batch {
                MergeState::Double(MergeVariant::InProgress(batch1, batch2, _)) => {
                    println!("double-batch(in-progress)!");
                    num_batches += 2;
                    num_entries += batch1.len() + batch2.len();
                    println!("batch {:?}, len: {} ", batch1.desc(), batch1.len());
                    println!("batch {:?}, len: {} ", batch2.desc(), batch2.len());
                }
                MergeState::Double(MergeVariant::Complete(Some((batch, _)))) => {
                    println!("double-batch(merged)!");
                    num_batches += 1;
                    num_entries += batch.len();
                    println!("batch {:?}, len: {} ", batch.desc(), batch.len());
                }
                MergeState::Single(Some(batch)) => {
                    println!("single-batch!");
                    num_batches += 1;
                    num_entries += batch.len();
                    println!("batch {:?}, len: {} ", batch.desc(), batch.len());
                }
                _ => {}
            }
        }

        println!("num_batches: {num_batches}, num_entries: {num_entries}");
        // println!("consolidated updates.len: {}", updates.len());

        println!("---\n");
    }
}
