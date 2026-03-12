// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Simulation harness: sets up turmoil with consensus/blob servers and
//! orchestrates client hosts running writer/reader workloads.

use std::sync::Arc;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Mutex;
use tracing::info;

use mz_persist::turmoil::{BlobState, ConsensusState, serve_blob, serve_consensus};

use crate::cache::PersistClientCache;
use crate::{Diagnostics, PersistLocation, ShardId};

use super::checker::{LivenessConfig, check_invariants};
use super::workload::{OperationLog, run_reader, run_writer};

/// Configuration for a simulation run.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Number of writer clients.
    pub num_writers: usize,
    /// Number of reader-only clients.
    pub num_readers: usize,
    /// Number of writes each writer client attempts.
    pub writes_per_client: usize,
    /// Number of reads each reader client attempts.
    pub reads_per_client: usize,
    /// Whether to inject network faults.
    pub inject_faults: bool,
    /// Maximum simulation duration.
    pub max_duration: Duration,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            num_writers: 3,
            num_readers: 1,
            writes_per_client: 10,
            reads_per_client: 5,
            inject_faults: false,
            max_duration: Duration::from_secs(120),
        }
    }
}

/// Initialize turmoil consensus and blob server hosts.
fn init_persist(sim: &mut turmoil::Sim) -> PersistLocation {
    sim.host("consensus", {
        let state = ConsensusState::new();
        move || serve_consensus(7000, state.clone())
    });
    sim.host("blob", {
        let state = BlobState::new();
        move || serve_blob(7000, state.clone())
    });

    PersistLocation {
        blob_uri: "turmoil://blob:7000".parse().unwrap(),
        consensus_uri: "turmoil://consensus:7000".parse().unwrap(),
    }
}

/// Run a full simulation with the given seed and configuration.
///
/// This is the main entry point for DST tests.
pub fn run_simulation(seed: u64, config: SimConfig) {
    configure_tracing();

    let mut rng = SmallRng::seed_from_u64(seed);
    let turmoil_seed: u64 = rng.random();

    info!(
        "DST simulation: seed={seed}, turmoil_seed={turmoil_seed}, \
         writers={}, readers={}, writes_per={}, reads_per={}, faults={}",
        config.num_writers,
        config.num_readers,
        config.writes_per_client,
        config.reads_per_client,
        config.inject_faults,
    );

    let mut sim = turmoil::Builder::new()
        .simulation_duration(config.max_duration)
        .enable_random_order()
        .rng_seed(turmoil_seed)
        .build();

    let persist_location = init_persist(&mut sim);
    let shard_id = ShardId::new();

    // Shared operation log — used for post-hoc invariant checking.
    let log = Arc::new(Mutex::new(OperationLog::default()));

    // Register writer clients. These use `sim.client()` (not `sim.host()`)
    // because turmoil's `run()` only tracks `client` tasks for determining
    // when the simulation is complete.
    for i in 0..config.num_writers {
        let location = persist_location.clone();
        let log = Arc::clone(&log);
        let writes = config.writes_per_client;

        sim.client(format!("writer-{i}"), async move {
            let persist_cache = PersistClientCache::new_for_turmoil();
            let client = persist_cache.open(location).await.unwrap();

            let (write, _read) = client
                .open::<String, String, u64, i64>(
                    shard_id,
                    Arc::new(Default::default()),
                    Arc::new(Default::default()),
                    Diagnostics::from_purpose(&format!("dst-writer-{i}")),
                    true,
                )
                .await
                .expect("open shard for writer");

            run_writer(i, write, writes, log).await;
            Ok(())
        });
    }

    // Register reader clients.
    for i in 0..config.num_readers {
        let location = persist_location.clone();
        let log = Arc::clone(&log);
        let reads = config.reads_per_client;

        sim.client(format!("reader-{i}"), async move {
            let persist_cache = PersistClientCache::new_for_turmoil();
            let client = persist_cache.open(location).await.unwrap();

            run_reader(i, client, shard_id, log, reads).await;
            Ok(())
        });
    }

    // Optionally register a fault-injector client that partitions and repairs
    // network connections mid-simulation.
    if config.inject_faults {
        let partition_seed: u64 = rng.random();
        let num_writers = config.num_writers;

        sim.client("fault-injector", async move {
            let target = format!("writer-{}", usize::try_from(partition_seed).unwrap_or(0) % num_writers);
            info!("fault-injector: waiting before partitioning {target} from consensus");

            // Wait for some simulated time to let writers get started.
            tokio::time::sleep(Duration::from_secs(2)).await;

            info!("fault-injector: partitioning {target} <-> consensus");
            turmoil::partition(target.as_str(), "consensus");

            // Keep the partition active for a while.
            tokio::time::sleep(Duration::from_secs(3)).await;

            info!("fault-injector: repairing {target} <-> consensus");
            turmoil::repair(target.as_str(), "consensus");

            Ok(())
        });
    }

    // Run the simulation until all clients complete.
    let result = sim.run();

    // Turmoil returns Err if a client returns Err or the duration is exceeded.
    match &result {
        Ok(()) => info!("DST simulation completed successfully"),
        Err(e) => info!("DST simulation ended: {e}"),
    }

    // Extract the log and run invariant checks.
    // Since the sim is done, no tasks are running, so we can safely extract.
    let log = Arc::try_unwrap(log)
        .expect("log still has other references after simulation")
        .into_inner();

    let liveness = LivenessConfig {
        min_writes: if config.inject_faults {
            // Under faults, some writers may not complete all their writes.
            1
        } else {
            config.num_writers
        },
        min_reads: if config.num_readers > 0 { 1 } else { 0 },
    };

    check_invariants(&log, &liveness);
    info!(
        "DST invariant checks passed: {} writes, {} reads",
        log.writes.len(),
        log.reads.len()
    );
}

/// Configure tracing for turmoil simulation tests.
fn configure_tracing() {
    use std::sync::Once;

    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));

        let fmt_layer = fmt::layer()
            .with_test_writer()
            .with_target(false);

        let subscriber = tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer);

        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}
