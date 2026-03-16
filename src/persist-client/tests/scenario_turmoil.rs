#![cfg(feature = "turmoil")]

mod scenario;

use mz_persist::turmoil::{BlobState, ConsensusState, serve_blob, serve_consensus};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::PersistLocation;
use scenario::{ScenarioFault, ScenarioRunner, end_to_end_smoke_ops};

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
        blob_uri: "turmoil://blob:7000".parse().expect("valid blob uri"),
        consensus_uri: "turmoil://consensus:7000"
            .parse()
            .expect("valid consensus uri"),
    }
}

fn apply_fault(sim: &mut turmoil::Sim, fault: ScenarioFault) {
    match fault {
        ScenarioFault::PartitionStorage => {
            sim.partition("client", "blob");
            sim.partition("client", "consensus");
        }
        ScenarioFault::RepairStorage => {
            sim.repair("client", "blob");
            sim.repair("client", "consensus");
        }
    }
}

#[test]
fn turmoil_runner_matches_oracle() {
    let mut sim = turmoil::Builder::new()
        .enable_random_order()
        .rng_seed(0x5EED_u64)
        .build();
    let location = init_persist(&mut sim);
    let ops = end_to_end_smoke_ops();

    sim.client("client", async move {
        let mut cache = PersistClientCache::new_for_turmoil();
        cache.cfg.compaction_enabled = true;
        let client = cache
            .open(location)
            .await
            .expect("turmoil persist client should open");
        let mut runner = ScenarioRunner::from_client(client);
        runner
            .run_and_assert(ops)
            .await
            .expect("turmoil scenario should match oracle");
        Ok(())
    });

    sim.run().expect("turmoil simulation should succeed");
}

#[test]
fn turmoil_runner_recovers_from_storage_partition_during_open() {
    let mut sim = turmoil::Builder::new()
        .enable_random_order()
        .rng_seed(0x5EED_u64 + 1)
        .build();
    let location = init_persist(&mut sim);
    let ops = end_to_end_smoke_ops();

    sim.client("client", async move {
        let mut cache = PersistClientCache::new_for_turmoil();
        cache.cfg.compaction_enabled = true;
        let client = cache
            .open(location)
            .await
            .expect("turmoil persist client should eventually open after repair");
        let mut runner = ScenarioRunner::from_client(client);
        runner
            .run_and_assert(ops)
            .await
            .expect("repaired turmoil scenario should match oracle");
        Ok(())
    });

    apply_fault(&mut sim, ScenarioFault::PartitionStorage);
    for _ in 0..3 {
        sim.step().expect("partitioned open should keep the simulation running");
    }
    apply_fault(&mut sim, ScenarioFault::RepairStorage);

    sim.run().expect("turmoil simulation should recover after repair");
}
