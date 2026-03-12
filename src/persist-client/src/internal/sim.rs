// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deterministic Simulation Testing (DST) for Persist.
//!
//! Uses [turmoil] to run multiple persist clients on separate simulated hosts,
//! each communicating with shared consensus/blob servers. Turmoil interleaves
//! their async operations, exposing concurrent-access bugs at the CaS level.

mod checker;
mod harness;
mod workload;

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::harness::{SimConfig, run_simulation};

    #[test]
    fn persist_dst() {
        let seed: u64 = std::env::var("SEED")
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or_else(rand::random);
        info!("DST seed: {seed}");
        run_simulation(seed, SimConfig::default());
    }

    #[test]
    fn persist_dst_many_writers() {
        let seed: u64 = std::env::var("SEED")
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or_else(rand::random);
        info!("DST seed: {seed}");
        run_simulation(
            seed,
            SimConfig {
                num_writers: 5,
                num_readers: 2,
                writes_per_client: 20,
                ..SimConfig::default()
            },
        );
    }

    #[test]
    fn persist_dst_with_faults() {
        let seed: u64 = std::env::var("SEED")
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or_else(rand::random);
        info!("DST seed: {seed}");
        run_simulation(
            seed,
            SimConfig {
                num_writers: 3,
                num_readers: 2,
                writes_per_client: 15,
                inject_faults: true,
                ..SimConfig::default()
            },
        );
    }

    #[test]
    #[ignore = "runs forever, for fuzzing"]
    fn fuzz_persist_dst() {
        loop {
            let seed: u64 = rand::random();
            info!("DST seed: {seed}");
            run_simulation(seed, SimConfig::default());
        }
    }
}
