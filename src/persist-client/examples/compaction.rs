// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TODO

use std::time::Duration;

pub mod upsert;

/// TODO
#[derive(Debug, Clone, clap::Parser)]
pub struct Args {
    #[clap(subcommand)]
    workload: Workload,
}

/// Write workloads
#[derive(Debug, Clone, clap::Subcommand)]
pub(crate) enum Workload {
    /// Each write containing -1 and +1 diffs
    Upsert(UpsertArgs),
}

/// Arguments for viewing the current state of a given shard
#[derive(Debug, Clone, clap::Parser)]
pub struct UpsertArgs {
    /// number of writes to perform
    #[clap(long, default_value = "1000")]
    num_rounds: u64,

    /// time to wait between writes
    #[clap(long, parse(try_from_str = humantime::parse_duration), default_value = "0ms")]
    write_delay_ms: Duration,

    /// # of keys to update each round
    #[clap(long, default_value = "10")]
    num_keys: usize,

    /// byte size of each value
    #[clap(long, default_value = "1")]
    value_size: usize,

    /// # of rounds to lag behind `since`
    #[clap(long, default_value = "0")]
    since_lag: u64,

    /// target blob size in bytes
    #[clap(long, default_value = "134217728")]
    blob_target_size: usize,

    /// compaction memory bound. must be >= 4x blob_target_size
    #[clap(long, default_value = "1073741824")]
    memory_bound: usize,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    match args.workload {
        Workload::Upsert(upsert) => {
            upsert::run(upsert).await?;
        }
    }
    Ok(())
}
