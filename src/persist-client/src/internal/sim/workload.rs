// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Workload generation for DST. Each client runs a loop of writes and reads
//! against a shared shard, recording all operations for post-hoc checking.

use std::sync::Arc;

use timely::progress::Antichain;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::read::ReadHandle;
use crate::write::WriteHandle;
use crate::{Diagnostics, PersistClient, ShardId};

/// A record of a successfully committed write.
#[derive(Debug, Clone)]
pub struct CommittedWrite {
    /// Which client performed the write.
    pub client_id: usize,
    /// The key that was written.
    pub key: String,
    /// The value that was written.
    pub val: String,
    /// The timestamp of the write.
    pub write_ts: u64,
}

/// A record of a completed read (snapshot).
#[derive(Debug, Clone)]
pub struct CompletedRead {
    /// Which client performed the read.
    pub client_id: usize,
    /// The `as_of` timestamp of the read.
    pub as_of: u64,
    /// The data returned by the read, consolidated.
    pub data: Vec<(String, String, u64, i64)>,
}

/// Shared log of all operations across all clients, protected by a mutex.
#[derive(Debug, Default)]
pub struct OperationLog {
    pub writes: Vec<CommittedWrite>,
    pub reads: Vec<CompletedRead>,
}

/// Run the writer workload for a single client.
///
/// The writer fetches the current upper, generates a write at that timestamp,
/// and attempts `compare_and_append`. On `UpperMismatch`, it retries with the
/// new upper. Records all successful writes to the shared operation log.
pub async fn run_writer(
    client_id: usize,
    mut write: WriteHandle<String, String, u64, i64>,
    num_writes: usize,
    log: Arc<Mutex<OperationLog>>,
) {
    let mut successful_writes = 0;
    let mut attempt = 0;

    while successful_writes < num_writes {
        // Fetch the most recent upper to know where we can write.
        let upper = write.fetch_recent_upper().await.clone();
        let write_ts = match upper.as_option() {
            Some(ts) => *ts,
            None => {
                // Shard is closed (upper is the empty antichain). Nothing more to do.
                warn!(
                    "client-{client_id}: shard upper is empty antichain, stopping writer"
                );
                break;
            }
        };

        let key = format!("k{client_id}");
        let val = format!("v{client_id}-{successful_writes}");
        let expected_upper = Antichain::from_elem(write_ts);
        let new_upper = Antichain::from_elem(write_ts + 1);

        debug!(
            "client-{client_id}: attempting write at ts={write_ts}, attempt={attempt}"
        );

        let updates = vec![((key.clone(), val.clone()), write_ts, 1i64)];
        let result = write
            .compare_and_append(updates, expected_upper, new_upper)
            .await;

        match result {
            Ok(Ok(())) => {
                debug!("client-{client_id}: committed write at ts={write_ts}");
                log.lock().await.writes.push(CommittedWrite {
                    client_id,
                    key,
                    val,
                    write_ts,
                });
                successful_writes += 1;
                attempt = 0;
            }
            Ok(Err(mismatch)) => {
                debug!(
                    "client-{client_id}: UpperMismatch at ts={write_ts}, current={:?}",
                    mismatch.current
                );
                attempt += 1;
                // Retry with the new upper — the loop will re-fetch.
            }
            Err(invalid) => {
                panic!("client-{client_id}: InvalidUsage during compare_and_append: {invalid}");
            }
        }
    }

    debug!("client-{client_id}: writer done, {successful_writes} writes committed");
}

/// Run the reader workload for a single client.
///
/// The reader periodically snapshots the shard at recent timestamps and records
/// the results for post-hoc checking. It also does a listen-based read to cross-check.
pub async fn run_reader(
    client_id: usize,
    persist_client: PersistClient,
    shard_id: ShardId,
    log: Arc<Mutex<OperationLog>>,
    num_reads: usize,
) {
    let mut read: ReadHandle<String, String, u64, i64> = persist_client
        .open_leased_reader(
            shard_id,
            Arc::new(Default::default()),
            Arc::new(Default::default()),
            Diagnostics::from_purpose(&format!("dst-reader-{client_id}")),
            true,
        )
        .await
        .expect("open_leased_reader");

    let mut successful_reads = 0;

    while successful_reads < num_reads {
        // Read the current since to know what timestamps are still readable.
        let since = read.since().clone();
        let since_ts = match since.as_option() {
            Some(ts) => *ts,
            None => {
                debug!("client-{client_id}: since is empty antichain, stopping reader");
                break;
            }
        };

        // Try to read at the since timestamp.
        let as_of = Antichain::from_elem(since_ts);
        debug!("client-{client_id}: snapshot_and_fetch at as_of={since_ts}");

        match read.snapshot_and_fetch(as_of).await {
            Ok(data) => {
                let consolidated: Vec<(String, String, u64, i64)> = data
                    .into_iter()
                    .map(|((k, v), t, d)| (k, v, t, d))
                    .collect();

                debug!(
                    "client-{client_id}: read returned {} updates at as_of={since_ts}",
                    consolidated.len()
                );

                log.lock().await.reads.push(CompletedRead {
                    client_id,
                    as_of: since_ts,
                    data: consolidated,
                });
                successful_reads += 1;
            }
            Err(since_err) => {
                debug!(
                    "client-{client_id}: read at as_of={since_ts} failed with Since({:?}), retrying",
                    since_err
                );
                // The since advanced past our as_of. Yield and retry.
                tokio::task::yield_now().await;
            }
        }

        // Yield to allow other tasks to run (important for turmoil interleaving).
        tokio::task::yield_now().await;
    }

    debug!("client-{client_id}: reader done, {successful_reads} reads completed");
}
