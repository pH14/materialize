// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Process-based actor factory: spawns acceptors and learners as child OS
//! processes and connects to them over Unix domain sockets.
//!
//! Each child process runs the same `mz-persist-shared-log` binary with a
//! subcommand (`acceptor` or `learner`) and `--run-dir` pointing to the shared
//! socket directory. A supervisor task monitors each child and restarts it on
//! crash.
//!
//! Used by the metashard in distributed mode to automatically bring up actors
//! when the partition map is created or changes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use timely::progress::Antichain;
use tokio::process::Command;
use tracing::{error, info, warn};

use mz_persist_client::ShardId;

use crate::rpc::{GrpcAcceptorHandle, GrpcLearnerHandle};
use crate::uds::connect_uds_with_retry;
use crate::RangeAssignment;

/// Factory that spawns acceptor and learner actors as child OS processes.
///
/// Each actor gets its own supervisor task that restarts the child on crash.
/// Handles are obtained by connecting to the child's Unix domain socket after
/// it starts listening.
pub struct ProcessActorFactory {
    /// Path to the mz-persist-shared-log binary.
    binary: PathBuf,
    /// Shared run directory for Unix sockets.
    run_dir: PathBuf,
    /// Persist blob storage URL, passed to child processes.
    blob_url: String,
    /// Persist consensus storage URL, passed to child processes.
    consensus_url: String,
    /// Timeout for connecting to a newly spawned child's Unix socket.
    connect_timeout: Duration,
    /// Cached acceptor handles (keyed by shard ID).
    acceptors: Mutex<BTreeMap<ShardId, GrpcAcceptorHandle>>,
    /// Cached learner handles (keyed by shard ID).
    learners: Mutex<BTreeMap<ShardId, GrpcLearnerHandle>>,
}

impl ProcessActorFactory {
    pub fn new(
        binary: PathBuf,
        run_dir: PathBuf,
        blob_url: String,
        consensus_url: String,
    ) -> Self {
        ProcessActorFactory {
            binary,
            run_dir,
            blob_url,
            consensus_url,
            connect_timeout: Duration::from_secs(30),
            acceptors: Mutex::new(BTreeMap::new()),
            learners: Mutex::new(BTreeMap::new()),
        }
    }

    /// Socket path for an acceptor's gRPC server.
    fn acceptor_socket(&self, shard_id: ShardId) -> PathBuf {
        self.run_dir
            .join(format!("acceptor-{shard_id}"))
            .join("grpc.sock")
    }

    /// Socket path for a learner's gRPC server (replica 0).
    fn learner_socket(&self, shard_id: ShardId) -> PathBuf {
        self.run_dir
            .join(format!("learner-{shard_id}-0"))
            .join("grpc.sock")
    }

    /// PID file path for a child process.
    fn pid_file(socket_path: &Path) -> PathBuf {
        socket_path.with_file_name("pid")
    }

    /// Spawn a supervisor task that runs a child process in a restart loop.
    fn spawn_supervisor(
        binary: PathBuf,
        args: Vec<String>,
        socket_path: PathBuf,
        label: String,
    ) {
        let pid_file = Self::pid_file(&socket_path);
        let task_name = format!("supervisor-{label}");
        mz_ore::task::spawn(|| task_name, async move {
            loop {
                // Ensure parent directory exists for the socket.
                if let Some(parent) = socket_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }

                info!(%label, ?binary, ?args, "spawning child process");
                let mut child = match Command::new(&binary).args(&args).spawn() {
                    Ok(child) => child,
                    Err(e) => {
                        error!(%label, "failed to spawn child: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };

                // Write PID file for debugging / orphan recovery.
                if let Some(pid) = child.id() {
                    let pid_str: String = pid.to_string();
                    let _ = std::fs::write(&pid_file, pid_str);
                }

                // Wait for the child to exit.
                match child.wait().await {
                    Ok(status) => {
                        warn!(%label, %status, "child process exited, restarting in 1s");
                    }
                    Err(e) => {
                        error!(%label, "error waiting for child: {e}, restarting in 1s");
                    }
                }

                // Clean up stale socket before restart.
                let _ = std::fs::remove_file(&socket_path);
                let _ = std::fs::remove_file(&pid_file);

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

#[async_trait::async_trait]
impl crate::factory::ActorFactory for ProcessActorFactory {
    type A = GrpcAcceptorHandle;
    type L = GrpcLearnerHandle;

    async fn create_acceptor(
        &self,
        shard_id: ShardId,
        epoch: u64,
        _predecessors: Vec<(ShardId, Antichain<u64>)>,
        range: RangeAssignment,
    ) -> Result<GrpcAcceptorHandle, String> {
        // Return cached handle if already running.
        if let Some(handle) = self.acceptors.lock().unwrap().get(&shard_id) {
            return Ok(handle.clone());
        }

        let socket_path = self.acceptor_socket(shard_id);
        let args = vec![
            "acceptor".to_string(),
            "--run-dir".to_string(),
            self.run_dir.to_string_lossy().to_string(),
            "--shard-id".to_string(),
            shard_id.to_string(),
            "--epoch".to_string(),
            epoch.to_string(),
            "--range-lo".to_string(),
            range.lo.to_string(),
            "--range-hi".to_string(),
            range.hi_exclusive.to_string(),
            "--blob-url".to_string(),
            self.blob_url.clone(),
            "--consensus-url".to_string(),
            self.consensus_url.clone(),
            // Bind metrics to a random port to avoid conflicts between children.
            "--metrics-listen-addr".to_string(),
            "0.0.0.0:0".to_string(),
        ];

        Self::spawn_supervisor(
            self.binary.clone(),
            args,
            socket_path.clone(),
            format!("acceptor-{shard_id}"),
        );

        // Wait for the child to start listening on the Unix socket.
        let socket_str = socket_path.to_string_lossy().to_string();
        let channel = connect_uds_with_retry(&socket_str, self.connect_timeout).await?;
        let handle = GrpcAcceptorHandle::from_channel(channel);
        self.acceptors
            .lock()
            .unwrap()
            .insert(shard_id, handle.clone());
        info!(%shard_id, "acceptor process ready");
        Ok(handle)
    }

    async fn create_learner(&self, shard_id: ShardId) -> Result<GrpcLearnerHandle, String> {
        // Return cached handle if already running.
        if let Some(handle) = self.learners.lock().unwrap().get(&shard_id) {
            return Ok(handle.clone());
        }

        let socket_path = self.learner_socket(shard_id);
        let args = vec![
            "learner".to_string(),
            "--run-dir".to_string(),
            self.run_dir.to_string_lossy().to_string(),
            "--shard-id".to_string(),
            shard_id.to_string(),
            "--replica-id".to_string(),
            "0".to_string(),
            "--blob-url".to_string(),
            self.blob_url.clone(),
            "--consensus-url".to_string(),
            self.consensus_url.clone(),
            // Bind metrics to a random port to avoid conflicts between children.
            "--metrics-listen-addr".to_string(),
            "0.0.0.0:0".to_string(),
        ];

        Self::spawn_supervisor(
            self.binary.clone(),
            args,
            socket_path.clone(),
            format!("learner-{shard_id}-0"),
        );

        // Wait for the child to start listening on the Unix socket.
        let socket_str = socket_path.to_string_lossy().to_string();
        let channel = connect_uds_with_retry(&socket_str, self.connect_timeout).await?;
        let handle = GrpcLearnerHandle::from_channel(channel);
        self.learners
            .lock()
            .unwrap()
            .insert(shard_id, handle.clone());
        info!(%shard_id, "learner process ready");
        Ok(handle)
    }
}
