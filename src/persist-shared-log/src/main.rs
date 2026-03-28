// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Binary entry point for the persist shared log service.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use clap::Parser;
use tonic::transport::Server;
use tracing::info;

use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLogServer;
use mz_persist_client::ShardId;
use mz_persist_shared_log::metrics::{AcceptorMetrics, LearnerMetrics};
use mz_persist_shared_log::persist_log::acceptor::PersistAcceptor;
use mz_persist_shared_log::persist_log::learner::{PersistLearner, PersistLearnerConfig};
use mz_persist_shared_log::persist_log::metashard::{MetashardState, PersistMetashardActor};
use mz_persist_shared_log::sharded_service::ShardedService;
use mz_persist_shared_log::{AcceptorConfig, PartitionMap, RangeAssignment};

/// CLI arguments for the persist shared log service.
#[derive(Parser, Debug)]
#[command(name = "mz-persist-shared-log")]
struct Args {
    /// Address to listen on for gRPC connections.
    #[arg(long, default_value = "0.0.0.0:6890")]
    listen_addr: SocketAddr,

    /// Address to listen on for the HTTP metrics endpoint (/metrics).
    #[arg(long, default_value = "0.0.0.0:6891")]
    metrics_listen_addr: SocketAddr,

    /// Blob storage URL for persist backend (e.g. file:///tmp/persist/blob).
    /// If omitted, uses in-memory storage.
    #[arg(long, env = "PERSIST_BLOB_URL")]
    blob_url: Option<String>,

    /// Consensus storage URL for persist backend (e.g. postgres://root@localhost:26257/consensus).
    /// If omitted, uses in-memory storage.
    #[arg(long, env = "PERSIST_CONSENSUS_URL")]
    consensus_url: Option<String>,

    /// Shard ID for the first log shard. If omitted, new shards are created.
    /// When using multiple log shards, subsequent shard IDs are auto-generated.
    #[arg(long, env = "PERSIST_SHARD_ID")]
    shard_id: Option<String>,

    /// Number of log shards to create. Each log shard gets its own acceptor and
    /// learner, with the key space range-partitioned across them.
    #[arg(long, default_value = "1")]
    num_log_shards: usize,
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(run(args));
}

/// Spawn the HTTP metrics server on a background task.
fn spawn_metrics_server(
    metrics_addr: SocketAddr,
    metrics_registry: mz_ore::metrics::MetricsRegistry,
) {
    mz_ore::task::spawn(|| "metrics-server", async move {
        let app = axum::Router::new().route(
            "/metrics",
            axum::routing::get(move || {
                let reg = metrics_registry.clone();
                async move { mz_http_util::handle_prometheus(&reg).await }
            }),
        );
        let listener = tokio::net::TcpListener::bind(metrics_addr)
            .await
            .expect("failed to bind metrics listener");
        info!(addr = %metrics_addr, "starting metrics HTTP server");
        axum::serve(listener, app)
            .await
            .expect("metrics server failed");
    });
}

/// Build a partition map that evenly divides [0x00, 0x100) across `n` shards.
fn build_partition_map(shard_ids: &[ShardId]) -> PartitionMap {
    let n = shard_ids.len();
    assert!(n > 0, "need at least one log shard");
    let range_size = 256 / n;
    let mut ranges = Vec::with_capacity(n);
    for (i, shard_id) in shard_ids.iter().enumerate() {
        let lo = u8::try_from(i * range_size).expect("range start fits u8");
        let hi_exclusive = if i == n - 1 {
            0x100u16
        } else {
            u16::try_from((i + 1) * range_size).expect("range end fits u16")
        };
        ranges.push(RangeAssignment {
            lo,
            hi_exclusive,
            log_shard: *shard_id,
        });
    }
    let map = PartitionMap { epoch: 0, ranges };
    map.validate()
        .expect("generated partition map must be valid");
    map
}

async fn run(args: Args) {
    let metrics_registry = mz_ore::metrics::MetricsRegistry::new();

    spawn_metrics_server(args.metrics_listen_addr, metrics_registry.clone());

    let persist_client = match (&args.blob_url, &args.consensus_url) {
        (Some(blob_url), Some(consensus_url)) => {
            info!(%blob_url, %consensus_url, "creating persist client with external storage");
            let persist_config = mz_persist_client::cfg::PersistConfig::new_default_configs(
                &mz_build_info::DUMMY_BUILD_INFO,
                mz_ore::now::SYSTEM_TIME.clone(),
            );
            let cache = mz_persist_client::cache::PersistClientCache::new(
                persist_config,
                &metrics_registry,
                |_, _| mz_persist_client::rpc::PubSubClientConnection::noop(),
            );
            let location = mz_persist_types::PersistLocation {
                blob_uri: blob_url.parse().expect("invalid --blob-url"),
                consensus_uri: consensus_url.parse().expect("invalid --consensus-url"),
            };
            cache
                .open(location)
                .await
                .expect("failed to open persist client")
        }
        (None, None) => {
            info!("creating in-memory persist client (non-durable)");
            mz_persist_client::PersistClient::new_for_tests().await
        }
        _ => {
            panic!("--blob-url and --consensus-url must both be provided, or both omitted");
        }
    };

    // Generate shard IDs for each log shard.
    let num_shards = args.num_log_shards;
    let mut shard_ids: Vec<ShardId> = Vec::with_capacity(num_shards);
    if let Some(id) = &args.shard_id {
        shard_ids.push(id.parse().expect("invalid --shard-id"));
    }
    while shard_ids.len() < num_shards {
        let id = ShardId::new();
        info!(%id, index = shard_ids.len(), "generated log shard ID");
        shard_ids.push(id);
    }

    let partition_map = build_partition_map(&shard_ids);
    info!(
        num_shards = num_shards,
        "partition map: {:?}",
        partition_map.ranges.iter().map(|r| format!(
            "[0x{:02x}, 0x{:03x}) -> {}",
            r.lo, r.hi_exclusive, r.log_shard
        )).collect::<Vec<_>>()
    );

    // Spawn acceptor + learner per log shard.
    let mut acceptor_handles = BTreeMap::new();
    let mut learner_handles = BTreeMap::new();

    for (i, &shard_id) in shard_ids.iter().enumerate() {
        let acceptor_metrics = AcceptorMetrics::register(&metrics_registry);
        let learner_metrics = LearnerMetrics::register(&metrics_registry);

        let acceptor_config = AcceptorConfig::default();
        let (acceptor_handle, _acceptor_task) = PersistAcceptor::spawn(
            acceptor_config,
            &persist_client,
            shard_id,
            acceptor_metrics,
            0,
        )
        .await;

        let learner_config = PersistLearnerConfig::default();
        let (learner_handle, _learner_task) = PersistLearner::spawn(
            learner_config,
            &persist_client,
            shard_id,
            learner_metrics,
        )
        .await;

        info!(%shard_id, index = i, "log shard ready");
        acceptor_handles.insert(shard_id, acceptor_handle);
        learner_handles.insert(shard_id, learner_handle);
    }

    info!(num_shards = num_shards, "all log shards ready");

    // Create ShardedService and get a handle to its routing state.
    let service = ShardedService::new(partition_map.clone(), acceptor_handles, learner_handles);
    let routing_handle = service.routing_handle();

    // Spawn metashard actor with access to the routing state for reconfiguration.
    let metashard_state = if num_shards == 1 {
        MetashardState::single(shard_ids[0])
    } else {
        MetashardState {
            epoch: 0,
            partition_map: partition_map.clone(),
            log_shards: BTreeMap::new(),
        }
    };
    let (_metashard_handle, _metashard_task) = PersistMetashardActor::spawn(
        metashard_state,
        256,
        persist_client,
        metrics_registry,
        routing_handle,
    );

    info!(addr = %args.listen_addr, "starting gRPC server");
    Server::builder()
        .add_service(PersistSharedLogServer::new(service))
        .serve(args.listen_addr)
        .await
        .expect("gRPC server failed");
}
