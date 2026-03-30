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
use std::sync::Arc;

use clap::Parser;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::{info, warn};

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLogServer;
use mz_persist_client::ShardId;
use mz_persist_shared_log::metrics::{AcceptorMetrics, LearnerMetrics};
use mz_persist_shared_log::persist_log::acceptor::PersistAcceptor;
use mz_persist_shared_log::persist_log::learner::{PersistLearner, PersistLearnerConfig};
use mz_persist_shared_log::persist_log::metashard::{MetashardState, PersistMetashardActor};
use mz_persist_shared_log::sharded_service::{RoutingState, ShardedService};
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

    /// Shard ID for the metashard persist shard (durable state). If provided,
    /// the metashard actor persists its partition map and reconfiguration
    /// intents to this shard, enabling crash recovery.
    #[arg(long, env = "METASHARD_ID")]
    metashard_id: Option<String>,
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

    // --- Step 1: Create metashard (source of truth) ---
    let metashard_shard_id = match &args.metashard_id {
        Some(id) => id.parse().expect("invalid --metashard-id"),
        None => {
            let id = ShardId::new();
            info!(%id, "generated metashard shard ID");
            id
        }
    };

    // Build a bootstrap partition map from CLI args. PersistMetashardActor::new
    // will override this if it recovers a committed map from durable state.
    let num_shards = args.num_log_shards;
    let mut bootstrap_shard_ids: Vec<ShardId> = Vec::with_capacity(num_shards);
    if let Some(id) = &args.shard_id {
        bootstrap_shard_ids.push(id.parse().expect("invalid --shard-id"));
    }
    while bootstrap_shard_ids.len() < num_shards {
        bootstrap_shard_ids.push(ShardId::new());
    }
    let bootstrap_map = build_partition_map(&bootstrap_shard_ids);
    let bootstrap_state = if num_shards == 1 {
        MetashardState::single(bootstrap_shard_ids[0])
    } else {
        MetashardState {
            epoch: 0,
            partition_map: bootstrap_map,
            log_shards: BTreeMap::new(),
            pending_intent: None,
        }
    };

    // The routing handle starts empty — we'll populate it after spawning
    // log shard actors. The metashard actor needs it for reconfiguration
    // but doesn't use it during construction.
    let empty_routing = Arc::new(RwLock::new(RoutingState::empty()));

    let (metashard_actor, metashard_handle) = PersistMetashardActor::new(
        bootstrap_state,
        256,
        persist_client.clone(),
        metrics_registry.clone(),
        Arc::clone(&empty_routing),
        metashard_shard_id,
    )
    .await;

    // --- Step 2: Read the (possibly recovered) partition map ---
    let partition_map = metashard_actor.state().partition_map.clone();
    let epoch = metashard_actor.state().epoch;
    info!(
        epoch,
        num_ranges = partition_map.ranges.len(),
        "active partition map: {:?}",
        partition_map.ranges.iter().map(|r| format!(
            "[0x{:02x}, 0x{:03x}) -> {}",
            r.lo, r.hi_exclusive, r.log_shard
        )).collect::<Vec<_>>()
    );

    // --- Step 3: Spawn acceptors first ---
    // Acceptors must be spawned before learners because they write setup batches
    // (batch_id=1 bulk snapshot, batch_id=2 delta snapshot) that advance upper
    // past T=0. The learner's subscribe blocks until upper > 0.
    let mut acceptor_handles = BTreeMap::new();

    for range in &partition_map.ranges {
        let shard_id = range.log_shard;
        let shard_registry = MetricsRegistry::new();
        let acceptor_metrics = AcceptorMetrics::register(&shard_registry);

        let (acceptor_handle, _) = PersistAcceptor::spawn(
            AcceptorConfig::default(),
            &persist_client,
            shard_id,
            acceptor_metrics,
            epoch,
            None, // Retraction source wired after learners are spawned.
            vec![],
            range.clone(),
        )
        .await;

        info!(
            %shard_id,
            range = %format!("[0x{:02x}, 0x{:03x})", range.lo, range.hi_exclusive),
            "acceptor ready"
        );
        acceptor_handles.insert(shard_id, acceptor_handle);
    }

    // --- Step 3.5: Spawn learners, then wire retraction sources ---
    let mut learner_handles = BTreeMap::new();

    for (i, range) in partition_map.ranges.iter().enumerate() {
        let shard_id = range.log_shard;
        let shard_registry = MetricsRegistry::new();
        let learner_metrics = LearnerMetrics::register(&shard_registry);

        let (learner_handle, _) = PersistLearner::spawn(
            PersistLearnerConfig::default(),
            &persist_client,
            shard_id,
            learner_metrics,
        )
        .await;

        info!(
            %shard_id,
            range = %format!("[0x{:02x}, 0x{:03x})", range.lo, range.hi_exclusive),
            index = i,
            "learner ready"
        );
        learner_handles.insert(shard_id, learner_handle);
    }

    // Wire retraction sources: learner handles feed retractions to acceptors.
    for range in &partition_map.ranges {
        let shard_id = range.log_shard;
        if let Some(acceptor) = acceptor_handles.get(&shard_id) {
            if let Some(learner) = learner_handles.get(&shard_id) {
                let source: Box<dyn mz_persist_shared_log::RetractionSource> =
                    Box::new(mz_persist_shared_log::sharded_service::ShardedRetractionSource::new(
                        vec![learner.clone()],
                    ));
                if let Err(e) = acceptor.set_retraction_source(source).await {
                    warn!(%shard_id, "failed to set retraction source: {}", e);
                }
            }
        }
    }

    info!(num_shards = partition_map.ranges.len(), "all log shards ready");

    // --- Step 4: Populate shared routing and build ShardedService ---
    // The metashard actor already holds a clone of `empty_routing`. Populate it
    // with the real acceptor/learner handles and then create the service from
    // the *same* Arc so that reconfiguration swaps are visible to the service.
    {
        let mut routing = empty_routing.write().await;
        *routing = RoutingState::new(
            partition_map.clone(),
            acceptor_handles,
            learner_handles,
        );
    }
    let service = ShardedService::from_routing(empty_routing);

    // --- Step 5: Start metashard actor + gRPC server ---
    let _metashard_task = mz_ore::task::spawn(|| "persist-metashard", metashard_actor.run());
    let service = service.with_metashard(metashard_handle);

    info!(addr = %args.listen_addr, "starting gRPC server");
    Server::builder()
        .add_service(PersistSharedLogServer::new(service))
        .serve(args.listen_addr)
        .await
        .expect("gRPC server failed");
}
