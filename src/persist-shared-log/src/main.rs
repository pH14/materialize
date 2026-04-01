// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Binary entry point for the persist shared log service.
//!
//! Supports three deployment modes:
//!
//! - **monolith** (default): all actors in a single process. The metashard,
//!   acceptors, learners, and router all share one `PersistClient` with
//!   same-process pubsub.
//!
//! - **acceptor**: a standalone acceptor for one log shard. Hosts a persist
//!   pubsub server so that remote learners get instant write notifications.
//!
//! - **learner**: a standalone learner for one log shard. Connects to the
//!   acceptor's pubsub server so that `Subscribe` gets instant notifications.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tonic::transport::Server;
use tracing::{error, info};

use mz_ore::metrics::MetricsRegistry;
use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLogServer;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::{
    GrpcPubSubClient, PersistGrpcPubSubServer, PersistPubSubClient, PersistPubSubClientConfig,
};
use mz_persist_client::{PersistClient, ShardId};
use mz_persist_shared_log::factory::InProcessActorFactory;
use mz_persist_shared_log::persist_log::metashard::{MetashardState, PersistMetashardActor};
use mz_persist_shared_log::rpc::{
    AcceptorGrpcServer, ConsensusAcceptorServer, ConsensusLearnerServer, LearnerGrpcServer,
};
use mz_persist_shared_log::persist_log::router::Router;
use mz_persist_shared_log::{AcceptorConfig, PartitionMap, RangeAssignment};

/// Persist shared log service.
#[derive(Parser, Debug)]
#[command(name = "mz-persist-shared-log")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run all actors in a single process (default).
    Monolith(MonolithArgs),

    /// Run a standalone acceptor for one log shard.
    ///
    /// Hosts a persist pubsub server on a separate port so that learners
    /// in other processes get instant write notifications.
    Acceptor(AcceptorArgs),

    /// Run a standalone learner for one log shard.
    ///
    /// Connects to the acceptor's persist pubsub server so that Subscribe
    /// gets instant notifications instead of polling consensus.
    Learner(LearnerArgs),

    /// Run a standalone metashard.
    ///
    /// Manages the partition map and persists reconfiguration state. Hosts a
    /// persist pubsub server so that routing tasks get instant partition map
    /// update notifications.
    Metashard(MetashardArgs),

    /// Run a standalone router (Router).
    ///
    /// Routes client requests to remote acceptors and learners via gRPC.
    /// Subscribes to the metashard persist shard for partition map updates.
    Router(RouterArgs),
}

// ---------------------------------------------------------------------------
// Shared args helpers
// ---------------------------------------------------------------------------

/// Persist storage backend arguments shared across modes.
#[derive(clap::Args, Debug, Clone)]
struct StorageArgs {
    /// Blob storage URL (e.g. file:///tmp/persist/blob or s3://bucket/prefix).
    #[arg(long, env = "PERSIST_BLOB_URL")]
    blob_url: String,

    /// Consensus storage URL (e.g. postgres://root@localhost:26257/consensus).
    #[arg(long, env = "PERSIST_CONSENSUS_URL")]
    consensus_url: String,
}

/// Delete all state from prior runs: blob directory, consensus schema, run directory.
async fn reset_state(storage: &StorageArgs, run_dir: Option<&std::path::Path>) {
    info!("--reset: clearing all prior state");

    // Delete blob directory (file:///path → /path).
    if let Some(path) = storage.blob_url.strip_prefix("file://") {
        let path = std::path::Path::new(path);
        if path.exists() {
            info!(?path, "deleting blob directory");
            std::fs::remove_dir_all(path).expect("delete blob directory");
        }
    }

    // Drop and recreate the consensus schema in Postgres.
    if storage.consensus_url.starts_with("postgres://") {
        info!("dropping and recreating consensus schema");
        let (client, connection) =
            tokio_postgres::connect(&storage.consensus_url, tokio_postgres::NoTls)
                .await
                .expect("connect to postgres for reset");
        mz_ore::task::spawn(|| "pg-reset-conn", async move {
            if let Err(e) = connection.await {
                error!("postgres connection error during reset: {e}");
            }
        });
        client
            .batch_execute("DROP SCHEMA IF EXISTS consensus CASCADE; CREATE SCHEMA IF NOT EXISTS consensus")
            .await
            .expect("reset consensus schema");
    }

    // Delete run directory (stale sockets, PID files).
    if let Some(run_dir) = run_dir {
        if run_dir.exists() {
            info!(?run_dir, "deleting run directory");
            std::fs::remove_dir_all(run_dir).expect("delete run directory");
        }
    }

    info!("reset complete");
}

/// Create a persist config suitable for the shared log service.
fn new_persist_config() -> PersistConfig {
    PersistConfig::new_default_configs(
        &mz_build_info::DUMMY_BUILD_INFO,
        mz_ore::now::SYSTEM_TIME.clone(),
    )
}

/// Open a `PersistClient` from external storage with same-process pubsub.
///
/// The returned server is consumed — its state lives on through the client's
/// pubsub connection. Use `open_persist_client_with_remote_pubsub` when the
/// pubsub server runs in another process.
async fn open_persist_client_with_local_pubsub(
    storage: &StorageArgs,
    metrics_registry: &MetricsRegistry,
) -> PersistClient {
    let persist_config = new_persist_config();
    persist_config.apply_from(&mz_dyncfg::ConfigUpdates::default());
    let pubsub_server = PersistGrpcPubSubServer::new(&persist_config, metrics_registry);
    let cache = mz_persist_client::cache::PersistClientCache::new(
        persist_config,
        metrics_registry,
        |_cfg, _metrics| pubsub_server.new_same_process_connection(),
    );
    let location = mz_persist_types::PersistLocation {
        blob_uri: storage.blob_url.parse().expect("invalid --blob-url"),
        consensus_uri: storage.consensus_url.parse().expect("invalid --consensus-url"),
    };
    cache
        .open(location)
        .await
        .expect("failed to open persist client")
}

/// Open a `PersistClient` that also hosts a persist pubsub server.
///
/// Returns the client (with same-process pubsub) and the server handle.
/// The caller must spawn `server.serve(addr)` to accept remote connections.
async fn open_persist_client_hosting_pubsub(
    storage: &StorageArgs,
    metrics_registry: &MetricsRegistry,
) -> (PersistClient, PersistGrpcPubSubServer) {
    let persist_config = new_persist_config();
    persist_config.apply_from(&mz_dyncfg::ConfigUpdates::default());
    let pubsub_server = PersistGrpcPubSubServer::new(&persist_config, metrics_registry);
    let same_process_conn = pubsub_server.new_same_process_connection();
    let cache = mz_persist_client::cache::PersistClientCache::new(
        persist_config,
        metrics_registry,
        |_cfg, _metrics| same_process_conn,
    );
    let location = mz_persist_types::PersistLocation {
        blob_uri: storage.blob_url.parse().expect("invalid --blob-url"),
        consensus_uri: storage.consensus_url.parse().expect("invalid --consensus-url"),
    };
    let client = cache
        .open(location)
        .await
        .expect("failed to open persist client");
    (client, pubsub_server)
}

/// Open a `PersistClient` that connects to a remote pubsub server.
async fn open_persist_client_with_remote_pubsub(
    storage: &StorageArgs,
    pubsub_url: &str,
    caller_id: &str,
    metrics_registry: &MetricsRegistry,
) -> PersistClient {
    let persist_config = new_persist_config();
    // Signal that dyncfgs are "synced" so the pubsub client connects
    // immediately. Without this, GrpcPubSubClient blocks on
    // configs_synced_once() waiting for an upstream config source that
    // doesn't exist in standalone mode.
    persist_config.apply_from(&mz_dyncfg::ConfigUpdates::default());
    let pubsub_url = pubsub_url.to_string();
    let caller_id = caller_id.to_string();
    let cache = mz_persist_client::cache::PersistClientCache::new(
        persist_config,
        metrics_registry,
        |cfg, metrics| {
            GrpcPubSubClient::connect(
                PersistPubSubClientConfig {
                    url: pubsub_url,
                    caller_id,
                    persist_cfg: cfg.clone(),
                },
                metrics,
            )
        },
    );
    let location = mz_persist_types::PersistLocation {
        blob_uri: storage.blob_url.parse().expect("invalid --blob-url"),
        consensus_uri: storage.consensus_url.parse().expect("invalid --consensus-url"),
    };
    cache
        .open(location)
        .await
        .expect("failed to open persist client")
}

// ---------------------------------------------------------------------------
// Monolith mode
// ---------------------------------------------------------------------------

/// Arguments for monolith mode (all actors in one process).
#[derive(clap::Args, Debug)]
struct MonolithArgs {
    /// Delete all prior state (blob, consensus, run directory) before starting.
    #[arg(long)]
    reset: bool,

    /// Address to listen on for gRPC connections.
    #[arg(long, default_value = "0.0.0.0:6890")]
    listen_addr: SocketAddr,

    /// Address to listen on for the HTTP metrics endpoint (/metrics).
    #[arg(long, default_value = "0.0.0.0:6891")]
    metrics_listen_addr: SocketAddr,

    /// Blob storage URL for persist backend. If omitted, uses in-memory storage.
    #[arg(long, env = "PERSIST_BLOB_URL")]
    blob_url: Option<String>,

    /// Consensus storage URL for persist backend. If omitted, uses in-memory storage.
    #[arg(long, env = "PERSIST_CONSENSUS_URL")]
    consensus_url: Option<String>,

    /// Shard ID for the first log shard. If omitted, a new shard is created.
    #[arg(long, env = "PERSIST_SHARD_ID")]
    shard_id: Option<String>,

    /// Number of log shards to create.
    #[arg(long, default_value = "1")]
    num_log_shards: usize,

    /// Shard ID for the metashard persist shard (durable state). Required for
    /// crash recovery.
    #[arg(long, env = "METASHARD_ID")]
    metashard_id: String,
}

// ---------------------------------------------------------------------------
// Standalone acceptor mode
// ---------------------------------------------------------------------------

/// Arguments for standalone acceptor mode.
#[derive(clap::Args, Debug)]
struct AcceptorArgs {
    /// Shared run directory for Unix domain sockets.
    #[arg(long, env = "RUN_DIR")]
    run_dir: std::path::PathBuf,

    /// Address to listen on for the HTTP metrics endpoint.
    #[arg(long, default_value = "0.0.0.0:6902")]
    metrics_listen_addr: SocketAddr,

    #[command(flatten)]
    storage: StorageArgs,

    /// Shard ID for the log shard this acceptor manages.
    #[arg(long, env = "PERSIST_SHARD_ID")]
    shard_id: String,

    /// Epoch for this acceptor. Must match the metashard's current epoch.
    #[arg(long, default_value = "0")]
    epoch: u64,

    /// Low byte of the key range (inclusive, hex). E.g., 0x00.
    #[arg(long, default_value = "0")]
    range_lo: u8,

    /// High byte of the key range (exclusive). E.g., 256 for the full range.
    #[arg(long, default_value = "256")]
    range_hi: u16,
}

// ---------------------------------------------------------------------------
// Standalone learner mode
// ---------------------------------------------------------------------------

/// Arguments for standalone learner mode.
#[derive(clap::Args, Debug)]
struct LearnerArgs {
    /// Shared run directory for Unix domain sockets.
    #[arg(long, env = "RUN_DIR")]
    run_dir: std::path::PathBuf,

    /// Address to listen on for the HTTP metrics endpoint.
    #[arg(long, default_value = "0.0.0.0:6911")]
    metrics_listen_addr: SocketAddr,

    #[command(flatten)]
    storage: StorageArgs,

    /// Shard ID for the log shard this learner subscribes to.
    #[arg(long, env = "PERSIST_SHARD_ID")]
    shard_id: String,

    /// Replica ID for this learner instance.
    ///
    /// Multiple learners for the same shard are distinguished by replica ID.
    /// The socket path is `<run_dir>/learner-<shard_id>-<replica_id>/grpc.sock`.
    #[arg(long, default_value = "0")]
    replica_id: u32,
}

// ---------------------------------------------------------------------------
// Standalone metashard mode
// ---------------------------------------------------------------------------

/// Arguments for standalone metashard mode.
#[derive(clap::Args, Debug)]
struct MetashardArgs {
    /// Delete all prior state (blob, consensus, run directory) before starting.
    #[arg(long)]
    reset: bool,

    /// Shared run directory for Unix domain sockets.
    #[arg(long, env = "RUN_DIR")]
    run_dir: std::path::PathBuf,

    /// Address to listen on for the HTTP metrics endpoint.
    #[arg(long, default_value = "0.0.0.0:6921")]
    metrics_listen_addr: SocketAddr,

    #[command(flatten)]
    storage: StorageArgs,

    /// Shard ID for the metashard persist shard (durable state).
    #[arg(long, env = "METASHARD_ID")]
    metashard_id: String,

    /// Shard ID for the initial log shard. If omitted, a new shard is created.
    #[arg(long, env = "PERSIST_SHARD_ID")]
    shard_id: Option<String>,

    /// Number of log shards in the initial partition map.
    #[arg(long, default_value = "1")]
    num_log_shards: usize,
}

// ---------------------------------------------------------------------------
// Standalone router mode
// ---------------------------------------------------------------------------

/// Arguments for standalone router (Router) mode.
#[derive(clap::Args, Debug)]
struct RouterArgs {
    /// Address to listen on for the PersistSharedLog gRPC service (TCP).
    ///
    /// The router is the external-facing entry point — it listens on TCP for
    /// client requests and routes them to actors over Unix sockets.
    #[arg(long, default_value = "0.0.0.0:6890")]
    listen_addr: SocketAddr,

    /// Address to listen on for the HTTP metrics endpoint.
    #[arg(long, default_value = "0.0.0.0:6891")]
    metrics_listen_addr: SocketAddr,

    #[command(flatten)]
    storage: StorageArgs,

    /// Shard ID of the metashard persist shard. The routing task subscribes to
    /// this shard for partition map updates.
    #[arg(long, env = "METASHARD_ID")]
    metashard_id: String,

    /// Shared run directory containing actor Unix sockets.
    ///
    /// The router discovers acceptors and learners via `ProcessDirectory` socket
    /// paths under this directory. Also connects to the metashard's pubsub socket
    /// for instant partition map notifications.
    #[arg(long, env = "RUN_DIR")]
    run_dir: std::path::PathBuf,

    /// Timeout for connecting to remote acceptors and learners.
    #[arg(long, default_value = "30")]
    connect_timeout_secs: u64,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let cli = Cli::parse();

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

    match cli.command {
        Commands::Monolith(args) => rt.block_on(run_monolith(args)),
        Commands::Acceptor(args) => rt.block_on(run_acceptor(args)),
        Commands::Learner(args) => rt.block_on(run_learner(args)),
        Commands::Metashard(args) => rt.block_on(run_metashard(args)),
        Commands::Router(args) => rt.block_on(run_router(args)),
    }
}

/// Spawn the HTTP metrics server on a background task.
fn spawn_metrics_server(metrics_addr: SocketAddr, metrics_registry: MetricsRegistry) {
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

// ===========================================================================
// Monolith mode
// ===========================================================================

async fn run_monolith(args: MonolithArgs) {
    if args.reset {
        if let (Some(blob_url), Some(consensus_url)) = (&args.blob_url, &args.consensus_url) {
            let storage = StorageArgs {
                blob_url: blob_url.clone(),
                consensus_url: consensus_url.clone(),
            };
            reset_state(&storage, None).await;
        }
    }

    let metrics_registry = MetricsRegistry::new();
    spawn_metrics_server(args.metrics_listen_addr, metrics_registry.clone());

    let persist_client = match (&args.blob_url, &args.consensus_url) {
        (Some(blob_url), Some(consensus_url)) => {
            info!(%blob_url, %consensus_url, "creating persist client with external storage");
            let storage = StorageArgs {
                blob_url: blob_url.clone(),
                consensus_url: consensus_url.clone(),
            };
            open_persist_client_with_local_pubsub(&storage, &metrics_registry).await
        }
        (None, None) => {
            info!("creating in-memory persist client (non-durable)");
            PersistClient::new_for_tests().await
        }
        _ => {
            panic!("--blob-url and --consensus-url must both be provided, or both omitted");
        }
    };

    // --- Step 1: Create metashard (source of truth) ---
    let metashard_shard_id: ShardId = args.metashard_id.parse().expect("invalid --metashard-id");

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

    let factory = Arc::new(InProcessActorFactory::new(persist_client.clone()));

    let (metashard_actor, metashard_handle) = PersistMetashardActor::new(
        bootstrap_state,
        256,
        persist_client.clone(),
        Arc::clone(&factory),
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
        partition_map
            .ranges
            .iter()
            .map(|r| format!(
                "[0x{:02x}, 0x{:03x}) -> {}",
                r.lo, r.hi_exclusive, r.log_shard
            ))
            .collect::<Vec<_>>()
    );

    // --- Step 3: Start metashard actor ---
    let _metashard_task = mz_ore::task::spawn(|| "persist-metashard", metashard_actor.run());

    // --- Step 4: Build Router ---
    let router = Router::new(
        PartitionMap {
            epoch: 0,
            ranges: vec![],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    );
    mz_persist_shared_log::persist_log::router::spawn_routing_task(
        &persist_client,
        metashard_shard_id,
        Arc::clone(&factory),
        router.routing_handle(),
        router.routing_notify(),
    )
    .await;
    let router = router.with_metashard(metashard_handle);

    info!(addr = %args.listen_addr, "starting gRPC server");
    Server::builder()
        .add_service(PersistSharedLogServer::new(router))
        .serve(args.listen_addr)
        .await
        .expect("gRPC server failed");
}

// ===========================================================================
// Standalone acceptor mode
// ===========================================================================

async fn run_acceptor(args: AcceptorArgs) {
    let metrics_registry = MetricsRegistry::new();
    spawn_metrics_server(args.metrics_listen_addr, metrics_registry.clone());

    let shard_id: ShardId = args.shard_id.parse().expect("invalid --shard-id");

    // Derive socket paths from ProcessDirectory convention.
    let acceptor_sock = args.run_dir.join(format!("acceptor-{shard_id}")).join("grpc.sock");
    let pubsub_sock = args.run_dir.join(format!("pubsub-{shard_id}")).join("grpc.sock");
    info!(%shard_id, epoch = args.epoch, ?acceptor_sock, ?pubsub_sock, "starting standalone acceptor");

    // Create PersistClient with same-process pubsub and get the server handle
    // so we can also serve pubsub to remote learners.
    let (persist_client, pubsub_server) =
        open_persist_client_hosting_pubsub(&args.storage, &metrics_registry).await;

    // Serve pubsub on a Unix socket for remote learners.
    let pubsub_path = pubsub_sock.clone();
    mz_ore::task::spawn(|| "persist-pubsub-server", async move {
        info!(?pubsub_path, "starting persist pubsub server on Unix socket");
        let uds = {
            if let Some(parent) = pubsub_path.parent() {
                std::fs::create_dir_all(parent).expect("create pubsub socket dir");
            }
            let _ = std::fs::remove_file(&pubsub_path);
            tokio::net::UnixListener::bind(&pubsub_path).expect("bind pubsub socket")
        };
        let stream = tokio_stream::wrappers::UnixListenerStream::new(uds);
        if let Err(e) = pubsub_server.serve_with_incoming(stream).await {
            error!("persist pubsub server exited: {e}");
        }
    });

    let range = RangeAssignment {
        lo: args.range_lo,
        hi_exclusive: args.range_hi,
        log_shard: shard_id,
    };

    let shard_registry = MetricsRegistry::new();
    let acceptor_metrics =
        mz_persist_shared_log::metrics::AcceptorMetrics::register(&shard_registry);

    let (handle, _task) = mz_persist_shared_log::persist_log::acceptor::PersistAcceptor::spawn(
        AcceptorConfig::default(),
        &persist_client,
        shard_id,
        acceptor_metrics,
        args.epoch,
        Box::new(mz_persist_shared_log::NoOpRetractionSource),
        vec![],
        range,
    )
    .await;

    info!(?acceptor_sock, "starting acceptor gRPC server on Unix socket");
    let router = Server::builder().add_service(ConsensusAcceptorServer::new(
        AcceptorGrpcServer::new(handle),
    ));
    mz_persist_shared_log::uds::serve_uds(&acceptor_sock, router)
        .await
        .expect("acceptor gRPC server failed");
}

// ===========================================================================
// Standalone learner mode
// ===========================================================================

async fn run_learner(args: LearnerArgs) {
    let metrics_registry = MetricsRegistry::new();
    spawn_metrics_server(args.metrics_listen_addr, metrics_registry.clone());

    let shard_id: ShardId = args.shard_id.parse().expect("invalid --shard-id");
    let replica_id = args.replica_id;

    // Derive socket paths from ProcessDirectory convention.
    let learner_sock = args
        .run_dir
        .join(format!("learner-{shard_id}-{replica_id}"))
        .join("grpc.sock");
    let pubsub_sock = args.run_dir.join(format!("pubsub-{shard_id}")).join("grpc.sock");
    let pubsub_path = pubsub_sock.to_string_lossy().to_string();
    info!(%shard_id, replica_id, ?learner_sock, pubsub = %pubsub_path, "starting standalone learner");

    // Create PersistClient with remote pubsub to the acceptor's Unix socket.
    let persist_client = open_persist_client_with_remote_pubsub(
        &args.storage,
        &pubsub_path,
        &format!("learner-{shard_id}-{replica_id}"),
        &metrics_registry,
    )
    .await;

    let shard_registry = MetricsRegistry::new();
    let learner_metrics =
        mz_persist_shared_log::metrics::LearnerMetrics::register(&shard_registry);

    let (handle, _task) = mz_persist_shared_log::persist_log::learner::PersistLearner::spawn(
        mz_persist_shared_log::persist_log::learner::PersistLearnerConfig::default(),
        &persist_client,
        shard_id,
        learner_metrics,
    )
    .await;

    info!(?learner_sock, "starting learner gRPC server on Unix socket");
    let router = Server::builder().add_service(ConsensusLearnerServer::new(
        LearnerGrpcServer::new(handle),
    ));
    mz_persist_shared_log::uds::serve_uds(&learner_sock, router)
        .await
        .expect("learner gRPC server failed");
}

// ===========================================================================
// Standalone metashard mode
// ===========================================================================

async fn run_metashard(args: MetashardArgs) {
    use mz_persist_shared_log::rpc::{ConsensusMetashardServer, MetashardGrpcServer};

    if args.reset {
        reset_state(&args.storage, Some(&args.run_dir)).await;
    }

    let metrics_registry = MetricsRegistry::new();
    spawn_metrics_server(args.metrics_listen_addr, metrics_registry.clone());

    let metashard_shard_id: ShardId = args.metashard_id.parse().expect("invalid --metashard-id");
    let metashard_sock = args
        .run_dir
        .join(format!("metashard-{metashard_shard_id}"))
        .join("grpc.sock");
    info!(%metashard_shard_id, ?metashard_sock, "starting standalone metashard");

    // Create PersistClient hosting pubsub — the pubsub server is served on
    // the same Unix socket as the metashard gRPC (via the same tonic Router).
    let (persist_client, pubsub_server) =
        open_persist_client_hosting_pubsub(&args.storage, &metrics_registry).await;

    // Serve pubsub on the metashard socket alongside the metashard gRPC service.
    // We'll use the pubsub server's serve_with_incoming after building a combined
    // tonic router. For now, pubsub goes on the same socket as the metashard gRPC.
    let pubsub_sock = args
        .run_dir
        .join(format!("metashard-{metashard_shard_id}"))
        .join("pubsub.sock");
    let pubsub_path = pubsub_sock.clone();
    mz_ore::task::spawn(|| "metashard-pubsub-server", async move {
        info!(?pubsub_path, "starting metashard pubsub server on Unix socket");
        let uds = {
            if let Some(parent) = pubsub_path.parent() {
                std::fs::create_dir_all(parent).expect("create pubsub socket dir");
            }
            let _ = std::fs::remove_file(&pubsub_path);
            tokio::net::UnixListener::bind(&pubsub_path).expect("bind pubsub socket")
        };
        let stream = tokio_stream::wrappers::UnixListenerStream::new(uds);
        if let Err(e) = pubsub_server.serve_with_incoming(stream).await {
            error!("metashard pubsub server exited: {e}");
        }
    });

    // Build bootstrap partition map.
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

    // Use ProcessActorFactory to spawn acceptors/learners as child processes.
    // The metashard creates actors at startup and during reconfiguration.
    let binary = std::env::current_exe().expect("resolve current binary path");
    let factory = Arc::new(
        mz_persist_shared_log::process_factory::ProcessActorFactory::new(
            binary,
            args.run_dir.clone(),
            args.storage.blob_url.clone(),
            args.storage.consensus_url.clone(),
        ),
    );

    let (metashard_actor, metashard_handle) = PersistMetashardActor::new(
        bootstrap_state,
        256,
        persist_client,
        Arc::clone(&factory),
        metashard_shard_id,
    )
    .await;

    let partition_map = metashard_actor.state().partition_map.clone();
    let epoch = metashard_actor.state().epoch;
    info!(
        epoch,
        num_ranges = partition_map.ranges.len(),
        "active partition map: {:?}",
        partition_map
            .ranges
            .iter()
            .map(|r| format!(
                "[0x{:02x}, 0x{:03x}) -> {}",
                r.lo, r.hi_exclusive, r.log_shard
            ))
            .collect::<Vec<_>>()
    );

    // Start the metashard actor on a background task.
    mz_ore::task::spawn(|| "persist-metashard", metashard_actor.run());

    // Serve the ConsensusMetashard gRPC service on a Unix socket (grpcurl-able).
    info!(?metashard_sock, "starting metashard gRPC server on Unix socket");
    let router = Server::builder().add_service(ConsensusMetashardServer::new(
        MetashardGrpcServer::new(metashard_handle),
    ));
    mz_persist_shared_log::uds::serve_uds(&metashard_sock, router)
        .await
        .expect("metashard gRPC server failed");
}

// ===========================================================================
// Standalone router mode
// ===========================================================================

async fn run_router(args: RouterArgs) {
    let metrics_registry = MetricsRegistry::new();
    spawn_metrics_server(args.metrics_listen_addr, metrics_registry.clone());

    let metashard_shard_id: ShardId = args.metashard_id.parse().expect("invalid --metashard-id");

    // Compute the metashard pubsub socket path before moving run_dir.
    let metashard_pubsub_path = args
        .run_dir
        .join(format!("metashard-{metashard_shard_id}"))
        .join("pubsub.sock")
        .to_string_lossy()
        .to_string();

    let directory = mz_persist_shared_log::directory::ProcessDirectory::new(
        args.run_dir,
        metashard_shard_id,
    );
    info!(
        %metashard_shard_id,
        metashard_pubsub = %metashard_pubsub_path,
        "starting standalone router"
    );

    // Create PersistClient with remote pubsub to the metashard over Unix socket.
    let persist_client = open_persist_client_with_remote_pubsub(
        &args.storage,
        &metashard_pubsub_path,
        "router",
        &metrics_registry,
    )
    .await;

    // GrpcActorFactory connects to actors over Unix sockets via ProcessDirectory.
    let factory = Arc::new(mz_persist_shared_log::factory::GrpcActorFactory::new(
        directory,
        std::time::Duration::from_secs(args.connect_timeout_secs),
    ));

    // Start with empty routing — the routing task populates it from the
    // metashard persist shard.
    let router = Router::from_routing(Arc::new(
        tokio::sync::RwLock::new(
            mz_persist_shared_log::persist_log::router::RoutingSnapshot::empty(),
        ),
    ));
    mz_persist_shared_log::persist_log::router::spawn_routing_task(
        &persist_client,
        metashard_shard_id,
        Arc::clone(&factory),
        router.routing_handle(),
        router.routing_notify(),
    )
    .await;

    info!(addr = %args.listen_addr, "starting router gRPC server");
    Server::builder()
        .add_service(PersistSharedLogServer::new(router))
        .serve(args.listen_addr)
        .await
        .expect("router gRPC server failed");
}
