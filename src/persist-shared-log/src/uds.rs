// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unix domain socket helpers for tonic gRPC.
//!
//! In multi-process mode, each actor listens on a Unix domain socket under a
//! shared run directory. The socket path is deterministic from the actor's role
//! and shard ID (see [`ProcessDirectory`](crate::directory::ProcessDirectory)).
//!
//! These helpers wrap the tonic/tokio APIs for serving and connecting over Unix
//! sockets. Tonic 0.14 has `impl Connected for tokio::net::UnixStream` on Unix
//! platforms, so `serve_with_incoming` accepts a `UnixListenerStream` directly.

use std::path::Path;

use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::server::Router;

/// Serve a tonic gRPC service on a Unix domain socket.
///
/// Creates parent directories and removes any stale socket file before binding.
pub async fn serve_uds(socket_path: &Path, router: Router) -> Result<(), anyhow::Error> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Remove stale socket from a previous run.
    let _ = std::fs::remove_file(socket_path);
    let uds = UnixListener::bind(socket_path)?;
    let stream = UnixListenerStream::new(uds);
    router.serve_with_incoming(stream).await?;
    Ok(())
}

/// Connect a tonic gRPC client to a Unix domain socket.
///
/// Returns a `Channel` that routes all requests to the given socket path.
/// The HTTP URI is a dummy — the custom connector ignores it. The `UnixStream`
/// is wrapped in `hyper_util::rt::TokioIo` so it satisfies hyper's `Read`/`Write`
/// trait bounds.
pub async fn connect_uds(socket_path: &str) -> Result<tonic::transport::Channel, anyhow::Error> {
    let path = socket_path.to_string();
    let channel = tonic::transport::Endpoint::from_static("http://[::]:0")
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let p = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(p).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await?;
    Ok(channel)
}

/// Connect a tonic gRPC client to a Unix domain socket, retrying until success
/// or timeout.
pub async fn connect_uds_with_retry(
    socket_path: &str,
    timeout: std::time::Duration,
) -> Result<tonic::transport::Channel, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match connect_uds(socket_path).await {
            Ok(channel) => return Ok(channel),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "failed to connect to {} after {:?}: {}",
                        socket_path, timeout, e
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}
