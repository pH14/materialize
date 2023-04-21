// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

use std::net::SocketAddr;
use std::time::Duration;

use mz_ore::task::spawn;
use mz_persist::location::{SeqNo, VersionedData};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::rpc::{PersistPubSubServer, PushClient};
use mz_persist_client::ShardId;
use tracing::{info, info_span, Span};

#[derive(Debug, clap::Parser)]
pub struct Args {
    #[clap(long, value_name = "HOST:PORT", default_value = "127.0.0.1:6878")]
    listen_addr: SocketAddr,

    connect_addrs: Vec<String>,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let span = Span::current();
    let server = spawn(|| "persist service", async move {
        let _guard = span.enter();
        info!("listening on {}", args.listen_addr);
        let cache = PersistClientCache::new_no_metrics();
        PersistPubSubServer::new(&cache)
            .serve(args.listen_addr.clone())
            .await
    });
    for addr in args.connect_addrs {
        info!("connecting to {}", addr);
        let client = PushClient::connect(addr.clone())
            .await?
            .into_conn(move |res| {
                let root_span = info_span!("persist::push::client");
                let _guard = root_span.enter();
                info!("client res: {:?}", res)
            });
        info!("connected to {}", addr);
        for seqno in 0u64..7 {
            let data = format!("diff{}", seqno).into_bytes();
            client.push_diff(
                &ShardId::new(),
                &VersionedData {
                    seqno: SeqNo(seqno),
                    data: data.into(),
                },
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let () = client.finish().await?;
        info!("pushed to {}", addr);
    }
    info!("waiting for server to exit");
    let res = server.await;
    info!("server existed {:?}", res);
    Ok(())
}
