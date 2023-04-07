// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, dead_code)] // WIP
#![allow(clippy::clone_on_ref_ptr, clippy::disallowed_methods)] // Generated code does this

use std::error::Error;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::Stream;
use mz_persist::location::VersionedData;
use mz_proto::ProtoType;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, info_span};

use crate::cache::StateCache;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.internal.service.rs"
));

#[derive(Debug)]
pub struct PersistService {
    conns: Arc<Mutex<Vec<(usize, UnboundedSender<Result<ProtoPushDiff, Status>>)>>>,
    state_cache: Arc<StateCache>,
}

impl PersistService {
    pub fn new(state_cache: Arc<StateCache>) -> Self {
        PersistService {
            conns: Default::default(),
            state_cache,
        }
    }
}

#[async_trait]
impl proto_persist_server::ProtoPersist for PersistService {
    type PushStream = Pin<Box<dyn Stream<Item = Result<ProtoPushDiff, Status>> + Send>>;

    async fn push(
        &self,
        req: Request<Streaming<ProtoPushDiff>>,
    ) -> Result<Response<Self::PushStream>, Status> {
        let root_span = info_span!("persist::push::server");
        let _guard = root_span.enter();
        info!("incoming push");

        let mut in_stream = req.into_inner();
        // WIP not unbounded
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let conn_idx = {
            let mut conns = self.conns.lock().expect("lock");
            // WIP this doesn't work if we remove conns
            let idx = conns.len();
            conns.push((idx, tx.clone()));
            idx
        };

        let conns = Arc::clone(&self.conns);
        let state_cache = Arc::clone(&self.state_cache);
        // this spawn here is required if you want to handle connection error.
        // If we just map `in_stream` and write it back as `out_stream` the `out_stream`
        // will be drooped when connection error occurs and error will never be propagated
        // to mapped version of `in_stream`.
        tokio::spawn(async move {
            let root_span = info_span!("persist::push::server_conn");
            let _guard = root_span.enter();
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => {
                        let shard_id = req.shard_id.parse().expect("WIP");
                        let diff = VersionedData {
                            seqno: req.seqno.into_rust().expect("WIP"),
                            data: req.diff.clone(),
                        };

                        {
                            // WIP Use a RwLock and only grab the write lock if we need to
                            let mut conns = conns.lock().expect("lock");
                            // info!(
                            //     "server forwarding req to {} conns {} {} {}",
                            //     conns.len(),
                            //     shard_id,
                            //     diff.seqno,
                            //     diff.data.len()
                            // );
                            #[allow(clippy::overly_complex_bool_expr)]
                            conns.retain(|(idx, tx)| {
                                if *idx == conn_idx {
                                    // don't send it back to whoever just sent it
                                    true
                                } else {
                                    let keep = tx.send(Ok(req.clone())).is_ok();
                                    // WIP our idx scheme breaks if we remove things.
                                    keep || true
                                }
                            });
                        }

                        // also apply it locally
                        state_cache.push_diff(&shard_id, diff);
                    }
                    Err(err) => {
                        if let Some(io_err) = match_for_io_error(&err) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                // here you can handle special case when client
                                // disconnected in unexpected way
                                info!("client disconnected: broken pipe");
                                break;
                            }
                        }

                        match tx.send(Err(err)) {
                            Ok(_) => (),
                            // response was dropped
                            Err(_err) => break,
                        }
                    }
                }
            }
            info!("push stream ended");
        });

        let out_stream: Self::PushStream = Box::pin(UnboundedReceiverStream::new(rx));
        Ok(Response::new(out_stream))
    }
}

fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}
