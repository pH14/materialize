// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use crate::internal::state::{ProtoStateDiff, ProtoStateRollup};
use crate::internal::state_diff::StateFieldDiff;
use crate::{Metrics, PersistConfig, ShardId, StateVersions};
use anyhow::anyhow;
use mz_build_info::{BuildInfo, DUMMY_BUILD_INFO};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::SeqNo;
use mz_proto::RustType;
use prost::Message;
use std::sync::Arc;

/// Fetches the current state of a given shard
pub async fn fetch_current_state(
    shard_id: ShardId,
    consensus_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let metrics = Metrics::new(&MetricsRegistry::new());
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus).await?;
    let consensus = consensus.clone().open().await?;

    if let Some(data) = consensus.head(&shard_id.to_string()).await? {
        let proto = ProtoStateRollup::decode(data.data).expect("invalid encoded state");
        return Ok(proto);
    }

    Err(anyhow!("unknown shard"))
}

/// Fetches each state in a shard
pub async fn fetch_state_diffs(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<Vec<impl serde::Serialize>, anyhow::Error> {
    let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
    let consensus =
        ConsensusConfig::try_from(&consensus_uri, 1, metrics.postgres_consensus.clone()).await?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(blob_uri)
        .await
        .expect("blob should exist")
        .open()
        .await?;

    let versions = StateVersions::new(
        PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
        consensus,
        blob,
        metrics,
    );

    let mut states_iter = versions
        .fetch_live_states::<(), (), u64, i64>(&shard_id)
        .await?;

    let mut states = vec![];
    while let Some(state) = states_iter.next() {
        states.push(state.into_proto());
    }

    Ok(states)
}
