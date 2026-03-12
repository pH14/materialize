// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A quorum-based [Blob] implementation wrapping 3 inner blob stores.
//!
//! Designed for use with per-zone object storage (e.g., S3 Express One Zone)
//! where each bucket lives in a single availability zone. By writing to 3
//! buckets across 3 zones with 2-of-3 quorum semantics, we achieve high
//! availability at lower latency and cost than standard multi-zone storage.
//!
//! # Correctness
//!
//! **Writes** require 2-of-3 successes. If fewer succeed, the caller receives
//! an error, and the key is never recorded in consensus, so any partial writes
//! are harmless orphans eventually cleaned up by GC.
//!
//! **Reads** race all 3 replicas and return the first `Some` result. This is
//! sound because persist only reads blob keys that are recorded in consensus.
//! A key only reaches consensus after a successful quorum write, guaranteeing
//! at least 2 replicas have the data. With 1 AZ down, the data is still
//! reachable from the surviving replica(s).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use mz_ore::bytes::SegmentedBytes;

use crate::cfg::{BlobConfig, BlobKnobs};
use crate::location::{Blob, BlobMetadata, ExternalError, Indeterminate};

/// The number of replicas in the quorum.
const REPLICAS: usize = 3;

/// The minimum number of successes required for a quorum.
const QUORUM: usize = 2;

/// Configuration for opening a [QuorumBlob].
#[derive(Debug, Clone)]
pub struct QuorumBlobConfig {
    /// The 3 inner blob configurations, one per zone.
    pub inner: [Box<BlobConfig>; REPLICAS],
}

/// A wrapper around [`BlobKnobs`] that is cheaply cloneable via [`Arc`].
///
/// `S3BlobConfig::new` takes `Box<dyn BlobKnobs>`, which is not `Clone`.
/// This wrapper lets us share a single set of knobs across 3 inner configs
/// without modifying the existing API.
#[derive(Debug, Clone)]
pub(crate) struct SharedBlobKnobs(pub Arc<dyn BlobKnobs>);

impl BlobKnobs for SharedBlobKnobs {
    fn operation_timeout(&self) -> Duration {
        self.0.operation_timeout()
    }
    fn operation_attempt_timeout(&self) -> Duration {
        self.0.operation_attempt_timeout()
    }
    fn connect_timeout(&self) -> Duration {
        self.0.connect_timeout()
    }
    fn read_timeout(&self) -> Duration {
        self.0.read_timeout()
    }
    fn is_cc_active(&self) -> bool {
        self.0.is_cc_active()
    }
}

/// A [Blob] implementation that wraps 3 inner blob stores with quorum
/// read/write semantics for higher availability.
///
/// Writes and deletes require 2-of-3 successes. Reads race all 3 and
/// return the first `Some` result, exploiting the write-once-modify-never
/// invariant plus the fact that persist only reads keys recorded in
/// consensus.
#[derive(Debug)]
pub struct QuorumBlob {
    blobs: [Arc<dyn Blob>; REPLICAS],
}

impl QuorumBlob {
    /// Returns a new [QuorumBlob] wrapping the given 3 blob stores.
    pub fn new(blobs: [Arc<dyn Blob>; REPLICAS]) -> Self {
        QuorumBlob { blobs }
    }

    /// Opens a [QuorumBlob] from the given config by opening all 3 inner
    /// blob stores concurrently.
    pub async fn open(config: QuorumBlobConfig) -> Result<Self, ExternalError> {
        let [c0, c1, c2] = config.inner;
        let (b0, b1, b2) = futures_util::future::try_join3(c0.open(), c1.open(), c2.open()).await?;
        Ok(QuorumBlob::new([b0, b1, b2]))
    }
}

/// Runs an async operation against all replicas and requires [QUORUM]
/// successes. Returns the first successful result once quorum is reached.
///
/// If fewer than [QUORUM] operations succeed, returns an [Indeterminate]
/// error (some replicas may have been mutated).
async fn quorum_write<T, F, Fut>(
    blobs: &[Arc<dyn Blob>; REPLICAS],
    op_name: &str,
    mut op: F,
) -> Result<T, ExternalError>
where
    T: Debug,
    F: FnMut(&Arc<dyn Blob>) -> Fut,
    Fut: std::future::Future<Output = Result<T, ExternalError>>,
{
    let mut futs: FuturesUnordered<_> = blobs.iter().map(&mut op).collect();
    let mut successes = 0usize;
    let mut first_ok: Option<T> = None;
    let mut errors = Vec::new();

    while let Some(result) = futs.next().await {
        match result {
            Ok(val) => {
                successes += 1;
                if first_ok.is_none() {
                    first_ok = Some(val);
                }
                if successes >= QUORUM {
                    return Ok(first_ok.expect("just set above"));
                }
            }
            Err(e) => {
                errors.push(e);
            }
        }
    }

    // If all errors are Determinate, the failure is definite (e.g., restore
    // on a backend that doesn't support it). Preserve that signal so callers
    // can distinguish "definitely failed" from "maybe succeeded".
    let all_determinate = errors
        .iter()
        .all(|e| matches!(e, ExternalError::Determinate(_)));
    if all_determinate && !errors.is_empty() {
        // Return the first Determinate error.
        Err(errors.into_iter().next().expect("checked non-empty"))
    } else {
        Err(ExternalError::Indeterminate(Indeterminate::new(anyhow!(
            "quorum {op_name} failed: {successes}/{REPLICAS} succeeded, errors: {errors:?}"
        ))))
    }
}

#[async_trait]
impl Blob for QuorumBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let mut futs: FuturesUnordered<_> = self.blobs.iter().map(|b| b.get(key)).collect();
        let mut any_none = false;
        let mut errors = Vec::new();

        while let Some(result) = futs.next().await {
            match result {
                Ok(Some(data)) => return Ok(Some(data)),
                Ok(None) => {
                    any_none = true;
                }
                Err(e) => {
                    errors.push(e);
                }
            }
        }

        // No replica returned Some. If any said None, the key doesn't exist
        // (or hasn't propagated yet, but consensus-gated keys guarantee ≥2
        // replicas have it after a successful write).
        if any_none {
            Ok(None)
        } else {
            // All replicas errored.
            Err(errors
                .into_iter()
                .next()
                .expect("at least one error if no None and no Some"))
        }
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        // Collect the union of keys across all replicas. We run sequentially
        // because the callback API (`&mut dyn FnMut`) doesn't permit concurrent
        // use. This is acceptable since listing is used for GC, not hot-path.
        let mut seen = BTreeMap::<String, u64>::new();
        let mut last_err = None;
        let mut successes = 0;

        for blob in &self.blobs {
            let result = blob
                .list_keys_and_metadata(key_prefix, &mut |meta: BlobMetadata| {
                    seen.entry(meta.key.to_owned())
                        .or_insert(meta.size_in_bytes);
                })
                .await;
            match result {
                Ok(()) => successes += 1,
                Err(e) => last_err = Some(e),
            }
        }

        if successes == 0 {
            return Err(last_err.expect("at least one error if no successes"));
        }

        for (key, size_in_bytes) in &seen {
            f(BlobMetadata {
                key: key.as_str(),
                size_in_bytes: *size_in_bytes,
            });
        }

        Ok(())
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        // Bytes::clone() is a refcount bump, not a data copy.
        quorum_write(&self.blobs, "set", |blob| {
            let blob = Arc::clone(blob);
            let value = value.clone();
            let key = key.to_owned();
            async move { blob.set(&key, value).await }
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        quorum_write(&self.blobs, "delete", |blob| {
            let blob = Arc::clone(blob);
            let key = key.to_owned();
            async move { blob.delete(&key).await }
        })
        .await
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        quorum_write(&self.blobs, "restore", |blob| {
            let blob = Arc::clone(blob);
            let key = key.to_owned();
            async move { blob.restore(&key).await }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::location::tests::blob_impl_test;
    use crate::mem::{MemBlob, MemBlobConfig, MemMultiRegistry};
    use crate::unreliable::{UnreliableBlob, UnreliableHandle};

    use super::*;

    /// Helper to create a QuorumBlob backed by 3 independent MemBlobs
    /// from shared registries (so that two QuorumBlobs created with the
    /// same path share underlying storage, as blob_impl_test expects).
    fn make_quorum_blob(
        registries: &[Arc<tokio::sync::Mutex<MemMultiRegistry>>; 3],
        path: &str,
    ) -> impl std::future::Future<Output = Result<QuorumBlob, ExternalError>> {
        let registries = registries.clone();
        let path = path.to_owned();
        async move {
            let b0 = registries[0].lock().await.blob(&path);
            let b1 = registries[1].lock().await.blob(&path);
            let b2 = registries[2].lock().await.blob(&path);
            Ok(QuorumBlob::new([Arc::new(b0), Arc::new(b1), Arc::new(b2)]))
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn quorum_blob_impl() -> Result<(), ExternalError> {
        let registries: [Arc<tokio::sync::Mutex<MemMultiRegistry>>; 3] = [
            Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(false))),
            Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(false))),
            Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(false))),
        ];

        blob_impl_test(move |path| {
            let registries = registries.clone();
            async move { make_quorum_blob(&registries, path).await }
        })
        .await?;

        // Also test with tombstone support (enables restore).
        let registries: [Arc<tokio::sync::Mutex<MemMultiRegistry>>; 3] = [
            Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(true))),
            Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(true))),
            Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(true))),
        ];

        blob_impl_test(move |path| {
            let registries = registries.clone();
            async move { make_quorum_blob(&registries, path).await }
        })
        .await?;

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn one_dead_replica() -> Result<(), ExternalError> {
        // Create 3 MemBlobs, wrap 1 in UnreliableBlob set to totally_unavailable.
        let b0: Arc<dyn Blob> = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let b1: Arc<dyn Blob> = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let handle = UnreliableHandle::new(0, 1.0, 0.0);
        let b2: Arc<dyn Blob> = Arc::new(UnreliableBlob::new(
            Arc::new(MemBlob::open(MemBlobConfig::default())),
            handle.clone(),
        ));

        let quorum = QuorumBlob::new([b0, b1, b2]);

        // Make the third replica totally unavailable.
        handle.totally_unavailable();

        // Writes should succeed (2 of 3 are up).
        quorum.set("key1", Bytes::from("value1")).await?;

        // Reads should succeed.
        let result = quorum.get("key1").await?;
        assert_eq!(
            result.map(|s| s.into_contiguous()),
            Some(b"value1".to_vec())
        );

        // Deletes should succeed.
        let deleted = quorum.delete("key1").await?;
        assert_eq!(deleted, Some(6));

        // Key should be gone.
        assert_eq!(quorum.get("key1").await?, None);

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn two_dead_replicas_write_fails() -> Result<(), ExternalError> {
        let b0: Arc<dyn Blob> = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let handle1 = UnreliableHandle::new(0, 1.0, 0.0);
        let b1: Arc<dyn Blob> = Arc::new(UnreliableBlob::new(
            Arc::new(MemBlob::open(MemBlobConfig::default())),
            handle1.clone(),
        ));
        let handle2 = UnreliableHandle::new(1, 1.0, 0.0);
        let b2: Arc<dyn Blob> = Arc::new(UnreliableBlob::new(
            Arc::new(MemBlob::open(MemBlobConfig::default())),
            handle2.clone(),
        ));

        let quorum = QuorumBlob::new([b0, b1, b2]);

        // Make 2 replicas unavailable.
        handle1.totally_unavailable();
        handle2.totally_unavailable();

        // Writes should fail (only 1 of 3 is up).
        let result = quorum.set("key1", Bytes::from("value1")).await;
        assert!(result.is_err());

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn read_survives_two_dead_after_write() -> Result<(), ExternalError> {
        // Write data while all 3 are healthy, then kill 2 and verify reads
        // still work (data is on all 3, so the 1 survivor has it).
        let handle1 = UnreliableHandle::new(0, 1.0, 0.0);
        let handle2 = UnreliableHandle::new(1, 1.0, 0.0);

        let b0: Arc<dyn Blob> = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let b1: Arc<dyn Blob> = Arc::new(UnreliableBlob::new(
            Arc::new(MemBlob::open(MemBlobConfig::default())),
            handle1.clone(),
        ));
        let b2: Arc<dyn Blob> = Arc::new(UnreliableBlob::new(
            Arc::new(MemBlob::open(MemBlobConfig::default())),
            handle2.clone(),
        ));

        let quorum = QuorumBlob::new([b0, b1, b2]);

        // Write while all are healthy.
        quorum.set("key1", Bytes::from("value1")).await?;

        // Kill 2 replicas.
        handle1.totally_unavailable();
        handle2.totally_unavailable();

        // Read should still succeed (first-Some-wins from the 1 good replica).
        let result = quorum.get("key1").await?;
        assert_eq!(
            result.map(|s| s.into_contiguous()),
            Some(b"value1".to_vec())
        );

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn list_keys_dedup() -> Result<(), ExternalError> {
        // Write different keys to different blobs directly, then verify
        // list_keys_and_metadata returns the union.
        let b0 = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let b1 = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let b2 = Arc::new(MemBlob::open(MemBlobConfig::default()));

        // Write "shared" to all 3, "only_0" to b0, "only_1" to b1.
        b0.set("shared", Bytes::from("data")).await?;
        b1.set("shared", Bytes::from("data")).await?;
        b2.set("shared", Bytes::from("data")).await?;
        b0.set("only_0", Bytes::from("aaa")).await?;
        b1.set("only_1", Bytes::from("bb")).await?;

        let quorum = QuorumBlob::new([b0, b1, b2]);

        let mut keys = Vec::new();
        quorum
            .list_keys_and_metadata("", &mut |meta| {
                keys.push((meta.key.to_owned(), meta.size_in_bytes));
            })
            .await?;

        keys.sort();
        assert_eq!(
            keys,
            vec![
                ("only_0".to_string(), 3),
                ("only_1".to_string(), 2),
                ("shared".to_string(), 4),
            ]
        );

        Ok(())
    }
}
