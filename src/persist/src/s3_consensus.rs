// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by S3 with quorum semantics.
//!
//! Designed for use with S3 Express One Zone directory buckets. Since
//! Express One Zone is single-AZ, we write to 3 buckets across 3 AZs
//! with 2-of-3 quorum semantics for availability.
//!
//! Key design:
//! - Uses If-None-Match CAS on S3 (one object per `(key, seqno)`)
//! - Adaptive probing instead of LIST for head/scan discovery
//!   (Express One Zone doesn't guarantee lexicographic LIST ordering)
//! - Delayed deletion instead of tombstones for truncation
//!
//! ## Data Model
//!
//! Objects per consensus key:
//! ```text
//! {key}/{seqno:020}     ->  VersionedData.data (Bytes)
//! {key}/_hint_head       ->  best-effort latest seqno (u64 LE bytes)
//! {key}/_hint_tail       ->  best-effort low water mark (u64 LE bytes)
//! ```

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;

use crate::location::{
    CaSResult, Consensus, ExternalError, Indeterminate, ResultStream, SeqNo, VersionedData,
};

/// The number of replicas in the quorum.
const REPLICAS: usize = 3;

/// The minimum number of successes required for a quorum.
const QUORUM: usize = 2;

/// Default adaptive probe window size.
const DEFAULT_PROBE_WINDOW: usize = 10;

/// Update hint pointers every Nth operation.
const HINT_UPDATE_INTERVAL: usize = 10;

// ---------- S3Ops trait ----------

/// Abstraction over S3 operations for testability.
///
/// Implementations must be linearizable within a single bucket/instance.
#[async_trait]
pub(crate) trait S3Ops: Debug + Send + Sync {
    /// GET object. Returns None on 404.
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ExternalError>;
    /// HEAD object. Returns Some(content_length) if exists, None on 404.
    async fn head_object(&self, key: &str) -> Result<Option<u64>, ExternalError>;
    /// PUT object (unconditional). Used for hint pointers.
    async fn put(&self, key: &str, data: Bytes) -> Result<(), ExternalError>;
    /// PUT with If-None-Match: *. Returns true if created, false if object
    /// already exists (412 Precondition Failed).
    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<bool, ExternalError>;
    /// DELETE object.
    async fn delete(&self, key: &str) -> Result<(), ExternalError>;
    /// LIST objects with given prefix. Returns keys (unordered for Express One Zone).
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ExternalError>;
}

// ---------- MemS3Ops (testing) ----------

/// In-memory implementation of [S3Ops] for testing.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct MemS3Ops {
    data: Arc<Mutex<std::collections::BTreeMap<String, Bytes>>>,
}

#[cfg(test)]
impl MemS3Ops {
    /// Creates a new, empty in-memory S3 store.
    pub fn new() -> Self {
        MemS3Ops {
            data: Arc::new(Mutex::new(std::collections::BTreeMap::new())),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl S3Ops for MemS3Ops {
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ExternalError> {
        let store = self.data.lock().map_err(|e| anyhow!("{e}"))?;
        Ok(store.get(key).cloned())
    }

    async fn head_object(&self, key: &str) -> Result<Option<u64>, ExternalError> {
        let store = self.data.lock().map_err(|e| anyhow!("{e}"))?;
        Ok(store
            .get(key)
            .map(|b| u64::try_from(b.len()).expect("data length fits in u64")))
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ExternalError> {
        let mut store = self.data.lock().map_err(|e| anyhow!("{e}"))?;
        store.insert(key.to_owned(), data);
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<bool, ExternalError> {
        let mut store = self.data.lock().map_err(|e| anyhow!("{e}"))?;
        if store.contains_key(key) {
            Ok(false) // 412 Precondition Failed
        } else {
            store.insert(key.to_owned(), data);
            Ok(true) // 201 Created
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ExternalError> {
        let mut store = self.data.lock().map_err(|e| anyhow!("{e}"))?;
        store.remove(key);
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ExternalError> {
        let store = self.data.lock().map_err(|e| anyhow!("{e}"))?;
        Ok(store
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

// ---------- RealS3Ops (production) ----------

/// Production implementation of [S3Ops] wrapping an AWS S3 client.
#[derive(Debug)]
pub(crate) struct RealS3Ops {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl RealS3Ops {
    /// Creates a new [RealS3Ops] for the given bucket and prefix.
    pub fn new(client: aws_sdk_s3::Client, bucket: String, prefix: String) -> Self {
        RealS3Ops {
            client,
            bucket,
            prefix,
        }
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{}", self.prefix, key)
        }
    }
}

#[async_trait]
impl S3Ops for RealS3Ops {
    async fn get(&self, key: &str) -> Result<Option<Bytes>, ExternalError> {
        use aws_sdk_s3::error::SdkError;
        let path = self.full_key(key);
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .await;
        match result {
            Ok(resp) => {
                let body = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| anyhow!("s3 get body collect: {e}"))?;
                Ok(Some(body.into_bytes()))
            }
            Err(SdkError::ServiceError(err)) if err.err().is_no_such_key() => Ok(None),
            Err(err) => Err(anyhow!("s3 get error: {err}").into()),
        }
    }

    async fn head_object(&self, key: &str) -> Result<Option<u64>, ExternalError> {
        use aws_sdk_s3::error::SdkError;
        let path = self.full_key(key);
        let result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .await;
        match result {
            Ok(resp) => {
                let len = resp.content_length.unwrap_or(0);
                Ok(Some(
                    u64::try_from(len).expect("content_length should be non-negative"),
                ))
            }
            Err(SdkError::ServiceError(err)) if err.err().is_not_found() => Ok(None),
            Err(err) => Err(anyhow!("s3 head error: {err}").into()),
        }
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<(), ExternalError> {
        use aws_sdk_s3::primitives::ByteStream;
        let path = self.full_key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&path)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|err| anyhow!("s3 put error: {err}"))?;
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<bool, ExternalError> {
        use aws_sdk_s3::error::SdkError;
        use aws_sdk_s3::primitives::ByteStream;
        let path = self.full_key(key);
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&path)
            .body(ByteStream::from(data))
            .if_none_match("*")
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(err)) => {
                // 412 Precondition Failed means the object already exists.
                let status = err.raw().status().as_u16();
                if status == 412 {
                    Ok(false)
                } else {
                    Err(anyhow!("s3 put_if_absent error: {}", err.err()).into())
                }
            }
            Err(err) => Err(anyhow!("s3 put_if_absent error: {err}").into()),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ExternalError> {
        let path = self.full_key(key);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .await
            .map_err(|err| anyhow!("s3 delete error: {err}"))?;
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ExternalError> {
        let full_prefix = self.full_key(prefix);
        let strippable_root = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };
        let mut keys = Vec::new();
        let mut continuation_token = None;
        loop {
            let resp = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|err| anyhow!("s3 list error: {err}"))?;
            if let Some(contents) = resp.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        let stripped = if strippable_root.is_empty() {
                            key
                        } else {
                            key.strip_prefix(&strippable_root)
                                .unwrap_or(&key)
                                .to_owned()
                        };
                        keys.push(stripped);
                    }
                }
            }
            if resp.next_continuation_token.is_some() {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }
        Ok(keys)
    }
}

// ---------- Quorum primitives ----------

/// Fire PUT with If-None-Match on all replicas. Require 2-of-3 agreement.
async fn quorum_put_if_absent(
    replicas: &[Arc<dyn S3Ops>; REPLICAS],
    key: &str,
    data: Bytes,
) -> Result<bool, ExternalError> {
    let mut futs: FuturesUnordered<_> = replicas
        .iter()
        .map(|r| {
            let r = Arc::clone(r);
            let key = key.to_owned();
            let data = data.clone();
            async move { r.put_if_absent(&key, data).await }
        })
        .collect();

    let mut created = 0usize;
    let mut existed = 0usize;
    let mut errors = Vec::new();

    while let Some(result) = futs.next().await {
        match result {
            Ok(true) => {
                created += 1;
                if created >= QUORUM {
                    return Ok(true);
                }
            }
            Ok(false) => {
                existed += 1;
                if existed >= QUORUM {
                    return Ok(false);
                }
            }
            Err(e) => errors.push(e),
        }
    }

    Err(ExternalError::Indeterminate(Indeterminate::new(anyhow!(
        "quorum put_if_absent failed: created={created}, existed={existed}, errors={errors:?}"
    ))))
}

/// Fire GET on all replicas. Require 2-of-3 agreement on Some/None.
async fn quorum_get(
    replicas: &[Arc<dyn S3Ops>; REPLICAS],
    key: &str,
) -> Result<Option<Bytes>, ExternalError> {
    let mut futs: FuturesUnordered<_> = replicas
        .iter()
        .map(|r| {
            let r = Arc::clone(r);
            let key = key.to_owned();
            async move { r.get(&key).await }
        })
        .collect();

    let mut some_count = 0usize;
    let mut none_count = 0usize;
    let mut first_data: Option<Bytes> = None;
    let mut errors = Vec::new();

    while let Some(result) = futs.next().await {
        match result {
            Ok(Some(data)) => {
                some_count += 1;
                if first_data.is_none() {
                    first_data = Some(data);
                }
                if some_count >= QUORUM {
                    return Ok(first_data);
                }
            }
            Ok(None) => {
                none_count += 1;
                if none_count >= QUORUM {
                    return Ok(None);
                }
            }
            Err(e) => errors.push(e),
        }
    }

    Err(ExternalError::Indeterminate(Indeterminate::new(anyhow!(
        "quorum get failed: some={some_count}, none={none_count}, errors={errors:?}"
    ))))
}

/// Fire HEAD on all replicas. 2-of-3 agreement on exists/not-exists.
async fn quorum_exists(
    replicas: &[Arc<dyn S3Ops>; REPLICAS],
    key: &str,
) -> Result<bool, ExternalError> {
    let mut futs: FuturesUnordered<_> = replicas
        .iter()
        .map(|r| {
            let r = Arc::clone(r);
            let key = key.to_owned();
            async move { r.head_object(&key).await }
        })
        .collect();

    let mut exists = 0usize;
    let mut not_exists = 0usize;
    let mut errors = Vec::new();

    while let Some(result) = futs.next().await {
        match result {
            Ok(Some(_)) => {
                exists += 1;
                if exists >= QUORUM {
                    return Ok(true);
                }
            }
            Ok(None) => {
                not_exists += 1;
                if not_exists >= QUORUM {
                    return Ok(false);
                }
            }
            Err(e) => errors.push(e),
        }
    }

    Err(ExternalError::Indeterminate(Indeterminate::new(anyhow!(
        "quorum exists failed: exists={exists}, not_exists={not_exists}, errors={errors:?}"
    ))))
}

/// Read hint pointers from all replicas, return MAX of successful reads.
/// Returns None if all successful reads returned 404.
async fn quorum_max_value(
    replicas: &[Arc<dyn S3Ops>; REPLICAS],
    key: &str,
) -> Result<Option<u64>, ExternalError> {
    let mut futs: FuturesUnordered<_> = replicas
        .iter()
        .map(|r| {
            let r = Arc::clone(r);
            let key = key.to_owned();
            async move { r.get(&key).await }
        })
        .collect();

    let mut max_val: Option<u64> = None;
    let mut successes = 0usize;
    let mut errors = Vec::new();

    while let Some(result) = futs.next().await {
        match result {
            Ok(Some(data)) if data.len() == 8 => {
                successes += 1;
                let val =
                    u64::from_le_bytes(<[u8; 8]>::try_from(&data[..8]).expect("checked len == 8"));
                max_val = Some(max_val.map_or(val, |m| m.max(val)));
            }
            Ok(Some(_)) => {
                // Malformed hint, count as success but ignore value.
                successes += 1;
            }
            Ok(None) => {
                successes += 1;
            }
            Err(e) => errors.push(e),
        }
    }

    if successes >= QUORUM {
        Ok(max_val)
    } else {
        Err(ExternalError::Indeterminate(Indeterminate::new(anyhow!(
            "quorum max_value failed: successes={successes}, errors={errors:?}"
        ))))
    }
}

// ---------- S3QuorumConsensus ----------

/// Configuration for opening an [S3QuorumConsensus].
#[derive(Debug, Clone)]
pub struct S3ConsensusConfig {
    /// The 3 S3 buckets, one per availability zone.
    pub buckets: [String; 3],
    /// Key prefix within each bucket.
    pub prefix: String,
    /// AWS region.
    pub region: Option<String>,
    /// IAM role ARN for AssumeRole.
    pub role_arn: Option<String>,
    /// S3 endpoint override (for testing against localstack, etc.).
    pub endpoint: Option<String>,
    /// Static AWS credentials (access_key_id, secret_access_key).
    pub credentials: Option<(String, String)>,
}

impl S3ConsensusConfig {
    /// Opens an [S3QuorumConsensus] by creating 3 S3 clients (one per bucket).
    pub async fn open(self) -> Result<S3QuorumConsensus, ExternalError> {
        use aws_credential_types::Credentials;
        use aws_types::region::Region;

        let mut replica_vec = Vec::with_capacity(3);
        for bucket in &self.buckets {
            let mut loader = mz_aws_util::defaults();
            if let Some(ref region) = self.region {
                loader = loader.region(Region::new(region.clone()));
            }
            if let Some(ref endpoint) = self.endpoint {
                loader = loader.endpoint_url(endpoint);
            }
            if let Some((ref access_key, ref secret_key)) = self.credentials {
                loader = loader
                    .credentials_provider(Credentials::from_keys(access_key, secret_key, None));
            }
            let sdk_config = loader.load().await;
            let client = mz_aws_util::s3::new_client(&sdk_config);
            let replica: Arc<dyn S3Ops> =
                Arc::new(RealS3Ops::new(client, bucket.clone(), self.prefix.clone()));
            replica_vec.push(replica);
        }
        let replicas: [Arc<dyn S3Ops>; 3] = replica_vec
            .try_into()
            .map_err(|_| anyhow!("expected exactly 3 replicas"))?;
        Ok(S3QuorumConsensus::new(replicas))
    }
}

/// Per-key cached state for the consensus store.
#[derive(Debug, Clone)]
struct KeyState {
    /// Highest known seqno for this key.
    head_seqno: Option<SeqNo>,
    /// Lowest non-truncated seqno (inclusive). Readers ignore seqnos below this.
    low_water_mark: SeqNo,
    /// Adaptive probe window for head discovery.
    probe_window: usize,
    /// CAS operations since last `_hint_head` write.
    cas_count: usize,
    /// Truncate operations since last `_hint_tail` write.
    truncate_count: usize,
}

impl Default for KeyState {
    fn default() -> Self {
        KeyState {
            head_seqno: None,
            low_water_mark: SeqNo(0),
            probe_window: DEFAULT_PROBE_WINDOW,
            cas_count: 0,
            truncate_count: 0,
        }
    }
}

/// Implementation of [Consensus] backed by S3 with 2-of-3 quorum semantics.
///
/// Each operation writes to (or reads from) 3 S3 buckets across 3 availability
/// zones. Writes use If-None-Match CAS for mutual exclusion. Head/scan use
/// adaptive probing instead of LIST (Express One Zone doesn't guarantee
/// lexicographic ordering in LIST results).
///
/// ## Known Limitations
///
/// - **Sparse seqnos with concurrent writers**: With sparse seqnos, there is a
///   TOCTOU window between the expected-verification probe and the put_if_absent
///   write where two writers with the same `expected` but different `new.seqno`
///   could both succeed. This doesn't occur in production (always dense seqnos)
///   or in the conformance test (always sequential).
/// - **Cold start**: Without hint pointers, the first `head()` call probes from
///   seqno 0. If the lowest seqno is very high, this requires many probe rounds.
///   Hint pointers mitigate this for warm restarts.
#[derive(Debug)]
pub struct S3QuorumConsensus {
    replicas: [Arc<dyn S3Ops>; REPLICAS],
    state: Mutex<BTreeMap<String, KeyState>>,
}

impl S3QuorumConsensus {
    /// Creates a new [S3QuorumConsensus] wrapping the given 3 S3Ops replicas.
    pub(crate) fn new(replicas: [Arc<dyn S3Ops>; REPLICAS]) -> Self {
        S3QuorumConsensus {
            replicas,
            state: Mutex::new(BTreeMap::new()),
        }
    }

    // --- Key formatting ---

    fn data_key(key: &str, seqno: SeqNo) -> String {
        format!("{}/{:020}", key, seqno.0)
    }

    fn hint_head_key(key: &str) -> String {
        format!("{}/_hint_head", key)
    }

    fn hint_tail_key(key: &str) -> String {
        format!("{}/_hint_tail", key)
    }

    fn is_hint_key(s3_key: &str) -> bool {
        s3_key.ends_with("/_hint_head") || s3_key.ends_with("/_hint_tail")
    }

    /// Extract the consensus key name from an S3 object path.
    /// E.g., `"mykey/00000000000000000005"` -> `"mykey"`.
    fn parse_consensus_key(s3_key: &str) -> Option<&str> {
        s3_key.split('/').next().filter(|k| !k.is_empty())
    }

    // --- Cache helpers ---

    fn get_cached_state(&self, key: &str) -> KeyState {
        self.state
            .lock()
            .expect("lock poisoned")
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    fn update_cached_state<F: FnOnce(&mut KeyState)>(&self, key: &str, f: F) {
        let mut states = self.state.lock().expect("lock poisoned");
        let state = states.entry(key.to_owned()).or_default();
        f(state);
    }

    // --- Head discovery via adaptive probing ---

    /// Discover the head seqno for a key using adaptive forward probing.
    ///
    /// On cold start (no cache), reads `_hint_head` for an approximate
    /// starting point. Then probes forward in windows of `probe_window` size
    /// to find the highest existing seqno.
    async fn find_head_seqno(&self, key: &str) -> Result<Option<SeqNo>, ExternalError> {
        let (cached_head, mut window) = {
            let states = self.state.lock().expect("lock poisoned");
            match states.get(key) {
                Some(ks) => (ks.head_seqno, ks.probe_window),
                None => (None, DEFAULT_PROBE_WINDOW),
            }
        };

        // Determine starting point.
        let start = match cached_head {
            Some(s) => s.0,
            None => {
                // Cold start: read _hint_head for an approximate starting point.
                quorum_max_value(&self.replicas, &Self::hint_head_key(key))
                    .await?
                    .unwrap_or_default()
            }
        };

        // Adaptive forward probe from `start`.
        let mut pos = start;
        let mut found_anything = false;

        loop {
            let probe_start = pos.saturating_add(1);
            let probe_end = pos.saturating_add(u64::try_from(window).expect("window fits in u64"));

            if probe_start > probe_end {
                break;
            }

            let mut highest_found: Option<u64> = None;
            let mut all_probes_hit = true;

            for seqno in probe_start..=probe_end {
                // Don't probe beyond i64::MAX (seqno bound).
                if i64::try_from(seqno).is_err() {
                    all_probes_hit = false;
                    break;
                }
                let s3_key = Self::data_key(key, SeqNo(seqno));
                if quorum_exists(&self.replicas, &s3_key).await? {
                    highest_found = Some(seqno);
                } else {
                    all_probes_hit = false;
                }
            }

            match highest_found {
                Some(found) => {
                    found_anything = true;
                    pos = found;
                    if all_probes_hit && window < 10_000 {
                        window *= 2;
                    }
                    // Continue probing from the new highest position.
                }
                None => {
                    // No hits in this window: pos is the head. Only shrink
                    // the window if no new data was found during this entire
                    // probing session (i.e., the cached head was accurate).
                    if !found_anything {
                        window = (window / 2).max(DEFAULT_PROBE_WINDOW);
                    }
                    break;
                }
            }
        }

        // Verify the final position exists.
        let head_seqno = if pos > start {
            // We found something higher via probing (verified by quorum_exists).
            Some(SeqNo(pos))
        } else {
            // pos == start: verify the starting position actually has data.
            let s3_key = Self::data_key(key, SeqNo(pos));
            if quorum_exists(&self.replicas, &s3_key).await? {
                Some(SeqNo(pos))
            } else {
                None
            }
        };

        // Update cache.
        self.update_cached_state(key, |ks| {
            if let Some(head) = head_seqno {
                if ks.head_seqno.map_or(true, |old| head > old) {
                    ks.head_seqno = Some(head);
                }
            }
            ks.probe_window = window;
        });

        Ok(head_seqno)
    }

    /// Fire-and-forget PUT of a hint pointer to all replicas.
    async fn write_hint(&self, hint_key: &str, value: u64) {
        let data = Bytes::from(value.to_le_bytes().to_vec());
        for replica in &self.replicas {
            let _ = replica.put(hint_key, data.clone()).await;
        }
    }
}

#[async_trait]
impl Consensus for S3QuorumConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        Box::pin(try_stream! {
            let mut all_keys = std::collections::BTreeSet::new();
            let mut successes = 0usize;
            let mut last_err = None;

            for replica in &self.replicas {
                match replica.list_prefix("").await {
                    Ok(keys) => {
                        successes += 1;
                        for s3_key in keys {
                            if !Self::is_hint_key(&s3_key) {
                                if let Some(consensus_key) = Self::parse_consensus_key(&s3_key) {
                                    all_keys.insert(consensus_key.to_owned());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        last_err = Some(e);
                    }
                }
            }

            if successes == 0 {
                Err(last_err.expect("at least one error if no successes"))?;
            }

            for key in all_keys {
                yield key;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let head_seqno = self.find_head_seqno(key).await?;
        match head_seqno {
            None => Ok(None),
            Some(seqno) => {
                let s3_key = Self::data_key(key, seqno);
                match quorum_get(&self.replicas, &s3_key).await? {
                    Some(data) => Ok(Some(VersionedData { seqno, data })),
                    None => {
                        // Object existed during HEAD but not during GET — shouldn't
                        // happen in normal operation. Treat as no data.
                        Ok(None)
                    }
                }
            }
        }
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        // Validate: new.seqno > expected (when expected is Some).
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(ExternalError::from(anyhow!(
                    "new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                    new.seqno,
                    expected
                )));
            }
        }

        // Validate: seqno must fit in [0, i64::MAX].
        if i64::try_from(new.seqno.0).is_err() {
            return Err(ExternalError::from(anyhow!(
                "sequence numbers must fit within [0, i64::MAX], received: {:?}",
                new.seqno
            )));
        }

        // Verify expected matches actual head.
        if let Some(expected) = expected {
            let head = self.find_head_seqno(key).await?;
            if head != Some(expected) {
                return Ok(CaSResult::ExpectationMismatch);
            }
        } else {
            // expected = None: verify no data exists via LIST.
            let mut has_data_count = 0usize;
            let mut no_data_count = 0usize;
            let mut errors = Vec::new();

            for replica in &self.replicas {
                let prefix = format!("{}/", key);
                match replica.list_prefix(&prefix).await {
                    Ok(keys) => {
                        let has_data_keys = keys.iter().any(|k| !Self::is_hint_key(k));
                        if has_data_keys {
                            has_data_count += 1;
                        } else {
                            no_data_count += 1;
                        }
                    }
                    Err(e) => errors.push(e),
                }
            }

            if has_data_count >= QUORUM {
                return Ok(CaSResult::ExpectationMismatch);
            }
            if no_data_count < QUORUM {
                return Err(ExternalError::Indeterminate(Indeterminate::new(anyhow!(
                    "quorum list for CAS init failed: has_data={has_data_count}, no_data={no_data_count}, errors={errors:?}"
                ))));
            }
        }

        // Write the new version via quorum put_if_absent.
        let s3_key = Self::data_key(key, new.seqno);
        match quorum_put_if_absent(&self.replicas, &s3_key, new.data.clone()).await? {
            true => {
                // Committed: update cache.
                let mut should_update_hint = false;
                self.update_cached_state(key, |ks| {
                    ks.head_seqno = Some(new.seqno);
                    ks.cas_count += 1;
                    should_update_hint = ks.cas_count % HINT_UPDATE_INTERVAL == 0;
                });

                // Lazy hint update (every Nth CAS).
                if should_update_hint {
                    self.write_hint(&Self::hint_head_key(key), new.seqno.0)
                        .await;
                }

                Ok(CaSResult::Committed)
            }
            false => {
                // 412: seqno already exists (concurrent write).
                Ok(CaSResult::ExpectationMismatch)
            }
        }
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let head_seqno = match self.find_head_seqno(key).await? {
            Some(s) => s,
            None => return Ok(vec![]),
        };

        // Get or seed low_water_mark.
        let low_water_mark = {
            let ks = self.get_cached_state(key);
            if ks.low_water_mark == SeqNo(0) {
                // Cold start: seed from _hint_tail.
                match quorum_max_value(&self.replicas, &Self::hint_tail_key(key)).await? {
                    Some(hint) => {
                        let lwm = SeqNo(hint);
                        self.update_cached_state(key, |ks| {
                            if lwm > ks.low_water_mark {
                                ks.low_water_mark = lwm;
                            }
                        });
                        lwm
                    }
                    None => SeqNo(0),
                }
            } else {
                ks.low_water_mark
            }
        };

        let effective_from = std::cmp::max(from, low_water_mark);

        if effective_from > head_seqno {
            return Ok(vec![]);
        }

        // Scan forward from effective_from to head_seqno, collecting up to `limit` entries.
        let mut results = Vec::new();
        let mut current = effective_from.0;

        while current <= head_seqno.0 && results.len() < limit {
            let s3_key = Self::data_key(key, SeqNo(current));
            match quorum_get(&self.replicas, &s3_key).await? {
                Some(data) => {
                    results.push(VersionedData {
                        seqno: SeqNo(current),
                        data,
                    });
                }
                None => {
                    // Gap in sparse seqnos — skip.
                }
            }
            current = current.saturating_add(1);
        }

        Ok(results)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        // Verify key has data and seqno <= head.
        let head_seqno = self.find_head_seqno(key).await?;
        match head_seqno {
            None => {
                return Err(ExternalError::from(anyhow!(
                    "upper bound too high for truncate: {:?}",
                    seqno
                )));
            }
            Some(head) if head < seqno => {
                return Err(ExternalError::from(anyhow!(
                    "upper bound too high for truncate: {:?}",
                    seqno
                )));
            }
            _ => {}
        }

        let old_lwm = self.get_cached_state(key).low_water_mark;

        // Update low_water_mark in cache.
        let mut should_update_hint = false;
        self.update_cached_state(key, |ks| {
            if seqno > ks.low_water_mark {
                ks.low_water_mark = seqno;
            }
            ks.truncate_count += 1;
            should_update_hint = ks.truncate_count % HINT_UPDATE_INTERVAL == 0;
        });

        // Lazy hint update (every Nth truncate).
        if should_update_hint {
            self.write_hint(&Self::hint_tail_key(key), seqno.0).await;
        }

        // Schedule background deletion of objects below the new low_water_mark.
        // In production this would use a delayed task (5-minute safety window).
        // For simplicity, we spawn an immediate task here.
        if seqno > old_lwm {
            let replicas = self.replicas.clone();
            let key = key.to_owned();
            mz_ore::task::spawn(|| "s3_consensus::truncate_cleanup", async move {
                for s in old_lwm.0..seqno.0 {
                    let s3_key = format!("{}/{:020}", key, s);
                    for replica in &replicas {
                        let _ = replica.delete(&s3_key).await;
                    }
                }
            });
        }

        // Return None: we don't know the exact count in the delayed-delete model.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mz_ore::assert_err;

    use crate::location::tests::consensus_impl_test;
    use crate::location::{CaSResult, ExternalError, SCAN_ALL, SeqNo, VersionedData};

    use super::*;

    /// Helper to create an `Arc<dyn S3Ops>` from a `MemS3Ops`.
    fn mem_ops() -> Arc<dyn S3Ops> {
        Arc::new(MemS3Ops::new())
    }

    /// Helper to upcast a concrete `Arc<MemS3Ops>` to `Arc<dyn S3Ops>`.
    fn upcast(ops: &Arc<MemS3Ops>) -> Arc<dyn S3Ops> {
        let cloned: Arc<MemS3Ops> = Arc::clone(ops);
        cloned
    }

    fn make_s3_quorum_consensus() -> S3QuorumConsensus {
        S3QuorumConsensus::new([mem_ops(), mem_ops(), mem_ops()])
    }

    // ---------- Conformance test ----------

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn s3_consensus_impl() -> Result<(), ExternalError> {
        consensus_impl_test(|| async { Ok(make_s3_quorum_consensus()) }).await
    }

    // ---------- Degraded mode: 1 dead replica ----------

    /// An S3Ops that always fails.
    #[derive(Debug)]
    struct FailingS3Ops;

    #[async_trait]
    impl S3Ops for FailingS3Ops {
        async fn get(&self, _: &str) -> Result<Option<Bytes>, ExternalError> {
            Err(ExternalError::from(anyhow!("replica unavailable")))
        }
        async fn head_object(&self, _: &str) -> Result<Option<u64>, ExternalError> {
            Err(ExternalError::from(anyhow!("replica unavailable")))
        }
        async fn put(&self, _: &str, _: Bytes) -> Result<(), ExternalError> {
            Err(ExternalError::from(anyhow!("replica unavailable")))
        }
        async fn put_if_absent(&self, _: &str, _: Bytes) -> Result<bool, ExternalError> {
            Err(ExternalError::from(anyhow!("replica unavailable")))
        }
        async fn delete(&self, _: &str) -> Result<(), ExternalError> {
            Err(ExternalError::from(anyhow!("replica unavailable")))
        }
        async fn list_prefix(&self, _: &str) -> Result<Vec<String>, ExternalError> {
            Err(ExternalError::from(anyhow!("replica unavailable")))
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn one_dead_replica() -> Result<(), ExternalError> {
        let ops: [Arc<dyn S3Ops>; 3] = [
            Arc::new(MemS3Ops::new()),
            Arc::new(MemS3Ops::new()),
            Arc::new(FailingS3Ops),
        ];
        let consensus = S3QuorumConsensus::new(ops);

        let key = "test_key";

        // CAS with expected=None should succeed (2 of 3 replicas up).
        let state = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("abc"),
        };
        assert_eq!(
            consensus.compare_and_set(key, None, state.clone()).await,
            Ok(CaSResult::Committed),
        );

        // head should succeed.
        assert_eq!(consensus.head(key).await, Ok(Some(state.clone())));

        // scan should succeed.
        assert_eq!(
            consensus.scan(key, SeqNo(0), SCAN_ALL).await,
            Ok(vec![state.clone()])
        );

        // truncate should succeed.
        assert!(consensus.truncate(key, SeqNo(0)).await.is_ok());

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn two_dead_replicas_write_fails() -> Result<(), ExternalError> {
        let ops: [Arc<dyn S3Ops>; 3] = [
            Arc::new(MemS3Ops::new()),
            Arc::new(FailingS3Ops),
            Arc::new(FailingS3Ops),
        ];
        let consensus = S3QuorumConsensus::new(ops);

        let state = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("abc"),
        };
        // CAS should fail (only 1 of 3 up, need 2 for quorum).
        assert_err!(consensus.compare_and_set("key", None, state).await);

        Ok(())
    }

    // ---------- Adaptive probe window ----------

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn adaptive_probe_window() -> Result<(), ExternalError> {
        // Write a dense run of seqnos directly to the S3Ops stores,
        // then probe from cold start to exercise window growth.
        let mem0 = Arc::new(MemS3Ops::new());
        let mem1 = Arc::new(MemS3Ops::new());
        let mem2 = Arc::new(MemS3Ops::new());

        let key = "probe_test";
        // Write seqnos 1..=25 directly to all replicas.
        for i in 1u64..=25 {
            let s3_key = S3QuorumConsensus::data_key(key, SeqNo(i));
            let data = Bytes::from(format!("data_{i}"));
            mem0.put(&s3_key, data.clone()).await?;
            mem1.put(&s3_key, data.clone()).await?;
            mem2.put(&s3_key, data).await?;
        }

        // Create a fresh consensus (no cache) — probing from 0 will
        // discover the dense run and double the window repeatedly.
        let consensus = S3QuorumConsensus::new([upcast(&mem0), upcast(&mem1), upcast(&mem2)]);

        let head = consensus.head(key).await?;
        assert_eq!(head.map(|v| v.seqno), Some(SeqNo(25)));

        // Probing from 0 through a dense 25-element run should have
        // doubled the window at least once.
        let ks = consensus.get_cached_state(key);
        assert!(
            ks.probe_window > DEFAULT_PROBE_WINDOW,
            "probe window should have grown from {DEFAULT_PROBE_WINDOW}, got {}",
            ks.probe_window
        );

        // Subsequent head() calls with no new writes shrink the window
        // back toward DEFAULT_PROBE_WINDOW (each call probes forward
        // and finds nothing, halving the window).
        for _ in 0..5 {
            let _ = consensus.head(key).await?;
        }
        let ks = consensus.get_cached_state(key);
        assert_eq!(
            ks.probe_window, DEFAULT_PROBE_WINDOW,
            "probe window should shrink back to {DEFAULT_PROBE_WINDOW}"
        );

        Ok(())
    }

    // ---------- Truncation and delayed deletion ----------

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn truncation_filters_by_low_water_mark() -> Result<(), ExternalError> {
        let consensus = make_s3_quorum_consensus();
        let key = "trunc_test";

        // Write seqnos 1..=5.
        for i in 1..=5 {
            let state = VersionedData {
                seqno: SeqNo(i),
                data: Bytes::from(format!("v{i}")),
            };
            let expected = if i == 1 { None } else { Some(SeqNo(i - 1)) };
            assert_eq!(
                consensus.compare_and_set(key, expected, state).await,
                Ok(CaSResult::Committed),
            );
        }

        // Scan returns all 5 versions.
        let results = consensus.scan(key, SeqNo(1), SCAN_ALL).await?;
        assert_eq!(results.len(), 5);

        // Truncate below seqno 3 (deletes seqnos 1, 2).
        consensus.truncate(key, SeqNo(3)).await?;

        // Scan from 0 should now skip seqnos below low_water_mark (3).
        let results = consensus.scan(key, SeqNo(0), SCAN_ALL).await?;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].seqno, SeqNo(3));
        assert_eq!(results[1].seqno, SeqNo(4));
        assert_eq!(results[2].seqno, SeqNo(5));

        // CAS targeting a truncated-but-not-yet-deleted seqno should return
        // 412 (object still exists within safety window) -> ExpectationMismatch
        // because the head is 5, not 2.
        let stale_write = VersionedData {
            seqno: SeqNo(3),
            data: Bytes::from("stale"),
        };
        assert_eq!(
            consensus
                .compare_and_set(key, Some(SeqNo(2)), stale_write)
                .await,
            Ok(CaSResult::ExpectationMismatch),
        );

        Ok(())
    }

    // ---------- Hint pointer tests ----------

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn hint_pointers_update_lazily() -> Result<(), ExternalError> {
        // Use shared MemS3Ops so we can inspect the stored hints.
        let mem0 = Arc::new(MemS3Ops::new());
        let mem1 = Arc::new(MemS3Ops::new());
        let mem2 = Arc::new(MemS3Ops::new());
        let consensus = S3QuorumConsensus::new([upcast(&mem0), upcast(&mem1), upcast(&mem2)]);

        let key = "hint_test";

        // Do HINT_UPDATE_INTERVAL CAS operations.
        for i in 1..=HINT_UPDATE_INTERVAL {
            let seqno = u64::try_from(i).expect("small constant");
            let state = VersionedData {
                seqno: SeqNo(seqno),
                data: Bytes::from("x"),
            };
            let expected = if i == 1 { None } else { Some(SeqNo(seqno - 1)) };
            assert_eq!(
                consensus.compare_and_set(key, expected, state).await,
                Ok(CaSResult::Committed),
            );
        }

        // After exactly HINT_UPDATE_INTERVAL CAS ops, _hint_head should be written.
        let hint_key = S3QuorumConsensus::hint_head_key(key);
        let hint = mem0.get(&hint_key).await?;
        assert!(
            hint.is_some(),
            "_hint_head should be written after {HINT_UPDATE_INTERVAL} CAS ops"
        );

        let hint_val =
            u64::from_le_bytes(<[u8; 8]>::try_from(&hint.unwrap()[..8]).expect("8 bytes"));
        let expected_seqno = u64::try_from(HINT_UPDATE_INTERVAL).expect("small constant");
        assert_eq!(hint_val, expected_seqno);

        // Verify cold-start reads the hint. Create a fresh consensus over
        // the SAME underlying storage but with no cache.
        let fresh_consensus = S3QuorumConsensus::new([upcast(&mem0), upcast(&mem1), upcast(&mem2)]);
        let head = fresh_consensus.head(key).await?;
        assert_eq!(head.map(|v| v.seqno), Some(SeqNo(expected_seqno)));

        Ok(())
    }

    // ---------- CAS correctness ----------

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn cas_duplicate_seqno_returns_mismatch() -> Result<(), ExternalError> {
        let consensus = make_s3_quorum_consensus();
        let key = "dup_test";

        // Write seqno 1.
        let v1 = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("first"),
        };
        assert_eq!(
            consensus.compare_and_set(key, None, v1).await,
            Ok(CaSResult::Committed),
        );

        // Attempt to write seqno 1 again with a different expected (simulating
        // a concurrent writer that already wrote seqno 1).
        // This uses expected=None which should fail because data exists.
        let v1_dup = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("duplicate"),
        };
        assert_eq!(
            consensus.compare_and_set(key, None, v1_dup).await,
            Ok(CaSResult::ExpectationMismatch),
        );

        // Write seqno 2 normally.
        let v2 = VersionedData {
            seqno: SeqNo(2),
            data: Bytes::from("second"),
        };
        assert_eq!(
            consensus.compare_and_set(key, Some(SeqNo(1)), v2).await,
            Ok(CaSResult::Committed),
        );

        // Verify head is correct.
        let head = consensus.head(key).await?;
        assert_eq!(head.map(|v| v.seqno), Some(SeqNo(2)));

        Ok(())
    }
}
