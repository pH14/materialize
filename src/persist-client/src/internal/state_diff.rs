// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
use mz_persist::location::{SeqNo, VersionedData};
use mz_persist_types::Codec64;
use mz_proto::TryFromProtoError;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::debug;

use crate::critical::CriticalReaderId;
use crate::internal::paths::PartialRollupKey;
use crate::internal::state::{
    CriticalReaderState, HollowBatch, LeasedReaderState, ProtoStateField, ProtoStateFieldDiffType,
    ProtoStateFieldDiffs, State, StateCollections, WriterState,
};
use crate::internal::trace::{FueledMergeRes, Trace};
use crate::read::LeasedReaderId;
use crate::write::WriterId;
use crate::{Metrics, PersistConfig};

use self::StateFieldValDiff::*;

#[derive(Clone, Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub enum StateFieldValDiff<V> {
    Insert(V),
    Update(V, V),
    Delete(V),
}

#[derive(Clone, Debug)]
#[cfg_attr(any(test, debug_assertions), derive(PartialEq))]
pub struct StateFieldDiff<K, V> {
    pub key: K,
    pub val: StateFieldValDiff<V>,
}

#[derive(Debug)]
#[cfg_attr(any(test, debug_assertions), derive(Clone, PartialEq))]
pub struct StateDiff<T> {
    pub(crate) applier_version: semver::Version,
    pub(crate) seqno_from: SeqNo,
    pub(crate) seqno_to: SeqNo,
    pub(crate) latest_rollup_key: PartialRollupKey,
    pub(crate) rollups: Vec<StateFieldDiff<SeqNo, PartialRollupKey>>,
    pub(crate) last_gc_req: Vec<StateFieldDiff<(), SeqNo>>,
    pub(crate) leased_readers: Vec<StateFieldDiff<LeasedReaderId, LeasedReaderState<T>>>,
    pub(crate) critical_readers: Vec<StateFieldDiff<CriticalReaderId, CriticalReaderState<T>>>,
    pub(crate) writers: Vec<StateFieldDiff<WriterId, WriterState>>,
    pub(crate) since: Vec<StateFieldDiff<(), Antichain<T>>>,
    pub(crate) spine: Vec<StateFieldDiff<HollowBatch<T>, ()>>,
}

impl<T: Timestamp + Codec64> StateDiff<T> {
    pub fn new(
        applier_version: semver::Version,
        seqno_from: SeqNo,
        seqno_to: SeqNo,
        latest_rollup_key: PartialRollupKey,
    ) -> Self {
        StateDiff {
            applier_version,
            seqno_from,
            seqno_to,
            latest_rollup_key,
            rollups: Vec::default(),
            last_gc_req: Vec::default(),
            leased_readers: Vec::default(),
            critical_readers: Vec::default(),
            writers: Vec::default(),
            since: Vec::default(),
            spine: Vec::default(),
        }
    }
}

impl<T: Timestamp + Lattice + Codec64> StateDiff<T> {
    pub fn from_diff<K, V, D>(from: &State<K, V, T, D>, to: &State<K, V, T, D>) -> Self {
        // Deconstruct from and to so we get a compile failure if new
        // fields are added.
        let State {
            applier_version: _,
            shard_id: from_shard_id,
            seqno: from_seqno,
            collections:
                StateCollections {
                    last_gc_req: from_last_gc_req,
                    rollups: from_rollups,
                    leased_readers: from_leased_readers,
                    critical_readers: from_critical_readers,
                    writers: from_writers,
                    trace: from_trace,
                },
            _phantom: _,
        } = from;
        let State {
            applier_version: to_applier_version,
            shard_id: to_shard_id,
            seqno: to_seqno,
            collections:
                StateCollections {
                    last_gc_req: to_last_gc_req,
                    rollups: to_rollups,
                    leased_readers: to_leased_readers,
                    critical_readers: to_critical_readers,
                    writers: to_writers,
                    trace: to_trace,
                },
            _phantom: _,
        } = to;
        assert_eq!(from_shard_id, to_shard_id);

        let (_, latest_rollup_key) = to.latest_rollup();
        let mut diffs = Self::new(
            to_applier_version.clone(),
            *from_seqno,
            *to_seqno,
            latest_rollup_key.clone(),
        );
        diff_field_single(from_last_gc_req, to_last_gc_req, &mut diffs.last_gc_req);
        diff_field_sorted_iter(from_rollups.iter(), to_rollups, &mut diffs.rollups);
        diff_field_sorted_iter(
            from_leased_readers.iter(),
            to_leased_readers,
            &mut diffs.leased_readers,
        );
        diff_field_sorted_iter(
            from_critical_readers.iter(),
            to_critical_readers,
            &mut diffs.critical_readers,
        );
        diff_field_sorted_iter(from_writers.iter(), to_writers, &mut diffs.writers);
        diff_field_single(from_trace.since(), to_trace.since(), &mut diffs.since);
        diff_field_spine(from_trace, to_trace, &mut diffs.spine);
        diffs
    }

    #[cfg(any(test, debug_assertions))]
    #[allow(dead_code)]
    pub fn validate_roundtrip<K, V, D>(
        metrics: &Metrics,
        from_state: &State<K, V, T, D>,
        diff: &Self,
        to_state: &State<K, V, T, D>,
    ) -> Result<(), String>
    where
        K: mz_persist_types::Codec + std::fmt::Debug,
        V: mz_persist_types::Codec + std::fmt::Debug,
        D: differential_dataflow::difference::Semigroup + Codec64,
    {
        use crate::internal::state::ProtoStateDiff;
        use mz_proto::RustType;
        use prost::Message;

        let mut roundtrip_state = from_state.clone(to_state.applier_version.clone());
        roundtrip_state.apply_diff(metrics, diff.clone())?;

        if &roundtrip_state != to_state {
            // The weird spacing in this format string is so they all line up
            // when printed out.
            return Err(format!("state didn't roundtrip\n  from_state {:?}\n  to_state   {:?}\n  rt_state   {:?}\n  diff       {:?}\n", from_state, to_state, roundtrip_state, diff));
        }

        let encoded_diff = diff.into_proto().encode_to_vec();
        let roundtrip_diff = Self::from_proto(
            ProtoStateDiff::decode(encoded_diff.as_slice()).map_err(|err| err.to_string())?,
        )
        .map_err(|err| err.to_string())?;

        if &roundtrip_diff != diff {
            // The weird spacing in this format string is so they all line up
            // when printed out.
            return Err(format!(
                "diff didn't roundtrip\n  diff    {:?}\n  rt_diff {:?}",
                diff, roundtrip_diff
            ));
        }

        Ok(())
    }
}

impl<K, V, T: Timestamp + Lattice + Codec64, D> State<K, V, T, D> {
    pub fn apply_encoded_diffs<'a, I: IntoIterator<Item = &'a VersionedData>>(
        &mut self,
        cfg: &PersistConfig,
        metrics: &Metrics,
        diffs: I,
    ) {
        let mut state_seqno = self.seqno;
        let diffs = diffs.into_iter().filter_map(move |x| {
            if x.seqno == state_seqno {
                // No-op.
                return None;
            }
            let diff = metrics
                .codecs
                .state_diff
                .decode(|| StateDiff::decode(&cfg.build_version, &x.data));
            assert_eq!(diff.seqno_from, state_seqno);
            state_seqno = diff.seqno_to;
            Some(diff)
        });
        self.apply_diffs(metrics, diffs);
    }
}

impl<K, V, T: Timestamp + Lattice, D> State<K, V, T, D> {
    pub fn apply_diffs<I: IntoIterator<Item = StateDiff<T>>>(
        &mut self,
        metrics: &Metrics,
        diffs: I,
    ) {
        for diff in diffs {
            // TODO: This could special-case batch apply for diffs where it's
            // more efficient (in particular, spine batches that hit the slow
            // path).
            let pretty_diff = format!("{:?}", diff);
            match self.apply_diff(metrics, diff) {
                Ok(()) => {}
                Err(err) => panic!(
                    "state diff should apply cleanly: {} diff {} state {:?}",
                    err, pretty_diff, self
                ),
            }
        }
    }

    // Intentionally not even pub(crate) because all callers should use
    // [Self::apply_diffs].
    fn apply_diff(&mut self, metrics: &Metrics, diff: StateDiff<T>) -> Result<(), String> {
        if self.seqno == diff.seqno_to {
            return Ok(());
        }
        if self.seqno != diff.seqno_from {
            return Err(format!(
                "could not apply diff {} -> {} to state {}",
                diff.seqno_from, diff.seqno_to, self.seqno
            ));
        }
        self.seqno = diff.seqno_to;

        // Deconstruct collections so we get a compile failure if new fields are
        // added.
        let StateCollections {
            last_gc_req,
            rollups,
            leased_readers,
            critical_readers,
            writers,
            trace,
        } = &mut self.collections;

        apply_diffs_map("rollups", diff.rollups, rollups)?;
        apply_diffs_single("last_gc_req", diff.last_gc_req, last_gc_req)?;
        apply_diffs_map("leased_readers", diff.leased_readers, leased_readers)?;
        apply_diffs_map("critical_readers", diff.critical_readers, critical_readers)?;
        apply_diffs_map("writers", diff.writers, writers)?;

        for x in diff.since {
            match x.val {
                Update(from, to) => {
                    if trace.since() != &from {
                        return Err(format!(
                            "since update didn't match: {:?} vs {:?}",
                            self.collections.trace.since(),
                            &from
                        ));
                    }
                    trace.downgrade_since(&to);
                }
                Insert(_) => return Err("cannot insert since field".to_string()),
                Delete(_) => return Err("cannot delete since field".to_string()),
            }
        }
        apply_diffs_spine(metrics, diff.spine, trace)?;

        // There's various sanity checks that this method could run (e.g. since,
        // upper, seqno_since, etc don't regress or that diff.latest_rollup ==
        // state.rollups.last()), are they a good idea? On one hand, I like
        // sanity checks, other the other, one of the goals here is to keep
        // apply logic as straightforward and unchanging as possible.
        Ok(())
    }
}

fn diff_field_single<T: PartialEq + Clone>(
    from: &T,
    to: &T,
    diffs: &mut Vec<StateFieldDiff<(), T>>,
) {
    // This could use the `diff_field_sorted_iter(once(from), once(to), diffs)`
    // general impl, but we just do the obvious thing.
    if from != to {
        diffs.push(StateFieldDiff {
            key: (),
            val: Update(from.clone(), to.clone()),
        })
    }
}

fn apply_diffs_single<X: PartialEq + Debug>(
    name: &str,
    diffs: Vec<StateFieldDiff<(), X>>,
    single: &mut X,
) -> Result<(), String> {
    for diff in diffs {
        apply_diff_single(name, diff, single)?;
    }
    Ok(())
}

fn apply_diff_single<X: PartialEq + Debug>(
    name: &str,
    diff: StateFieldDiff<(), X>,
    single: &mut X,
) -> Result<(), String> {
    match diff.val {
        Update(from, to) => {
            if single != &from {
                return Err(format!(
                    "{} update didn't match: {:?} vs {:?}",
                    name, single, &from
                ));
            }
            *single = to
        }
        Insert(_) => return Err(format!("cannot insert {} field", name)),
        Delete(_) => return Err(format!("cannot delete {} field", name)),
    }
    Ok(())
}

fn diff_field_sorted_iter<'a, K, V, IF, IT>(from: IF, to: IT, diffs: &mut Vec<StateFieldDiff<K, V>>)
where
    K: Ord + Clone + 'a,
    V: PartialEq + Clone + 'a,
    IF: IntoIterator<Item = (&'a K, &'a V)>,
    IT: IntoIterator<Item = (&'a K, &'a V)>,
{
    let (mut from, mut to) = (from.into_iter(), to.into_iter());
    let (mut f, mut t) = (from.next(), to.next());
    loop {
        match (f, t) {
            (None, None) => break,
            (Some((fk, fv)), Some((tk, tv))) => match fk.cmp(tk) {
                Ordering::Less => {
                    diffs.push(StateFieldDiff {
                        key: fk.clone(),
                        val: Delete(fv.clone()),
                    });
                    let f_next = from.next();
                    debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                    f = f_next;
                }
                Ordering::Greater => {
                    diffs.push(StateFieldDiff {
                        key: tk.clone(),
                        val: Insert(tv.clone()),
                    });
                    let t_next = to.next();
                    debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                    t = t_next;
                }
                Ordering::Equal => {
                    // TODO: regression test for this if, I missed it in the
                    // original impl :)
                    if fv != tv {
                        diffs.push(StateFieldDiff {
                            key: fk.clone(),
                            val: Update(fv.clone(), tv.clone()),
                        });
                    }
                    let f_next = from.next();
                    debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                    f = f_next;
                    let t_next = to.next();
                    debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                    t = t_next;
                }
            },
            (None, Some((tk, tv))) => {
                diffs.push(StateFieldDiff {
                    key: tk.clone(),
                    val: Insert(tv.clone()),
                });
                let t_next = to.next();
                debug_assert!(t_next.as_ref().map_or(true, |(tk_next, _)| tk_next > &tk));
                t = t_next;
            }
            (Some((fk, fv)), None) => {
                diffs.push(StateFieldDiff {
                    key: fk.clone(),
                    val: Delete(fv.clone()),
                });
                let f_next = from.next();
                debug_assert!(f_next.as_ref().map_or(true, |(fk_next, _)| fk_next > &fk));
                f = f_next;
            }
        }
    }
}

fn apply_diffs_map<K: Ord, V: PartialEq + Debug>(
    name: &str,
    diffs: Vec<StateFieldDiff<K, V>>,
    map: &mut BTreeMap<K, V>,
) -> Result<(), String> {
    for diff in diffs {
        apply_diff_map(name, diff, map)?;
    }
    Ok(())
}

// This might leave state in an invalid (umm) state when returning an error. The
// caller ultimately ends up panic'ing on error, but if that changes, we might
// want to revisit this.
fn apply_diff_map<K: Ord, V: PartialEq + Debug>(
    name: &str,
    diff: StateFieldDiff<K, V>,
    map: &mut BTreeMap<K, V>,
) -> Result<(), String> {
    match diff.val {
        Insert(to) => {
            let prev = map.insert(diff.key, to);
            if prev != None {
                return Err(format!("{} insert found existing value: {:?}", name, prev));
            }
        }
        Update(from, to) => {
            let prev = map.insert(diff.key, to);
            if prev.as_ref() != Some(&from) {
                return Err(format!(
                    "{} update didn't match: {:?} vs {:?}",
                    name,
                    prev,
                    Some(from),
                ));
            }
        }
        Delete(from) => {
            let prev = map.remove(&diff.key);
            if prev.as_ref() != Some(&from) {
                return Err(format!(
                    "{} delete didn't match: {:?} vs {:?}",
                    name,
                    prev,
                    Some(from),
                ));
            }
        }
    };
    Ok(())
}

fn diff_field_spine<T: Timestamp + Lattice>(
    from: &Trace<T>,
    to: &Trace<T>,
    diffs: &mut Vec<StateFieldDiff<HollowBatch<T>, ()>>,
) {
    let from_batches = from.batches().into_iter().map(|b| (b, &()));
    let to_batches = to.batches().into_iter().map(|b| (b, &()));
    diff_field_sorted_iter(from_batches, to_batches, diffs);
}

// This might leave state in an invalid (umm) state when returning an error. The
// caller ultimately ends up panic'ing on error, but if that changes, we might
// want to revisit this.
fn apply_diffs_spine<T: Timestamp + Lattice>(
    metrics: &Metrics,
    diffs: Vec<StateFieldDiff<HollowBatch<T>, ()>>,
    trace: &mut Trace<T>,
) -> Result<(), String> {
    match &diffs[..] {
        // Fast-path: no diffs.
        [] => return Ok(()),

        // Fast-path: batch insert.
        [StateFieldDiff {
            key,
            val: StateFieldValDiff::Insert(()),
        }] => {
            // Ignore merge_reqs because whichever process generated this diff is
            // assigned the work.
            let _merge_reqs = trace.push_batch(key.clone());
            metrics.state.apply_spine_fast_path.inc();
            return Ok(());
        }

        // Fast-path: batch insert with both new and most recent batch empty.
        // Spine will happily merge these empty batches together without a call
        // out to compaction.
        [StateFieldDiff {
            key: del,
            val: StateFieldValDiff::Delete(()),
        }, StateFieldDiff {
            key: ins,
            val: StateFieldValDiff::Insert(()),
        }] => {
            if del.parts.len() == 0
                && ins.parts.len() == 0
                && del.desc.lower() == ins.desc.lower()
                && PartialOrder::less_than(del.desc.upper(), ins.desc.upper())
            {
                // Ignore merge_reqs because whichever process generated this diff is
                // assigned the work.
                let _merge_reqs = trace.push_batch(HollowBatch {
                    desc: Description::new(
                        del.desc.upper().clone(),
                        ins.desc.upper().clone(),
                        // `keys.len() == 0` for both `del` and `ins` means we
                        // don't have to think about what the compaction
                        // frontier is for these batches (nothing in them, so nothing could have been compacted.
                        Antichain::from_elem(T::minimum()),
                    ),
                    parts: vec![],
                    len: 0,
                    runs: vec![],
                });
                metrics.state.apply_spine_fast_path.inc();
                return Ok(());
            }
        }
        // Fall-through
        _ => {}
    }

    // Fast-path: compaction
    if let Some((_inputs, output)) = sniff_compaction(&diffs) {
        let res = FueledMergeRes { output };
        // We can't predict how spine will arrange the batches when it's
        // hydrated. This means that something that is maintaining a Spine
        // starting at some seqno may not exactly match something else
        // maintaining the same spine starting at a different seqno. (Plus,
        // maybe these aren't even on the same version of the code and we've
        // changed the spine logic.) Because apply_merge_res is strict,
        // we're not _guaranteed_ that we can apply a compaction response
        // that was generated elsewhere. Most of the time we can, though, so
        // count the good ones and fall back to the slow path below when we
        // can't.
        if trace.apply_merge_res(&res).applied() {
            // Maybe return the replaced batches from apply_merge_res and verify
            // that they match _inputs?
            metrics.state.apply_spine_fast_path.inc();
            return Ok(());
        }

        // Otherwise, try our lenient application of a compaction result.
        let mut batches = Vec::new();
        trace.map_batches(|b| batches.push(b.clone()));
        match apply_compaction_lenient(metrics, batches, &res.output) {
            Ok(batches) => {
                let mut new_trace = Trace::default();
                new_trace.downgrade_since(trace.since());
                for batch in batches {
                    // Ignore merge_reqs because whichever process generated
                    // this diff is assigned the work.
                    let _merge_reqs = new_trace.push_batch(batch.clone());
                }
                *trace = new_trace;
                metrics.state.apply_spine_slow_path_lenient.inc();
                return Ok(());
            }
            Err(err) => {
                return Err(format!(
                    "lenient compaction result apply unexpectedly failed: {}",
                    err
                ))
            }
        }
    }

    // Something complicated is going on, so reconstruct the Trace from scratch.
    metrics.state.apply_spine_slow_path.inc();
    debug!(
        "apply_diffs_spine didn't hit a fast-path diffs={:?} trace={:?}",
        diffs, trace
    );

    let batches = {
        let mut batches = BTreeMap::new();
        trace.map_batches(|b| assert!(batches.insert(b.clone(), ()).is_none()));
        apply_diffs_map("spine", diffs.clone(), &mut batches).map(|_ok| batches)
    };

    let batches = match batches {
        Ok(batches) => batches,
        Err(err) => {
            metrics
                .state
                .apply_spine_slow_path_with_reconstruction
                .inc();
            debug!(
                "apply_diffs_spines could not apply diffs directly to existing trace batches: {}. diffs={:?} trace={:?}",
                err, diffs, trace
            );
            // if we couldn't apply our diffs directly to our trace's batches, we can
            // try one more trick: reconstruct a new spine with our existing batches,
            // in an attempt to create different merges than we currently have. then,
            // we can try to apply our diffs on top of these new (potentially) merged
            // batches.
            let mut reconstructed_spine = Trace::default();
            trace.map_batches(|b| {
                let _merge_reqs = reconstructed_spine.push_batch(b.clone());
            });

            let mut batches = BTreeMap::new();
            reconstructed_spine.map_batches(|b| assert!(batches.insert(b.clone(), ()).is_none()));
            apply_diffs_map("spine", diffs, &mut batches)?;
            batches
        }
    };

    let mut new_trace = Trace::default();
    new_trace.downgrade_since(trace.since());
    for (batch, ()) in batches {
        // Ignore merge_reqs because whichever process generated this diff is
        // assigned the work.
        let _merge_reqs = new_trace.push_batch(batch);
    }
    *trace = new_trace;
    Ok(())
}

// TODO: Instead of trying to sniff out a compaction from diffs, should we just
// be explicit?
#[allow(dead_code)]
fn sniff_compaction<'a, T: Timestamp + Lattice>(
    diffs: &'a [StateFieldDiff<HollowBatch<T>, ()>],
) -> Option<(Vec<&'a HollowBatch<T>>, HollowBatch<T>)> {
    // Compaction always produces exactly one output batch (with possibly many
    // parts, but we get one Insert for the whole batch.
    let mut inserts = diffs.iter().flat_map(|x| match x.val {
        StateFieldValDiff::Insert(()) => Some(&x.key),
        _ => None,
    });
    let compaction_output = match inserts.next() {
        Some(x) => x,
        None => return None,
    };
    if let Some(_) = inserts.next() {
        return None;
    }

    // Grab all deletes and sanity check that there are no updates.
    let mut compaction_inputs = Vec::with_capacity(diffs.len() - 1);
    for diff in diffs.iter() {
        match diff.val {
            StateFieldValDiff::Delete(()) => {
                compaction_inputs.push(&diff.key);
            }
            StateFieldValDiff::Insert(()) => {}
            StateFieldValDiff::Update((), ()) => {
                // Fall through to let the general case create the error
                // message.
                return None;
            }
        }
    }

    Some((compaction_inputs, compaction_output.clone()))
}

/// Apply a compaction diff that doesn't exactly line up with the set of
/// HollowBatches.
///
/// Because of the way Spine internally optimizes only _some_ empty batches
/// (immediately merges them in), we can end up in a situation where a
/// compaction res applied on another copy of state, but when we replay all of
/// the state diffs against a new Spine locally, it merges empty batches
/// differently in-mem and we can't exactly apply the compaction diff. Example:
///
/// - compact: [1,2),[2,3) -> [1,3)
/// - this spine: [0,2),[2,3) (0,1 is empty)
///
/// Ideally, we'd figure out a way to avoid this, but nothing immediately comes
/// to mind. In the meantime, force the application (otherwise the shard is
/// stuck and we can't do anything with it) by manually splitting the empty
/// batch back out. For the example above:
///
/// - [0,1),[1,3) (0,1 is empty)
///
/// This can only happen when the batch needing to be split is empty, so error
/// out if it isn't because that means something unexpected is going on.
///
/// TODO: This implementation is certainly not correct if T is actually only
/// partially ordered.
fn apply_compaction_lenient<'a, T: Timestamp + Lattice>(
    metrics: &Metrics,
    mut trace: Vec<HollowBatch<T>>,
    replacement: &'a HollowBatch<T>,
) -> Result<Vec<HollowBatch<T>>, String> {
    let mut overlapping_batches = Vec::new();
    trace.retain(|b| {
        let before_replacement = PartialOrder::less_equal(b.desc.upper(), replacement.desc.lower());
        let after_replacement = PartialOrder::less_equal(replacement.desc.upper(), b.desc.lower());
        let overlaps_replacement = !(before_replacement || after_replacement);
        if overlaps_replacement {
            overlapping_batches.push(b.clone());
            false
        } else {
            true
        }
    });

    {
        let first_overlapping_batch = match overlapping_batches.first() {
            Some(x) => x,
            None => return Err("replacement didn't overlap any batches".into()),
        };
        if PartialOrder::less_than(
            first_overlapping_batch.desc.lower(),
            replacement.desc.lower(),
        ) {
            if first_overlapping_batch.len > 0 {
                return Err(format!(
                    "overlapping batch was unexpectedly non-empty: {:?}",
                    first_overlapping_batch
                ));
            }
            let desc = Description::new(
                first_overlapping_batch.desc.lower().clone(),
                replacement.desc.lower().clone(),
                first_overlapping_batch.desc.since().clone(),
            );
            trace.push(HollowBatch {
                desc,
                parts: Vec::new(),
                len: 0,
                runs: Vec::new(),
            });
            metrics.state.apply_spine_slow_path_lenient_adjustment.inc();
        }
    }

    {
        let last_overlapping_batch = match overlapping_batches.last() {
            Some(x) => x,
            None => return Err("replacement didn't overlap any batches".into()),
        };
        if PartialOrder::less_than(
            replacement.desc.upper(),
            last_overlapping_batch.desc.upper(),
        ) {
            if last_overlapping_batch.len > 0 {
                return Err(format!(
                    "overlapping batch was unexpectedly non-empty: {:?}",
                    last_overlapping_batch
                ));
            }
            let desc = Description::new(
                replacement.desc.upper().clone(),
                last_overlapping_batch.desc.upper().clone(),
                last_overlapping_batch.desc.since().clone(),
            );
            trace.push(HollowBatch {
                desc,
                parts: Vec::new(),
                len: 0,
                runs: Vec::new(),
            });
            metrics.state.apply_spine_slow_path_lenient_adjustment.inc();
        }
    }
    trace.push(replacement.clone());

    // We just inserted stuff at the end, so re-sort them into place.
    trace.sort_by(|a, b| a.desc.lower().elements().cmp(b.desc.lower().elements()));

    // This impl is a touch complex, so sanity check our work.
    let mut expected_lower = &Antichain::from_elem(T::minimum());
    for b in trace.iter() {
        if b.desc.lower() != expected_lower {
            return Err(format!(
                "lower {:?} did not match expected {:?}: {:?}",
                b.desc.lower(),
                expected_lower,
                trace
            ));
        }
        expected_lower = b.desc.upper();
    }
    Ok(trace)
}

impl ProtoStateFieldDiffs {
    pub fn push_data(&mut self, mut data: Vec<u8>) {
        self.data_lens.push(u64::cast_from(data.len()));
        self.data_bytes.append(&mut data);
    }

    pub fn iter<'a>(&'a self) -> ProtoStateFieldDiffsIter<'a> {
        let len = self.fields.len();
        assert_eq!(self.diff_types.len(), len);

        ProtoStateFieldDiffsIter {
            len,
            diff_idx: 0,
            data_idx: 0,
            data_offset: 0,
            diffs: self,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.fields.len() != self.diff_types.len() {
            return Err(format!(
                "fields {} and diff_types {} lengths disagree",
                self.fields.len(),
                self.diff_types.len()
            ));
        }

        let mut expected_data_slices = 0;
        for diff_type in self.diff_types.iter() {
            // We expect one for the key.
            expected_data_slices += 1;
            // And 1 or 2 for val depending on the diff type.
            match ProtoStateFieldDiffType::from_i32(*diff_type) {
                Some(ProtoStateFieldDiffType::Insert) => expected_data_slices += 1,
                Some(ProtoStateFieldDiffType::Update) => expected_data_slices += 2,
                Some(ProtoStateFieldDiffType::Delete) => expected_data_slices += 1,
                None => return Err(format!("unknown diff_type {}", diff_type)),
            }
        }
        if expected_data_slices != self.data_lens.len() {
            return Err(format!(
                "expected {} data slices got {}",
                expected_data_slices,
                self.data_lens.len()
            ));
        }

        let expected_data_bytes = usize::cast_from(self.data_lens.iter().copied().sum::<u64>());
        if expected_data_bytes != self.data_bytes.len() {
            return Err(format!(
                "expected {} data bytes got {}",
                expected_data_bytes,
                self.data_bytes.len()
            ));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ProtoStateFieldDiff<'a> {
    pub key: &'a [u8],
    pub diff_type: ProtoStateFieldDiffType,
    pub from: &'a [u8],
    pub to: &'a [u8],
}

pub struct ProtoStateFieldDiffsIter<'a> {
    len: usize,
    diff_idx: usize,
    data_idx: usize,
    data_offset: usize,
    diffs: &'a ProtoStateFieldDiffs,
}

impl<'a> Iterator for ProtoStateFieldDiffsIter<'a> {
    type Item = Result<(ProtoStateField, ProtoStateFieldDiff<'a>), TryFromProtoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.diff_idx >= self.len {
            return None;
        }
        let mut next_data = || {
            let start = self.data_offset;
            let end = start + usize::cast_from(self.diffs.data_lens[self.data_idx]);
            let data = &self.diffs.data_bytes[start..end];
            self.data_idx += 1;
            self.data_offset = end;
            data
        };
        let field = match ProtoStateField::from_i32(self.diffs.fields[self.diff_idx]) {
            Some(x) => x,
            None => {
                return Some(Err(TryFromProtoError::unknown_enum_variant(format!(
                    "ProtoStateField({})",
                    self.diffs.fields[self.diff_idx]
                ))))
            }
        };
        let diff_type =
            match ProtoStateFieldDiffType::from_i32(self.diffs.diff_types[self.diff_idx]) {
                Some(x) => x,
                None => {
                    return Some(Err(TryFromProtoError::unknown_enum_variant(format!(
                        "ProtoStateFieldDiffType({})",
                        self.diffs.diff_types[self.diff_idx]
                    ))))
                }
            };
        let key = next_data();
        let (from, to): (&[u8], &[u8]) = match diff_type {
            ProtoStateFieldDiffType::Insert => (&[], next_data()),
            ProtoStateFieldDiffType::Update => (next_data(), next_data()),
            ProtoStateFieldDiffType::Delete => (next_data(), &[]),
        };
        let diff = ProtoStateFieldDiff {
            key,
            diff_type,
            from,
            to,
        };
        self.diff_idx += 1;
        Some(Ok((field, diff)))
    }
}

#[cfg(test)]
mod tests {
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;

    use super::*;

    #[test]
    fn apply_lenient() {
        #[track_caller]
        fn testcase(
            replacement: (u64, u64, u64, usize),
            spine: &[(u64, u64, u64, usize)],
            expected: Result<&[(u64, u64, u64, usize)], &str>,
        ) {
            fn batch(x: &(u64, u64, u64, usize)) -> HollowBatch<u64> {
                let (lower, upper, since, len) = x;
                let desc = Description::new(
                    Antichain::from_elem(*lower),
                    Antichain::from_elem(*upper),
                    Antichain::from_elem(*since),
                );
                HollowBatch {
                    desc,
                    parts: Vec::new(),
                    len: *len,
                    runs: Vec::new(),
                }
            }
            let replacement = batch(&replacement);
            let batches = spine.iter().map(batch).collect::<Vec<_>>();

            let metrics = Metrics::new(
                &PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
                &MetricsRegistry::new(),
            );
            let actual = apply_compaction_lenient(&metrics, batches, &replacement);
            let expected = match expected {
                Ok(batches) => Ok(batches.iter().map(batch).collect::<Vec<_>>()),
                Err(err) => Err(err.to_owned()),
            };
            assert_eq!(actual, expected);
        }

        // Exact swap of N batches
        testcase(
            (0, 3, 0, 100),
            &[(0, 1, 0, 0), (1, 2, 0, 0), (2, 3, 0, 0)],
            Ok(&[(0, 3, 0, 100)]),
        );

        // Swap out the middle of a batch
        testcase(
            (1, 2, 0, 100),
            &[(0, 3, 0, 0)],
            Ok(&[(0, 1, 0, 0), (1, 2, 0, 100), (2, 3, 0, 0)]),
        );

        // Split batch at replacement lower
        testcase(
            (2, 4, 0, 100),
            &[(0, 3, 0, 0), (3, 4, 0, 0)],
            Ok(&[(0, 2, 0, 0), (2, 4, 0, 100)]),
        );

        // Err: split batch at replacement lower not empty
        testcase(
            (2, 4, 0, 100),
            &[(0, 3, 0, 1), (3, 4, 0, 0)],
            Err("overlapping batch was unexpectedly non-empty: HollowBatch { desc: Description { lower: Antichain { elements: [0] }, upper: Antichain { elements: [3] }, since: Antichain { elements: [0] } }, parts: [], len: 1, runs: [] }")
        );

        // Split batch at replacement lower (untouched batch before the split one)
        testcase(
            (2, 4, 0, 100),
            &[(0, 1, 0, 0), (1, 3, 0, 0), (3, 4, 0, 0)],
            Ok(&[(0, 1, 0, 0), (1, 2, 0, 0), (2, 4, 0, 100)]),
        );

        // Split batch at replacement lower (since is preserved)
        testcase(
            (2, 4, 0, 100),
            &[(0, 3, 200, 0), (3, 4, 0, 0)],
            Ok(&[(0, 2, 200, 0), (2, 4, 0, 100)]),
        );

        // Split batch at replacement upper
        testcase(
            (0, 2, 0, 100),
            &[(0, 1, 0, 0), (1, 4, 0, 0)],
            Ok(&[(0, 2, 0, 100), (2, 4, 0, 0)]),
        );

        // Err: split batch at replacement upper not empty
        testcase(
            (0, 2, 0, 100),
            &[(0, 1, 0, 0), (1, 4, 0, 1)],
            Err("overlapping batch was unexpectedly non-empty: HollowBatch { desc: Description { lower: Antichain { elements: [1] }, upper: Antichain { elements: [4] }, since: Antichain { elements: [0] } }, parts: [], len: 1, runs: [] }")
        );

        // Split batch at replacement upper (untouched batch after the split one)
        testcase(
            (0, 2, 0, 100),
            &[(0, 1, 0, 0), (1, 3, 0, 0), (3, 4, 0, 0)],
            Ok(&[(0, 2, 0, 100), (2, 3, 0, 0), (3, 4, 0, 0)]),
        );

        // Split batch at replacement upper (since is preserved)
        testcase(
            (0, 2, 0, 100),
            &[(0, 1, 0, 0), (1, 4, 200, 0)],
            Ok(&[(0, 2, 0, 100), (2, 4, 200, 0)]),
        );

        // Split batch at replacement lower and upper
        testcase(
            (2, 6, 0, 100),
            &[(0, 3, 0, 0), (3, 5, 0, 0), (5, 8, 0, 0)],
            Ok(&[(0, 2, 0, 0), (2, 6, 0, 100), (6, 8, 0, 0)]),
        );

        // Replacement doesn't overlap (after)
        testcase(
            (2, 3, 0, 100),
            &[(0, 1, 0, 0)],
            Err("replacement didn't overlap any batches"),
        );

        // Replacement doesn't overlap (before, though this would never happen in practice)
        testcase(
            (2, 3, 0, 100),
            &[(4, 5, 0, 0)],
            Err("replacement didn't overlap any batches"),
        );
    }
}
