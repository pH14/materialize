#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Result, anyhow, ensure};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use timely::progress::Antichain;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Update {
    pub key: String,
    pub time: u64,
    pub diff: i64,
}

impl Update {
    pub fn new(key: impl Into<String>, time: u64, diff: i64) -> Self {
        Self {
            key: key.into(),
            time,
            diff,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum SnapshotError {
    Since(u64),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum CompareAndAppendResult {
    Committed,
    UpperMismatch { expected: u64, current: u64 },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ScenarioObservation {
    WriterOpened { upper: u64 },
    ReaderOpened { since: u64 },
    CompareAndAppend(CompareAndAppendResult),
    SinceDowngraded { since: u64 },
    Snapshot(Result<Vec<Update>, SnapshotError>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ScenarioOp {
    OpenWriter {
        writer: String,
    },
    OpenReader {
        reader: String,
    },
    CompareAndAppend {
        writer: String,
        updates: Vec<Update>,
        expected_upper: u64,
        new_upper: u64,
    },
    DowngradeSince {
        reader: String,
        new_since: u64,
    },
    Snapshot {
        reader: String,
        as_of: u64,
    },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ScenarioHistoryOp {
    CompareAndAppend {
        writer: String,
        updates: Vec<Update>,
        expected_upper: u64,
        new_upper: u64,
    },
    DowngradeSince {
        reader: String,
        new_since: u64,
    },
    Snapshot {
        reader: String,
        as_of: u64,
    },
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum ScenarioFault {
    PartitionStorage,
    RepairStorage,
}

impl ScenarioOp {
    pub fn open_writer(writer: impl Into<String>) -> Self {
        Self::OpenWriter {
            writer: writer.into(),
        }
    }

    pub fn open_reader(reader: impl Into<String>) -> Self {
        Self::OpenReader {
            reader: reader.into(),
        }
    }

    pub fn compare_and_append(
        writer: impl Into<String>,
        updates: Vec<Update>,
        expected_upper: u64,
        new_upper: u64,
    ) -> Self {
        Self::CompareAndAppend {
            writer: writer.into(),
            updates,
            expected_upper,
            new_upper,
        }
    }

    pub fn downgrade_since(reader: impl Into<String>, new_since: u64) -> Self {
        Self::DowngradeSince {
            reader: reader.into(),
            new_since,
        }
    }

    pub fn snapshot(reader: impl Into<String>, as_of: u64) -> Self {
        Self::Snapshot {
            reader: reader.into(),
            as_of,
        }
    }

    pub fn history_op(&self) -> Option<ScenarioHistoryOp> {
        match self {
            ScenarioOp::OpenWriter { writer: _writer } => None,
            ScenarioOp::OpenReader { reader: _reader } => None,
            ScenarioOp::CompareAndAppend {
                writer,
                updates,
                expected_upper,
                new_upper,
            } => Some(ScenarioHistoryOp::CompareAndAppend {
                writer: writer.clone(),
                updates: updates.clone(),
                expected_upper: *expected_upper,
                new_upper: *new_upper,
            }),
            ScenarioOp::DowngradeSince { reader, new_since } => {
                Some(ScenarioHistoryOp::DowngradeSince {
                    reader: reader.clone(),
                    new_since: *new_since,
                })
            }
            ScenarioOp::Snapshot { reader, as_of } => Some(ScenarioHistoryOp::Snapshot {
                reader: reader.clone(),
                as_of: *as_of,
            }),
        }
    }

    pub fn enters_linearizability_history(&self) -> bool {
        self.history_op().is_some()
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScenarioThread {
    Writer0,
    Writer1,
    Reader0,
}

impl ScenarioThread {
    pub const ALL: [Self; 3] = [Self::Writer0, Self::Writer1, Self::Reader0];
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ConcurrentScenarioProgram {
    pub writer0: Vec<ScenarioOp>,
    pub writer1: Vec<ScenarioOp>,
    pub reader0: Vec<ScenarioOp>,
}

pub fn linearizability_smoke_program() -> ConcurrentScenarioProgram {
    ConcurrentScenarioProgram {
        writer0: vec![
            ScenarioOp::open_writer("w0"),
            ScenarioOp::compare_and_append("w0", vec![Update::new("a", 0, 1)], 0, 1),
        ],
        writer1: vec![
            ScenarioOp::open_writer("w1"),
            ScenarioOp::compare_and_append("w1", vec![Update::new("b", 1, 1)], 1, 2),
        ],
        reader0: vec![
            ScenarioOp::open_reader("r0"),
            ScenarioOp::snapshot("r0", 0),
            ScenarioOp::downgrade_since("r0", 1),
            ScenarioOp::snapshot("r0", 1),
        ],
    }
}

pub fn end_to_end_smoke_ops() -> Vec<ScenarioOp> {
    vec![
        ScenarioOp::open_writer("w0"),
        ScenarioOp::open_reader("r0"),
        ScenarioOp::compare_and_append(
            "w0",
            vec![Update::new("a", 0, 1), Update::new("b", 1, 1)],
            0,
            2,
        ),
        ScenarioOp::snapshot("r0", 1),
        ScenarioOp::downgrade_since("r0", 1),
        ScenarioOp::snapshot("r0", 0),
        ScenarioOp::compare_and_append(
            "w0",
            vec![Update::new("a", 2, -1), Update::new("c", 2, 1)],
            2,
            3,
        ),
        ScenarioOp::snapshot("r0", 2),
        ScenarioOp::open_reader("r1"),
        ScenarioOp::downgrade_since("r1", 2),
        ScenarioOp::snapshot("r1", 1),
        ScenarioOp::downgrade_since("r0", 2),
        ScenarioOp::snapshot("r1", 2),
    ]
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct ShardOracle {
    upper: u64,
    since: u64,
    writers: BTreeSet<String>,
    readers: BTreeMap<String, u64>,
    updates: Vec<Update>,
}

impl ShardOracle {
    pub fn upper(&self) -> u64 {
        self.upper
    }

    pub fn since(&self) -> u64 {
        self.since
    }

    pub fn apply(&mut self, op: &ScenarioOp) -> Result<ScenarioObservation> {
        match op {
            ScenarioOp::OpenWriter { writer } => {
                ensure!(
                    self.writers.insert(writer.clone()),
                    "writer {writer} already exists"
                );
                Ok(ScenarioObservation::WriterOpened { upper: self.upper })
            }
            ScenarioOp::OpenReader { reader } => {
                ensure!(
                    self.readers.insert(reader.clone(), self.since).is_none(),
                    "reader {reader} already exists"
                );
                Ok(ScenarioObservation::ReaderOpened { since: self.since })
            }
            ScenarioOp::CompareAndAppend {
                writer,
                updates,
                expected_upper,
                new_upper,
            } => {
                ensure!(self.writers.contains(writer), "unknown writer {writer}");
                ensure!(
                    expected_upper <= new_upper,
                    "invalid append bounds [{expected_upper}, {new_upper})"
                );
                for update in updates {
                    ensure!(
                        *expected_upper <= update.time && update.time < *new_upper,
                        "update {:?} is outside [{expected_upper}, {new_upper})",
                        update
                    );
                }
                if *expected_upper != self.upper {
                    return Ok(ScenarioObservation::CompareAndAppend(
                        CompareAndAppendResult::UpperMismatch {
                            expected: *expected_upper,
                            current: self.upper,
                        },
                    ));
                }
                self.updates.extend(updates.iter().cloned());
                self.upper = *new_upper;
                Ok(ScenarioObservation::CompareAndAppend(
                    CompareAndAppendResult::Committed,
                ))
            }
            ScenarioOp::DowngradeSince { reader, new_since } => {
                let since = self
                    .readers
                    .get_mut(reader)
                    .ok_or_else(|| anyhow!("unknown reader {reader}"))?;
                ensure!(
                    *since <= *new_since,
                    "reader {reader} attempted to regress since from {} to {new_since}",
                    *since
                );
                *since = *new_since;
                let reader_since = *since;
                let shard_since = self
                    .readers
                    .values()
                    .copied()
                    .min()
                    .unwrap_or(self.since);
                self.since = shard_since;
                Ok(ScenarioObservation::SinceDowngraded { since: reader_since })
            }
            ScenarioOp::Snapshot { reader, as_of } => {
                ensure!(self.readers.contains_key(reader), "unknown reader {reader}");
                if *as_of < self.since {
                    return Ok(ScenarioObservation::Snapshot(Err(SnapshotError::Since(
                        self.since,
                    ))));
                }
                ensure!(
                    *as_of < self.upper,
                    "snapshot at as_of={} is not yet complete because shard upper is {}",
                    as_of,
                    self.upper
                );
                Ok(ScenarioObservation::Snapshot(Ok(self.snapshot(*as_of))))
            }
        }
    }

    fn snapshot(&self, as_of: u64) -> Vec<Update> {
        let mut diffs = BTreeMap::<String, i64>::new();
        for update in &self.updates {
            if update.time <= as_of {
                *diffs.entry(update.key.clone()).or_insert(0) += update.diff;
            }
        }
        diffs.into_iter()
            .filter_map(|(key, diff)| (diff != 0).then(|| Update::new(key, as_of, diff)))
            .collect()
    }
}

#[derive(Debug)]
pub struct ScenarioRunner {
    client: PersistClient,
    shard_id: ShardId,
    oracle: ShardOracle,
    writers: BTreeMap<String, WriteHandle<String, (), u64, i64>>,
    readers: BTreeMap<String, ReadHandle<String, (), u64, i64>>,
}

impl ScenarioRunner {
    pub fn from_client(client: PersistClient) -> Self {
        Self {
            client,
            shard_id: ShardId::new(),
            oracle: ShardOracle::default(),
            writers: BTreeMap::new(),
            readers: BTreeMap::new(),
        }
    }

    pub async fn new_in_mem() -> Result<Self> {
        let mut cache = PersistClientCache::new_no_metrics();
        cache.cfg.compaction_enabled = true;
        let client = cache.open(PersistLocation::new_in_mem()).await?;
        Ok(Self::from_client(client))
    }

    pub async fn apply_and_assert(&mut self, op: ScenarioOp) -> Result<ScenarioObservation> {
        let expected = self.oracle.apply(&op)?;
        let actual = self.apply_real(&op).await?;
        assert_eq!(actual, expected, "op {op:?}");
        Ok(actual)
    }

    pub async fn run_and_assert(
        &mut self,
        ops: impl IntoIterator<Item = ScenarioOp>,
    ) -> Result<Vec<ScenarioObservation>> {
        let mut observations = Vec::new();
        for op in ops {
            observations.push(self.apply_and_assert(op).await?);
        }
        Ok(observations)
    }

    async fn apply_real(&mut self, op: &ScenarioOp) -> Result<ScenarioObservation> {
        match op {
            ScenarioOp::OpenWriter { writer } => {
                ensure!(
                    !self.writers.contains_key(writer),
                    "writer {writer} already exists"
                );
                let handle = self
                    .client
                    .open_writer(
                        self.shard_id,
                        Arc::new(StringSchema),
                        Arc::new(UnitSchema),
                        Diagnostics::for_tests(),
                    )
                    .await?;
                let upper = singleton_frontier(handle.upper())?;
                self.writers.insert(writer.clone(), handle);
                Ok(ScenarioObservation::WriterOpened { upper })
            }
            ScenarioOp::OpenReader { reader } => {
                ensure!(
                    !self.readers.contains_key(reader),
                    "reader {reader} already exists"
                );
                let handle = self
                    .client
                    .open_leased_reader(
                        self.shard_id,
                        Arc::new(StringSchema),
                        Arc::new(UnitSchema),
                        Diagnostics::for_tests(),
                        true,
                    )
                    .await?;
                let since = singleton_frontier(handle.since())?;
                self.readers.insert(reader.clone(), handle);
                Ok(ScenarioObservation::ReaderOpened { since })
            }
            ScenarioOp::CompareAndAppend {
                writer,
                updates,
                expected_upper,
                new_upper,
            } => {
                let handle = self
                    .writers
                    .get_mut(writer)
                    .ok_or_else(|| anyhow!("unknown writer {writer}"))?;
                let encoded_updates: Vec<_> = updates
                    .iter()
                    .map(|update| ((&update.key, &()), &update.time, &update.diff))
                    .collect();
                let result = handle
                    .compare_and_append(
                        encoded_updates,
                        Antichain::from_elem(*expected_upper),
                        Antichain::from_elem(*new_upper),
                    )
                    .await?;
                Ok(ScenarioObservation::CompareAndAppend(match result {
                    Ok(()) => CompareAndAppendResult::Committed,
                    Err(UpperMismatch { expected, current }) => {
                        CompareAndAppendResult::UpperMismatch {
                            expected: singleton_frontier(&expected)?,
                            current: singleton_frontier(&current)?,
                        }
                    }
                }))
            }
            ScenarioOp::DowngradeSince { reader, new_since } => {
                let handle = self
                    .readers
                    .get_mut(reader)
                    .ok_or_else(|| anyhow!("unknown reader {reader}"))?;
                handle.downgrade_since(&Antichain::from_elem(*new_since)).await;
                let since = singleton_frontier(handle.since())?;
                Ok(ScenarioObservation::SinceDowngraded { since })
            }
            ScenarioOp::Snapshot { reader, as_of } => {
                let handle = self
                    .readers
                    .get_mut(reader)
                    .ok_or_else(|| anyhow!("unknown reader {reader}"))?;
                let snapshot = tokio::time::timeout(
                    std::time::Duration::from_millis(250),
                    handle.snapshot_and_fetch(Antichain::from_elem(*as_of)),
                )
                .await;
                let observation = match snapshot {
                    Ok(Ok(updates)) => ScenarioObservation::Snapshot(Ok(
                        updates
                            .into_iter()
                            .map(|((key, ()), time, diff)| Update::new(key, time, diff))
                            .collect(),
                    )),
                    Ok(Err(since)) => {
                        ScenarioObservation::Snapshot(Err(SnapshotError::Since(singleton_frontier(
                            &since.0,
                        )?)))
                    }
                    Err(_elapsed) => {
                        return Err(anyhow!(
                            "snapshot at as_of={} timed out waiting for data to become available",
                            as_of
                        ));
                    }
                };
                Ok(observation)
            }
        }
    }
}

fn singleton_frontier(frontier: &Antichain<u64>) -> Result<u64> {
    frontier
        .as_option()
        .copied()
        .ok_or_else(|| anyhow!("expected a singleton frontier, got {:?}", frontier.elements()))
}
