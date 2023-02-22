// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WIP

use mz_ore::cast::CastFrom;
use proptest::bits::BitSetLike;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use mz_persist_types::columnar::{ColumnFormat, DataType, PartDecoder, Schema};
use mz_persist_types::parquet::decode_part;
use mz_persist_types::part::Part;
use mz_persist_types::Codec;

use crate::internal::paths::PartialBatchKey;
use crate::internal::state::{HollowBatch, HollowBatchStats};

#[derive(Debug)]
pub struct BatchStats {
    stats: Vec<Part>,
    keys: HashMap<PartialBatchKey, (usize, usize)>,
    stats_v2: Vec<HashMap<String, HollowBatchStats>>,
    keys_v2: HashMap<PartialBatchKey, (usize, usize)>,
}

// WIP: this is blurring together many layers of abstraction
impl BatchStats {
    pub fn get(
        &self,
        batch_part_key: &PartialBatchKey,
    ) -> (&HashMap<String, HollowBatchStats>, usize) {
        let (part_idx, row_idx) = self.keys_v2.get(batch_part_key).expect("WIP");

        (self.stats_v2.get(*part_idx).expect("WIP"), *row_idx)
        //
        // for (colname, typ, has_stats) in key_schema.columns() {
        //     if !has_stats {
        //         continue;
        //     }
        //
        //     if let Some(column_stats) = stats.get(&colname) {
        //         let byte_range = usize::cast_from(column_stats.min_data_lens[*row_idx]);
        //         let offset = usize::cast_from(column_stats.min_data_indices[*row_idx]);
        //
        //         if byte_range.len() == 0 {
        //             continue;
        //         }
        //
        //         match typ.format {
        //             ColumnFormat::I64 => {
        //                 let i64 = i64::from_le_bytes(
        //                     <[u8; 8]>::try_from(
        //                         &column_stats.min_data_bytes[offset..offset + byte_range],
        //                     )
        //                     .expect("WIP"),
        //                 );
        //                 info!("Column {} has val: {}", colname, i64);
        //             }
        //             _ => {}
        //         }
        //     }
        // }

        // let Some((part_idx, row_idx)) = self.keys.get(batch_part_key) else {
        //     return false;
        // };
        //
        // info!(
        //     "Stored indices: {:?}. Looking up ({}, {}) for {:?}",
        //     self.keys, part_idx, row_idx, batch_part_key,
        // );
        //
        // let part: &Part = self.stats.get(*part_idx).expect("WIP");
        //
        // let key_decoder = key_schema.decoder(part.key_ref()).expect("decodeable key");
        // let val_decoder = val_schema.decoder(part.val_ref()).expect("decodeable val");
        //
        // key_decoder.decode(*row_idx, min_key);
        // val_decoder.decode(*row_idx, min_val);
        //
        // key_decoder.decode(*row_idx + 1, max_key);
        // val_decoder.decode(*row_idx + 1, max_val);
    }
}

#[derive(Debug)]
pub struct BatchStatsBuilder<K: Codec, V: Codec> {
    stats: Vec<Part>,
    stats_v2: Vec<HashMap<String, HollowBatchStats>>,
    keys_v2: HashMap<PartialBatchKey, (usize, usize)>,
    keys: HashMap<PartialBatchKey, (usize, usize)>,
    // keys_v2: HashMap<String, HollowBatchStats>,
    current_batch_idx: usize,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
}

impl<K: Codec, V: Codec> BatchStatsBuilder<K, V> {
    pub fn new(key_schema: Arc<K::Schema>, val_schema: Arc<V::Schema>) -> Self {
        Self {
            stats: Vec::new(),
            keys: HashMap::new(),
            stats_v2: Vec::new(),
            keys_v2: HashMap::new(),
            current_batch_idx: 0,
            key_schema,
            val_schema,
        }
    }

    pub fn add_batch<T>(&mut self, batch: &HollowBatch<T>) -> Result<(), anyhow::Error> {
        let mut stats_by_column = HashMap::new();
        for stats in &batch.stats_v2 {
            info!(
                "Inserting stats: {:?} into {}",
                stats, self.current_batch_idx
            );
            stats_by_column.insert(stats.column_name.clone(), stats.clone());
        }

        self.stats_v2.push(stats_by_column);
        for (row_idx, batch_part) in batch.parts.iter().enumerate() {
            self.keys_v2
                .insert(batch_part.key.clone(), (self.current_batch_idx, row_idx));
        }
        self.current_batch_idx += 1;

        // if batch.stats.len() > 0 {
        //     let parquet_stats = decode_part(
        //         &mut std::io::Cursor::new(&batch.stats),
        //         self.key_schema.as_ref(),
        //         self.val_schema.as_ref(),
        //     )?;
        //
        //     self.stats.push(parquet_stats);
        //
        //     for (row_idx, batch_part) in batch.parts.iter().enumerate() {
        //         self.keys.insert(
        //             batch_part.key.clone(),
        //             (self.current_batch_idx, 2 * row_idx),
        //         );
        //     }
        //
        //     self.current_batch_idx += 1;
        // };

        Ok(())
    }

    pub fn finish(self) -> BatchStats {
        BatchStats {
            stats: self.stats,
            keys: self.keys,
            stats_v2: self.stats_v2,
            keys_v2: self.keys_v2,
        }
    }
}
