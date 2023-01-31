// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WIP

use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use mz_persist_types::columnar::{PartDecoder, Schema};
use mz_persist_types::parquet::decode_part;
use mz_persist_types::part::Part;
use mz_persist_types::Codec;

use crate::internal::paths::PartialBatchKey;
use crate::internal::state::HollowBatch;

#[derive(Debug)]
pub struct BatchStats {
    stats: Vec<Part>,
    keys: HashMap<PartialBatchKey, (usize, usize)>,
}

impl BatchStats {
    pub fn get<K: Codec, V: Codec>(
        &self,
        batch_part_key: &PartialBatchKey,
        key_schema: &K::Schema,
        val_schema: &V::Schema,
        min_key: &mut K,
        min_val: &mut V,
        max_key: &mut K,
        max_val: &mut V,
    ) {
        let (part_idx, row_idx) = self.keys.get(batch_part_key).expect("WIP: unknown key");

        let part: &Part = self.stats.get(*part_idx).expect("WIP");

        let key_decoder = key_schema.decoder(part.key_ref()).expect("decodeable key");
        let val_decoder = val_schema.decoder(part.val_ref()).expect("decodeable val");

        key_decoder.decode(*row_idx, min_key);
        val_decoder.decode(*row_idx, min_val);

        key_decoder.decode(*row_idx + 1, max_key);
        val_decoder.decode(*row_idx + 1, max_val);
    }
}

#[derive(Debug)]
pub struct BatchStatsBuilder<K: Codec, V: Codec> {
    stats: Vec<Part>,
    keys: HashMap<PartialBatchKey, (usize, usize)>,
    current_batch_idx: usize,
    key_schema: Arc<K::Schema>,
    val_schema: Arc<V::Schema>,
}

impl<K: Codec, V: Codec> BatchStatsBuilder<K, V> {
    pub fn new(key_schema: Arc<K::Schema>, val_schema: Arc<V::Schema>) -> Self {
        Self {
            stats: Vec::new(),
            keys: HashMap::new(),
            current_batch_idx: 0,
            key_schema,
            val_schema,
        }
    }

    pub fn add_batch<T>(&mut self, batch: &mut HollowBatch<T>) -> Result<(), anyhow::Error> {
        if batch.stats.len() > 0 {
            info!("Batch stats: {:?}", batch.stats);
            let parquet_stats = decode_part(
                &mut std::io::Cursor::new(&batch.stats),
                self.key_schema.as_ref(),
                self.val_schema.as_ref(),
            )?;

            self.stats.push(parquet_stats);

            for (row_idx, batch_part) in batch.parts.iter().enumerate() {
                self.keys.insert(
                    batch_part.key.clone(),
                    (self.current_batch_idx, 2 * row_idx),
                );
            }
        }

        self.current_batch_idx += 1;

        Ok(())
    }

    pub fn finish(self) -> BatchStats {
        BatchStats {
            stats: self.stats,
            keys: self.keys,
        }
    }
}
