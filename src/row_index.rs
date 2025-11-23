// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Row index structures for ORC files
//!
//! This module provides structures for parsing and accessing row-level indexes
//! from ORC stripes. Row indexes contain statistics for each row group (default
//! 10,000 rows) enabling efficient predicate pushdown and row group pruning.

use std::collections::HashMap;

use crate::error::Result;
use crate::proto;
use crate::statistics::ColumnStatistics;

/// A single row group entry in a row index
///
/// According to ORC spec, each entry contains:
/// - Statistics for the row group (min/max/null count)
/// - Stream positions for seeking to the row group (for future use)
#[derive(Debug, Clone)]
pub struct RowGroupEntry {
    /// Statistics for this row group
    pub statistics: Option<ColumnStatistics>,

    /// Stream positions for seeking
    ///
    /// According to ORC spec, positions encode differently for:
    /// - Uncompressed: [RLE_run_byte_offset, num_values_to_consume]
    /// - Compressed: [compression_chunk_start, decompressed_bytes, num_values]
    ///
    /// For columns with multiple streams, positions are concatenated:
    /// [PRESENT_positions..., DATA_positions..., LENGTH_positions...]
    ///
    /// Note: Dictionary positions are NOT included (dictionary must be fully read)
    pub positions: Vec<u64>,
}

impl RowGroupEntry {
    pub fn new(statistics: Option<ColumnStatistics>, positions: Vec<u64>) -> Self {
        Self {
            statistics,
            positions,
        }
    }
}

/// Row index for a single column in a stripe
///
/// Only primitive columns have row indexes. Compound types (struct/list/map)
/// delegate to their child columns.
#[derive(Debug, Clone)]
pub struct RowGroupIndex {
    /// Row group entries, one per row group
    entries: Vec<RowGroupEntry>,

    /// Number of rows per row group (from row_index_stride, default 10,000)
    rows_per_group: usize,

    /// Column index this row index belongs to
    column_index: usize,
}

impl RowGroupIndex {
    pub fn new(entries: Vec<RowGroupEntry>, rows_per_group: usize, column_index: usize) -> Self {
        Self {
            entries,
            rows_per_group,
            column_index,
        }
    }

    /// Get the number of row groups in this index
    pub fn num_row_groups(&self) -> usize {
        self.entries.len()
    }

    /// Get the number of rows per row group
    pub fn rows_per_group(&self) -> usize {
        self.rows_per_group
    }

    /// Get the column index this row index belongs to
    pub fn column_index(&self) -> usize {
        self.column_index
    }

    /// Get statistics for a specific row group
    pub fn row_group_stats(&self, row_group_idx: usize) -> Option<&ColumnStatistics> {
        self.entries
            .get(row_group_idx)
            .and_then(|entry| entry.statistics.as_ref())
    }

    /// Get an iterator over row group entries
    pub fn entries(&self) -> impl Iterator<Item = &RowGroupEntry> {
        self.entries.iter()
    }

    /// Get a specific row group entry
    pub fn entry(&self, row_group_idx: usize) -> Option<&RowGroupEntry> {
        self.entries.get(row_group_idx)
    }
}

/// Row indexes for all columns in a stripe
///
/// This structure provides access to row group statistics for all primitive
/// columns in a stripe, enabling row group-level filtering.
#[derive(Debug, Clone)]
pub struct StripeRowIndex {
    /// Map from column index to its row group index
    columns: HashMap<usize, RowGroupIndex>,

    /// Total number of rows in the stripe
    total_rows: usize,

    /// Number of rows per row group
    rows_per_group: usize,
}

impl StripeRowIndex {
    pub fn new(
        columns: HashMap<usize, RowGroupIndex>,
        total_rows: usize,
        rows_per_group: usize,
    ) -> Self {
        Self {
            columns,
            total_rows,
            rows_per_group,
        }
    }

    /// Get the row group index for a column
    pub fn column(&self, column_idx: usize) -> Option<&RowGroupIndex> {
        self.columns.get(&column_idx)
    }

    /// Get the number of row groups in this stripe
    pub fn num_row_groups(&self) -> usize {
        if self.rows_per_group == 0 {
            return 0;
        }
        self.total_rows.div_ceil(self.rows_per_group)
    }

    /// Get statistics for a specific row group and column
    pub fn row_group_stats(
        &self,
        column_idx: usize,
        row_group_idx: usize,
    ) -> Option<&ColumnStatistics> {
        self.column(column_idx)
            .and_then(|col_index| col_index.row_group_stats(row_group_idx))
    }

    /// Get the total number of rows in this stripe
    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Get the number of rows per row group
    pub fn rows_per_group(&self) -> usize {
        self.rows_per_group
    }

    /// Get an iterator over all column indices that have row indexes
    pub fn column_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.columns.keys().copied()
    }
}

/// Parse a `RowIndex` protobuf message into a `RowGroupIndex`
fn parse_row_index(
    proto: &proto::RowIndex,
    column_index: usize,
    rows_per_group: usize,
) -> Result<RowGroupIndex> {
    use crate::statistics::ColumnStatistics;

    let entries: Result<Vec<RowGroupEntry>> = proto
        .entry
        .iter()
        .map(|entry| {
            let statistics = entry
                .statistics
                .as_ref()
                .map(ColumnStatistics::try_from)
                .transpose()?;
            Ok(RowGroupEntry::new(statistics, entry.positions.clone()))
        })
        .collect();

    Ok(RowGroupIndex::new(entries?, rows_per_group, column_index))
}

/// Parse row indexes from a stripe
///
/// According to ORC spec:
/// - Only primitive columns have row indexes
/// - Row indexes are stored in ROW_INDEX streams in the index section
/// - Indexes are only loaded when predicate pushdown is used or seeking
///
/// This function parses all ROW_INDEX streams from the stripe's stream map.
pub fn parse_stripe_row_indexes(
    stripe_stream_map: &crate::stripe::StreamMap,
    columns: &[crate::column::Column],
    total_rows: usize,
    rows_per_group: usize,
) -> Result<StripeRowIndex> {
    use crate::error::{DecodeProtoSnafu, IoSnafu};
    use crate::proto::stream::Kind;
    use prost::Message;
    use snafu::ResultExt;

    let mut row_indexes = HashMap::new();

    for column in columns {
        let column_id = column.column_id();

        // Try to get ROW_INDEX stream for this column
        let row_index_stream = stripe_stream_map.get_opt(column, Kind::RowIndex);

        if let Some(mut decompressor) = row_index_stream {
            // Decompress the stream
            let mut buffer = Vec::new();
            std::io::Read::read_to_end(&mut decompressor, &mut buffer).context(IoSnafu)?;

            // Parse the protobuf message
            let proto_row_index =
                proto::RowIndex::decode(buffer.as_slice()).context(DecodeProtoSnafu)?;

            // Parse into RowGroupIndex
            let row_group_index =
                parse_row_index(&proto_row_index, column_id as usize, rows_per_group)?;
            row_indexes.insert(column_id as usize, row_group_index);
        }
    }

    Ok(StripeRowIndex::new(row_indexes, total_rows, rows_per_group))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_group_index() {
        let entries = vec![
            RowGroupEntry::new(None, vec![1, 2, 3]),
            RowGroupEntry::new(None, vec![4, 5, 6]),
        ];
        let index = RowGroupIndex::new(entries, 10000, 0);

        assert_eq!(index.num_row_groups(), 2);
        assert_eq!(index.rows_per_group(), 10000);
        assert_eq!(index.column_index(), 0);
    }

    #[test]
    fn test_stripe_row_index() {
        let mut columns = HashMap::new();
        let entries = vec![RowGroupEntry::new(None, vec![])];
        columns.insert(0, RowGroupIndex::new(entries, 10000, 0));

        let stripe_index = StripeRowIndex::new(columns, 50000, 10000);

        assert_eq!(stripe_index.num_row_groups(), 5);
        assert_eq!(stripe_index.total_rows(), 50000);
        assert_eq!(stripe_index.rows_per_group(), 10000);
    }
}
