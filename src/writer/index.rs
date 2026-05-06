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

use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{
        DataType as ArrowDataType, Float32Type, Float64Type, GenericBinaryType, GenericStringType,
        Int16Type, Int32Type, Int64Type, Int8Type,
    },
};

use crate::proto;

#[derive(Debug)]
pub(crate) struct RowIndexBuilder {
    data_type: ArrowDataType,
    rows_per_group: usize,
    row_groups: Vec<proto::RowIndexEntry>,
    current: ColumnStatsBuilder,
    rows_in_current_group: usize,
}

impl RowIndexBuilder {
    pub(crate) fn new(data_type: ArrowDataType, rows_per_group: usize) -> Self {
        Self {
            current: ColumnStatsBuilder::new(&data_type),
            data_type,
            rows_per_group,
            row_groups: Vec::new(),
            rows_in_current_group: 0,
        }
    }

    pub(crate) fn update(&mut self, array: &ArrayRef) {
        for row_index in 0..array.len() {
            if self.rows_in_current_group == self.rows_per_group {
                self.finish_current_group();
            }

            self.current.update(&self.data_type, array, row_index);
            self.rows_in_current_group += 1;
        }
    }

    pub(crate) fn finish(&mut self) -> proto::RowIndex {
        if self.rows_in_current_group > 0 {
            self.finish_current_group();
        }

        proto::RowIndex {
            entry: std::mem::take(&mut self.row_groups),
        }
    }

    fn finish_current_group(&mut self) {
        let statistics = self.current.finish(&self.data_type);
        self.row_groups.push(proto::RowIndexEntry {
            // The current reader uses row-group statistics for predicate pruning.
            // Stream seek positions require encoder-level position recorders.
            positions: vec![],
            statistics: Some(statistics),
        });
        self.current = ColumnStatsBuilder::new(&self.data_type);
        self.rows_in_current_group = 0;
    }
}

#[derive(Debug)]
struct ColumnStatsBuilder {
    number_of_values: u64,
    has_null: bool,
    type_stats: TypeStatsBuilder,
}

impl ColumnStatsBuilder {
    fn new(data_type: &ArrowDataType) -> Self {
        Self {
            number_of_values: 0,
            has_null: false,
            type_stats: TypeStatsBuilder::new(data_type),
        }
    }

    fn update(&mut self, data_type: &ArrowDataType, array: &ArrayRef, row_index: usize) {
        if array.is_null(row_index) {
            self.has_null = true;
            return;
        }

        self.number_of_values += 1;

        match data_type {
            ArrowDataType::Int8 => {
                self.type_stats
                    .update_integer(array.as_primitive::<Int8Type>().value(row_index) as i64);
            }
            ArrowDataType::Int16 => {
                self.type_stats
                    .update_integer(array.as_primitive::<Int16Type>().value(row_index) as i64);
            }
            ArrowDataType::Int32 => {
                self.type_stats
                    .update_integer(array.as_primitive::<Int32Type>().value(row_index) as i64);
            }
            ArrowDataType::Int64 => {
                self.type_stats
                    .update_integer(array.as_primitive::<Int64Type>().value(row_index));
            }
            ArrowDataType::Float32 => {
                self.type_stats
                    .update_double(array.as_primitive::<Float32Type>().value(row_index) as f64);
            }
            ArrowDataType::Float64 => {
                self.type_stats
                    .update_double(array.as_primitive::<Float64Type>().value(row_index));
            }
            ArrowDataType::Utf8 => {
                self.type_stats
                    .update_string(array.as_bytes::<GenericStringType<i32>>().value(row_index));
            }
            ArrowDataType::LargeUtf8 => {
                self.type_stats
                    .update_string(array.as_bytes::<GenericStringType<i64>>().value(row_index));
            }
            ArrowDataType::Binary => {
                self.type_stats.update_binary(
                    array
                        .as_bytes::<GenericBinaryType<i32>>()
                        .value(row_index)
                        .len(),
                );
            }
            ArrowDataType::LargeBinary => {
                self.type_stats.update_binary(
                    array
                        .as_bytes::<GenericBinaryType<i64>>()
                        .value(row_index)
                        .len(),
                );
            }
            ArrowDataType::Boolean => {
                self.type_stats
                    .update_boolean(array.as_boolean().value(row_index));
            }
            _ => {}
        }
    }

    fn finish(&self, data_type: &ArrowDataType) -> proto::ColumnStatistics {
        let mut statistics = proto::ColumnStatistics {
            number_of_values: Some(self.number_of_values),
            has_null: Some(self.has_null),
            ..Default::default()
        };

        if self.number_of_values == 0 {
            return statistics;
        }

        match (&self.type_stats, data_type) {
            (TypeStatsBuilder::Integer(stats), _) => {
                if let (Some(minimum), Some(maximum)) = (stats.minimum, stats.maximum) {
                    statistics.int_statistics = Some(proto::IntegerStatistics {
                        minimum: Some(minimum),
                        maximum: Some(maximum),
                        sum: stats.sum,
                    });
                }
            }
            (TypeStatsBuilder::Double(stats), _) => {
                if let (Some(minimum), Some(maximum)) = (stats.minimum, stats.maximum) {
                    statistics.double_statistics = Some(proto::DoubleStatistics {
                        minimum: Some(minimum),
                        maximum: Some(maximum),
                        sum: stats.sum,
                    });
                }
            }
            (TypeStatsBuilder::String(stats), ArrowDataType::Utf8 | ArrowDataType::LargeUtf8) => {
                if let (Some(minimum), Some(maximum)) = (&stats.minimum, &stats.maximum) {
                    statistics.string_statistics = Some(proto::StringStatistics {
                        minimum: Some(minimum.clone()),
                        maximum: Some(maximum.clone()),
                        sum: Some(stats.sum),
                        lower_bound: None,
                        upper_bound: None,
                    });
                }
            }
            (
                TypeStatsBuilder::Binary(stats),
                ArrowDataType::Binary | ArrowDataType::LargeBinary,
            ) => {
                statistics.binary_statistics = Some(proto::BinaryStatistics {
                    sum: Some(stats.sum),
                });
            }
            (TypeStatsBuilder::Boolean(stats), ArrowDataType::Boolean) => {
                statistics.bucket_statistics = Some(proto::BucketStatistics {
                    count: vec![stats.true_count],
                });
            }
            _ => {}
        }

        statistics
    }
}

#[derive(Debug)]
enum TypeStatsBuilder {
    Integer(IntegerStatsBuilder),
    Double(DoubleStatsBuilder),
    String(StringStatsBuilder),
    Binary(BinaryStatsBuilder),
    Boolean(BooleanStatsBuilder),
    Unsupported,
}

impl TypeStatsBuilder {
    fn new(data_type: &ArrowDataType) -> Self {
        match data_type {
            ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64 => Self::Integer(IntegerStatsBuilder::default()),
            ArrowDataType::Float32 | ArrowDataType::Float64 => {
                Self::Double(DoubleStatsBuilder::default())
            }
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
                Self::String(StringStatsBuilder::default())
            }
            ArrowDataType::Binary | ArrowDataType::LargeBinary => {
                Self::Binary(BinaryStatsBuilder::default())
            }
            ArrowDataType::Boolean => Self::Boolean(BooleanStatsBuilder::default()),
            _ => Self::Unsupported,
        }
    }

    fn update_integer(&mut self, value: i64) {
        if let Self::Integer(stats) = self {
            stats.update(value);
        }
    }

    fn update_double(&mut self, value: f64) {
        if let Self::Double(stats) = self {
            stats.update(value);
        }
    }

    fn update_string(&mut self, value: &str) {
        if let Self::String(stats) = self {
            stats.update(value);
        }
    }

    fn update_binary(&mut self, length: usize) {
        if let Self::Binary(stats) = self {
            stats.update(length);
        }
    }

    fn update_boolean(&mut self, value: bool) {
        if let Self::Boolean(stats) = self {
            stats.update(value);
        }
    }
}

#[derive(Debug, Default)]
struct IntegerStatsBuilder {
    minimum: Option<i64>,
    maximum: Option<i64>,
    sum: Option<i64>,
}

impl IntegerStatsBuilder {
    fn update(&mut self, value: i64) {
        self.minimum = Some(self.minimum.map_or(value, |minimum| minimum.min(value)));
        self.maximum = Some(self.maximum.map_or(value, |maximum| maximum.max(value)));
        self.sum = match self.sum {
            Some(sum) => sum.checked_add(value),
            None => Some(value),
        };
    }
}

#[derive(Debug, Default)]
struct DoubleStatsBuilder {
    minimum: Option<f64>,
    maximum: Option<f64>,
    sum: Option<f64>,
}

impl DoubleStatsBuilder {
    fn update(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }

        self.minimum = Some(self.minimum.map_or(value, |minimum| minimum.min(value)));
        self.maximum = Some(self.maximum.map_or(value, |maximum| maximum.max(value)));
        self.sum = Some(self.sum.unwrap_or(0.0) + value);
    }
}

#[derive(Debug, Default)]
struct StringStatsBuilder {
    minimum: Option<String>,
    maximum: Option<String>,
    sum: i64,
}

impl StringStatsBuilder {
    fn update(&mut self, value: &str) {
        self.minimum = Some(self.minimum.as_deref().map_or_else(
            || value.to_string(),
            |minimum| minimum.min(value).to_string(),
        ));
        self.maximum = Some(self.maximum.as_deref().map_or_else(
            || value.to_string(),
            |maximum| maximum.max(value).to_string(),
        ));
        self.sum += value.len() as i64;
    }
}

#[derive(Debug, Default)]
struct BinaryStatsBuilder {
    sum: i64,
}

impl BinaryStatsBuilder {
    fn update(&mut self, length: usize) {
        self.sum += length as i64;
    }
}

#[derive(Debug, Default)]
struct BooleanStatsBuilder {
    true_count: u64,
}

impl BooleanStatsBuilder {
    fn update(&mut self, value: bool) {
        if value {
            self.true_count += 1;
        }
    }
}
