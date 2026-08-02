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
        DataType as ArrowDataType, Date32Type, Decimal128Type, Float32Type, Float64Type,
        GenericBinaryType, GenericStringType, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    },
};

use crate::{bloom_filter::BloomFilter, proto};

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
pub(crate) struct BloomFilterIndexBuilder {
    data_type: ArrowDataType,
    rows_per_group: usize,
    row_groups: Vec<proto::BloomFilter>,
    current: BloomFilter,
    rows_in_current_group: usize,
    enabled: bool,
}

impl BloomFilterIndexBuilder {
    pub(crate) fn new(data_type: ArrowDataType, rows_per_group: usize) -> Self {
        let enabled = bloom_supported(&data_type);
        Self {
            current: new_bloom_filter(rows_per_group),
            data_type,
            rows_per_group,
            row_groups: Vec::new(),
            rows_in_current_group: 0,
            enabled,
        }
    }

    pub(crate) fn update(&mut self, array: &ArrayRef) {
        if !self.enabled {
            return;
        }

        for row_index in 0..array.len() {
            if self.rows_in_current_group == self.rows_per_group {
                self.finish_current_group();
            }

            if !array.is_null(row_index) {
                if let Some(hash) = bloom_hash(&self.data_type, array, row_index) {
                    self.current.add_hash(hash);
                }
            }
            self.rows_in_current_group += 1;
        }
    }

    pub(crate) fn finish(&mut self) -> proto::BloomFilterIndex {
        if self.enabled && self.rows_in_current_group > 0 {
            self.finish_current_group();
        }

        proto::BloomFilterIndex {
            bloom_filter: std::mem::take(&mut self.row_groups),
        }
    }

    fn finish_current_group(&mut self) {
        self.row_groups.push(self.current.to_proto_utf8());
        self.current = new_bloom_filter(self.rows_per_group);
        self.rows_in_current_group = 0;
    }
}

fn new_bloom_filter(rows_per_group: usize) -> BloomFilter {
    let bit_count = rows_per_group.saturating_mul(16).max(1024);
    let word_count = bit_count.div_ceil(64).min(8192);
    BloomFilter::from_parts(6, vec![0; word_count])
}

fn bloom_supported(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::Float32
            | ArrowDataType::Float64
            | ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Boolean
    )
}

fn bloom_hash(data_type: &ArrowDataType, array: &ArrayRef, row_index: usize) -> Option<u64> {
    match data_type {
        ArrowDataType::Int8 => Some(BloomFilter::hash_long(
            array.as_primitive::<Int8Type>().value(row_index) as i64,
        )),
        ArrowDataType::Int16 => Some(BloomFilter::hash_long(
            array.as_primitive::<Int16Type>().value(row_index) as i64,
        )),
        ArrowDataType::Int32 => Some(BloomFilter::hash_long(
            array.as_primitive::<Int32Type>().value(row_index) as i64,
        )),
        ArrowDataType::Int64 => Some(BloomFilter::hash_long(
            array.as_primitive::<Int64Type>().value(row_index),
        )),
        ArrowDataType::Float32 => Some(BloomFilter::hash_long(
            (array.as_primitive::<Float32Type>().value(row_index) as f64).to_bits() as i64,
        )),
        ArrowDataType::Float64 => Some(BloomFilter::hash_long(
            array
                .as_primitive::<Float64Type>()
                .value(row_index)
                .to_bits() as i64,
        )),
        ArrowDataType::Utf8 => Some(BloomFilter::hash_bytes(
            array
                .as_bytes::<GenericStringType<i32>>()
                .value(row_index)
                .as_bytes(),
        )),
        ArrowDataType::LargeUtf8 => Some(BloomFilter::hash_bytes(
            array
                .as_bytes::<GenericStringType<i64>>()
                .value(row_index)
                .as_bytes(),
        )),
        ArrowDataType::Boolean => Some(BloomFilter::hash_long(
            if array.as_boolean().value(row_index) {
                1
            } else {
                0
            },
        )),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) struct ColumnStatsBuilder {
    number_of_values: u64,
    has_null: bool,
    type_stats: TypeStatsBuilder,
}

impl ColumnStatsBuilder {
    pub(crate) fn new(data_type: &ArrowDataType) -> Self {
        Self {
            number_of_values: 0,
            has_null: false,
            type_stats: TypeStatsBuilder::new(data_type),
        }
    }

    pub(crate) fn update_array(&mut self, data_type: &ArrowDataType, array: &ArrayRef) {
        for row_index in 0..array.len() {
            self.update(data_type, array, row_index);
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
            ArrowDataType::Decimal128(_, _) => {
                self.type_stats
                    .update_decimal(array.as_primitive::<Decimal128Type>().value(row_index));
            }
            ArrowDataType::Date32 => {
                self.type_stats
                    .update_date(array.as_primitive::<Date32Type>().value(row_index));
            }
            ArrowDataType::Timestamp(TimeUnit::Second, _) => {
                self.type_stats.update_timestamp_nanos(
                    (array.as_primitive::<TimestampSecondType>().value(row_index) as i128)
                        * 1_000_000_000,
                );
            }
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                self.type_stats.update_timestamp_nanos(
                    (array
                        .as_primitive::<TimestampMillisecondType>()
                        .value(row_index) as i128)
                        * 1_000_000,
                );
            }
            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
                self.type_stats.update_timestamp_nanos(
                    (array
                        .as_primitive::<TimestampMicrosecondType>()
                        .value(row_index) as i128)
                        * 1_000,
                );
            }
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
                self.type_stats.update_timestamp_nanos(
                    array
                        .as_primitive::<TimestampNanosecondType>()
                        .value(row_index) as i128,
                );
            }
            _ => {}
        }
    }

    pub(crate) fn finish(&self, data_type: &ArrowDataType) -> proto::ColumnStatistics {
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
            (TypeStatsBuilder::Decimal(stats), ArrowDataType::Decimal128(_, scale)) => {
                if let (Some(minimum), Some(maximum)) = (stats.minimum, stats.maximum) {
                    statistics.decimal_statistics = Some(proto::DecimalStatistics {
                        minimum: Some(format_decimal(minimum, *scale)),
                        maximum: Some(format_decimal(maximum, *scale)),
                        sum: stats.sum.map(|sum| format_decimal(sum, *scale)),
                    });
                }
            }
            (TypeStatsBuilder::Date(stats), ArrowDataType::Date32) => {
                if let (Some(minimum), Some(maximum)) = (stats.minimum, stats.maximum) {
                    statistics.date_statistics = Some(proto::DateStatistics {
                        minimum: Some(minimum),
                        maximum: Some(maximum),
                    });
                }
            }
            (TypeStatsBuilder::Timestamp(stats), ArrowDataType::Timestamp(_, _)) => {
                if let (Some(minimum), Some(maximum)) = (stats.minimum_nanos, stats.maximum_nanos) {
                    statistics.timestamp_statistics = Some(proto::TimestampStatistics {
                        minimum: Some(timestamp_millis(minimum)),
                        maximum: Some(timestamp_millis(maximum)),
                        minimum_utc: Some(timestamp_millis(minimum)),
                        maximum_utc: Some(timestamp_millis(maximum)),
                        minimum_nanos: Some(timestamp_sub_millis_nanos(minimum)),
                        maximum_nanos: Some(timestamp_sub_millis_nanos(maximum)),
                    });
                }
            }
            _ => {}
        }

        statistics
    }
}

pub(crate) fn root_column_statistics(number_of_rows: u64) -> proto::ColumnStatistics {
    proto::ColumnStatistics {
        number_of_values: Some(number_of_rows),
        has_null: Some(false),
        ..Default::default()
    }
}

#[derive(Debug)]
enum TypeStatsBuilder {
    Integer(IntegerStatsBuilder),
    Double(DoubleStatsBuilder),
    String(StringStatsBuilder),
    Binary(BinaryStatsBuilder),
    Boolean(BooleanStatsBuilder),
    Decimal(DecimalStatsBuilder),
    Date(DateStatsBuilder),
    Timestamp(TimestampStatsBuilder),
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
            ArrowDataType::Decimal128(_, _) => Self::Decimal(DecimalStatsBuilder::default()),
            ArrowDataType::Date32 => Self::Date(DateStatsBuilder::default()),
            ArrowDataType::Timestamp(_, _) => Self::Timestamp(TimestampStatsBuilder::default()),
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

    fn update_decimal(&mut self, value: i128) {
        if let Self::Decimal(stats) = self {
            stats.update(value);
        }
    }

    fn update_date(&mut self, value: i32) {
        if let Self::Date(stats) = self {
            stats.update(value);
        }
    }

    fn update_timestamp_nanos(&mut self, value: i128) {
        if let Self::Timestamp(stats) = self {
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

#[derive(Debug, Default)]
struct DecimalStatsBuilder {
    minimum: Option<i128>,
    maximum: Option<i128>,
    sum: Option<i128>,
}

impl DecimalStatsBuilder {
    fn update(&mut self, value: i128) {
        self.minimum = Some(self.minimum.map_or(value, |minimum| minimum.min(value)));
        self.maximum = Some(self.maximum.map_or(value, |maximum| maximum.max(value)));
        self.sum = match self.sum {
            Some(sum) => sum.checked_add(value),
            None => Some(value),
        };
    }
}

#[derive(Debug, Default)]
struct DateStatsBuilder {
    minimum: Option<i32>,
    maximum: Option<i32>,
}

impl DateStatsBuilder {
    fn update(&mut self, value: i32) {
        self.minimum = Some(self.minimum.map_or(value, |minimum| minimum.min(value)));
        self.maximum = Some(self.maximum.map_or(value, |maximum| maximum.max(value)));
    }
}

#[derive(Debug, Default)]
struct TimestampStatsBuilder {
    minimum_nanos: Option<i128>,
    maximum_nanos: Option<i128>,
}

impl TimestampStatsBuilder {
    fn update(&mut self, value: i128) {
        self.minimum_nanos = Some(
            self.minimum_nanos
                .map_or(value, |minimum| minimum.min(value)),
        );
        self.maximum_nanos = Some(
            self.maximum_nanos
                .map_or(value, |maximum| maximum.max(value)),
        );
    }
}

fn timestamp_millis(nanos_since_epoch: i128) -> i64 {
    nanos_since_epoch.div_euclid(1_000_000) as i64
}

fn timestamp_sub_millis_nanos(nanos_since_epoch: i128) -> i32 {
    nanos_since_epoch.rem_euclid(1_000_000) as i32
}

fn format_decimal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        let multiplier = 10_i128.pow((-scale) as u32);
        return (value * multiplier).to_string();
    }

    let scale = scale as usize;
    let sign = if value < 0 { "-" } else { "" };
    let digits = if value == i128::MIN {
        "170141183460469231731687303715884105728".to_string()
    } else {
        value.abs().to_string()
    };
    if digits.len() <= scale {
        let padding = "0".repeat(scale + 1 - digits.len());
        let digits = format!("{padding}{digits}");
        let split = digits.len() - scale;
        return format!("{sign}{}.{}", &digits[..split], &digits[split..]);
    }

    let split = digits.len() - scale;
    format!("{sign}{}.{}", &digits[..split], &digits[split..])
}
