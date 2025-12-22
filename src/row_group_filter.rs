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

//! Row group filtering based on predicate evaluation
//!
//! This module implements predicate evaluation against row group statistics
//! to determine which row groups should be read or skipped.

use crate::bloom_filter::BloomFilter;
use crate::error::{Result, UnexpectedSnafu};
use crate::predicate::{ComparisonOp, Predicate, PredicateValue};
use crate::row_index::RowGroupEntry;
use crate::row_index::StripeRowIndex;
use crate::schema::RootDataType;
use log::debug;
use snafu::OptionExt;

/// Evaluate a predicate against row group statistics
///
/// Returns a boolean vector where each element indicates whether the corresponding
/// row group should be kept (`true`) or skipped (`false`).
///
/// # Evaluation Logic
///
/// For a predicate like `col > 10`:
/// - If `max(row_group) <= 10`: **definitely false** → skip row group (`false`)
/// - If `min(row_group) > 10`: **definitely true** → keep row group (`true`)
/// - Otherwise: **maybe** → keep row group (`true`, let decoding phase verify)
///
/// # Arguments
///
/// * `predicate` - The predicate to evaluate
/// * `row_index` - Row group statistics for the stripe
/// * `schema` - The schema to resolve column names
///
/// # Returns
///
/// Vector of booleans, one per row group, indicating whether to keep the row group.
/// Returns an error if column is not found or evaluation fails.
pub fn evaluate_predicate(
    predicate: &Predicate,
    row_index: &StripeRowIndex,
    schema: &RootDataType,
) -> Result<Vec<bool>> {
    let num_row_groups = row_index.num_row_groups();
    let mut result = vec![true; num_row_groups]; // Default: keep all

    evaluate_predicate_recursive(predicate, row_index, schema, &mut result)?;

    Ok(result)
}

fn evaluate_predicate_recursive(
    predicate: &Predicate,
    row_index: &StripeRowIndex,
    schema: &RootDataType,
    result: &mut [bool],
) -> Result<()> {
    match predicate {
        Predicate::Comparison { column, op, value } => {
            evaluate_comparison(column, *op, value, row_index, schema, result)?;
        }
        Predicate::IsNull { column } => {
            evaluate_is_null(column, row_index, schema, result)?;
        }
        Predicate::IsNotNull { column } => {
            evaluate_is_not_null(column, row_index, schema, result)?;
        }
        Predicate::And(predicates) => {
            // For AND: start with all true, then apply each predicate
            // Row group is kept only if ALL predicates allow it
            for pred in predicates {
                let mut temp_result = vec![true; result.len()];
                evaluate_predicate_recursive(pred, row_index, schema, &mut temp_result)?;
                // AND logic: result[i] = result[i] && temp_result[i]
                for (r, t) in result.iter_mut().zip(temp_result.iter()) {
                    *r = *r && *t;
                }
            }
        }
        Predicate::Or(predicates) => {
            // For OR: start with all false, then apply each predicate
            // Row group is kept if ANY predicate allows it
            let mut temp_results = Vec::new();
            for pred in predicates {
                let mut temp_result = vec![true; result.len()];
                evaluate_predicate_recursive(pred, row_index, schema, &mut temp_result)?;
                temp_results.push(temp_result);
            }
            // OR logic: result[i] = any(temp_results[j][i])
            for i in 0..result.len() {
                result[i] = temp_results.iter().any(|tr| tr[i]);
            }
        }
        Predicate::Not(predicate) => {
            // For NOT: evaluate predicate, then negate
            let mut temp_result = vec![true; result.len()];
            evaluate_predicate_recursive(predicate, row_index, schema, &mut temp_result)?;
            // NOT logic: result[i] = !temp_result[i]
            for (r, t) in result.iter_mut().zip(temp_result.iter()) {
                *r = !*t;
            }
        }
    }

    Ok(())
}

fn find_column_index(schema: &RootDataType, column_name: &str) -> Result<usize> {
    schema
        .children()
        .iter()
        .find(|col| col.name() == column_name)
        .map(|col| col.data_type().column_index())
        .context(UnexpectedSnafu {
            msg: format!("Column '{column_name}' not found in schema"),
        })
}

fn evaluate_comparison(
    column: &str,
    op: ComparisonOp,
    value: &PredicateValue,
    row_index: &StripeRowIndex,
    schema: &RootDataType,
    result: &mut [bool],
) -> Result<()> {
    // Find column index
    let column_idx = find_column_index(schema, column)?;

    // Get row group index for this column
    let col_index = row_index.column(column_idx).context(UnexpectedSnafu {
        msg: format!("Row index not found for column '{column}' (index {column_idx})",),
    })?;

    // Evaluate each row group
    for (row_group_idx, result_item) in result
        .iter_mut()
        .enumerate()
        .take(col_index.num_row_groups())
    {
        let entry = col_index.entry(row_group_idx);
        let entry = entry.context(UnexpectedSnafu {
            msg: format!(
                "Row group entry not found for column {column_idx}, row group {row_group_idx}",
            ),
        })?;

        // Get statistics for this row group
        let stats_match = if let Some(stats) = &entry.statistics {
            evaluate_comparison_with_stats(stats, op, value)?
        } else {
            // No statistics available, keep row group (maybe)
            true
        };

        if !stats_match {
            *result_item = false;
            continue;
        }

        // After statistics say "maybe", use bloom filter (if available) to rule out equality predicates
        *result_item = row_group_might_match_bloom(entry, op, value);
    }

    Ok(())
}

fn evaluate_comparison_with_stats(
    stats: &crate::statistics::ColumnStatistics,
    op: ComparisonOp,
    value: &PredicateValue,
) -> Result<bool> {
    use crate::statistics::TypeStatistics;

    let type_stats = stats.type_statistics().context(UnexpectedSnafu {
        msg: "Statistics missing type-specific information",
    })?;

    let matches = match type_stats {
        // Integer comparisons
        TypeStatistics::Integer { min, max, .. } => {
            let v = match value {
                PredicateValue::Int8(Some(v)) => *v as i64,
                PredicateValue::Int16(Some(v)) => *v as i64,
                PredicateValue::Int32(Some(v)) => *v as i64,
                PredicateValue::Int64(Some(v)) => *v,
                _ => {
                    return Err(UnexpectedSnafu {
                        msg: "Type mismatch: expected integer value".to_string(),
                    }
                    .build());
                }
            };
            evaluate_integer_comparison(*min, *max, op, v)
        }

        // Float comparisons
        TypeStatistics::Double { min, max, .. } => {
            let v = match value {
                PredicateValue::Float32(Some(v)) => *v as f64,
                PredicateValue::Float64(Some(v)) => *v,
                _ => {
                    return Err(UnexpectedSnafu {
                        msg: "Type mismatch: expected float value".to_string(),
                    }
                    .build());
                }
            };
            evaluate_float_comparison(*min, *max, op, v)
        }

        // String comparisons
        TypeStatistics::String { min, max, .. } => match value {
            PredicateValue::Utf8(Some(v)) => evaluate_string_comparison(min, max, op, v),
            _ => {
                return Err(UnexpectedSnafu {
                    msg: "Type mismatch: expected string value".to_string(),
                }
                .build());
            }
        },

        // Date comparisons
        TypeStatistics::Date { min, max } => {
            let v = match value {
                PredicateValue::Int32(Some(v)) => *v as i64,
                PredicateValue::Int64(Some(v)) => *v,
                _ => {
                    return Err(UnexpectedSnafu {
                        msg: "Type mismatch: expected integer value for date".to_string(),
                    }
                    .build());
                }
            };
            evaluate_integer_comparison(*min as i64, *max as i64, op, v)
        }

        // Timestamp comparisons (using UTC)
        TypeStatistics::Timestamp {
            min_utc, max_utc, ..
        } => match value {
            PredicateValue::Int64(Some(v)) => {
                evaluate_integer_comparison(*min_utc, *max_utc, op, *v)
            }
            _ => {
                return Err(UnexpectedSnafu {
                    msg: "Type mismatch: expected integer value for timestamp".to_string(),
                }
                .build());
            }
        },

        // Decimal comparisons
        TypeStatistics::Decimal { min, max, .. } => {
            match value {
                PredicateValue::Utf8(Some(v)) => {
                    // For decimal, we need to compare strings
                    // This is a simplified implementation
                    evaluate_string_comparison(min, max, op, v)
                }
                _ => {
                    return Err(UnexpectedSnafu {
                        msg: "Type mismatch: expected string value for decimal".to_string(),
                    }
                    .build());
                }
            }
        }

        // Boolean comparisons
        TypeStatistics::Bucket { true_count } => {
            match value {
                PredicateValue::Boolean(Some(v)) => {
                    let total_values = stats.number_of_values();
                    let false_count = total_values - *true_count;
                    match (v, op) {
                        (true, ComparisonOp::Equal) => {
                            // col = true: keep if true_count > 0
                            *true_count > 0
                        }
                        (true, ComparisonOp::NotEqual) => {
                            // col != true: keep if false_count > 0
                            false_count > 0
                        }
                        (false, ComparisonOp::Equal) => {
                            // col = false: keep if false_count > 0
                            false_count > 0
                        }
                        (false, ComparisonOp::NotEqual) => {
                            // col != false: keep if true_count > 0
                            *true_count > 0
                        }
                        _ => {
                            // For other ops on boolean, always keep (can't determine)
                            true
                        }
                    }
                }
                _ => {
                    return Err(UnexpectedSnafu {
                        msg: "Type mismatch: expected boolean value".to_string(),
                    }
                    .build());
                }
            }
        }

        // Unsupported type or missing stats
        _ => {
            // Can't determine, keep row group
            true
        }
    };

    Ok(matches)
}

fn row_group_might_match_bloom(
    entry: &RowGroupEntry,
    op: ComparisonOp,
    value: &PredicateValue,
) -> bool {
    // We only apply bloom filters for equality predicates
    if op != ComparisonOp::Equal {
        return true;
    }

    let bloom_filter = match entry.bloom_filter.as_ref() {
        Some(filter) => filter,
        None => return true,
    };

    if let Some(hash) = bloom_value_hash64(value) {
        bloom_filter.test_hash(hash)
    } else {
        debug!("Skipping bloom filter: unsupported predicate value type");
        true
    }
}

fn bloom_value_hash64(value: &PredicateValue) -> Option<u64> {
    match value {
        PredicateValue::Utf8(Some(v)) => Some(BloomFilter::hash_bytes(v.as_bytes())),
        PredicateValue::Int8(Some(v)) => Some(BloomFilter::hash_long(*v as i64)),
        PredicateValue::Int16(Some(v)) => Some(BloomFilter::hash_long(*v as i64)),
        PredicateValue::Int32(Some(v)) => Some(BloomFilter::hash_long(*v as i64)),
        PredicateValue::Int64(Some(v)) => Some(BloomFilter::hash_long(*v)),
        PredicateValue::Float32(Some(v)) => {
            let as_double = *v as f64;
            Some(BloomFilter::hash_long(as_double.to_bits() as i64))
        }
        PredicateValue::Float64(Some(v)) => Some(BloomFilter::hash_long(v.to_bits() as i64)),
        PredicateValue::Boolean(Some(v)) => Some(BloomFilter::hash_long(if *v { 1 } else { 0 })),
        _ => None,
    }
}

fn evaluate_integer_comparison(min: i64, max: i64, op: ComparisonOp, value: i64) -> bool {
    match op {
        ComparisonOp::Equal => {
            // col = value: keep if value is within [min, max]
            min <= value && value <= max
        }
        ComparisonOp::NotEqual => {
            // col != value: keep if value is not the only value
            // If min == max == value, then all values equal value → skip
            // Otherwise → keep
            !(min == value && max == value)
        }
        ComparisonOp::LessThan => {
            // col < value: keep if min < value
            // If max < value: definitely true → keep
            // If min >= value: definitely false → skip
            // Otherwise: maybe → keep
            min < value
        }
        ComparisonOp::LessThanOrEqual => {
            // col <= value: keep if min <= value
            min <= value
        }
        ComparisonOp::GreaterThan => {
            // col > value: keep if max > value
            max > value
        }
        ComparisonOp::GreaterThanOrEqual => {
            // col >= value: keep if max >= value
            max >= value
        }
    }
}

fn evaluate_float_comparison(min: f64, max: f64, op: ComparisonOp, value: f64) -> bool {
    match op {
        ComparisonOp::Equal => {
            // col = value: keep if value is within [min, max]
            // Use epsilon for floating point comparison
            const EPSILON: f64 = 1e-9;
            (min - EPSILON) <= value && value <= (max + EPSILON)
        }
        ComparisonOp::NotEqual => {
            // col != value: keep if value is not the only value
            // If min and max are very close to value, skip
            const EPSILON: f64 = 1e-9;
            !((min - value).abs() < EPSILON && (max - value).abs() < EPSILON)
        }
        ComparisonOp::LessThan => {
            // col < value: keep if min < value
            min < value
        }
        ComparisonOp::LessThanOrEqual => {
            // col <= value: keep if min <= value
            min <= value
        }
        ComparisonOp::GreaterThan => {
            // col > value: keep if max > value
            max > value
        }
        ComparisonOp::GreaterThanOrEqual => {
            // col >= value: keep if max >= value
            max >= value
        }
    }
}

fn evaluate_string_comparison(min: &str, max: &str, op: ComparisonOp, value: &str) -> bool {
    match op {
        ComparisonOp::Equal => {
            // col = value: keep if value is within [min, max] lexicographically
            min <= value && value <= max
        }
        ComparisonOp::NotEqual => {
            // col != value: keep if value is not the only value
            !(min == value && max == value)
        }
        ComparisonOp::LessThan => {
            // col < value: keep if min < value
            min < value
        }
        ComparisonOp::LessThanOrEqual => {
            // col <= value: keep if min <= value
            min <= value
        }
        ComparisonOp::GreaterThan => {
            // col > value: keep if max > value
            max > value
        }
        ComparisonOp::GreaterThanOrEqual => {
            // col >= value: keep if max >= value
            max >= value
        }
    }
}

fn evaluate_is_null(
    column: &str,
    row_index: &StripeRowIndex,
    schema: &RootDataType,
    result: &mut [bool],
) -> Result<()> {
    let column_idx = find_column_index(schema, column)?;
    let col_index = row_index.column(column_idx).context(UnexpectedSnafu {
        msg: format!("Row index not found for column '{column}' (index {column_idx})",),
    })?;

    for (row_group_idx, result_item) in result
        .iter_mut()
        .enumerate()
        .take(col_index.num_row_groups())
    {
        if let Some(entry) = col_index.entry(row_group_idx) {
            if let Some(stats) = &entry.statistics {
                // IS NULL: keep if has_null is true
                *result_item = stats.has_null();
            } else {
                // No statistics, keep row group (maybe)
                *result_item = true;
            }
        }
    }

    Ok(())
}

fn evaluate_is_not_null(
    column: &str,
    row_index: &StripeRowIndex,
    schema: &RootDataType,
    result: &mut [bool],
) -> Result<()> {
    let column_idx = find_column_index(schema, column)?;
    let col_index = row_index.column(column_idx).context(UnexpectedSnafu {
        msg: format!("Row index not found for column '{column}' (index {column_idx})",),
    })?;

    for (row_group_idx, result_item) in result
        .iter_mut()
        .enumerate()
        .take(col_index.num_row_groups())
    {
        if let Some(entry) = col_index.entry(row_group_idx) {
            if let Some(stats) = &entry.statistics {
                // IS NOT NULL: keep if number_of_values > 0 (has non-null values)
                *result_item = stats.number_of_values() > 0;
            } else {
                // No statistics, keep row group (maybe)
                *result_item = true;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::bloom_filter::BloomFilter;
    use crate::proto;
    use crate::row_index::{RowGroupEntry, RowGroupIndex, StripeRowIndex};
    use crate::statistics::ColumnStatistics;
    use std::collections::HashMap;

    // Note: Tests are simplified as we can't directly construct RootDataType and NamedColumn
    // due to private fields. In real usage, these would come from parsing an ORC file.

    fn create_test_row_index(rows_per_group: usize, total_rows: usize) -> StripeRowIndex {
        let mut columns = HashMap::new();

        // Column 1 (age): two row groups

        let age_entries = vec![
            RowGroupEntry::new(
                Some({
                    let proto_stats = proto::ColumnStatistics {
                        number_of_values: Some(5000),
                        has_null: Some(false),
                        int_statistics: Some(proto::IntegerStatistics {
                            minimum: Some(18),
                            maximum: Some(25),
                            sum: Some(107500),
                        }),
                        ..Default::default()
                    };
                    ColumnStatistics::try_from(&proto_stats).unwrap()
                }),
                vec![],
            ),
            RowGroupEntry::new(
                Some({
                    let proto_stats = proto::ColumnStatistics {
                        number_of_values: Some(5000),
                        has_null: Some(false),
                        int_statistics: Some(proto::IntegerStatistics {
                            minimum: Some(26),
                            maximum: Some(65),
                            sum: Some(227500),
                        }),
                        ..Default::default()
                    };
                    ColumnStatistics::try_from(&proto_stats).unwrap()
                }),
                vec![],
            ),
        ];
        columns.insert(1, RowGroupIndex::new(age_entries, rows_per_group, 1));

        StripeRowIndex::new(columns, total_rows, rows_per_group)
    }

    // Integration tests would require a full ORC file or mock schema
    // These tests verify the row index structure is created correctly
    #[test]
    fn test_row_index_creation() {
        let row_index = create_test_row_index(10000, 20000);
        assert_eq!(row_index.num_row_groups(), 2);
        assert_eq!(row_index.total_rows(), 20000);
        assert_eq!(row_index.rows_per_group(), 10000);

        if let Some(col_index) = row_index.column(1) {
            assert_eq!(col_index.num_row_groups(), 2);
        }
    }

    // Helper function to create a simple schema for testing
    // The schema will have "age" column at index 1 (matching row_index)
    fn create_test_schema() -> crate::schema::RootDataType {
        use crate::proto::r#type::Kind;
        use crate::proto::Type;

        // Create proto types:
        // Index 0: root (struct)
        // Index 1: age (int) - this matches the column index in row_index
        let types = vec![
            Type {
                kind: Some(Kind::Struct as i32),
                subtypes: vec![1], // age column at index 1
                field_names: vec!["age".to_string()],
                ..Default::default()
            },
            Type {
                kind: Some(Kind::Int as i32),
                subtypes: vec![],
                field_names: vec![],
                ..Default::default()
            },
        ];

        crate::schema::RootDataType::from_proto(&types).unwrap()
    }

    #[test]
    fn test_evaluate_predicate_integer_greater_than() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test: age > 20
        // Row group 0: min=18, max=25 -> should keep (20 is within range)
        // Row group 1: min=26, max=65 -> should keep (definitely > 20)
        let predicate = Predicate::gt("age", PredicateValue::Int32(Some(20)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0]); // Row group 0: might contain values > 20
        assert!(result[1]); // Row group 1: definitely contains values > 20
    }

    #[test]
    fn test_evaluate_predicate_integer_greater_than_or_equal() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test: age >= 30
        // Row group 0: min=18, max=25 -> should skip (max < 30)
        // Row group 1: min=26, max=65 -> should keep (might contain >= 30)
        let predicate = Predicate::gte("age", PredicateValue::Int32(Some(30)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        assert!(!result[0]); // Row group 0: max=25 < 30, skip
        assert!(result[1]); // Row group 1: max=65 >= 30, keep
    }

    #[test]
    fn test_evaluate_predicate_integer_less_than() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test: age < 30
        // Row group 0: min=18, max=25 -> should keep (definitely < 30)
        // Row group 1: min=26, max=65 -> should keep (might contain < 30)
        let predicate = Predicate::lt("age", PredicateValue::Int32(Some(30)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0]); // Row group 0: max=25 < 30, keep
        assert!(result[1]); // Row group 1: min=26 < 30, keep
    }

    #[test]
    fn test_evaluate_predicate_integer_less_than_or_equal() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test: age <= 20
        // Row group 0: min=18, max=25 -> should keep (might contain <= 20)
        // Row group 1: min=26, max=65 -> should skip (min > 20)
        let predicate = Predicate::lte("age", PredicateValue::Int32(Some(20)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0]); // Row group 0: min=18 <= 20, keep
        assert!(!result[1]); // Row group 1: min=26 > 20, skip
    }

    #[test]
    fn test_evaluate_predicate_integer_equal() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test: age = 20
        // Row group 0: min=18, max=25 -> should keep (20 is within range)
        // Row group 1: min=26, max=65 -> should skip (20 is not in range)
        let predicate = Predicate::eq("age", PredicateValue::Int32(Some(20)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0]); // Row group 0: 20 is within [18, 25]
        assert!(!result[1]); // Row group 1: 20 is not within [26, 65]
    }

    #[test]
    fn test_evaluate_predicate_integer_not_equal() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test: age != 20
        // Row group 0: min=18, max=25 -> should keep (might have values != 20)
        // Row group 1: min=26, max=65 -> should keep (definitely != 20)
        let predicate = Predicate::ne("age", PredicateValue::Int32(Some(20)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        // For !=, we keep if value is not the only value in the range
        // Row group 0: [18, 25] contains more than just 20, so keep
        assert!(result[0]);
        // Row group 1: [26, 65] doesn't contain 20, so keep
        assert!(result[1]);
    }

    #[test]
    fn test_evaluate_predicate_integer_not_equal_single_value() {
        use crate::predicate::{Predicate, PredicateValue};
        use crate::row_index::{RowGroupEntry, RowGroupIndex};
        use std::collections::HashMap;

        // Create a row index where one row group has min=max=20
        let mut columns = HashMap::new();
        let entries = vec![RowGroupEntry::new(
            Some({
                let proto_stats = proto::ColumnStatistics {
                    number_of_values: Some(1000),
                    has_null: Some(false),
                    int_statistics: Some(proto::IntegerStatistics {
                        minimum: Some(20),
                        maximum: Some(20), // Single value
                        sum: Some(20000),
                    }),
                    ..Default::default()
                };
                ColumnStatistics::try_from(&proto_stats).unwrap()
            }),
            vec![],
        )];
        columns.insert(1, RowGroupIndex::new(entries, 10000, 1));
        let row_index = StripeRowIndex::new(columns, 10000, 10000);
        let schema = create_test_schema();

        // Test: age != 20
        // Row group: min=20, max=20 -> should skip (all values equal 20)
        let predicate = Predicate::ne("age", PredicateValue::Int32(Some(20)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result[0]); // All values are 20, so skip
    }

    #[test]
    fn test_bloom_filter_rejects_non_matching_value() {
        use crate::predicate::{Predicate, PredicateValue};

        // Build a bloom filter that only contains the value 10
        let num_hash_functions = 3;
        let mut bloom_filter = BloomFilter::from_parts(num_hash_functions, vec![0u64; 2]);
        bloom_filter.add_hash(BloomFilter::hash_long(10));

        // Attach bloom filter to a single row group
        let entry = RowGroupEntry::new(None, vec![]).with_bloom_filter(Some(bloom_filter));
        let mut columns = HashMap::new();
        columns.insert(1, RowGroupIndex::new(vec![entry], 10000, 1));
        let row_index = StripeRowIndex::new(columns, 10000, 10000);
        let schema = create_test_schema();

        // Predicate seeks value 20, which the bloom filter should reject
        let predicate = Predicate::eq("age", PredicateValue::Int32(Some(20)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result[0]); // Bloom filter proves value 20 absent
    }

    #[test]
    fn test_statistics_short_circuit_before_bloom_filter() {
        use crate::predicate::{Predicate, PredicateValue};

        // Build a bloom filter that claims value 50 may exist
        let num_hash_functions = 3;
        let mut bloom_filter = BloomFilter::from_parts(num_hash_functions, vec![0u64; 2]);
        bloom_filter.add_hash(BloomFilter::hash_long(50));

        // Row group stats that make the predicate impossible (min/max do not cover 50)
        let stats = {
            let proto_stats = proto::ColumnStatistics {
                number_of_values: Some(1000),
                has_null: Some(false),
                int_statistics: Some(proto::IntegerStatistics {
                    minimum: Some(100),
                    maximum: Some(200),
                    sum: Some(150000),
                }),
                ..Default::default()
            };
            ColumnStatistics::try_from(&proto_stats).unwrap()
        };

        let entry = RowGroupEntry::new(Some(stats), vec![]).with_bloom_filter(Some(bloom_filter));
        let mut columns = HashMap::new();
        columns.insert(1, RowGroupIndex::new(vec![entry], 10_000, 1));
        let row_index = StripeRowIndex::new(columns, 10_000, 10_000);
        let schema = create_test_schema();

        // Predicate seeks value 50; stats should short-circuit to false regardless of bloom filter
        let predicate = Predicate::eq("age", PredicateValue::Int32(Some(50)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result[0]); // min/max exclude 50, so stats short-circuit before bloom
    }

    #[test]
    fn test_evaluate_predicate_and_combination() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test: age >= 20 AND age <= 30
        // Row group 0: min=18, max=25 -> should keep (overlaps with [20, 30])
        // Row group 1: min=26, max=65 -> should keep (overlaps with [20, 30])
        let predicate = Predicate::and(vec![
            Predicate::gte("age", PredicateValue::Int32(Some(20))),
            Predicate::lte("age", PredicateValue::Int32(Some(30))),
        ]);
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        // Both row groups might contain values in [20, 30]
        assert!(result[0]);
        assert!(result[1]);
    }

    #[test]
    fn test_evaluate_predicate_or_combination() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test: age < 20 OR age > 30
        // Row group 0: min=18, max=25 -> should keep (might have < 20 or > 30)
        // Row group 1: min=26, max=65 -> should keep (definitely has > 30)
        let predicate = Predicate::or(vec![
            Predicate::lt("age", PredicateValue::Int32(Some(20))),
            Predicate::gt("age", PredicateValue::Int32(Some(30))),
        ]);
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        // Row group 0: might have values < 20
        assert!(result[0]);
        // Row group 1: definitely has values > 30
        assert!(result[1]);
    }

    #[test]
    fn test_evaluate_predicate_is_null() {
        use crate::predicate::Predicate;
        use crate::row_index::{RowGroupEntry, RowGroupIndex};
        use std::collections::HashMap;

        // Create row index with has_null information
        let mut columns = HashMap::new();
        let entries = vec![
            RowGroupEntry::new(
                Some({
                    let proto_stats = proto::ColumnStatistics {
                        number_of_values: Some(5000),
                        has_null: Some(true), // Has nulls
                        int_statistics: Some(proto::IntegerStatistics {
                            minimum: Some(18),
                            maximum: Some(25),
                            sum: Some(107500),
                        }),
                        ..Default::default()
                    };
                    ColumnStatistics::try_from(&proto_stats).unwrap()
                }),
                vec![],
            ),
            RowGroupEntry::new(
                Some({
                    let proto_stats = proto::ColumnStatistics {
                        number_of_values: Some(5000),
                        has_null: Some(false), // No nulls
                        int_statistics: Some(proto::IntegerStatistics {
                            minimum: Some(26),
                            maximum: Some(65),
                            sum: Some(227500),
                        }),
                        ..Default::default()
                    };
                    ColumnStatistics::try_from(&proto_stats).unwrap()
                }),
                vec![],
            ),
        ];
        columns.insert(1, RowGroupIndex::new(entries, 10000, 1));
        let row_index = StripeRowIndex::new(columns, 20000, 10000);
        let schema = create_test_schema();

        // Test: age IS NULL
        let predicate = Predicate::is_null("age");
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0]); // Row group 0: has_null = true
        assert!(!result[1]); // Row group 1: has_null = false
    }

    #[test]
    fn test_evaluate_predicate_is_not_null() {
        use crate::predicate::Predicate;
        use crate::row_index::{RowGroupEntry, RowGroupIndex};
        use std::collections::HashMap;

        // Create row index with number_of_values information
        let mut columns = HashMap::new();
        let entries = vec![
            RowGroupEntry::new(
                Some({
                    let proto_stats = proto::ColumnStatistics {
                        number_of_values: Some(5000), // Has non-null values
                        has_null: Some(true),
                        int_statistics: Some(proto::IntegerStatistics {
                            minimum: Some(18),
                            maximum: Some(25),
                            sum: Some(107500),
                        }),
                        ..Default::default()
                    };
                    ColumnStatistics::try_from(&proto_stats).unwrap()
                }),
                vec![],
            ),
            RowGroupEntry::new(
                Some({
                    let proto_stats = proto::ColumnStatistics {
                        number_of_values: Some(0), // All nulls
                        has_null: Some(true),
                        int_statistics: None,
                        ..Default::default()
                    };
                    ColumnStatistics::try_from(&proto_stats).unwrap()
                }),
                vec![],
            ),
        ];
        columns.insert(1, RowGroupIndex::new(entries, 10000, 1));
        let row_index = StripeRowIndex::new(columns, 20000, 10000);
        let schema = create_test_schema();

        // Test: age IS NOT NULL
        let predicate = Predicate::is_not_null("age");
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result[0]); // Row group 0: number_of_values > 0
        assert!(!result[1]); // Row group 1: number_of_values = 0
    }

    #[test]
    fn test_evaluate_predicate_missing_column() {
        use crate::predicate::{Predicate, PredicateValue};

        let row_index = create_test_row_index(10000, 20000);
        let schema = create_test_schema();

        // Test with non-existent column
        let predicate = Predicate::gt("nonexistent", PredicateValue::Int32(Some(10)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_predicate_missing_row_index() {
        use crate::predicate::{Predicate, PredicateValue};
        use crate::row_index::StripeRowIndex;
        use std::collections::HashMap;

        // Create row index without the column we're querying
        let row_index = StripeRowIndex::new(HashMap::new(), 20000, 10000);
        let schema = create_test_schema();

        // Test with column that has no row index
        let predicate = Predicate::gt("age", PredicateValue::Int32(Some(10)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema);

        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_predicate_missing_statistics() {
        use crate::predicate::{Predicate, PredicateValue};
        use crate::row_index::{RowGroupEntry, RowGroupIndex};
        use std::collections::HashMap;

        // Create row index with missing statistics
        let mut columns = HashMap::new();
        let entries = vec![
            RowGroupEntry::new(None, vec![]), // No statistics
        ];
        columns.insert(1, RowGroupIndex::new(entries, 10000, 1));
        let row_index = StripeRowIndex::new(columns, 10000, 10000);
        let schema = create_test_schema();

        // Test: age > 10
        // Should keep row group when statistics are missing (conservative)
        let predicate = Predicate::gt("age", PredicateValue::Int32(Some(10)));
        let result = super::evaluate_predicate(&predicate, &row_index, &schema).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0]); // Keep when statistics missing
    }
}
