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

use crate::error::{Result, UnexpectedSnafu};
use crate::predicate::{ComparisonOp, Predicate, PredicateValue};
use crate::row_index::StripeRowIndex;
use crate::schema::RootDataType;
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
        .enumerate()
        .find(|(_, col)| col.name() == column_name)
        .map(|(idx, _)| idx)
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
        if let Some(stats) = &entry.statistics {
            let matches = evaluate_comparison_with_stats(stats, op, value)?;
            *result_item = matches;
        } else {
            // No statistics available, keep row group (maybe)
            *result_item = true;
        }
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
}
