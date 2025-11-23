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

//! Predicate types for row group filtering
//!
//! This module provides simplified predicate expressions that can be evaluated
//! against row group statistics to filter out row groups before decoding.

/// A simplified value type for predicates
///
/// This is a simplified representation of scalar values for predicate evaluation.
/// In the future, this could be replaced with arrow's ScalarValue if available.
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateValue {
    /// Boolean value
    Boolean(Option<bool>),
    /// 8-bit signed integer
    Int8(Option<i8>),
    /// 16-bit signed integer
    Int16(Option<i16>),
    /// 32-bit signed integer
    Int32(Option<i32>),
    /// 64-bit signed integer
    Int64(Option<i64>),
    /// 32-bit floating point
    Float32(Option<f32>),
    /// 64-bit floating point
    Float64(Option<f64>),
    /// UTF-8 string
    Utf8(Option<String>),
}

// For backward compatibility, we'll use PredicateValue as the value type
// Users can convert from arrow types if needed
pub type ScalarValue = PredicateValue;

/// Comparison operator for predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    /// Equal to
    Equal,
    /// Not equal to
    NotEqual,
    /// Less than
    LessThan,
    /// Less than or equal to
    LessThanOrEqual,
    /// Greater than
    GreaterThan,
    /// Greater than or equal to
    GreaterThanOrEqual,
}

/// A predicate that can be evaluated against row group statistics
///
/// Predicates are simplified expressions used for filtering row groups before
/// decoding. They support basic comparison operations, NULL checks, and logical
/// combinations (AND, OR, NOT).
///
/// # Example
///
/// ```rust
/// use orc_rust::predicate::{Predicate, ComparisonOp, PredicateValue};
///
/// // Create a predicate: age >= 18
/// let predicate = Predicate::gte("age", PredicateValue::Int32(Some(18)));
///
/// // Create a compound predicate: age >= 18 AND city = 'NYC'
/// let predicate = Predicate::and(vec![
///     Predicate::gte("age", PredicateValue::Int32(Some(18))),
///     Predicate::eq("city", PredicateValue::Utf8(Some("NYC".to_string()))),
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Column comparison: column <op> literal
    Comparison {
        /// Column name to compare
        column: String,
        /// Comparison operator
        op: ComparisonOp,
        /// Value to compare against
        value: ScalarValue,
    },
    /// IS NULL check
    IsNull {
        /// Column name to check
        column: String,
    },
    /// IS NOT NULL check
    IsNotNull {
        /// Column name to check
        column: String,
    },
    /// Logical AND of predicates
    And(Vec<Predicate>),
    /// Logical OR of predicates
    Or(Vec<Predicate>),
    /// Logical NOT
    Not(Box<Predicate>),
}

impl Predicate {
    /// Create a comparison predicate: column <op> value
    pub fn comparison(column: &str, op: ComparisonOp, value: ScalarValue) -> Self {
        Self::Comparison {
            column: column.to_string(),
            op,
            value,
        }
    }

    /// Create a predicate for column == value
    pub fn eq(column: &str, value: ScalarValue) -> Self {
        Self::comparison(column, ComparisonOp::Equal, value)
    }

    /// Create a predicate for column != value
    pub fn ne(column: &str, value: ScalarValue) -> Self {
        Self::comparison(column, ComparisonOp::NotEqual, value)
    }

    /// Create a predicate for column < value
    pub fn lt(column: &str, value: ScalarValue) -> Self {
        Self::comparison(column, ComparisonOp::LessThan, value)
    }

    /// Create a predicate for column <= value
    pub fn lte(column: &str, value: ScalarValue) -> Self {
        Self::comparison(column, ComparisonOp::LessThanOrEqual, value)
    }

    /// Create a predicate for column > value
    pub fn gt(column: &str, value: ScalarValue) -> Self {
        Self::comparison(column, ComparisonOp::GreaterThan, value)
    }

    /// Create a predicate for column >= value
    pub fn gte(column: &str, value: ScalarValue) -> Self {
        Self::comparison(column, ComparisonOp::GreaterThanOrEqual, value)
    }

    /// Create a predicate for column IS NULL
    pub fn is_null(column: &str) -> Self {
        Self::IsNull {
            column: column.to_string(),
        }
    }

    /// Create a predicate for column IS NOT NULL
    pub fn is_not_null(column: &str) -> Self {
        Self::IsNotNull {
            column: column.to_string(),
        }
    }

    /// Combine predicates with AND
    pub fn and(predicates: Vec<Predicate>) -> Self {
        Self::And(predicates)
    }

    /// Combine predicates with OR
    pub fn or(predicates: Vec<Predicate>) -> Self {
        Self::Or(predicates)
    }

    /// Negate a predicate
    #[allow(clippy::should_implement_trait)]
    pub fn not(predicate: Predicate) -> Self {
        Self::Not(Box::new(predicate))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicate_creation() {
        let p1 = Predicate::eq("age", ScalarValue::Int32(Some(18)));
        assert!(
            matches!(p1, Predicate::Comparison { ref column, op: ComparisonOp::Equal, .. } if column == "age")
        );

        let p2 = Predicate::gt("price", ScalarValue::Float64(Some(100.0)));
        assert!(
            matches!(p2, Predicate::Comparison { ref column, op: ComparisonOp::GreaterThan, .. } if column == "price")
        );

        let p3 = Predicate::is_null("description");
        assert!(matches!(p3, Predicate::IsNull { ref column } if column == "description"));

        let combined = Predicate::and(vec![p1, p2]);
        assert!(matches!(combined, Predicate::And(_)));
    }
}
