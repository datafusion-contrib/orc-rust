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

//! ORC Row Selection and Filtering
//!
//! This module provides row-level filtering capabilities for ORC files.
//! It focuses on decoding-time filtering and row selection during
//! the ORC file reading process.

use arrow::array::{Array, BooleanArray};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_select::filter::SlicesIterator;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;

/// [`RowSelector`] specifies whether to select or skip a number of rows
/// when scanning an ORC file
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RowSelector {
    /// The number of rows
    pub row_count: usize,
    /// If true, skip `row_count` rows
    pub skip: bool,
}

impl RowSelector {
    /// Select `row_count` rows
    pub fn select(row_count: usize) -> Self {
        Self {
            row_count,
            skip: false,
        }
    }

    /// Skip `row_count` rows
    pub fn skip(row_count: usize) -> Self {
        Self {
            row_count,
            skip: true,
        }
    }
}

/// [`RowSelection`] allows selecting or skipping a provided number of rows
/// when scanning the ORC file.
///
/// This is applied prior to reading column data, and can therefore
/// be used to skip IO to fetch data into memory
///
/// A typical use-case would be using statistics to filter out rows
/// that don't satisfy a predicate
///
/// # Example
/// ```
/// use orc_rust::selection::{RowSelection, RowSelector};
///
/// // Select first 100 rows, skip next 50, then select 200 more
/// let selection = RowSelection::from(vec![
///     RowSelector::select(100),
///     RowSelector::skip(50),
///     RowSelector::select(200),
/// ]);
/// ```
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct RowSelection {
    selectors: Vec<RowSelector>,
}

impl RowSelection {
    /// Creates a [`RowSelection`] from a vector of [`RowSelector`]
    pub fn from(selectors: Vec<RowSelector>) -> Self {
        Self { selectors }
    }

    /// Creates a [`RowSelection`] from a slice of [`BooleanArray`]
    ///
    /// # Panic
    ///
    /// Panics if any of the [`BooleanArray`] contain nulls
    pub fn from_filters(filters: &[BooleanArray]) -> Self {
        let mut next_offset = 0;
        let total_rows = filters.iter().map(|x| x.len()).sum();

        let iter = filters.iter().flat_map(|filter| {
            let offset = next_offset;
            next_offset += filter.len();
            assert_eq!(filter.null_count(), 0);
            SlicesIterator::new(filter).map(move |(start, end)| start + offset..end + offset)
        });

        Self::from_consecutive_ranges(iter, total_rows)
    }

    /// Creates a [`RowSelection`] from an iterator of consecutive ranges to keep
    pub fn from_consecutive_ranges<I: Iterator<Item = Range<usize>>>(
        ranges: I,
        total_rows: usize,
    ) -> Self {
        let mut selectors: Vec<RowSelector> = Vec::with_capacity(ranges.size_hint().0);
        let mut last_end = 0;
        for range in ranges {
            let len = range.end - range.start;
            if len == 0 {
                continue;
            }

            match range.start.cmp(&last_end) {
                Ordering::Equal => match selectors.last_mut() {
                    Some(last) => last.row_count = last.row_count.checked_add(len).unwrap(),
                    None => selectors.push(RowSelector::select(len)),
                },
                Ordering::Greater => {
                    selectors.push(RowSelector::skip(range.start - last_end));
                    selectors.push(RowSelector::select(len))
                }
                Ordering::Less => panic!("out of order"),
            }
            last_end = range.end;
        }

        if last_end != total_rows {
            selectors.push(RowSelector::skip(total_rows - last_end))
        }

        Self { selectors }
    }

    /// Returns `true` if this [`RowSelection`] selects any rows
    pub fn selects_any(&self) -> bool {
        self.selectors.iter().any(|x| !x.skip)
    }

    /// Trims this [`RowSelection`] removing any trailing skips
    pub(crate) fn trim(mut self) -> Self {
        while self.selectors.last().map(|x| x.skip).unwrap_or(false) {
            self.selectors.pop();
        }
        self
    }

    /// Applies an offset to this [`RowSelection`], skipping the first `offset` selected rows
    pub(crate) fn offset(mut self, offset: usize) -> Self {
        if offset == 0 {
            return self;
        }

        let mut selected_count = 0;
        let mut skipped_count = 0;

        // Find the index where the selector exceeds the row count
        let find = self
            .selectors
            .iter()
            .position(|selector| match selector.skip {
                true => {
                    skipped_count += selector.row_count;
                    false
                }
                false => {
                    selected_count += selector.row_count;
                    selected_count > offset
                }
            });

        let split_idx = match find {
            Some(idx) => idx,
            None => {
                self.selectors.clear();
                return self;
            }
        };

        let mut selectors = Vec::with_capacity(self.selectors.len() - split_idx + 1);
        selectors.push(RowSelector::skip(skipped_count + offset));
        selectors.push(RowSelector::select(selected_count - offset));
        selectors.extend_from_slice(&self.selectors[split_idx + 1..]);

        Self { selectors }
    }

    /// Limit this [`RowSelection`] to only select `limit` rows
    pub(crate) fn limit(mut self, mut limit: usize) -> Self {
        if limit == 0 {
            self.selectors.clear();
        }

        for (idx, selection) in self.selectors.iter_mut().enumerate() {
            if !selection.skip {
                if selection.row_count >= limit {
                    selection.row_count = limit;
                    self.selectors.truncate(idx + 1);
                    break;
                } else {
                    limit -= selection.row_count;
                }
            }
        }
        self
    }

    /// Returns an iterator over the [`RowSelector`]s for this
    /// [`RowSelection`].
    pub fn iter(&self) -> impl Iterator<Item = &RowSelector> {
        self.selectors.iter()
    }

    /// Returns the number of selected rows
    pub fn row_count(&self) -> usize {
        self.iter().filter(|s| !s.skip).map(|s| s.row_count).sum()
    }

    /// Returns the number of de-selected rows
    pub fn skipped_row_count(&self) -> usize {
        self.iter().filter(|s| s.skip).map(|s| s.row_count).sum()
    }
}

impl From<RowSelection> for VecDeque<RowSelector> {
    fn from(value: RowSelection) -> Self {
        value.selectors.into()
    }
}

impl From<Vec<RowSelector>> for RowSelection {
    fn from(selectors: Vec<RowSelector>) -> Self {
        Self::from(selectors)
    }
}

impl From<RowSelection> for Vec<RowSelector> {
    fn from(value: RowSelection) -> Self {
        value.selectors
    }
}

/// Trait for predicates that can be evaluated on Arrow RecordBatches
/// during ORC decoding.
///
/// This trait allows for custom filtering logic to be applied
/// during the ORC file reading process.
pub trait ArrowPredicate: Send + 'static {
    /// Returns the projection mask that describes the columns required
    /// to evaluate this predicate
    fn projection(&self) -> &crate::projection::ProjectionMask;

    /// Evaluate this predicate for the given RecordBatch
    /// Must return a BooleanArray with the same length as the input batch
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError>;
}

/// An ArrowPredicate created from a function and a projection mask
pub struct ArrowPredicateFn<F> {
    f: F,
    projection: crate::projection::ProjectionMask,
}

impl<F> ArrowPredicateFn<F>
where
    F: FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static,
{
    pub fn new(f: F, projection: crate::projection::ProjectionMask) -> Self {
        Self { f, projection }
    }
}

impl<F> ArrowPredicate for ArrowPredicateFn<F>
where
    F: FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static,
{
    fn projection(&self) -> &crate::projection::ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        (self.f)(batch)
    }
}

/// Filter applied *during* the ORC read process
///
/// RowFilter applies predicates in order, after decoding only the columns
/// required. As predicates eliminate rows, fewer rows from subsequent columns
/// may be required, thus potentially reducing IO and decode.
///
/// A RowFilter consists of a list of ArrowPredicates. Only the rows for which
/// all the predicates evaluate to true will be returned.
pub struct RowFilter {
    predicates: Vec<Box<dyn ArrowPredicate>>,
}

impl RowFilter {
    /// Create a new RowFilter with the given predicates
    pub fn new(predicates: Vec<Box<dyn ArrowPredicate>>) -> Self {
        Self { predicates }
    }

    /// Add a predicate to this filter
    pub fn add_predicate(&mut self, predicate: Box<dyn ArrowPredicate>) {
        self.predicates.push(predicate);
    }

    /// Get the predicates
    pub fn predicates(&self) -> &[Box<dyn ArrowPredicate>] {
        &self.predicates
    }

    /// Get mutable access to the predicates
    pub fn predicates_mut(&mut self) -> &mut Vec<Box<dyn ArrowPredicate>> {
        &mut self.predicates
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_row_selector() {
        let selector = RowSelector::select(100);
        assert_eq!(selector.row_count, 100);
        assert!(!selector.skip);

        let selector = RowSelector::skip(50);
        assert_eq!(selector.row_count, 50);
        assert!(selector.skip);
    }

    #[test]
    fn test_row_selection_from_selectors() {
        let selection = RowSelection::from(vec![
            RowSelector::select(100),
            RowSelector::skip(50),
            RowSelector::select(200),
        ]);

        assert_eq!(selection.row_count(), 300);
        assert_eq!(selection.skipped_row_count(), 50);
        assert!(selection.selects_any());
    }

    #[test]
    fn test_row_selection_from_filters() {
        // Create a boolean array that selects rows 1, 3, 4
        let filter = BooleanArray::from(vec![false, true, false, true, true]);
        let selection = RowSelection::from_filters(&[filter]);

        assert_eq!(selection.row_count(), 3);
        assert_eq!(selection.skipped_row_count(), 2);
    }

    #[test]
    fn test_arrow_predicate_fn() {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let col = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let batch = RecordBatch::try_new(schema, vec![col]).unwrap();

        let projection = crate::projection::ProjectionMask::all();
        let mut predicate = ArrowPredicateFn::new(
            |batch| {
                let col = batch.column(0);
                let int_array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(BooleanArray::from(
                    int_array
                        .iter()
                        .map(|v| v.map(|x| x > 2))
                        .collect::<Vec<_>>(),
                ))
            },
            projection,
        );

        let result = predicate.evaluate(batch).unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result.value(0), false);
        assert_eq!(result.value(1), false);
        assert_eq!(result.value(2), false);
        assert_eq!(result.value(3), true);
        assert_eq!(result.value(4), true);
    }

    #[test]
    fn test_row_filter() {
        let projection = crate::projection::ProjectionMask::all();
        let predicate1 = Box::new(ArrowPredicateFn::new(
            |_batch| Ok(BooleanArray::from(vec![true, false, true])),
            projection.clone(),
        ));
        let predicate2 = Box::new(ArrowPredicateFn::new(
            |_batch| Ok(BooleanArray::from(vec![true, true, false])),
            projection,
        ));

        let mut filter = RowFilter::new(vec![predicate1]);
        filter.add_predicate(predicate2);

        assert_eq!(filter.predicates().len(), 2);
    }
}
