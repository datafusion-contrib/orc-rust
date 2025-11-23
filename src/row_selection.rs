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

//! Row selection for ORC files
//!
//! This module provides [`RowSelection`] and [`RowSelector`] types for
//! efficiently skipping rows when scanning ORC files.

use arrow::array::{Array, BooleanArray};
use std::cmp::Ordering;
use std::ops::Range;

/// [`RowSelector`] represents a consecutive range of rows to either select or skip
/// when scanning an ORC file.
///
/// A [`RowSelector`] is a building block of [`RowSelection`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RowSelector {
    /// The number of rows
    pub row_count: usize,

    /// If true, skip `row_count` rows; otherwise select them
    pub skip: bool,
}

impl RowSelector {
    /// Create a selector to select `row_count` rows
    pub fn select(row_count: usize) -> Self {
        Self {
            row_count,
            skip: false,
        }
    }

    /// Create a selector to skip `row_count` rows
    pub fn skip(row_count: usize) -> Self {
        Self {
            row_count,
            skip: true,
        }
    }
}

/// [`RowSelection`] allows selecting or skipping rows when scanning an ORC file.
///
/// This is applied prior to reading column data, and can therefore be used to
/// skip IO to fetch data into memory, improving query performance.
///
/// A typical use-case would be using ORC stripe statistics or file-level
/// indexes to filter out rows that don't satisfy a predicate.
///
/// # Example
///
/// ```
/// use orc_rust::row_selection::{RowSelection, RowSelector};
///
/// // Create selectors: skip 100 rows, select 50, skip 200
/// let selectors = vec![
///     RowSelector::skip(100),
///     RowSelector::select(50),
///     RowSelector::skip(200),
/// ];
///
/// let selection: RowSelection = selectors.into();
///
/// // Query properties
/// assert_eq!(selection.row_count(), 350);
/// assert_eq!(selection.selects_any(), true);
/// ```
///
/// A [`RowSelection`] maintains the following invariants:
///
/// * It contains no [`RowSelector`] with 0 rows
/// * Consecutive [`RowSelector`]s alternate between skipping and selecting rows
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct RowSelection {
    selectors: Vec<RowSelector>,
}

impl RowSelection {
    /// Create a new empty [`RowSelection`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a [`RowSelection`] from a slice of [`BooleanArray`]
    ///
    /// # Panics
    ///
    /// Panics if any of the [`BooleanArray`] contain nulls
    pub fn from_filters(filters: &[BooleanArray]) -> Self {
        let mut next_offset = 0;
        let total_rows = filters.iter().map(|x| x.len()).sum();

        let iter = filters.iter().flat_map(|filter| {
            let offset = next_offset;
            next_offset += filter.len();
            assert_eq!(
                filter.null_count(),
                0,
                "filter arrays must not contain nulls"
            );

            // Find consecutive ranges of true values
            let mut ranges = vec![];
            let mut start = None;
            for (idx, value) in filter.iter().enumerate() {
                match (value, start) {
                    (Some(true), None) => start = Some(idx),
                    (Some(false), Some(s)) | (None, Some(s)) => {
                        ranges.push(s + offset..idx + offset);
                        start = None;
                    }
                    _ => {}
                }
            }
            if let Some(s) = start {
                ranges.push(s + offset..filter.len() + offset);
            }
            ranges
        });

        Self::from_consecutive_ranges(iter, total_rows)
    }

    /// Create a [`RowSelection`] from an iterator of consecutive ranges to keep
    ///
    /// # Arguments
    ///
    /// * `ranges` - Iterator of consecutive ranges (e.g., `10..20`, `30..40`)
    /// * `total_rows` - Total number of rows in the stripe/file
    ///
    /// # Example
    ///
    /// ```
    /// use orc_rust::row_selection::RowSelection;
    ///
    /// // Select rows 10-19 and 30-39 out of 50 total rows
    /// let selection = RowSelection::from_consecutive_ranges(
    ///     vec![10..20, 30..40].into_iter(),
    ///     50
    /// );
    /// ```
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
                Ordering::Equal => {
                    // Extend the last selector
                    match selectors.last_mut() {
                        Some(last) if !last.skip => {
                            last.row_count = last.row_count.checked_add(len).unwrap()
                        }
                        _ => selectors.push(RowSelector::select(len)),
                    }
                }
                Ordering::Greater => {
                    // Add a skip selector for the gap, then a select selector
                    selectors.push(RowSelector::skip(range.start - last_end));
                    selectors.push(RowSelector::select(len));
                }
                Ordering::Less => {
                    panic!("ranges must be provided in order and must not overlap")
                }
            }
            last_end = range.end;
        }

        // Add final skip if we didn't cover all rows
        if last_end < total_rows {
            selectors.push(RowSelector::skip(total_rows - last_end));
        }

        Self { selectors }
    }

    /// Create a [`RowSelection`] that selects all `row_count` rows
    pub fn select_all(row_count: usize) -> Self {
        if row_count == 0 {
            return Self::default();
        }
        Self {
            selectors: vec![RowSelector::select(row_count)],
        }
    }

    /// Create a [`RowSelection`] that skips all `row_count` rows
    pub fn skip_all(row_count: usize) -> Self {
        if row_count == 0 {
            return Self::default();
        }
        Self {
            selectors: vec![RowSelector::skip(row_count)],
        }
    }

    /// Returns the total number of rows (selected + skipped)
    pub fn row_count(&self) -> usize {
        self.selectors.iter().map(|s| s.row_count).sum()
    }

    /// Returns the number of selected rows
    pub fn selected_row_count(&self) -> usize {
        self.selectors
            .iter()
            .filter(|s| !s.skip)
            .map(|s| s.row_count)
            .sum()
    }

    /// Returns the number of skipped rows
    pub fn skipped_row_count(&self) -> usize {
        self.selectors
            .iter()
            .filter(|s| s.skip)
            .map(|s| s.row_count)
            .sum()
    }

    /// Returns true if this selection selects any rows
    pub fn selects_any(&self) -> bool {
        self.selectors.iter().any(|s| !s.skip)
    }

    /// Returns an iterator over the [`RowSelector`]s
    pub fn iter(&self) -> impl Iterator<Item = &RowSelector> {
        self.selectors.iter()
    }

    /// Returns a slice of the underlying [`RowSelector`]s
    pub fn selectors(&self) -> &[RowSelector] {
        &self.selectors
    }

    /// Splits off the first `row_count` rows from this [`RowSelection`]
    ///
    /// Returns a new [`RowSelection`] containing the first `row_count` rows,
    /// and updates `self` to contain the remaining rows.
    ///
    /// # Example
    ///
    /// ```
    /// use orc_rust::row_selection::{RowSelection, RowSelector};
    ///
    /// let mut selection = RowSelection::from_consecutive_ranges(
    ///     vec![10..20, 30..40].into_iter(),
    ///     50
    /// );
    ///
    /// let first = selection.split_off(25);
    /// assert_eq!(first.row_count(), 25);
    /// assert_eq!(selection.row_count(), 25);
    /// ```
    pub fn split_off(&mut self, row_count: usize) -> Self {
        let mut total_count = 0;

        // Find the index where the selector exceeds the row count
        let find = self.selectors.iter().position(|selector| {
            total_count += selector.row_count;
            total_count > row_count
        });

        let split_idx = match find {
            Some(idx) => idx,
            None => {
                // Return all selectors if row_count exceeds total
                let selectors = std::mem::take(&mut self.selectors);
                return Self { selectors };
            }
        };

        let mut remaining = self.selectors.split_off(split_idx);

        // Split the selector that crosses the boundary
        let next = remaining.first_mut().unwrap();
        let overflow = total_count - row_count;

        if next.row_count != overflow {
            self.selectors.push(RowSelector {
                row_count: next.row_count - overflow,
                skip: next.skip,
            });
        }
        next.row_count = overflow;

        std::mem::swap(&mut remaining, &mut self.selectors);
        Self {
            selectors: remaining,
        }
    }

    /// Create a [`RowSelection`] from row group filter results
    ///
    /// This function converts a boolean vector (one per row group) into a
    /// [`RowSelection`] that skips or selects entire row groups.
    ///
    /// # Arguments
    ///
    /// * `row_group_filter` - Boolean vector where `true` means keep the row group,
    ///   `false` means skip it. Each element corresponds to one row group.
    /// * `rows_per_group` - Number of rows in each row group (typically 10,000)
    /// * `total_rows` - Total number of rows in the stripe/file
    ///
    /// # Returns
    ///
    /// A [`RowSelection`] where:
    /// - `false` entries become `RowSelector::skip(rows_per_group)`
    /// - `true` entries become `RowSelector::select(rows_per_group)`
    /// - Adjacent selectors of the same type are merged
    ///
    /// # Example
    ///
    /// ```
    /// use orc_rust::row_selection::RowSelection;
    ///
    /// // 3 row groups: skip first, keep second, skip third
    /// let filter = vec![false, true, false];
    /// let selection = RowSelection::from_row_group_filter(&filter, 10000, 30000);
    ///
    /// // Result: skip(10000) + select(10000) + skip(10000)
    /// assert_eq!(selection.row_count(), 30000);
    /// assert_eq!(selection.selected_row_count(), 10000);
    /// ```
    pub fn from_row_group_filter(
        row_group_filter: &[bool],
        rows_per_group: usize,
        total_rows: usize,
    ) -> Self {
        if row_group_filter.is_empty() {
            return Self::skip_all(total_rows);
        }

        let num_row_groups = row_group_filter.len();
        let mut selectors: Vec<RowSelector> = Vec::new();

        for &keep in row_group_filter {
            let selector = if keep {
                RowSelector::select(rows_per_group)
            } else {
                RowSelector::skip(rows_per_group)
            };

            // Merge with previous selector if same type
            match selectors.last_mut() {
                Some(last) if last.skip == selector.skip => {
                    last.row_count = last.row_count.checked_add(rows_per_group).unwrap();
                }
                _ => selectors.push(selector),
            }
        }

        // Handle remaining rows if row groups don't cover all rows
        let covered_rows = num_row_groups * rows_per_group;
        if covered_rows < total_rows {
            let remaining = total_rows - covered_rows;
            // Add remaining rows as skip (they're not in any row group)
            match selectors.last_mut() {
                Some(last) if last.skip => {
                    last.row_count = last.row_count.checked_add(remaining).unwrap();
                }
                _ => selectors.push(RowSelector::skip(remaining)),
            }
        }

        Self { selectors }
    }

    /// Combine two [`RowSelection`]s using logical AND
    ///
    /// Returns a new [`RowSelection`] representing rows that are selected
    /// in both input selections.
    ///
    /// # Panics
    ///
    /// Panics if `other` does not have a length equal to the number of rows
    /// selected by this RowSelection
    pub fn and_then(&self, other: &Self) -> Self {
        let mut selectors = vec![];
        let mut first = self.selectors.iter().cloned().peekable();
        let mut second = other.selectors.iter().cloned().peekable();

        let mut to_skip = 0;
        while let Some(b) = second.peek_mut() {
            let a = first
                .peek_mut()
                .expect("selection exceeds the number of selected rows");

            if b.row_count == 0 {
                second.next().unwrap();
                continue;
            }

            if a.row_count == 0 {
                first.next().unwrap();
                continue;
            }

            if a.skip {
                // Records were skipped when producing second
                to_skip += a.row_count;
                first.next().unwrap();
                continue;
            }

            let skip = b.skip;
            let to_process = a.row_count.min(b.row_count);

            a.row_count -= to_process;
            b.row_count -= to_process;

            match skip {
                true => to_skip += to_process,
                false => {
                    if to_skip != 0 {
                        selectors.push(RowSelector::skip(to_skip));
                        to_skip = 0;
                    }
                    selectors.push(RowSelector::select(to_process));
                }
            }
        }

        // Process any remaining selectors from first (should all be skip)
        for v in first {
            if v.row_count != 0 {
                assert!(
                    v.skip,
                    "selection contains less than the number of selected rows"
                );
                to_skip += v.row_count;
            }
        }

        if to_skip != 0 {
            selectors.push(RowSelector::skip(to_skip));
        }

        Self { selectors }
    }
}

impl From<Vec<RowSelector>> for RowSelection {
    fn from(selectors: Vec<RowSelector>) -> Self {
        let mut result: Vec<RowSelector> = Vec::new();
        for selector in selectors {
            if selector.row_count == 0 {
                continue;
            }
            match result.last_mut() {
                Some(last) if last.skip == selector.skip => {
                    last.row_count += selector.row_count;
                }
                _ => result.push(selector),
            }
        }
        Self { selectors: result }
    }
}

impl From<RowSelection> for Vec<RowSelector> {
    fn from(selection: RowSelection) -> Self {
        selection.selectors
    }
}

impl FromIterator<RowSelector> for RowSelection {
    fn from_iter<T: IntoIterator<Item = RowSelector>>(iter: T) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_selector_select() {
        let selector = RowSelector::select(100);
        assert_eq!(selector.row_count, 100);
        assert!(!selector.skip);
    }

    #[test]
    fn test_row_selector_skip() {
        let selector = RowSelector::skip(50);
        assert_eq!(selector.row_count, 50);
        assert!(selector.skip);
    }

    #[test]
    fn test_row_selection_from_consecutive_ranges() {
        let selection = RowSelection::from_consecutive_ranges(vec![5..10, 15..20].into_iter(), 25);

        let expected = vec![
            RowSelector::skip(5),
            RowSelector::select(5),
            RowSelector::skip(5),
            RowSelector::select(5),
            RowSelector::skip(5),
        ];

        assert_eq!(selection.selectors, expected);
        assert_eq!(selection.row_count(), 25);
        assert_eq!(selection.selected_row_count(), 10);
        assert_eq!(selection.skipped_row_count(), 15);
    }

    #[test]
    fn test_row_selection_consolidation() {
        let selectors = vec![
            RowSelector::skip(5),
            RowSelector::skip(5),
            RowSelector::select(10),
            RowSelector::select(5),
        ];

        let selection: RowSelection = selectors.into();

        let expected = vec![RowSelector::skip(10), RowSelector::select(15)];

        assert_eq!(selection.selectors, expected);
    }

    #[test]
    fn test_row_selection_select_all() {
        let selection = RowSelection::select_all(100);
        assert_eq!(selection.row_count(), 100);
        assert_eq!(selection.selected_row_count(), 100);
        assert_eq!(selection.skipped_row_count(), 0);
        assert!(selection.selects_any());
    }

    #[test]
    fn test_row_selection_skip_all() {
        let selection = RowSelection::skip_all(100);
        assert_eq!(selection.row_count(), 100);
        assert_eq!(selection.selected_row_count(), 0);
        assert_eq!(selection.skipped_row_count(), 100);
        assert!(!selection.selects_any());
    }

    #[test]
    fn test_row_selection_split_off() {
        let mut selection =
            RowSelection::from_consecutive_ranges(vec![10..30, 40..60].into_iter(), 100);

        let first = selection.split_off(35);

        assert_eq!(first.row_count(), 35);
        assert_eq!(selection.row_count(), 65);

        // First should have: skip(10) + select(20) + skip(5)
        assert_eq!(first.selected_row_count(), 20);

        // Remaining should have: skip(5) + select(20) + skip(40)
        assert_eq!(selection.selected_row_count(), 20);
    }

    #[test]
    fn test_row_selection_and_then() {
        // First selection: skip 5, select 10, skip 5
        let first = RowSelection::from_consecutive_ranges(std::iter::once(5..15), 20);

        // Second selection (operates on the 10 selected rows): skip 2, select 5, skip 3
        let second = RowSelection::from_consecutive_ranges(std::iter::once(2..7), 10);

        let result = first.and_then(&second);

        // Should skip first 5, then skip 2 more (= 7), then select 5, then skip rest
        assert_eq!(result.row_count(), 20);
        assert_eq!(result.selected_row_count(), 5);

        let expected = vec![
            RowSelector::skip(7),
            RowSelector::select(5),
            RowSelector::skip(8),
        ];
        assert_eq!(result.selectors, expected);
    }

    #[test]
    fn test_row_selection_from_filters() {
        use arrow::array::BooleanArray;

        // Create a boolean filter: [false, false, true, true, false]
        let filter = BooleanArray::from(vec![false, false, true, true, false]);

        let selection = RowSelection::from_filters(&[filter]);

        let expected = vec![
            RowSelector::skip(2),
            RowSelector::select(2),
            RowSelector::skip(1),
        ];

        assert_eq!(selection.selectors, expected);
    }

    #[test]
    fn test_row_selection_empty() {
        let selection = RowSelection::new();
        assert_eq!(selection.row_count(), 0);
        assert_eq!(selection.selected_row_count(), 0);
        assert!(!selection.selects_any());
    }

    #[test]
    #[should_panic(expected = "ranges must be provided in order")]
    fn test_row_selection_out_of_order() {
        RowSelection::from_consecutive_ranges(vec![10..20, 5..15].into_iter(), 25);
    }

    #[test]
    fn test_row_selection_from_row_group_filter() {
        // 3 row groups: skip first, keep second, skip third
        let filter = vec![false, true, false];
        let selection = RowSelection::from_row_group_filter(&filter, 10000, 30000);

        let expected = vec![
            RowSelector::skip(10000),
            RowSelector::select(10000),
            RowSelector::skip(10000),
        ];

        assert_eq!(selection.selectors, expected);
        assert_eq!(selection.row_count(), 30000);
        assert_eq!(selection.selected_row_count(), 10000);
        assert_eq!(selection.skipped_row_count(), 20000);
    }

    #[test]
    fn test_row_selection_from_row_group_filter_all_keep() {
        // All row groups kept
        let filter = vec![true, true, true];
        let selection = RowSelection::from_row_group_filter(&filter, 10000, 30000);

        let expected = vec![RowSelector::select(30000)];

        assert_eq!(selection.selectors, expected);
        assert_eq!(selection.selected_row_count(), 30000);
    }

    #[test]
    fn test_row_selection_from_row_group_filter_all_skip() {
        // All row groups skipped
        let filter = vec![false, false, false];
        let selection = RowSelection::from_row_group_filter(&filter, 10000, 30000);

        let expected = vec![RowSelector::skip(30000)];

        assert_eq!(selection.selectors, expected);
        assert_eq!(selection.selected_row_count(), 0);
    }

    #[test]
    fn test_row_selection_from_row_group_filter_merge() {
        // Test merging: skip, skip, keep, keep, skip
        let filter = vec![false, false, true, true, false];
        let selection = RowSelection::from_row_group_filter(&filter, 10000, 50000);

        // Should merge consecutive skip/select selectors
        let expected = vec![
            RowSelector::skip(20000),   // Merged 2 skips
            RowSelector::select(20000), // Merged 2 selects
            RowSelector::skip(10000),
        ];

        assert_eq!(selection.selectors, expected);
        assert_eq!(selection.row_count(), 50000);
    }

    #[test]
    fn test_row_selection_from_row_group_filter_remaining_rows() {
        // 2 row groups covering 20000 rows, but total is 25000
        let filter = vec![true, false];
        let selection = RowSelection::from_row_group_filter(&filter, 10000, 25000);

        // Should add remaining 5000 rows as skip
        let expected = vec![
            RowSelector::select(10000),
            RowSelector::skip(15000), // 10000 skipped + 5000 remaining
        ];

        assert_eq!(selection.selectors, expected);
        assert_eq!(selection.row_count(), 25000);
    }

    #[test]
    fn test_row_selection_from_row_group_filter_empty() {
        // Empty filter with non-zero total rows
        let filter = vec![];
        let selection = RowSelection::from_row_group_filter(&filter, 10000, 50000);

        // Should skip all rows
        let expected = vec![RowSelector::skip(50000)];
        assert_eq!(selection.selectors, expected);
    }
}
