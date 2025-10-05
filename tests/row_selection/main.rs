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

//! Regression tests for RowSelection functionality

use std::fs::File;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use orc_rust::row_selection::{RowSelection, RowSelector};

fn basic_path(path: &str) -> String {
    let dir = env!("CARGO_MANIFEST_DIR");
    format!("{dir}/tests/basic/data/{path}")
}

// Helper function to compare batches with expected output
fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
    let formatted = pretty::pretty_format_batches(batches).unwrap().to_string();
    let actual_lines: Vec<_> = formatted.trim().lines().collect();
    assert_eq!(
        &actual_lines, expected_lines,
        "\n\nexpected:\n\n{expected_lines:#?}\nactual:\n\n{actual_lines:#?}\n\n"
    );
}

#[test]
fn test_row_selection_skip_first_select_middle() {
    // Skip first 2 rows, select next 2 rows, skip rest
    let path = basic_path("test.orc");
    let f = File::open(path).expect("no file found");

    let selection = vec![
        RowSelector::skip(2),
        RowSelector::select(2),
        RowSelector::skip(1),
    ]
    .into();

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Should only read 2 rows (rows index 2 and 3 from the file)
    assert_eq!(total_rows, 2);

    // Verify data content - should be rows 2 and 3 (0-indexed)
    let expected = [
        "+-----+------+------------+-----+----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+---------------------+-------------+----------------+",
        "| a   | b    | str_direct | d   | e  | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple    | date_simple | tinyint_simple |",
        "+-----+------+------------+-----+----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+---------------------+-------------+----------------+",
        "|     |      |            |     |    |       |                    |                        |           |               |            |                |               |                   | 1            | ccc           | ccc           | 2023-01-01T00:00:00 | 2023-01-01  | 1              |",
        "| 4.0 | true | ddd        | ccc | bb | ccccc | 5                  | -5                     | 4         | 2             | 3          | -3             | 3             | -3                | 5            | dddd          | bb            | 2023-02-01T00:00:00 | 2023-02-01  | 127            |",
        "+-----+------+------------+-----+----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+---------------------+-------------+----------------+",
    ];
    assert_batches_eq(&batches, &expected);
}

#[test]
fn test_row_selection_select_all() {
    let path = basic_path("test.orc");
    let f = File::open(path).expect("no file found");

    // Select all 5 rows
    let selection = RowSelection::select_all(5);

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 5);
}

#[test]
fn test_row_selection_skip_all() {
    let path = basic_path("test.orc");
    let f = File::open(path).expect("no file found");

    // Skip all 5 rows
    let selection = RowSelection::skip_all(5);

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Should read 0 rows
    assert_eq!(total_rows, 0);
}

#[test]
fn test_row_selection_select_first_only() {
    let path = basic_path("test.orc");
    let f = File::open(path).expect("no file found");

    // Select only first row
    let selection = vec![RowSelector::select(1), RowSelector::skip(4)].into();

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 1);

    let expected = [
        "+-----+------+------------+---+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+-------------------------+-------------+----------------+",
        "| a   | b    | str_direct | d | e   | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple        | date_simple | tinyint_simple |",
        "+-----+------+------------+---+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+-------------------------+-------------+----------------+",
        "| 1.0 | true | a          | a | ddd | aaaaa | 5                  | -5                     | 1         | 5             | 1          | -1             | 1             | -1                | 5            | a             | eeeee         | 2023-04-01T20:15:30.002 | 2023-04-01  | -1             |",
        "+-----+------+------------+---+-----+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+-------------------------+-------------+----------------+",
    ];
    assert_batches_eq(&batches, &expected);
}

#[test]
fn test_row_selection_select_last_only() {
    let path = basic_path("test.orc");
    let f = File::open(path).expect("no file found");

    // Skip first 4 rows, select last row
    let selection = vec![RowSelector::skip(4), RowSelector::select(1)].into();

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 1);

    let expected = [
        "+-----+-------+------------+-----+---+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+---------------------+-------------+----------------+",
        "| a   | b     | str_direct | d   | e | f     | int_short_repeated | int_neg_short_repeated | int_delta | int_neg_delta | int_direct | int_neg_direct | bigint_direct | bigint_neg_direct | bigint_other | utf8_increase | utf8_decrease | timestamp_simple    | date_simple | tinyint_simple |",
        "+-----+-------+------------+-----+---+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+---------------------+-------------+----------------+",
        "| 5.0 | false | ee         | ddd | a | ddddd | 5                  | -5                     | 5         | 1             | 2          | -2             | 2             | -2                | 5            | eeeee         | a             | 2023-03-01T00:00:00 | 2023-03-01  | -127           |",
        "+-----+-------+------------+-----+---+-------+--------------------+------------------------+-----------+---------------+------------+----------------+---------------+-------------------+--------------+---------------+---------------+---------------------+-------------+----------------+",
    ];
    assert_batches_eq(&batches, &expected);
}

#[test]
fn test_row_selection_with_consecutive_ranges() {
    let path = basic_path("test.orc");
    let f = File::open(path).expect("no file found");

    // Select rows at indices 0-1 and 3-4 (skip row 2)
    let selection = RowSelection::from_consecutive_ranges(vec![0..2, 3..5].into_iter(), 5);

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Should read 4 rows (skip the middle one)
    assert_eq!(total_rows, 4);
}

#[test]
fn test_row_selection_with_projection() {
    // Test that row selection works with column projection
    let path = basic_path("test.orc");
    let f = File::open(path).expect("no file found");

    let builder = ArrowReaderBuilder::try_new(f).unwrap();
    let projection =
        ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &["a", "b"]);

    let selection = vec![
        RowSelector::skip(1),
        RowSelector::select(2),
        RowSelector::skip(2),
    ]
    .into();

    let reader = builder
        .with_projection(projection)
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 2);
    assert_eq!(batches[0].num_columns(), 2); // Only 2 columns projected

    let expected = [
        "+-----+-------+",
        "| a   | b     |",
        "+-----+-------+",
        "| 2.0 | false |",
        "|     |       |",
        "+-----+-------+",
    ];
    assert_batches_eq(&batches, &expected);
}

#[test]
fn test_row_selection_with_nested_struct() {
    let path = basic_path("nested_struct.orc");
    let f = File::open(path).expect("no file found");

    // Select first 2 rows and last row
    let selection = vec![
        RowSelector::select(2),
        RowSelector::skip(2),
        RowSelector::select(1),
    ]
    .into();

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 3);

    let expected = [
        "+-------------------+",
        "| nest              |",
        "+-------------------+",
        "| {a: 1.0, b: true} |",
        "| {a: 3.0, b: }     |",
        "| {a: -3.0, b: }    |",
        "+-------------------+",
    ];
    assert_batches_eq(&batches, &expected);
}

#[test]
fn test_row_selection_with_nested_array() {
    let path = basic_path("nested_array.orc");
    let f = File::open(path).expect("no file found");

    // Select middle rows (index 1-2)
    let selection = vec![
        RowSelector::skip(1),
        RowSelector::select(2),
        RowSelector::skip(2),
    ]
    .into();

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 2);

    let expected = [
        "+--------------------+",
        "| value              |",
        "+--------------------+",
        "| [5, , 32, 4, 15]   |",
        "| [16, , 3, 4, 5, 6] |",
        "+--------------------+",
    ];
    assert_batches_eq(&batches, &expected);
}

#[test]
fn test_row_selection_with_large_file() {
    // Test with a larger file that spans multiple stripes
    let path = basic_path("string_long_long.orc");
    let f = File::open(path).expect("no file found");

    // Skip first 1000 rows, select next 500, skip rest
    let selection = vec![
        RowSelector::skip(1000),
        RowSelector::select(500),
        RowSelector::skip(8500),
    ]
    .into();

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 500);
}

#[test]
fn test_row_selection_empty_selection() {
    let path = basic_path("test.orc");
    let f = File::open(path).expect("no file found");

    // Empty selection - skip all rows
    let selection = RowSelection::skip_all(5);

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Empty selection should read 0 rows
    assert_eq!(total_rows, 0);
}

#[test]
fn test_row_selection_with_compression() {
    // Test that row selection works with compressed files
    let path = basic_path("string_dict_gzip.orc");
    let f = File::open(path).expect("no file found");

    let selection = vec![
        RowSelector::skip(10),
        RowSelector::select(20),
        RowSelector::skip(34),
    ]
    .into();

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_row_selection(selection)
        .build();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(total_rows, 20);
}

// TODO: Async version doesn't support row_selection yet
// Need to update async_arrow_reader.rs to pass row_selection to NaiveStripeDecoder
// #[cfg(feature = "async")]
// #[tokio::test]
// async fn test_row_selection_async() {
//     let path = basic_path("test.orc");
//     let f = tokio::fs::File::open(path).await.unwrap();
//
//     let selection = vec![
//         RowSelector::skip(1),
//         RowSelector::select(3),
//         RowSelector::skip(1),
//     ]
//     .into();
//
//     let reader = ArrowReaderBuilder::try_new_async(f)
//         .await
//         .unwrap()
//         .with_row_selection(selection)
//         .build_async();
//
//     let batches = reader.try_collect::<Vec<_>>().await.unwrap();
//     let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//
//     assert_eq!(total_rows, 3);
// }
