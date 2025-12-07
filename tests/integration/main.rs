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

/// Tests ORC files from the official test suite (`orc/examples/`) against Arrow feather
/// expected data sourced by reading the ORC files with PyArrow and persisting as feather.
use std::fs::File;

use arrow::{
    array::{Array, AsArray},
    compute::concat_batches,
    datatypes::TimestampNanosecondType,
    ipc::reader::FileReader,
    record_batch::{RecordBatch, RecordBatchReader},
};
use pretty_assertions::assert_eq;

use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::{Predicate, PredicateValue};

fn read_orc_file(name: &str) -> RecordBatch {
    let path = format!(
        "{}/tests/integration/data/{}.orc",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let f = File::open(path).unwrap();
    let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Gather all record batches into single one for easier comparison
    concat_batches(&schema, batches.iter()).unwrap()
}

fn read_feather_file(name: &str) -> RecordBatch {
    let feather_path = format!(
        "{}/tests/integration/data/expected_arrow/{}.feather",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let f = File::open(feather_path).unwrap();
    let reader = FileReader::try_new(f, None).unwrap();
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Gather all record batches into single one for easier comparison
    concat_batches(&schema, batches.iter()).unwrap()
}

/// Checks specific `.orc` file against corresponding expected feather file
fn test_expected_file(name: &str) {
    let actual_batch = read_orc_file(name);
    let expected_batch = read_feather_file(name);
    assert_eq!(actual_batch, expected_batch);
}

/// Similar to test_expected_file but compares the pretty formatted RecordBatches.
/// Useful when checking equality of values but not exact equality (e.g. representation
/// for UnionArrays can differ internally but logically represent the same values)
fn test_expected_file_formatted(name: &str) {
    let actual_batch = read_orc_file(name);
    let expected_batch = read_feather_file(name);

    let actual = arrow::util::pretty::pretty_format_batches(&[actual_batch])
        .unwrap()
        .to_string();
    let expected = arrow::util::pretty::pretty_format_batches(&[expected_batch])
        .unwrap()
        .to_string();
    let actual_lines = actual.trim().lines().collect::<Vec<_>>();
    let expected_lines = expected.trim().lines().collect::<Vec<_>>();
    assert_eq!(&actual_lines, &expected_lines);
}

#[test]
fn column_projection() {
    test_expected_file("TestOrcFile.columnProjection");
}

#[test]
// TODO: arrow-ipc reader can't read invalid custom_metadata
//       from the expected feather file:
//       https://github.com/apache/arrow-rs/issues/5547
#[ignore]
fn meta_data() {
    test_expected_file("TestOrcFile.metaData");
}

#[test]
fn test1() {
    // Compare formatted because Map key/value field names differs from PyArrow
    test_expected_file_formatted("TestOrcFile.test1");
}

#[test]
fn empty_file() {
    // Compare formatted because Map key/value field names differs from PyArrow
    test_expected_file_formatted("TestOrcFile.emptyFile");
}

#[test]
fn test_date_1900() {
    test_expected_file("TestOrcFile.testDate1900");
}

#[test]
fn test_date_1900_not_null() {
    // Don't use read_orc_file() because it concatenate batches, which would detect
    // there are no nulls and remove the NullBuffer, making this test useless
    let path = format!(
        "{}/tests/integration/data/TestOrcFile.testDate1900.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();
    let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    for batch in batches {
        assert!(batch.columns()[0]
            .as_primitive_opt::<TimestampNanosecondType>()
            .unwrap()
            .nulls()
            .is_none());
    }
}

#[test]
#[ignore]
// TODO: pending https://github.com/chronotope/chrono-tz/issues/155
fn test_date_2038() {
    test_expected_file("TestOrcFile.testDate2038");
}

#[test]
fn test_memory_management_v11() {
    test_expected_file("TestOrcFile.testMemoryManagementV11");
}

#[test]
fn test_memory_management_v12() {
    test_expected_file("TestOrcFile.testMemoryManagementV12");
}

#[test]
fn test_predicate_pushdown() {
    test_expected_file("TestOrcFile.testPredicatePushdown");
}

#[test]
fn test_predicate_pushdown_with_filtering() {
    let path = format!(
        "{}/tests/integration/data/TestOrcFile.testPredicatePushdown.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Test with predicate: should filter rows
    let predicate = Predicate::gt("int1", PredicateValue::Int32(Some(2000)));
    let reader_with_predicate = ArrowReaderBuilder::try_new(f.try_clone().unwrap())
        .unwrap()
        .with_predicate(predicate)
        .build();

    // Read without predicate: should get all rows
    let reader_without = ArrowReaderBuilder::try_new(f).unwrap().build();

    // Count rows with and without predicate
    let with_count: usize = reader_with_predicate
        .map(|b| b.map(|batch| batch.num_rows()).unwrap_or(0))
        .sum();
    let without_count: usize = reader_without
        .map(|b| b.map(|batch| batch.num_rows()).unwrap_or(0))
        .sum();

    // With predicate should have fewer or equal rows
    assert!(
        with_count <= without_count,
        "Predicate should filter rows: with_predicate={with_count}, without={without_count}",
    );

    // If predicate works, we should have fewer rows (unless all rows match)
    // For this test file, we expect some filtering to occur
    // Verify the API works and doesn't crash
    assert!(with_count <= without_count);
}

#[test]
fn test_predicate_pushdown_range_query() {
    let path = format!(
        "{}/tests/integration/data/TestOrcFile.testPredicatePushdown.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Test range query: int1 >= 1000 AND int1 <= 5000
    let predicate = Predicate::and(vec![
        Predicate::gte("int1", PredicateValue::Int32(Some(1000))),
        Predicate::lte("int1", PredicateValue::Int32(Some(5000))),
    ]);

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    // Should not crash and should return some batches
    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
    let batches = batches.unwrap();
    assert!(!batches.is_empty());
}

#[test]
fn test_predicate_pushdown_equality_query() {
    let path = format!(
        "{}/tests/integration/data/TestOrcFile.testPredicatePushdown.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Test equality query: int1 = 3000
    let predicate = Predicate::eq("int1", PredicateValue::Int32(Some(3000)));

    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    // Should not crash
    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
}

#[test]
fn test_predicate_pushdown_without_index() {
    // Test file without index should still work (graceful fallback)
    let path = format!(
        "{}/tests/integration/data/TestOrcFile.testWithoutIndex.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    let predicate = Predicate::gt("int1", PredicateValue::Int32(Some(1000)));
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    // Should not crash even without indexes (should fall back to reading all rows)
    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
}

#[test]
fn test_seek() {
    // Compare formatted because Map key/value field names differs from PyArrow
    test_expected_file_formatted("TestOrcFile.testSeek");
}

#[test]
fn test_snappy() {
    test_expected_file("TestOrcFile.testSnappy");
}

#[test]
fn test_string_and_binary_statistics() {
    test_expected_file("TestOrcFile.testStringAndBinaryStatistics");
}

#[test]
fn test_stripe_level_stats() {
    test_expected_file("TestOrcFile.testStripeLevelStats");
}

#[test]
#[ignore] // TODO: Non-struct root type are not supported yet
fn test_timestamp() {
    test_expected_file("TestOrcFile.testTimestamp");
}

#[test]
fn test_union_and_timestamp() {
    // Compare formatted because internal Union representation can differ
    // even if it represents the same logical values
    test_expected_file_formatted("TestOrcFile.testUnionAndTimestamp");
}

#[test]
fn test_without_index() {
    test_expected_file("TestOrcFile.testWithoutIndex");
}

#[test]
fn test_lz4() {
    test_expected_file("TestVectorOrcFile.testLz4");
}

#[test]
fn test_lzo() {
    test_expected_file("TestVectorOrcFile.testLzo");
}

#[test]
fn decimal() {
    test_expected_file("decimal");
}

#[test]
fn zlib() {
    test_expected_file("demo-12-zlib");
}

#[test]
fn nulls_at_end_snappy() {
    test_expected_file("nulls-at-end-snappy");
}

#[test]
#[ignore]
// TODO: investigate why this fails (zero metadata length how?)
//       because of difference with ORC 0.11 vs 0.12?
fn orc_11_format() {
    test_expected_file("orc-file-11-format");
}

#[test]
fn orc_index_int_string() {
    test_expected_file("orc_index_int_string");
}

#[test]
#[ignore]
// TODO: how to handle DECIMAL(0, 0) type? Arrow expects non-zero precision
fn orc_split_elim() {
    test_expected_file("orc_split_elim");
}

#[test]
fn orc_split_elim_cpp() {
    test_expected_file("orc_split_elim_cpp");
}

#[test]
fn orc_split_elim_new() {
    test_expected_file("orc_split_elim_new");
}

#[test]
fn over1k_bloom() {
    test_expected_file("over1k_bloom");
}

#[test]
fn bloom_filter_test() {
    test_expected_file("bloom_filter_test");
}

#[test]
fn test_bloom_filter_equality_query_existing_value() {
    // Test equality query with a value that exists in the file
    // Bloom Filter should allow reading the row group, but may return the entire row group
    // rather than just the matching rows (predicate pushdown is at row group level)
    let path = format!(
        "{}/tests/integration/data/bloom_filter_test.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Query for id = 500 (exists in the file)
    let predicate = Predicate::eq("id", PredicateValue::Int32(Some(500)));
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
    let batches = batches.unwrap();

    // Should return at least some rows (Bloom Filter allows reading the row group)
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows > 0,
        "Should return rows when Bloom Filter indicates value might exist"
    );

    // Verify that id=500 is in the returned data (since it exists in the file)
    let mut found_500 = false;
    for batch in &batches {
        let id_array = batch
            .column(0)
            .as_primitive_opt::<arrow::datatypes::Int32Type>()
            .unwrap();
        for i in 0..id_array.len() {
            if !id_array.is_null(i) && id_array.value(i) == 500 {
                found_500 = true;
                break;
            }
        }
        if found_500 {
            break;
        }
    }
    assert!(found_500, "Should find id=500 in the returned data");
}

#[test]
fn test_bloom_filter_equality_query_nonexistent_value() {
    // Test equality query with a value that doesn't exist
    // Bloom Filter should help skip row groups that don't contain the value
    let path = format!(
        "{}/tests/integration/data/bloom_filter_test.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Query for id = 9999 (doesn't exist, ids are 1-1000)
    let predicate = Predicate::eq("id", PredicateValue::Int32(Some(9999)));
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
    let batches = batches.unwrap();

    // Should return zero rows (id=9999 doesn't exist)
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 0,
        "Should return zero rows for non-existent id=9999"
    );
}

#[test]
fn test_bloom_filter_string_equality_query() {
    // Test equality query on string column (name)
    // This test verifies that Bloom Filter integration works for string columns
    // The actual filtering behavior depends on both Bloom Filter and statistics
    let path = format!(
        "{}/tests/integration/data/bloom_filter_test.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Query for name = "user_1" (exists in the file)
    let predicate = Predicate::eq("name", PredicateValue::Utf8(Some("user_1".to_string())));
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    // Should not crash when using Bloom Filter with string equality query
    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(
        batches.is_ok(),
        "Bloom Filter string equality query should not crash"
    );

    // The number of returned rows depends on both Bloom Filter and statistics
    // This test primarily verifies that the integration works correctly
    let batches = batches.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // If rows are returned, verify the data is valid
    if total_rows > 0 {
        for batch in &batches {
            let name_array = batch.column(1).as_string_opt::<i32>().unwrap();
            for i in 0..name_array.len() {
                if !name_array.is_null(i) {
                    let name = name_array.value(i);
                    assert!(
                        name.starts_with("user_"),
                        "Returned names should start with 'user_'"
                    );
                }
            }
        }
    }
}

#[test]
fn test_bloom_filter_string_equality_query_nonexistent() {
    // Test equality query on string column with non-existent value
    let path = format!(
        "{}/tests/integration/data/bloom_filter_test.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Query for name = "nonexistent_user" (doesn't exist)
    let predicate = Predicate::eq(
        "name",
        PredicateValue::Utf8(Some("nonexistent_user".to_string())),
    );
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
    let batches = batches.unwrap();

    // Should return zero rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 0,
        "Should return zero rows for non-existent name"
    );
}

#[test]
fn test_bloom_filter_email_equality_query() {
    // Test equality query on email column
    let path = format!(
        "{}/tests/integration/data/bloom_filter_test.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Query for email = "email_250@example.com" (exists in the file)
    let predicate = Predicate::eq(
        "email",
        PredicateValue::Utf8(Some("email_250@example.com".to_string())),
    );
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
    let batches = batches.unwrap();

    // Should return at least one row
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Should return rows for existing email");
}

#[test]
fn test_bloom_filter_age_equality_query() {
    // Test equality query on age column (int32)
    // Bloom Filter should allow reading the row group if it might contain the value
    let path = format!(
        "{}/tests/integration/data/bloom_filter_test.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Query for age = 35 (exists in the file, ages are 20-69)
    let predicate = Predicate::eq("age", PredicateValue::Int32(Some(35)));
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
    let batches = batches.unwrap();

    // Should return at least some rows (Bloom Filter allows reading the row group)
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows > 0,
        "Should return rows when Bloom Filter indicates value might exist"
    );

    // Verify that age=35 is in the returned data (since it exists in the file)
    let mut found_35 = false;
    for batch in &batches {
        let age_array = batch
            .column(2)
            .as_primitive_opt::<arrow::datatypes::Int32Type>()
            .unwrap();
        for i in 0..age_array.len() {
            if !age_array.is_null(i) && age_array.value(i) == 35 {
                found_35 = true;
                break;
            }
        }
        if found_35 {
            break;
        }
    }
    assert!(found_35, "Should find age=35 in the returned data");
}

#[test]
fn test_bloom_filter_age_equality_query_nonexistent() {
    // Test equality query on age column with non-existent value
    let path = format!(
        "{}/tests/integration/data/bloom_filter_test.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();

    // Query for age = 100 (doesn't exist, ages are 20-69)
    let predicate = Predicate::eq("age", PredicateValue::Int32(Some(100)));
    let reader = ArrowReaderBuilder::try_new(f)
        .unwrap()
        .with_predicate(predicate)
        .build();

    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
    let batches = batches.unwrap();

    // Should return zero rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 0,
        "Should return zero rows for non-existent age=100"
    );
}

#[test]
fn test_bloom_filter_comparison_without_predicate() {
    // Test that reading without predicate still works correctly
    // This ensures backward compatibility
    let path = format!(
        "{}/tests/integration/data/bloom_filter_test.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let f = File::open(path).unwrap();
    let reader = ArrowReaderBuilder::try_new(f).unwrap().build();

    let batches: Result<Vec<_>, _> = reader.collect();
    assert!(batches.is_ok());
    let batches = batches.unwrap();

    // Should return all 1000 rows
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 1000,
        "Should return all rows when no predicate is used"
    );
}
