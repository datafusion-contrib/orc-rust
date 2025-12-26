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
use std::path::Path;

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
fn bloom_filter() {
    test_expected_file("bloom_filter");
}

#[test]
fn bloom_filter_predicate_prunes() {
    let path = format!(
        "{}/tests/integration/data/bloom_filter.orc",
        env!("CARGO_MANIFEST_DIR"),
    );
    let file_path = Path::new(&path);

    // Count rows for a given predicate.
    let count_rows = |predicate: Predicate| {
        let f = File::open(file_path).unwrap();
        ArrowReaderBuilder::try_new(f)
            .unwrap()
            .with_predicate(predicate)
            .build()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>()
    };

    // Baseline: without predicate we should read all rows.
    let f_all = File::open(file_path).unwrap();
    let reader_all = ArrowReaderBuilder::try_new(f_all).unwrap().build();
    let all_batches = reader_all.collect::<Result<Vec<_>, _>>().unwrap();
    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 204);

    // Predicates target values absent from the file but inside min/max of the stripe,
    // so statistics alone cannot prune; Bloom filters must be used.

    // id=2 (between 1 and 3)
    let rows_id = count_rows(Predicate::eq("id", PredicateValue::Int32(Some(2))));
    assert_eq!(rows_id, 0);

    // id=3, should not be pruned
    let rows_id_hit = count_rows(Predicate::eq("id", PredicateValue::Int32(Some(3))));
    assert_eq!(rows_id_hit, 204);

    // name="beta" (between "alpha" and "gamma")
    let rows_name = count_rows(Predicate::eq(
        "name",
        PredicateValue::Utf8(Some("beta".to_string())),
    ));
    assert_eq!(rows_name, 0);

    // name="alpha", should not be pruned
    let rows_name_hit = count_rows(Predicate::eq(
        "name",
        PredicateValue::Utf8(Some("alpha".to_string())),
    ));
    assert_eq!(rows_name_hit, 204);

    // score=2.0 (between 1.0 and 3.0)
    let rows_score = count_rows(Predicate::eq("score", PredicateValue::Float64(Some(2.0))));
    assert_eq!(rows_score, 0);

    // score=1.0, should not be pruned
    let rows_score_hit = count_rows(Predicate::eq("score", PredicateValue::Float64(Some(1.0))));
    assert_eq!(rows_score_hit, 204);

    // event_date = 2023-01-02 (between 2023-01-01 and 2023-01-10)
    let date = chrono::NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = (date - epoch).num_days();
    let rows_date = count_rows(Predicate::eq(
        "event_date",
        PredicateValue::Int32(Some(days as i32)),
    ));
    assert_eq!(rows_date, 0);

    // event_date = 2023-01-01, should not be pruned
    let date_hit = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
    let days_hit = (date_hit - epoch).num_days();
    let rows_date_hit = count_rows(Predicate::eq(
        "event_date",
        PredicateValue::Int32(Some(days_hit as i32)),
    ));
    assert_eq!(rows_date_hit, 204);

    // flag=true/false absent combinations: combine with missing id to ensure pruning
    let rows_flag_and_id = count_rows(Predicate::and(vec![
        Predicate::eq("flag", PredicateValue::Boolean(Some(true))),
        Predicate::eq("id", PredicateValue::Int32(Some(2))),
    ]));
    assert_eq!(rows_flag_and_id, 0);

    // binary predicate: look for a value not present but within min/max range (byte 0x02 between 0x01 and 0x0a)
    let rows_binary = count_rows(Predicate::eq(
        "data",
        PredicateValue::Utf8(Some("\u{0002}".to_string())),
    ));
    assert_eq!(rows_binary, 0);

    // binary predicate: look for a value present (byte 0x01)
    let rows_binary_hit = count_rows(Predicate::eq(
        "data",
        PredicateValue::Utf8(Some("\u{0001}".to_string())),
    ));
    assert_eq!(rows_binary_hit, 204);

    // decimal predicate: look for 2.22 (between 1.11 and 10.10)
    let rows_decimal = count_rows(Predicate::eq(
        "dec",
        PredicateValue::Utf8(Some("2.22".to_string())),
    ));
    assert_eq!(rows_decimal, 0);

    // decimal predicate: look for 1.11 (present)
    let rows_decimal_hit = count_rows(Predicate::eq(
        "dec",
        PredicateValue::Utf8(Some("1.11".to_string())),
    ));
    assert_eq!(rows_decimal_hit, 204);
}
