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

//! Comprehensive tests for the unified `orc` CLI binary.
//!
//! Tests compare actual output against expected output files in `tests/bin/expected/`.
//! To regenerate expected outputs, run:
//! ```bash
//! cargo build --features cli --release
//! cd tests/basic/data && ../../../target/release/orc info test.orc > ../expected/info_basic.out
//! # (use relative paths when generating expected outputs)
//! ```

#![cfg(feature = "cli")]

use std::fs;
use std::path::PathBuf;
use std::process::Command;

/// Get the cargo manifest directory
fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Get path to integration test data files (relative)
fn integration_data_rel(name: &str) -> String {
    format!("tests/integration/data/{}", name)
}

/// Get path to basic test data files (relative)
fn basic_data_rel(name: &str) -> String {
    format!("tests/basic/data/{}", name)
}

/// Get path to expected output files
fn expected_path(name: &str) -> PathBuf {
    manifest_dir()
        .join("tests")
        .join("bin")
        .join("expected")
        .join(name)
}

/// Load expected output from file
fn load_expected(name: &str) -> String {
    let path = expected_path(name);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e))
}

/// Run orc command in the project root directory
fn run_orc(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_orc"))
        .current_dir(manifest_dir())
        .args(args)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (output.status.success(), stdout, stderr)
}

/// Assert that actual output matches expected output file
fn assert_output_matches(actual: &str, expected_file: &str) {
    let expected = load_expected(expected_file);
    pretty_assertions::assert_eq!(actual, expected, "Output mismatch for {}", expected_file);
}

// =============================================================================
// Info Subcommand Tests
// =============================================================================

#[test]
fn test_info_basic() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["info", &file]);
    assert!(ok, "orc info failed");
    assert_output_matches(&stdout, "info_basic.out");
}

#[test]
fn test_info_verbose() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["info", "--verbose", &file]);
    assert!(ok, "orc info --verbose failed");
    assert_output_matches(&stdout, "info_verbose.out");
}

#[test]
fn test_info_rowcount() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["info", "--row-count-only", &file]);
    assert!(ok, "orc info --row-count-only failed");
    assert_output_matches(&stdout, "info_rowcount.out");
}

#[test]
fn test_info_rowcount_multiple_files() {
    let file1 = basic_data_rel("test.orc");
    let file2 = basic_data_rel("demo-11-zlib.orc");
    let (ok, stdout, _) = run_orc(&["info", "--row-count-only", &file1, &file2]);
    assert!(ok, "orc info --row-count-only with multiple files failed");

    // Verify output contains both file names and row counts
    assert!(stdout.contains("test.orc: 5"), "missing test.orc count");
    assert!(
        stdout.contains("demo-11-zlib.orc: 1920800"),
        "missing demo-11-zlib.orc count"
    );
}

// =============================================================================
// Export Subcommand Tests
// =============================================================================

#[test]
fn test_export_csv() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["export", &file]);
    assert!(ok, "orc export failed");
    assert_output_matches(&stdout, "export_csv.out");
}

#[test]
fn test_export_json() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["export", "-f", "json", &file]);
    assert!(ok, "orc export -f json failed");
    assert_output_matches(&stdout, "export_json.out");
}

#[test]
fn test_export_json_limit() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["export", "-f", "json", "-n", "2", &file]);
    assert!(ok, "orc export -f json -n 2 failed");
    assert_output_matches(&stdout, "export_json_limit.out");
}

#[test]
fn test_export_with_batch_size() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["export", "-f", "json", "--batch-size", "1", &file]);
    assert!(ok, "orc export with batch-size failed");
    // Should produce same output as default, just processed differently
    assert_output_matches(&stdout, "export_json.out");
}

// =============================================================================
// Stats Subcommand Tests
// =============================================================================

#[test]
fn test_stats() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["stats", &file]);
    assert!(ok, "orc stats failed");
    assert_output_matches(&stdout, "stats.out");
}

// =============================================================================
// Layout Subcommand Tests
// =============================================================================

#[test]
fn test_layout() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["layout", &file]);
    assert!(ok, "orc layout failed");
    assert_output_matches(&stdout, "layout.out");
}

#[test]
fn test_layout_json_valid() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["layout", &file]);
    assert!(ok, "orc layout failed");
    // Verify it's valid JSON
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("layout should be valid JSON");
    assert!(v["stripes"].is_array(), "layout should have stripes array");
}

// =============================================================================
// Index Subcommand Tests
// =============================================================================

#[test]
fn test_index() {
    let file = integration_data_rel("TestOrcFile.testPredicatePushdown.orc");
    let (ok, stdout, _) = run_orc(&["index", &file, "int1"]);
    assert!(ok, "orc index failed");
    assert_output_matches(&stdout, "index.out");
}

#[test]
fn test_index_invalid_column() {
    let file = integration_data_rel("TestOrcFile.testPredicatePushdown.orc");
    let (ok, _, stderr) = run_orc(&["index", &file, "nonexistent_column"]);
    assert!(!ok, "orc index should fail for invalid column");
    assert!(
        stderr.contains("not found"),
        "error should mention column not found"
    );
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_info_missing_file() {
    let (ok, _, stderr) = run_orc(&["info", "/nonexistent/path/file.orc"]);
    assert!(!ok, "should fail for missing file");
    assert!(
        stderr.contains("failed") || stderr.contains("error") || stderr.contains("Error"),
        "should show error message"
    );
}

#[test]
fn test_export_missing_file() {
    let (ok, _, stderr) = run_orc(&["export", "/nonexistent/path/file.orc"]);
    assert!(!ok, "should fail for missing file");
    assert!(
        stderr.contains("failed") || stderr.contains("error") || stderr.contains("Error"),
        "should show error message"
    );
}

#[test]
fn test_no_subcommand() {
    let output = Command::new(env!("CARGO_BIN_EXE_orc")).output().unwrap();
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("Usage:") || combined.contains("COMMAND"),
        "should show usage info when no subcommand given"
    );
}

// =============================================================================
// Bloom Filter Subcommand Tests
// =============================================================================

#[test]
fn test_bloom() {
    let file = integration_data_rel("bloom_filter.orc");
    let (ok, stdout, _) = run_orc(&["bloom", &file]);
    assert!(ok, "orc bloom failed");
    assert_output_matches(&stdout, "bloom.out");
}

#[test]
fn test_bloom_with_test_value() {
    let file = integration_data_rel("bloom_filter.orc");
    let (ok, stdout, _) = run_orc(&["bloom", &file, "--column", "name", "--test", "Alice"]);
    assert!(ok, "orc bloom with test value failed");
    assert_output_matches(&stdout, "bloom_test.out");
}

#[test]
fn test_bloom_might_contain_true() {
    // Test with a value that exists in the file ("alpha" is in the first row)
    // Expected: might_contain("alpha") = true
    let file = integration_data_rel("bloom_filter.orc");
    let (ok, stdout, _) = run_orc(&["bloom", &file, "--column", "name", "--test", "alpha"]);
    assert!(ok, "orc bloom with existing value failed");
    assert_output_matches(&stdout, "bloom_might_contain_true.out");
}

#[test]
fn test_bloom_no_filters() {
    let file = basic_data_rel("test.orc");
    let (ok, stdout, _) = run_orc(&["bloom", &file]);
    assert!(ok, "orc bloom on file without filters failed");
    assert!(
        stdout.contains("No bloom filters found"),
        "should indicate no bloom filters"
    );
}

#[test]
fn test_bloom_invalid_column() {
    let file = integration_data_rel("bloom_filter.orc");
    let (ok, _, stderr) = run_orc(&["bloom", &file, "--column", "nonexistent"]);
    assert!(!ok, "orc bloom should fail for invalid column");
    assert!(
        stderr.contains("not found"),
        "error should mention column not found"
    );
}

// =============================================================================
// Version Test
// =============================================================================

#[test]
fn test_version() {
    let (ok, stdout, _) = run_orc(&["--version"]);
    assert!(ok, "orc --version failed");
    assert!(stdout.contains("orc"), "version should contain binary name");
    // Version format: "orc X.Y.Z"
    assert!(
        stdout.trim().starts_with("orc "),
        "version should start with 'orc '"
    );
}
