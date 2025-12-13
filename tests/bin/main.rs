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

//! Smoke tests for CLI binaries.

#![cfg(feature = "cli")]

use std::fs::File;
use std::path::PathBuf;
use std::process::Command;

use orc_rust::reader::metadata::read_metadata;
use serde_json::Value;

fn data_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integration")
        .join("data")
        .join(name)
}

fn run_cmd(bin_env: &str, args: &[&str]) -> (bool, String) {
    let output = Command::new(bin_env).args(args).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    (output.status.success(), stdout)
}

#[test]
fn orc_rowcount_matches_metadata() {
    let file = data_path("TestOrcFile.test1.orc");
    let mut fh = File::open(&file).unwrap();
    let expected = read_metadata(&mut fh).unwrap().number_of_rows().to_string();

    let (ok, stdout) = run_cmd(
        env!("CARGO_BIN_EXE_orc-rowcount"),
        &[file.to_str().unwrap()],
    );
    assert!(ok, "orc-rowcount failed");
    assert!(
        stdout.contains(&expected),
        "expected rowcount {expected}, got {stdout}"
    );
}

#[test]
fn orc_schema_prints_schema() {
    let file = data_path("TestOrcFile.test1.orc");
    let (ok, stdout) = run_cmd(env!("CARGO_BIN_EXE_orc-schema"), &[file.to_str().unwrap()]);
    assert!(ok);
    assert!(
        stdout.contains("Schema:"),
        "schema output missing Schema header"
    );
}

#[test]
fn orc_read_limits_records() {
    let file = data_path("TestOrcFile.test1.orc");
    let (ok, stdout) = run_cmd(
        env!("CARGO_BIN_EXE_orc-read"),
        &["--json", "--num-records", "2", file.to_str().unwrap()],
    );
    assert!(ok);
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(2, lines.len(), "expected exactly 2 JSON lines");
    for line in lines {
        serde_json::from_str::<Value>(line).expect("valid JSON line");
    }
}

#[test]
fn orc_layout_json_matches_stripe_count() {
    let file = data_path("TestOrcFile.test1.orc");
    let mut fh = File::open(&file).unwrap();
    let metadata = read_metadata(&mut fh).unwrap();

    let (ok, stdout) = run_cmd(env!("CARGO_BIN_EXE_orc-layout"), &[file.to_str().unwrap()]);
    assert!(ok);
    let v: Value = serde_json::from_str(&stdout).unwrap();
    let stripes = v["stripes"].as_array().expect("stripes is array").len();
    assert_eq!(metadata.stripe_metadatas().len(), stripes);
}

#[test]
fn orc_index_completes() {
    let file = data_path("TestOrcFile.testPredicatePushdown.orc");
    let (ok, stdout) = run_cmd(
        env!("CARGO_BIN_EXE_orc-index"),
        &[file.to_str().unwrap(), "int1"],
    );
    assert!(ok);
    assert!(
        stdout.contains("Stripe"),
        "expected stripe output from orc-index"
    );
}
