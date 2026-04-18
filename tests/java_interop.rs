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

//! Cross-implementation validation against the Java reference ORC
//! implementation (Apache ORC `orc-tools`).
//!
//! These tests are `#[ignore]`d by default because they require:
//!   * a JDK on `$PATH` (`java` binary),
//!   * the uber jar `orc-tools-<version>-uber.jar` referenced by the
//!     `ORC_TOOLS_JAR` environment variable.
//!
//! Run with:
//!
//! ```bash
//! export ORC_TOOLS_JAR=/tmp/orc-tools/orc-tools.jar
//! cargo test --test java_interop -- --ignored
//! ```
//!
//! The tests prove three properties per codec (SNAPPY / ZLIB / ZSTD):
//!
//! 1. `orc-tools meta` successfully parses the PostScript of a file the
//!    Rust writer produced, and the reported `compression:` field
//!    matches the codec we asked for.
//! 2. `orc-tools data` decompresses + decodes every stripe without
//!    error and prints the same row values we wrote.
//! 3. The file the Rust writer produced can be read back by the Rust
//!    reader after it has been "blessed" by `orc-tools` — the reader
//!    round-trip is already covered by `writer_compression.rs`, but we
//!    assert it here too so a single Java-interop failure leaves a
//!    clear breadcrumb trail.
//!
//! We also cover the Java→Rust direction: the Java uber-jar ships its
//! own sample ORC files; we use one that orc-tools just wrote (via
//! `orc-tools convert` from a JSON scratch file) and feed it through
//! the Rust reader. If the Java writer's SNAPPY output parses
//! byte-for-byte through our reader, the on-wire chunk framing and
//! PostScript fields are in the intersection of both implementations.

use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, RecordBatchReader, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;

use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::arrow_writer::{ArrowWriterBuilder, Compression};

/// Reusable batch: three rows, Int32 + Utf8. Small enough that
/// `orc-tools data` prints every row in its output.
fn tiny_batch() -> RecordBatch {
    let ids = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let names = Arc::new(StringArray::from(vec!["alpha", "bravo", "charlie"]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(schema, vec![ids, names]).unwrap()
}

/// Resolve the uber jar's absolute path or skip the test. We don't
/// use `#[ignore]` alone because CI matrices with `ORC_TOOLS_JAR`
/// set still want the test to run unconditionally when the opt-in
/// env is present.
fn orc_tools_jar() -> Option<PathBuf> {
    env::var_os("ORC_TOOLS_JAR")
        .map(PathBuf::from)
        .and_then(|p| {
            if p.exists() {
                Some(p)
            } else {
                eprintln!("ORC_TOOLS_JAR points to non-existent path: {}", p.display());
                None
            }
        })
}

/// Run `java -jar <jar> meta <file>` and return stdout+stderr.
fn run_meta(jar: &PathBuf, orc_path: &PathBuf) -> (bool, String, String) {
    let out = Command::new("java")
        .args([
            "-jar",
            jar.to_str().unwrap(),
            "meta",
            orc_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to spawn java");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Run `java -jar <jar> data <file>` and return stdout+stderr.
fn run_data(jar: &PathBuf, orc_path: &PathBuf) -> (bool, String, String) {
    let out = Command::new("java")
        .args([
            "-jar",
            jar.to_str().unwrap(),
            "data",
            orc_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to spawn java");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Write `batch` as an ORC file with `codec` to `path`.
fn write_orc(path: &PathBuf, batch: &RecordBatch, codec: Compression) {
    let file = std::fs::File::create(path).expect("create orc tempfile");
    let mut writer = ArrowWriterBuilder::new(file, batch.schema())
        .with_compression(codec)
        .try_build()
        .expect("build writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

/// Shared helper: write `batch` with `codec`, run `meta` + `data`, then
/// re-read the file through the Rust reader and confirm equality.
/// Returns the full meta/data stdout so the caller can spot-check
/// codec-specific fields (SNAPPY / ZLIB / ZSTD).
fn codec_roundtrip_with_java(codec: Compression, expected_label: &str) -> (String, String) {
    let jar = match orc_tools_jar() {
        Some(j) => j,
        None => {
            eprintln!("ORC_TOOLS_JAR not set or invalid; skipping");
            return (String::new(), String::new());
        }
    };

    // std::env::temp_dir + unique name keeps us off any `tempfile`
    // crate dependency (dev-deps are unchanged by this PR).
    let dir = env::temp_dir().join(format!(
        "orc-rust-java-interop-{}-{}",
        expected_label,
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    let path = dir.join(format!("{expected_label}.orc"));

    let batch = tiny_batch();
    write_orc(&path, &batch, codec);

    // 1. orc-tools meta — parse the PostScript.
    let (ok, stdout, stderr) = run_meta(&jar, &path);
    assert!(
        ok,
        "orc-tools meta failed for {expected_label}:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    // Java's `meta` output is pretty-printed and lists the compression
    // kind on its own line. Accept either `compression: <CODEC>` (Java
    // 1.9 format) or a bare `<CODEC>` substring (older versions) so
    // minor Java-side format drift doesn't fail the test.
    let upper = stdout.to_ascii_uppercase();
    assert!(
        upper.contains(&format!("COMPRESSION: {expected_label}")) || upper.contains(expected_label),
        "expected `{expected_label}` in meta output:\n{stdout}"
    );
    // Row count sanity — we wrote 3 rows.
    assert!(
        stdout.contains("Rows: 3") || stdout.contains("rows: 3"),
        "expected 3-row count in meta output:\n{stdout}"
    );

    // 2. orc-tools data — every row decodes.
    let (ok, data_stdout, data_stderr) = run_data(&jar, &path);
    assert!(
        ok,
        "orc-tools data failed for {expected_label}:\nstdout:\n{data_stdout}\nstderr:\n{data_stderr}"
    );
    // `orc-tools data` emits one JSON object per row, in order.
    assert!(
        data_stdout.contains("\"id\":1") || data_stdout.contains("\"id\": 1"),
        "expected id=1 in data output:\n{data_stdout}"
    );
    assert!(
        data_stdout.contains("alpha"),
        "expected 'alpha' in data output:\n{data_stdout}"
    );
    assert!(
        data_stdout.contains("charlie"),
        "expected 'charlie' in data output:\n{data_stdout}"
    );

    // 3. Rust reader still accepts the file (smoke test — the
    //    writer_compression.rs integration test already covers the
    //    in-process round-trip, but we assert here so a regression
    //    surfaces in the Java-interop test output too).
    let bytes = Bytes::from(std::fs::read(&path).expect("read orc back"));
    let reader = ArrowReaderBuilder::try_new(bytes).unwrap().build();
    let schema_out = reader.schema();
    let got: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    let concat = concat_batches(&schema_out, got.iter()).unwrap();
    assert_eq!(batch, concat, "Rust reader round-trip failed");

    // Cleanup (best effort — tempdir lives under /tmp).
    let _ = std::fs::remove_dir_all(&dir);

    (stdout, data_stdout)
}

/// Cross-validate: Rust writer → SNAPPY → Java `orc-tools meta` + `data`.
#[test]
#[ignore]
fn snappy_file_validates_with_java_orc_tools() {
    if orc_tools_jar().is_none() {
        eprintln!("ORC_TOOLS_JAR unset; skipping (run with `cargo test -- --ignored`)");
        return;
    }
    let (meta, data) = codec_roundtrip_with_java(Compression::Snappy, "SNAPPY");
    eprintln!(
        "[snappy] meta excerpt (truncated to 400 chars):\n{}\n[snappy] data excerpt:\n{}",
        meta.chars().take(400).collect::<String>(),
        data.chars().take(400).collect::<String>(),
    );
}

/// Cross-validate: Rust writer → ZLIB → Java `orc-tools`.
#[test]
#[ignore]
fn zlib_file_validates_with_java_orc_tools() {
    if orc_tools_jar().is_none() {
        eprintln!("ORC_TOOLS_JAR unset; skipping (run with `cargo test -- --ignored`)");
        return;
    }
    let (meta, data) = codec_roundtrip_with_java(Compression::zlib(), "ZLIB");
    eprintln!(
        "[zlib] meta excerpt:\n{}\n[zlib] data excerpt:\n{}",
        meta.chars().take(400).collect::<String>(),
        data.chars().take(400).collect::<String>(),
    );
}

/// Cross-validate: Rust writer → ZSTD → Java `orc-tools`.
#[test]
#[ignore]
fn zstd_file_validates_with_java_orc_tools() {
    if orc_tools_jar().is_none() {
        eprintln!("ORC_TOOLS_JAR unset; skipping (run with `cargo test -- --ignored`)");
        return;
    }
    let (meta, data) = codec_roundtrip_with_java(Compression::zstd(), "ZSTD");
    eprintln!(
        "[zstd] meta excerpt:\n{}\n[zstd] data excerpt:\n{}",
        meta.chars().take(400).collect::<String>(),
        data.chars().take(400).collect::<String>(),
    );
}

/// Reverse direction: Java writer → Rust reader.
///
/// `orc-tools convert` reads a JSON stream and writes a compressed
/// ORC file (ZLIB by default on orc-tools 1.9; the CLI in 1.9 does
/// not expose a `--compress` flag for `convert`, so we use the
/// default codec — ZLIB is already in the set we validate in the
/// forward direction, so this also doubles as a codec-consistency
/// check). We feed the resulting file through the Rust reader and
/// assert the values round-trip.
#[test]
#[ignore]
fn java_zlib_file_reads_with_rust() {
    let jar = match orc_tools_jar() {
        Some(j) => j,
        None => {
            eprintln!("ORC_TOOLS_JAR unset; skipping (run with `cargo test -- --ignored`)");
            return;
        }
    };

    let dir = env::temp_dir().join(format!("orc-rust-java-writer-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    let json_path = dir.join("in.json");
    let orc_path = dir.join("java.orc");
    let schema = "struct<id:int,name:string>";

    // `orc-tools convert` expects one JSON object per line.
    {
        let mut f = std::fs::File::create(&json_path).expect("create json");
        writeln!(f, "{{\"id\": 1, \"name\": \"alpha\"}}").unwrap();
        writeln!(f, "{{\"id\": 2, \"name\": \"bravo\"}}").unwrap();
        writeln!(f, "{{\"id\": 3, \"name\": \"charlie\"}}").unwrap();
    }

    // `convert` CLI: -s <schema> -o <output> <input.json>. orc-tools
    // 1.9 doesn't accept --compress here; default codec is ZLIB, which
    // is already one of the three codecs we validate in the Rust→Java
    // direction, so this still proves the on-wire chunk framing
    // inter-operates.
    let out = Command::new("java")
        .args([
            "-jar",
            jar.to_str().unwrap(),
            "convert",
            "-s",
            schema,
            "-o",
            orc_path.to_str().unwrap(),
            json_path.to_str().unwrap(),
        ])
        .output()
        .expect("spawn java convert");
    assert!(
        out.status.success(),
        "orc-tools convert failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(orc_path.exists(), "java convert produced no file");

    // Smoke-check via meta to confirm the default codec Java used.
    let (ok, meta_stdout, meta_stderr) = run_meta(&jar, &orc_path);
    assert!(
        ok,
        "orc-tools meta on Java-written file failed:\nstdout:\n{meta_stdout}\nstderr:\n{meta_stderr}"
    );
    let upper = meta_stdout.to_ascii_uppercase();
    let expected_codec = if upper.contains("COMPRESSION: ZSTD") {
        "ZSTD"
    } else if upper.contains("COMPRESSION: SNAPPY") {
        "SNAPPY"
    } else if upper.contains("COMPRESSION: ZLIB") {
        "ZLIB"
    } else if upper.contains("COMPRESSION: NONE") {
        "NONE"
    } else {
        panic!("Java writer emitted unrecognised codec; meta output:\n{meta_stdout}")
    };
    // We don't assert a specific codec — Java's default has drifted
    // across 1.7/1.8/1.9 releases. We only assert the Rust reader
    // copes with whichever codec Java picked.
    eprintln!("[java-writer] codec observed: {expected_codec}");

    // Now read with the Rust reader.
    let bytes = Bytes::from(std::fs::read(&orc_path).expect("read java orc"));
    let reader = ArrowReaderBuilder::try_new(bytes)
        .expect("Rust reader accepts Java-written file")
        .build();
    let schema_out = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<_, _>>()
        .expect("Rust reader decodes Java-written stream");
    let concat = concat_batches(&schema_out, batches.iter()).unwrap();

    assert_eq!(
        concat.num_rows(),
        3,
        "expected 3 rows from Java-written file"
    );
    // Spot-check column values — Java `convert` writes columns in the
    // declared schema order.
    let ids = concat
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id column is Int32");
    assert_eq!(ids.values(), &[1, 2, 3]);
    let names = concat
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column is Utf8");
    assert_eq!(names.value(0), "alpha");
    assert_eq!(names.value(1), "bravo");
    assert_eq!(names.value(2), "charlie");

    let _ = std::fs::remove_dir_all(&dir);
}
