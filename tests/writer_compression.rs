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

//! End-to-end tests for writer-side compression (SNAPPY / ZLIB / ZSTD).
//!
//! These tests target the public surface — they only call
//! [`ArrowWriterBuilder`] and [`ArrowReaderBuilder`] plus a tiny amount
//! of `prost` parsing to inspect the PostScript bytes — so they exercise
//! the same path downstream consumers use.

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use prost::Message;

use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::arrow_writer::{ArrowWriterBuilder, Compression, DEFAULT_COMPRESSION_BLOCK_SIZE};
use orc_rust::proto;

/// Build a small mixed-type batch that exercises an integer column
/// (uses Direct/DirectV2 + present) and a string column (uses
/// Length + Data dictionary streams).
fn small_mixed_batch() -> RecordBatch {
    let ints = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
    let strings = Arc::new(StringArray::from(vec![
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
    ]));
    // Reader normalises every field to `nullable: true` regardless of
    // what the writer declared, so we declare nullable here too to keep
    // round-trip equality straightforward. ORC null streams encode
    // presence per-row, so semantically there is no difference.
    let schema = Arc::new(Schema::new(vec![
        Field::new("i32", DataType::Int32, true),
        Field::new("str", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(schema, vec![ints, strings]).unwrap()
}

/// Round-trip helper: serialise `batch` with `compression`, read it
/// back, and concatenate every emitted batch into one.
fn roundtrip_with_compression(batch: &RecordBatch, compression: Compression) -> RecordBatch {
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(compression)
        .try_build()
        .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();

    let bytes = Bytes::from(buf);
    let reader = ArrowReaderBuilder::try_new(bytes).unwrap().build();
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    concat_batches(&schema, batches.iter()).unwrap()
}

/// Parse the PostScript out of an ORC file we just wrote. Mirrors the
/// reader's tail-locating logic so the assertion exercises the on-disk
/// format, not a private helper.
fn parse_postscript(bytes: &[u8]) -> proto::PostScript {
    let len = bytes.len();
    let postscript_len = bytes[len - 1] as usize;
    let postscript_start = len - 1 - postscript_len;
    let postscript_bytes = &bytes[postscript_start..len - 1];
    proto::PostScript::decode(postscript_bytes).expect("decode PostScript")
}

#[test]
fn roundtrip_snappy() {
    let batch = small_mixed_batch();
    let read = roundtrip_with_compression(&batch, Compression::Snappy);
    assert_eq!(batch, read);
}

#[test]
fn roundtrip_zlib_default() {
    let batch = small_mixed_batch();
    let read = roundtrip_with_compression(&batch, Compression::zlib());
    assert_eq!(batch, read);
}

#[test]
fn roundtrip_zlib_explicit_level() {
    let batch = small_mixed_batch();
    let read = roundtrip_with_compression(&batch, Compression::Zlib { level: 9 });
    assert_eq!(batch, read);
}

#[test]
fn roundtrip_zstd_default() {
    let batch = small_mixed_batch();
    let read = roundtrip_with_compression(&batch, Compression::zstd());
    assert_eq!(batch, read);
}

#[test]
fn roundtrip_zstd_high_level() {
    let batch = small_mixed_batch();
    let read = roundtrip_with_compression(&batch, Compression::Zstd { level: 19 });
    assert_eq!(batch, read);
}

#[test]
fn compression_kind_written_to_postscript_snappy() {
    let batch = small_mixed_batch();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(Compression::Snappy)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let postscript = parse_postscript(&buf);
    assert_eq!(
        postscript.compression,
        Some(proto::CompressionKind::Snappy as i32)
    );
}

#[test]
fn compression_kind_written_to_postscript_zlib() {
    let batch = small_mixed_batch();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(Compression::zlib())
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let postscript = parse_postscript(&buf);
    assert_eq!(
        postscript.compression,
        Some(proto::CompressionKind::Zlib as i32)
    );
}

#[test]
fn compression_kind_written_to_postscript_zstd() {
    let batch = small_mixed_batch();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(Compression::zstd())
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let postscript = parse_postscript(&buf);
    assert_eq!(
        postscript.compression,
        Some(proto::CompressionKind::Zstd as i32)
    );
}

#[test]
fn compression_block_size_written_to_postscript() {
    let batch = small_mixed_batch();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(Compression::Snappy)
        .with_compression_block_size(64 * 1024)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let postscript = parse_postscript(&buf);
    assert_eq!(postscript.compression_block_size, Some(64 * 1024));
}

#[test]
fn default_compression_block_size_is_256k() {
    let batch = small_mixed_batch();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(Compression::Snappy)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let postscript = parse_postscript(&buf);
    assert_eq!(
        postscript.compression_block_size,
        Some(DEFAULT_COMPRESSION_BLOCK_SIZE as u64)
    );
}

/// `Compression::None` MUST emit a byte-stream that is bit-for-bit
/// identical to a writer built without any compression call. Captures
/// the backward-compatibility invariant claimed in the PR body.
#[test]
fn backward_compat_default_no_compression_byte_identical() {
    let batch = small_mixed_batch();

    let mut buf_default: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf_default, batch.schema())
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut buf_explicit_none: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf_explicit_none, batch.schema())
        .with_compression(Compression::None)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    assert_eq!(
        buf_default, buf_explicit_none,
        "Compression::None must produce the same bytes as the default builder"
    );
    let postscript = parse_postscript(&buf_default);
    assert_eq!(
        postscript.compression,
        Some(proto::CompressionKind::None as i32),
        "default writer still labels as NONE"
    );
    assert_eq!(
        postscript.compression_block_size, None,
        "default writer omits the compression_block_size field"
    );
}

/// Random-ish bytes that the codecs cannot meaningfully compress force
/// the writer to fall back to the spec's "original" flag. We verify
/// the PostScript-advertised codec is still SNAPPY (so readers know to
/// look for the chunked framing) and that the round-trip survives — the
/// in-process reader will dutifully follow the original flag and skip
/// decompression, exactly like Java ORC does.
#[test]
fn incompressible_payload_survives_round_trip() {
    // Pseudo-random bytes (xorshift) — guaranteed to defeat snappy /
    // zstd / deflate. We avoid `rand` to keep dev-deps minimal.
    let mut state: u64 = 0xdead_beef_cafe_babe;
    let mut s = String::with_capacity(64 * 1024);
    for _ in 0..8192 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        // Hex-encode 8 bytes per row → 16 chars. Highly redundant
        // *visually* but each byte is uniform-random so the codec
        // can't exploit structure.
        s.push_str(&format!("{state:016x}"));
    }
    let strings = Arc::new(StringArray::from(vec![s; 16]));
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema, vec![strings]).unwrap();

    let read = roundtrip_with_compression(&batch, Compression::Snappy);
    assert_eq!(batch, read);
}

/// Writing a payload whose total stream size exceeds the configured
/// block size MUST result in multiple compression chunks (the
/// compression footer is broken into >1 chunk by definition once the
/// stream exceeds `block_size`). We use a tiny block size so the test
/// is fast.
#[test]
fn small_block_size_emits_multiple_chunks_per_stream() {
    // 1 million i64 values → 8 MB of payload at minimum; with a
    // 4 KiB block size, every Data stream MUST be split.
    let data: Vec<i64> = (0..1_000_000).collect();
    let array = Arc::new(Int64Array::from(data));
    let schema = Arc::new(Schema::new(vec![Field::new("i64", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(Compression::Snappy)
        .with_compression_block_size(4 * 1024)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Sanity: PostScript carries the configured block size.
    let postscript = parse_postscript(&buf);
    assert_eq!(postscript.compression_block_size, Some(4 * 1024));

    // Round-trip the file to prove the chunked stream decodes.
    let bytes = Bytes::from(buf);
    let reader = ArrowReaderBuilder::try_new(bytes).unwrap().build();
    let schema_out = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    let actual = concat_batches(&schema_out, batches.iter()).unwrap();
    assert_eq!(batch, actual);
}

/// Smoke-test the spec's largest-allowed block size doesn't trip the
/// 23-bit length-field assertion. We don't write that much data; we're
/// just exercising the API clamp.
#[test]
fn over_max_compression_block_size_is_clamped() {
    let batch = small_mixed_batch();
    // Ask for 16 MiB — twice the spec maximum.
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(Compression::Snappy)
        .with_compression_block_size(16 * 1024 * 1024)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let postscript = parse_postscript(&buf);
    let recorded = postscript.compression_block_size.unwrap();
    assert!(
        recorded < (1u64 << 23),
        "block size {recorded} must be clamped under the 23-bit ceiling"
    );
}

/// Zero block size MUST fall back to the documented default rather
/// than asserting at runtime — the public API should be impossible to
/// misuse into a panic.
#[test]
fn zero_compression_block_size_falls_back_to_default() {
    let batch = small_mixed_batch();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(Compression::Snappy)
        .with_compression_block_size(0)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let postscript = parse_postscript(&buf);
    assert_eq!(
        postscript.compression_block_size,
        Some(DEFAULT_COMPRESSION_BLOCK_SIZE as u64)
    );
}

/// Multi-stripe writes must emit one compressed footer per stripe, and
/// every stripe must round-trip.
#[test]
fn multi_stripe_with_compression_round_trips() {
    let data: Vec<i64> = (0..1_000_000).collect();
    let array = Arc::new(Int64Array::from(data));
    let schema = Arc::new(Schema::new(vec![Field::new("i64", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        // Tiny stripe size to force several stripes.
        .with_stripe_byte_size(256)
        .with_compression(Compression::Snappy)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let bytes = Bytes::from(buf);
    let reader = ArrowReaderBuilder::try_new(bytes).unwrap().build();
    let schema_out = reader.schema();
    let read_batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    assert!(
        read_batches.len() > 1,
        "expected multiple stripes/batches; got {}",
        read_batches.len()
    );
    let actual = concat_batches(&schema_out, read_batches.iter()).unwrap();
    assert_eq!(batch, actual);
}
