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

//! Benchmarks comparing writer-side compression codecs on a 10k-row
//! mixed Int64 + Utf8 batch — the exact workload most production Hive /
//! Trino tables emit. Each benchmark measures end-to-end write time
//! from `RecordBatch` to closed ORC file. The reported throughput is
//! single-stripe (the batch fits comfortably under the 64 MiB stripe
//! size), so the variance between codecs is dominated by encoder cost
//! and the resulting on-disk size — both of which we surface in the
//! benchmark printouts so reviewers can sanity-check the trade-off.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use orc_rust::arrow_writer::{ArrowWriterBuilder, Compression};

fn build_batch() -> RecordBatch {
    let n = 10_000;
    let ints: Vec<i64> = (0..n as i64).collect();
    // Repeating-but-not-trivial strings — gives every codec something
    // to chew on without making the input pathologically compressible.
    let strs: Vec<String> = (0..n)
        .map(|i| format!("event-{:08x}-payload-{}", i, i % 17))
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ints)),
            Arc::new(StringArray::from(strs)),
        ],
    )
    .unwrap()
}

fn write_orc(batch: &RecordBatch, compression: Compression) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024 * 1024);
    let mut writer = ArrowWriterBuilder::new(&mut buf, batch.schema())
        .with_compression(compression)
        .try_build()
        .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    buf
}

fn writer_compression(c: &mut Criterion) {
    let batch = build_batch();
    // Headline: how many bytes of *input* (rows × column count × ~16
    // bytes for the payload column) we are compressing.
    let approx_input_bytes = batch.num_rows() as u64 * 32;

    let codecs: Vec<(&str, Compression)> = vec![
        ("none", Compression::None),
        ("snappy", Compression::Snappy),
        ("zlib_1", Compression::Zlib { level: 1 }),
        ("zlib_6", Compression::Zlib { level: 6 }),
        ("zlib_9", Compression::Zlib { level: 9 }),
        ("zstd_1", Compression::Zstd { level: 1 }),
        ("zstd_3", Compression::Zstd { level: 3 }),
        ("zstd_9", Compression::Zstd { level: 9 }),
        ("zstd_19", Compression::Zstd { level: 19 }),
    ];

    let mut group = c.benchmark_group("write_10k_rows");
    group.throughput(Throughput::Bytes(approx_input_bytes));
    for (label, codec) in &codecs {
        // Surface output file size as a stderr line — Criterion doesn't
        // model this natively, but reviewers care a lot.
        let bytes = write_orc(&batch, *codec);
        eprintln!(
            "[writer_compression] codec={label:>7}  output_bytes={:>8}  ratio={:.2}x",
            bytes.len(),
            approx_input_bytes as f64 / bytes.len() as f64,
        );
        group.bench_function(*label, |b| {
            b.iter(|| {
                let _ = write_orc(&batch, *codec);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, writer_compression);
criterion_main!(benches);
