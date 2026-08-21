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

use std::{fs, process::Command, sync::Arc};

use arrow::{
    array::{RecordBatch, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use orc_rust::{compression::CompressionType, ArrowWriterBuilder};

#[test]
#[ignore = "requires ./venv/bin/python with pyorc installed"]
fn timestamp_file_is_readable_by_pyorc() {
    let values = vec![
        Some(-2_208_988_800_000_000),
        Some(-1_001_000),
        Some(0),
        None,
        Some(1_420_070_400_000_000),
        Some(1_709_210_096_123_456),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_instant",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(values.clone())),
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")),
        ],
    )
    .unwrap();

    let path = std::env::temp_dir().join(format!(
        "orc-rust-writer-timestamp-{}.orc",
        std::process::id()
    ));
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriterBuilder::new(file, batch.schema())
        .with_compression(CompressionType::Zstd)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let status = Command::new(format!("{}/venv/bin/python", env!("CARGO_MANIFEST_DIR")))
        .arg("-c")
        .arg(
            "import datetime, pyorc, sys\n\
         with open(sys.argv[1], 'rb') as source:\n\
         \treader = pyorc.Reader(source)\n\
         \tassert str(reader.schema) == 'struct<timestamp:timestamp,timestamp_instant:timestamp with local time zone>', reader.schema\n\
         \tvalues = [datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc),\n\
         \t          datetime.datetime(1969, 12, 31, 23, 59, 58, 999000, tzinfo=datetime.timezone.utc),\n\
         \t          datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc),\n\
         \t          None,\n\
         \t          datetime.datetime(2015, 1, 1, tzinfo=datetime.timezone.utc),\n\
         \t          datetime.datetime(2024, 2, 29, 12, 34, 56, 123456, tzinfo=datetime.timezone.utc)]\n\
         \tassert list(reader) == [(value, value) for value in values]",
        )
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success(), "pyorc failed to read timestamp file");

    fs::remove_file(path).unwrap();
}
