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
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use orc_rust::{compression::CompressionType, ArrowWriterBuilder};

#[test]
#[ignore = "requires ./venv/bin/python with pyorc installed"]
fn compressed_files_are_readable_by_pyorc() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["same"; 256])),
            Arc::new(Int64Array::from_iter_values(0..256)),
        ],
    )
    .unwrap();

    let temp_dir = std::env::temp_dir().join(format!(
        "orc-rust-writer-compression-{}",
        std::process::id()
    ));
    fs::create_dir_all(&temp_dir).unwrap();

    for (name, compression) in [
        ("zlib", CompressionType::Zlib),
        ("snappy", CompressionType::Snappy),
        ("lz4", CompressionType::Lz4),
        ("zstd", CompressionType::Zstd),
    ] {
        let path = temp_dir.join(format!("{name}.orc"));
        let file = fs::File::create(&path).unwrap();
        let mut writer = ArrowWriterBuilder::new(file, batch.schema())
            .with_compression(compression)
            .with_compression_block_size(64)
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // The first string DATA stream starts immediately after the ORC magic.
        // Its first block must be compressed so pyorc exercises the actual codec,
        // rather than only the original-block fallback framing.
        let bytes = fs::read(&path).unwrap();
        let header = u32::from_le_bytes([bytes[3], bytes[4], bytes[5], 0]);
        assert_eq!(header & 1, 0, "{name} did not produce a compressed block");
        assert!(header >> 1 < 64, "{name} did not shrink the first block");

        let status = Command::new(format!("{}/venv/bin/python", env!("CARGO_MANIFEST_DIR")))
            .arg("-c")
            .arg(
                "import pyorc, sys\n\
                 with open(sys.argv[1], 'rb') as source:\n\
                 \trows = list(pyorc.Reader(source))\n\
                 assert rows == [('same', i) for i in range(256)], rows",
            )
            .arg(&path)
            .status()
            .unwrap();
        assert!(status.success(), "pyorc failed for {name}");
    }

    fs::remove_dir_all(temp_dir).unwrap();
}
