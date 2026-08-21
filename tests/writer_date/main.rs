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
    array::{Date32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use orc_rust::{compression::CompressionType, ArrowWriterBuilder};

#[test]
#[ignore = "requires ./venv/bin/python with pyorc installed"]
fn date32_file_is_readable_by_pyorc() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "date",
        DataType::Date32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Date32Array::from(vec![
            Some(-1),
            Some(0),
            Some(1),
            Some(11_016),
            None,
            Some(18_321),
            Some(19_782),
        ]))],
    )
    .unwrap();

    let path =
        std::env::temp_dir().join(format!("orc-rust-writer-date32-{}.orc", std::process::id()));
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
         \tassert str(reader.schema) == 'struct<date:date>', reader.schema\n\
         \trows = list(reader)\n\
         \texpected = [(datetime.date(1969, 12, 31),),\n\
         \t            (datetime.date(1970, 1, 1),),\n\
         \t            (datetime.date(1970, 1, 2),),\n\
         \t            (datetime.date(2000, 2, 29),),\n\
         \t            (None,),\n\
         \t            (datetime.date(2020, 2, 29),),\n\
         \t            (datetime.date(2024, 2, 29),)]\n\
         \tassert rows == expected, rows",
        )
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success(), "pyorc failed to read Date32 file");

    fs::remove_file(path).unwrap();
}
