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

//! Read and write ORC with Apache OpenDAL using an in-memory service.
//!
//! Run with `cargo run --example opendal --features opendal`.
//! Replace the Memory service and enable the corresponding OpenDAL service feature
//! to use remote storage. Reads use the built-in `AsyncOpendalReader` for range I/O.
//! The synchronous ORC writer buffers this small file in memory before uploading it.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use futures::TryStreamExt;
use opendal::{services::Memory, Operator};
use orc_rust::{reader::AsyncOpendalReader, ArrowReaderBuilder, ArrowWriterBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let operator = Operator::new(Memory::default())?.finish();
    let path = "example.orc";
    let col = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
    let batch = RecordBatch::try_from_iter([("col", col)])?;

    let mut buffer = Vec::new();
    let mut writer = ArrowWriterBuilder::new(&mut buffer, batch.schema()).try_build()?;
    writer.write(&batch)?;
    // Finish the ORC footer before uploading the complete file.
    writer.close()?;
    operator.write(path, buffer).await?;

    let reader = AsyncOpendalReader::new(operator, path);
    let builder = ArrowReaderBuilder::try_new_async(reader).await?;
    let read: Vec<RecordBatch> = builder.build_async().try_collect().await?;
    assert_eq!(read, vec![batch]);
    println!("read {} rows", read[0].num_rows());

    Ok(())
}
