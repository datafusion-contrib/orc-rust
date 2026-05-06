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

//! Async API for writing Arrow [`RecordBatch`]es into ORC files.

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    arrow_writer::{ArrowWriter, ArrowWriterBuilder},
    compression::{WriterCompression, DEFAULT_COMPRESSION_BLOCK_SIZE},
    error::{IoSnafu, Result, UnexpectedSnafu},
};
use snafu::{ensure, ResultExt};

/// Construct an [`AsyncArrowWriter`] backed by an async writer.
pub struct AsyncArrowWriterBuilder<W> {
    writer: W,
    schema: SchemaRef,
    batch_size: usize,
    stripe_byte_size: usize,
    compression: WriterCompression,
    compression_block_size: usize,
    row_index_stride: Option<usize>,
    bloom_filters: bool,
}

impl<W> AsyncArrowWriterBuilder<W> {
    /// Create a new builder for writing ORC bytes to `writer`.
    pub fn new(writer: W, schema: SchemaRef) -> Self {
        Self {
            writer,
            schema,
            batch_size: 1024,
            stripe_byte_size: 64 * 1024 * 1024,
            compression: WriterCompression::None,
            compression_block_size: DEFAULT_COMPRESSION_BLOCK_SIZE as usize,
            row_index_stride: None,
            bloom_filters: false,
        }
    }

    /// Batch size controls the encoding behaviour. Default is `1024`.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// The approximate size of stripes. Default is `64MiB`.
    pub fn with_stripe_byte_size(mut self, stripe_byte_size: usize) -> Self {
        self.stripe_byte_size = stripe_byte_size;
        self
    }

    /// Compress ORC streams and metadata with the provided writer codec.
    pub fn with_compression(mut self, compression: WriterCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Enable ORC `ZLIB` compression.
    pub fn with_gzip_compression(mut self) -> Self {
        self.compression = WriterCompression::Zlib;
        self
    }

    /// The maximum uncompressed size of each ORC compression block.
    pub fn with_compression_block_size(mut self, compression_block_size: usize) -> Self {
        self.compression_block_size = compression_block_size;
        self
    }

    /// Enable writer row indexes with `rows_per_group` rows per row group.
    pub fn with_row_index_stride(mut self, rows_per_group: usize) -> Self {
        self.row_index_stride = Some(rows_per_group);
        self
    }

    /// Enable writer bloom filter streams for supported primitive columns.
    pub fn with_bloom_filters(mut self) -> Self {
        self.bloom_filters = true;
        self
    }

    /// Construct an [`AsyncArrowWriter`].
    pub fn try_build(self) -> Result<AsyncArrowWriter<W>> {
        ensure!(
            self.compression_block_size > 0,
            UnexpectedSnafu {
                msg: "compression block size must be greater than zero"
            }
        );
        ensure!(
            self.row_index_stride.map_or(true, |stride| stride > 0),
            UnexpectedSnafu {
                msg: "row index stride must be greater than zero"
            }
        );
        ensure!(
            !self.bloom_filters || self.row_index_stride.is_some(),
            UnexpectedSnafu {
                msg: "bloom filters require row indexes to define row group boundaries"
            }
        );

        let mut builder = ArrowWriterBuilder::new(Vec::new(), self.schema)
            .with_batch_size(self.batch_size)
            .with_stripe_byte_size(self.stripe_byte_size)
            .with_compression(self.compression)
            .with_compression_block_size(self.compression_block_size);
        if let Some(row_index_stride) = self.row_index_stride {
            builder = builder.with_row_index_stride(row_index_stride);
        }
        if self.bloom_filters {
            builder = builder.with_bloom_filters();
        }
        let inner = builder.try_build()?;
        Ok(AsyncArrowWriter {
            writer: self.writer,
            inner,
        })
    }
}

/// Encodes [`RecordBatch`]es into ORC and writes the final bytes asynchronously.
pub struct AsyncArrowWriter<W> {
    writer: W,
    inner: ArrowWriter<Vec<u8>>,
}

impl<W: AsyncWrite + Unpin> AsyncArrowWriter<W> {
    /// Encode the provided batch into the ORC stream.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch)
    }

    /// Flush buffered ORC bytes to the async writer and close this writer.
    pub async fn close(self) -> Result<()> {
        self.finish().await.map(|_| ())
    }

    /// Flush buffered ORC bytes to the async writer and return the inner writer.
    pub async fn finish(mut self) -> Result<W> {
        let buffer = self.inner.finish()?;
        self.writer.write_all(&buffer).await.context(IoSnafu)?;
        self.writer.flush().await.context(IoSnafu)?;
        Ok(self.writer)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType as ArrowDataType, Field, Schema},
    };
    use bytes::Bytes;
    use tokio::io::AsyncReadExt;

    use crate::{ArrowReaderBuilder, AsyncArrowWriterBuilder};

    #[tokio::test]
    async fn test_async_writer_roundtrip() {
        let array = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "int64",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let (client, mut server) = tokio::io::duplex(4096);
        let read_handle = tokio::spawn(async move {
            let mut out = Vec::new();
            server.read_to_end(&mut out).await.unwrap();
            out
        });

        let mut writer = AsyncArrowWriterBuilder::new(client, batch.schema())
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().await.unwrap();

        let bytes = Bytes::from(read_handle.await.unwrap());
        let rows = ArrowReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batch, rows[0]);
    }
}
