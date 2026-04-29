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

use std::io::Write;

use arrow::{
    array::RecordBatch,
    datatypes::{DataType as ArrowDataType, SchemaRef},
};
use prost::Message;
use snafu::{ensure, ResultExt};

use crate::{
    error::{IoSnafu, Result, UnexpectedSnafu},
    memory::EstimateMemory,
    proto,
    writer::compression::compress_stream,
    writer::stripe::{StripeCompression, StripeInformation, StripeWriter},
};

// Re-export the writer-side compression API at the same level as
// `ArrowWriterBuilder` so users can reach for both via a single
// `use orc_rust::arrow_writer::*;`. The constants are exposed so
// callers can derive their own defaults from the canonical values.
pub use crate::writer::compression::{
    Compression, DEFAULT_COMPRESSION_BLOCK_SIZE, DEFAULT_ZLIB_LEVEL, DEFAULT_ZSTD_LEVEL,
};

/// Maximum compression block size representable in the ORC chunk
/// header's 23-bit length field. The header encodes payload length in
/// the upper 23 bits, so the largest legal block size is
/// `2^23 − 1 = 8 388 607` bytes.
const MAX_COMPRESSION_BLOCK_SIZE: usize = (1 << 23) - 1;

/// Construct an [`ArrowWriter`] to encode [`RecordBatch`]es into a single
/// ORC file.
///
/// # Compression
///
/// By default, output is uncompressed and byte-identical to a build of
/// orc-rust without writer-side compression. Pass [`Compression::Snappy`],
/// [`Compression::zlib`], or [`Compression::zstd`] (or the level-bearing
/// variants) to [`Self::with_compression`] to wrap every emitted stream
/// in the ORC v1 spec's per-chunk compression frames. The default
/// per-chunk block size is 256 KiB, configurable via
/// [`Self::with_compression_block_size`].
///
/// ```no_run
/// # use std::fs::File;
/// # use arrow::array::RecordBatch;
/// # use orc_rust::arrow_writer::{ArrowWriterBuilder, Compression};
/// # fn batch() -> RecordBatch { unimplemented!() }
/// let file = File::create("/tmp/out.orc").unwrap();
/// let batch = batch();
/// let mut writer = ArrowWriterBuilder::new(file, batch.schema())
///     .with_compression(Compression::Snappy)
///     .try_build()
///     .unwrap();
/// writer.write(&batch).unwrap();
/// writer.close().unwrap();
/// ```
pub struct ArrowWriterBuilder<W> {
    writer: W,
    schema: SchemaRef,
    batch_size: usize,
    stripe_byte_size: usize,
    compression: Compression,
    compression_block_size: usize,
}

impl<W: Write> ArrowWriterBuilder<W> {
    /// Create a new [`ArrowWriterBuilder`], which will write an ORC file to
    /// the provided writer, with the expected Arrow schema. Defaults to
    /// uncompressed output; use [`Self::with_compression`] to opt in to
    /// SNAPPY / ZLIB / ZSTD.
    pub fn new(writer: W, schema: SchemaRef) -> Self {
        Self {
            writer,
            schema,
            batch_size: 1024,
            // 64 MiB
            stripe_byte_size: 64 * 1024 * 1024,
            compression: Compression::None,
            compression_block_size: DEFAULT_COMPRESSION_BLOCK_SIZE,
        }
    }

    /// Batch size controls the encoding behaviour, where `batch_size` values
    /// are encoded at a time. Default is `1024`.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// The approximate size of stripes. Default is `64MiB`.
    pub fn with_stripe_byte_size(mut self, stripe_byte_size: usize) -> Self {
        self.stripe_byte_size = stripe_byte_size;
        self
    }

    /// Select the compression codec applied to every emitted stream,
    /// the stripe footers, and the file footer. Default is
    /// [`Compression::None`].
    ///
    /// The codec choice is recorded in the file's PostScript so any
    /// conformant ORC reader (Java ORC, DuckDB, Spark, orc-rust's own
    /// reader) can decompress the file.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Per-chunk compression block size in bytes. Default 256 KiB,
    /// matching the ORC spec and Java ORC's `OrcConf.BUFFER_SIZE`
    /// default. The value is recorded in the PostScript so the reader
    /// uses the same block size.
    ///
    /// The value is silently clamped to the spec's 23-bit limit
    /// (`2^23 - 1` bytes); zero is treated as "use default".
    pub fn with_compression_block_size(mut self, block_size: usize) -> Self {
        self.compression_block_size = if block_size == 0 {
            DEFAULT_COMPRESSION_BLOCK_SIZE
        } else {
            block_size.min(MAX_COMPRESSION_BLOCK_SIZE)
        };
        self
    }

    /// Construct an [`ArrowWriter`] ready to encode [`RecordBatch`]es into
    /// an ORC file.
    pub fn try_build(mut self) -> Result<ArrowWriter<W>> {
        // Required magic "ORC" bytes at start of file
        self.writer.write_all(b"ORC").context(IoSnafu)?;
        let stripe_compression = self.compression.is_active().then_some(StripeCompression {
            compression: self.compression,
            block_size: self.compression_block_size,
        });
        let writer = StripeWriter::with_compression(self.writer, &self.schema, stripe_compression);
        Ok(ArrowWriter {
            writer,
            schema: self.schema,
            batch_size: self.batch_size,
            stripe_byte_size: self.stripe_byte_size,
            written_stripes: vec![],
            // Accounting for the 3 magic bytes above
            total_bytes_written: 3,
            compression: self.compression,
            compression_block_size: self.compression_block_size,
        })
    }
}

/// Encodes [`RecordBatch`]es into an ORC file. Will encode `batch_size` rows
/// at a time into a stripe, flushing the stripe to the underlying writer when
/// it's estimated memory footprint exceeds the configures `stripe_byte_size`.
pub struct ArrowWriter<W> {
    writer: StripeWriter<W>,
    schema: SchemaRef,
    batch_size: usize,
    stripe_byte_size: usize,
    written_stripes: Vec<StripeInformation>,
    /// Used to keep track of progress in file so far (instead of needing Seek on the writer)
    total_bytes_written: u64,
    /// Compression codec configured on this writer. Echoed into the
    /// file footer's PostScript so the reader runs the matching
    /// decompressor.
    compression: Compression,
    /// Per-chunk compression block size in bytes — also recorded in the
    /// PostScript per the ORC spec.
    compression_block_size: usize,
}

impl<W: Write> ArrowWriter<W> {
    /// Encode the provided batch at `batch_size` rows at a time, flushing any
    /// stripes that exceed the configured stripe size.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        ensure!(
            batch.schema() == self.schema,
            UnexpectedSnafu {
                msg: "RecordBatch doesn't match expected schema"
            }
        );

        for offset in (0..batch.num_rows()).step_by(self.batch_size) {
            let length = self.batch_size.min(batch.num_rows() - offset);
            let batch = batch.slice(offset, length);
            self.writer.encode_batch(&batch)?;

            // TODO: be able to flush whilst writing a batch (instead of between batches)
            // Flush stripe when it exceeds estimated configured size
            if self.writer.estimate_memory_size() > self.stripe_byte_size {
                self.flush_stripe()?;
            }
        }
        Ok(())
    }

    /// Flush any buffered data that hasn't been written, and write the stripe
    /// footer metadata.
    pub fn flush_stripe(&mut self) -> Result<()> {
        let info = self.writer.finish_stripe(self.total_bytes_written)?;
        self.total_bytes_written += info.total_byte_size();
        self.written_stripes.push(info);
        Ok(())
    }

    /// Flush the current stripe if it is still in progress, and write the tail
    /// metadata and close the writer.
    pub fn close(mut self) -> Result<()> {
        // Flush in-progress stripe
        if self.writer.row_count > 0 {
            self.flush_stripe()?;
        }
        let footer = serialize_footer(&self.written_stripes, &self.schema);
        let footer = footer.encode_to_vec();
        // Per the ORC spec the file footer is also compressed when a
        // codec is configured. The PostScript itself is *not*
        // compressed (it's written in fixed format at the end of the
        // file so readers can locate it without first knowing the
        // codec).
        let footer_bytes = if self.compression.is_active() {
            compress_stream(self.compression, self.compression_block_size, &footer)?
        } else {
            footer
        };
        let postscript = serialize_postscript(
            footer_bytes.len() as u64,
            self.compression,
            self.compression_block_size,
        );
        let postscript = postscript.encode_to_vec();
        let postscript_len = postscript.len() as u8;

        let mut writer = self.writer.finish();
        writer.write_all(&footer_bytes).context(IoSnafu)?;
        writer.write_all(&postscript).context(IoSnafu)?;
        // Postscript length as last byte
        writer.write_all(&[postscript_len]).context(IoSnafu)?;

        Ok(())
    }
}

fn serialize_schema(schema: &SchemaRef) -> Vec<proto::Type> {
    let mut types = vec![];

    let field_names = schema
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect();
    // TODO: consider nested types
    let subtypes = (1..(schema.fields().len() as u32 + 1)).collect();
    let root_type = proto::Type {
        kind: Some(proto::r#type::Kind::Struct.into()),
        subtypes,
        field_names,
        maximum_length: None,
        precision: None,
        scale: None,
        attributes: vec![],
    };
    types.push(root_type);
    for field in schema.fields() {
        let t = match field.data_type() {
            ArrowDataType::Float32 => proto::Type {
                kind: Some(proto::r#type::Kind::Float.into()),
                ..Default::default()
            },
            ArrowDataType::Float64 => proto::Type {
                kind: Some(proto::r#type::Kind::Double.into()),
                ..Default::default()
            },
            ArrowDataType::Int8 => proto::Type {
                kind: Some(proto::r#type::Kind::Byte.into()),
                ..Default::default()
            },
            ArrowDataType::Int16 => proto::Type {
                kind: Some(proto::r#type::Kind::Short.into()),
                ..Default::default()
            },
            ArrowDataType::Int32 => proto::Type {
                kind: Some(proto::r#type::Kind::Int.into()),
                ..Default::default()
            },
            ArrowDataType::Int64 => proto::Type {
                kind: Some(proto::r#type::Kind::Long.into()),
                ..Default::default()
            },
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => proto::Type {
                kind: Some(proto::r#type::Kind::String.into()),
                ..Default::default()
            },
            ArrowDataType::Binary | ArrowDataType::LargeBinary => proto::Type {
                kind: Some(proto::r#type::Kind::Binary.into()),
                ..Default::default()
            },
            ArrowDataType::Boolean => proto::Type {
                kind: Some(proto::r#type::Kind::Boolean.into()),
                ..Default::default()
            },
            // TODO: support more types
            _ => unimplemented!("unsupported datatype"),
        };
        types.push(t);
    }
    types
}

fn serialize_footer(stripes: &[StripeInformation], schema: &SchemaRef) -> proto::Footer {
    let body_length = stripes
        .iter()
        .map(|s| s.index_length + s.data_length + s.footer_length)
        .sum::<u64>();
    let number_of_rows = stripes.iter().map(|s| s.row_count as u64).sum::<u64>();
    let stripes = stripes.iter().map(From::from).collect();
    let types = serialize_schema(schema);
    proto::Footer {
        header_length: Some(3),
        content_length: Some(body_length + 3),
        stripes,
        types,
        metadata: vec![],
        number_of_rows: Some(number_of_rows),
        statistics: vec![],
        row_index_stride: None,
        writer: Some(u32::MAX),
        encryption: None,
        calendar: None,
        software_version: None,
    }
}

fn serialize_postscript(
    footer_length: u64,
    compression: Compression,
    compression_block_size: usize,
) -> proto::PostScript {
    let kind = compression.kind();
    // The ORC spec says `compressionBlockSize` is "the block size used
    // for compression" and is recorded only when the codec is not
    // NONE. The Java reader tolerates the field being present with
    // any value when CompressionKind is NONE, but matching Java's
    // writer (which omits it when uncompressed) keeps byte-for-byte
    // backward compatibility with pre-compression-feature output.
    let compression_block_size = if compression.is_active() {
        Some(compression_block_size as u64)
    } else {
        None
    };
    proto::PostScript {
        footer_length: Some(footer_length),
        compression: Some(kind.into()),
        compression_block_size,
        version: vec![0, 12],
        metadata_length: Some(0),       // TODO: statistics
        writer_version: Some(u32::MAX), // TODO: check which version to use
        stripe_statistics_length: None,
        magic: Some("ORC".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
            Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatchReader,
            StringArray,
        },
        buffer::NullBuffer,
        compute::concat_batches,
        datatypes::{DataType as ArrowDataType, Field, Schema},
    };
    use bytes::Bytes;

    use crate::{stripe::Stripe, ArrowReaderBuilder};

    use super::*;

    fn roundtrip(batches: &[RecordBatch]) -> Vec<RecordBatch> {
        let mut f = vec![];
        let mut writer = ArrowWriterBuilder::new(&mut f, batches[0].schema())
            .try_build()
            .unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();

        let f = Bytes::from(f);
        let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
        reader.collect::<Result<Vec<_>, _>>().unwrap()
    }

    #[test]
    fn test_roundtrip_write() {
        let f32_array = Arc::new(Float32Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
        let f64_array = Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
        let int8_array = Arc::new(Int8Array::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let int16_array = Arc::new(Int16Array::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let int32_array = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let int64_array = Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let utf8_array = Arc::new(StringArray::from(vec![
            "Hello",
            "there",
            "楡井希実",
            "💯",
            "ORC",
            "",
            "123",
        ]));
        let binary_array = Arc::new(BinaryArray::from(vec![
            "Hello".as_bytes(),
            "there".as_bytes(),
            "楡井希実".as_bytes(),
            "💯".as_bytes(),
            "ORC".as_bytes(),
            "".as_bytes(),
            "123".as_bytes(),
        ]));
        let boolean_array = Arc::new(BooleanArray::from(vec![
            true, false, true, false, true, true, false,
        ]));
        let schema = Schema::new(vec![
            Field::new("f32", ArrowDataType::Float32, false),
            Field::new("f64", ArrowDataType::Float64, false),
            Field::new("int8", ArrowDataType::Int8, false),
            Field::new("int16", ArrowDataType::Int16, false),
            Field::new("int32", ArrowDataType::Int32, false),
            Field::new("int64", ArrowDataType::Int64, false),
            Field::new("utf8", ArrowDataType::Utf8, false),
            Field::new("binary", ArrowDataType::Binary, false),
            Field::new("boolean", ArrowDataType::Boolean, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                f32_array,
                f64_array,
                int8_array,
                int16_array,
                int32_array,
                int64_array,
                utf8_array,
                binary_array,
                boolean_array,
            ],
        )
        .unwrap();

        let rows = roundtrip(std::slice::from_ref(&batch));
        assert_eq!(batch, rows[0]);
    }

    #[test]
    fn test_roundtrip_write_large_type() {
        let large_utf8_array = Arc::new(LargeStringArray::from(vec![
            "Hello",
            "there",
            "楡井希実",
            "💯",
            "ORC",
            "",
            "123",
        ]));
        let large_binary_array = Arc::new(LargeBinaryArray::from(vec![
            "Hello".as_bytes(),
            "there".as_bytes(),
            "楡井希実".as_bytes(),
            "💯".as_bytes(),
            "ORC".as_bytes(),
            "".as_bytes(),
            "123".as_bytes(),
        ]));
        let schema = Schema::new(vec![
            Field::new("large_utf8", ArrowDataType::LargeUtf8, false),
            Field::new("large_binary", ArrowDataType::LargeBinary, false),
        ]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![large_utf8_array, large_binary_array])
                .unwrap();

        let rows = roundtrip(&[batch]);

        // Currently we read all String/Binary columns from ORC as plain StringArray/BinaryArray
        let utf8_array = Arc::new(StringArray::from(vec![
            "Hello",
            "there",
            "楡井希実",
            "💯",
            "ORC",
            "",
            "123",
        ]));
        let binary_array = Arc::new(BinaryArray::from(vec![
            "Hello".as_bytes(),
            "there".as_bytes(),
            "楡井希実".as_bytes(),
            "💯".as_bytes(),
            "ORC".as_bytes(),
            "".as_bytes(),
            "123".as_bytes(),
        ]));
        let schema = Schema::new(vec![
            Field::new("large_utf8", ArrowDataType::Utf8, false),
            Field::new("large_binary", ArrowDataType::Binary, false),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![utf8_array, binary_array]).unwrap();
        assert_eq!(batch, rows[0]);
    }

    #[test]
    fn test_write_small_stripes() {
        // Set small stripe size to ensure writing across multiple stripes works
        let data: Vec<i64> = (0..1_000_000).collect();
        let int64_array = Arc::new(Int64Array::from(data));
        let schema = Schema::new(vec![Field::new("int64", ArrowDataType::Int64, true)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![int64_array]).unwrap();

        let mut f = vec![];
        let mut writer = ArrowWriterBuilder::new(&mut f, batch.schema())
            .with_stripe_byte_size(256)
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let f = Bytes::from(f);
        let reader = ArrowReaderBuilder::try_new(f).unwrap().build();
        let schema = reader.schema();
        // Current reader doesn't read a batch across stripe boundaries, so we expect
        // more than one batch to prove multiple stripes are being written here
        let rows = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert!(
            rows.len() > 1,
            "must have written more than 1 stripe (each stripe read as separate recordbatch)"
        );
        let actual = concat_batches(&schema, rows.iter()).unwrap();
        assert_eq!(batch, actual);
    }

    #[test]
    fn test_write_inconsistent_null_buffers() {
        // When writing arrays where null buffer can appear/disappear between writes
        let schema = Arc::new(Schema::new(vec![Field::new(
            "int64",
            ArrowDataType::Int64,
            true,
        )]));

        // Ensure first batch has array with no null buffer
        let array_no_nulls = Arc::new(Int64Array::from(vec![1, 2, 3]));
        assert!(array_no_nulls.nulls().is_none());
        // But subsequent batch has array with null buffer
        let array_with_nulls = Arc::new(Int64Array::from(vec![None, Some(4), None]));
        assert!(array_with_nulls.nulls().is_some());

        let batch1 = RecordBatch::try_new(schema.clone(), vec![array_no_nulls]).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), vec![array_with_nulls]).unwrap();

        // ORC writer should be able to handle this gracefully
        let expected_array = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(4),
            None,
        ]));
        let expected_batch = RecordBatch::try_new(schema, vec![expected_array]).unwrap();

        let rows = roundtrip(&[batch1, batch2]);
        assert_eq!(expected_batch, rows[0]);
    }

    #[test]
    fn test_empty_null_buffers() {
        // Create an ORC file with present streams, but which have no nulls.
        // When this file is read then the resulting Arrow arrays show have
        // NO null buffer, even though there is a present stream.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "int64",
            ArrowDataType::Int64,
            true,
        )]));

        // Array with null buffer but has no nulls
        let array_empty_nulls = Arc::new(Int64Array::from_iter_values_with_nulls(
            vec![1],
            Some(NullBuffer::from_iter(vec![true])),
        ));
        assert!(array_empty_nulls.nulls().is_some());
        assert!(array_empty_nulls.null_count() == 0);

        let batch = RecordBatch::try_new(schema, vec![array_empty_nulls]).unwrap();

        // Encoding to bytes
        let mut f = vec![];
        let mut writer = ArrowWriterBuilder::new(&mut f, batch.schema())
            .try_build()
            .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut f = Bytes::from(f);
        let builder = ArrowReaderBuilder::try_new(f.clone()).unwrap();

        // Ensure the ORC file we wrote indeed has a present stream
        let stripe = Stripe::new(
            &mut f,
            &builder.file_metadata,
            builder.file_metadata().root_data_type(),
            &builder.file_metadata().stripe_metadatas()[0],
        )
        .unwrap();
        assert_eq!(stripe.columns().len(), 1);
        // Make sure we're getting the right column
        assert_eq!(stripe.columns()[0].name(), "int64");
        // Then check present stream
        let present_stream = stripe
            .stream_map()
            .get_opt(&stripe.columns()[0], proto::stream::Kind::Present);
        assert!(present_stream.is_some());

        // Decoding from bytes
        let reader = builder.build();
        let rows = reader.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].num_columns(), 1);
        // Ensure read array has no null buffer
        assert!(rows[0].column(0).nulls().is_none());
    }
}
