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
    compression::{CompressionType, Compressor},
    error::{IoSnafu, Result, UnexpectedSnafu},
    memory::EstimateMemory,
    proto,
    writer::stripe::{StripeInformation, StripeWriter},
};

/// Construct an [`ArrowWriter`] to encode [`RecordBatch`]es into a single
/// ORC file.
pub struct ArrowWriterBuilder<W> {
    writer: W,
    schema: SchemaRef,
    batch_size: usize,
    stripe_byte_size: usize,
    compression: Option<CompressionType>,
    compression_block_size: Option<usize>,
}

impl<W: Write> ArrowWriterBuilder<W> {
    /// Create a new [`ArrowWriterBuilder`], which will write an ORC file to
    /// the provided writer, with the expected Arrow schema.
    pub fn new(writer: W, schema: SchemaRef) -> Self {
        Self {
            writer,
            schema,
            batch_size: 1024,
            // 64 MiB
            stripe_byte_size: 64 * 1024 * 1024,
            compression: None,
            compression_block_size: None,
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

    /// Set the compression codec for this ORC file.
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Set the compression block size. Default is 256 KiB.
    pub fn with_compression_block_size(mut self, block_size: usize) -> Self {
        self.compression_block_size = Some(block_size);
        self
    }

    /// Construct an [`ArrowWriter`] ready to encode [`RecordBatch`]es into
    /// an ORC file.
    pub fn try_build(mut self) -> Result<ArrowWriter<W>> {
        let timezone = self
            .schema
            .fields()
            .iter()
            .any(|field| matches!(field.data_type(), ArrowDataType::Timestamp(_, None)))
            .then(|| "UTC".to_owned());
        let compressor = Compressor::new(self.compression, self.compression_block_size)?;
        // Required magic "ORC" bytes at start of file
        self.writer.write_all(b"ORC").context(IoSnafu)?;
        let writer = StripeWriter::new(self.writer, &self.schema, compressor, timezone);
        Ok(ArrowWriter {
            writer,
            schema: self.schema,
            batch_size: self.batch_size,
            stripe_byte_size: self.stripe_byte_size,
            written_stripes: vec![],
            // Accounting for the 3 magic bytes above
            total_bytes_written: 3,
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
        let footer = serialize_footer(&self.written_stripes, &self.schema).encode_to_vec();
        let metadata = proto::Metadata::default().encode_to_vec();

        let (mut writer, mut compressor) = self.writer.finish();
        let metadata = compressor.compress(&metadata)?;
        let footer = compressor.compress(&footer)?;

        let postscript = serialize_postscript(
            footer.len() as u64,
            metadata.len() as u64,
            compressor.compression(),
            compressor.block_size().map(|size| size as u64),
        )
        .encode_to_vec();
        let postscript_len = postscript.len() as u8;

        writer.write_all(&metadata).context(IoSnafu)?;
        writer.write_all(&footer).context(IoSnafu)?;
        writer.write_all(&postscript).context(IoSnafu)?;
        // Postscript length as last byte
        writer.write_all(&[postscript_len]).context(IoSnafu)?;

        // TODO: return file metadata
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
            ArrowDataType::Date32 => proto::Type {
                kind: Some(proto::r#type::Kind::Date.into()),
                ..Default::default()
            },
            ArrowDataType::Timestamp(_, None) => proto::Type {
                kind: Some(proto::r#type::Kind::Timestamp.into()),
                ..Default::default()
            },
            ArrowDataType::Timestamp(_, Some(timezone)) if timezone.as_ref() == "UTC" => {
                proto::Type {
                    kind: Some(proto::r#type::Kind::TimestampInstant.into()),
                    ..Default::default()
                }
            }
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
    metadata_length: u64,
    compression: Option<CompressionType>,
    compression_block_size: Option<u64>,
) -> proto::PostScript {
    proto::PostScript {
        footer_length: Some(footer_length),
        compression: Some(
            compression
                .map(CompressionType::to_proto)
                .unwrap_or(proto::CompressionKind::None)
                .into(),
        ),
        compression_block_size,
        version: vec![0, 12],
        metadata_length: Some(metadata_length),
        writer_version: Some(u32::MAX), // TODO: check which version to use
        stripe_statistics_length: None,
        magic: Some("ORC".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Read, sync::Arc};

    use arrow::{
        array::{
            Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array,
            Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
            RecordBatchReader, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray,
        },
        buffer::NullBuffer,
        compute::concat_batches,
        datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit},
    };
    use bytes::Bytes;
    use prost::Message;

    use crate::{
        compression::{CompressionType, Decompressor},
        schema::DataType as OrcDataType,
        stripe::Stripe,
        ArrowReaderBuilder,
    };

    use super::*;

    fn encode(batches: &[RecordBatch]) -> Bytes {
        let mut f = vec![];
        let mut writer = ArrowWriterBuilder::new(&mut f, batches[0].schema())
            .try_build()
            .unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();

        Bytes::from(f)
    }

    fn roundtrip(batches: &[RecordBatch]) -> Vec<RecordBatch> {
        let f = encode(batches);
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
    fn test_roundtrip_write_date32() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "date",
            ArrowDataType::Date32,
            true,
        )]));

        let batch_without_nulls = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![-1, 0, 1, 11_016]))],
        )
        .unwrap();
        let batch_with_nulls = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![
                Some(18_321),
                None,
                Some(19_782),
            ]))],
        )
        .unwrap();

        let file = encode(&[batch_without_nulls, batch_with_nulls]);
        let builder = ArrowReaderBuilder::try_new(file).unwrap();
        assert!(matches!(
            builder.file_metadata().root_data_type().children()[0].data_type(),
            OrcDataType::Date { .. }
        ));

        let rows = builder.build().collect::<Result<Vec<_>, _>>().unwrap();
        let actual = concat_batches(&schema, rows.iter()).unwrap();
        let expected = RecordBatch::try_new(
            schema,
            vec![Arc::new(Date32Array::from(vec![
                Some(-1),
                Some(0),
                Some(1),
                Some(11_016),
                Some(18_321),
                None,
                Some(19_782),
            ]))],
        )
        .unwrap();

        assert_eq!(expected, actual);
    }

    #[test]
    fn test_roundtrip_write_timestamps() {
        let second = vec![Some(-2), Some(-1), Some(0), None, Some(1), Some(2)];
        let millisecond = vec![
            Some(-1_001),
            Some(-1_000),
            Some(-1_999),
            None,
            Some(1_001),
            Some(1_234),
        ];
        let microsecond = vec![
            Some(-1_001_001),
            Some(-1_000_000),
            Some(-999_999),
            None,
            Some(1_001_001),
            Some(1_234_567),
        ];
        let nanosecond = vec![
            Some(-1_001_001_001),
            Some(-1_000_000_000),
            Some(-999_999_999),
            None,
            Some(1_001_001_001),
            Some(1_234_567_890),
        ];

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(TimestampSecondArray::from(second.clone())),
            Arc::new(TimestampMillisecondArray::from(millisecond.clone())),
            Arc::new(TimestampMicrosecondArray::from(microsecond.clone())),
            Arc::new(TimestampNanosecondArray::from(nanosecond.clone())),
            Arc::new(TimestampSecondArray::from(second).with_timezone("UTC")),
            Arc::new(TimestampMillisecondArray::from(millisecond).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(microsecond).with_timezone("UTC")),
            Arc::new(TimestampNanosecondArray::from(nanosecond).with_timezone("UTC")),
        ];
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(index, array)| {
                    Field::new(
                        format!("timestamp_{index}"),
                        array.data_type().clone(),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

        let mut file = Vec::new();
        let mut writer = ArrowWriterBuilder::new(&mut file, schema.clone())
            .try_build()
            .unwrap();
        writer.write(&batch.slice(0, 3)).unwrap();
        writer.flush_stripe().unwrap();
        writer.write(&batch.slice(3, 3)).unwrap();
        writer.close().unwrap();

        let file = Bytes::from(file);
        let builder = ArrowReaderBuilder::try_new(file.clone()).unwrap();
        let children = builder.file_metadata().root_data_type().children();
        for child in &children[..4] {
            assert!(matches!(child.data_type(), OrcDataType::Timestamp { .. }));
        }
        for child in &children[4..] {
            assert!(matches!(
                child.data_type(),
                OrcDataType::TimestampWithLocalTimezone { .. }
            ));
        }

        let stripes = builder.file_metadata().stripe_metadatas();
        assert_eq!(stripes.len(), 2);
        for stripe in stripes {
            let footer_start = stripe.footer_offset() as usize;
            let footer_end = footer_start + stripe.footer_length() as usize;
            let mut decoded_footer = Vec::new();
            Decompressor::new(
                file.slice(footer_start..footer_end),
                builder.file_metadata().compression(),
                Vec::new(),
            )
            .read_to_end(&mut decoded_footer)
            .unwrap();
            let footer = proto::StripeFooter::decode(decoded_footer.as_slice()).unwrap();
            assert_eq!(footer.writer_timezone.as_deref(), Some("UTC"));
            assert_eq!(
                footer
                    .streams
                    .iter()
                    .filter(|stream| stream.kind() == proto::stream::Kind::Secondary)
                    .count(),
                8
            );
        }

        let rows = builder
            .with_schema(schema.clone())
            .build()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let actual = concat_batches(&schema, rows.iter()).unwrap();
        assert_eq!(batch, actual);
    }

    #[test]
    fn test_write_timestamps_with_inconsistent_null_buffers() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));

        let without_nulls = Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3]));
        let with_nulls = Arc::new(TimestampNanosecondArray::from(vec![None, Some(4), None]));
        assert!(without_nulls.nulls().is_none());
        assert!(with_nulls.nulls().is_some());

        let batch1 = RecordBatch::try_new(schema.clone(), vec![without_nulls]).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), vec![with_nulls]).unwrap();
        let expected = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(4),
                None,
            ]))],
        )
        .unwrap();

        let rows = roundtrip(&[batch1, batch2]);
        assert_eq!(expected, rows[0]);
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

    fn write_file(
        schema: SchemaRef,
        batch: Option<&RecordBatch>,
        compression: Option<CompressionType>,
        block_size: Option<usize>,
        stripe_byte_size: usize,
    ) -> Bytes {
        let mut file = Vec::new();
        let mut builder =
            ArrowWriterBuilder::new(&mut file, schema).with_stripe_byte_size(stripe_byte_size);
        if let Some(compression) = compression {
            builder = builder.with_compression(compression);
        }
        if let Some(block_size) = block_size {
            builder = builder.with_compression_block_size(block_size);
        }
        let mut writer = builder.try_build().unwrap();
        if let Some(batch) = batch {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
        Bytes::from(file)
    }

    fn decode_postscript(file: &Bytes) -> proto::PostScript {
        let postscript_len = file[file.len() - 1] as usize;
        let postscript_start = file.len() - 1 - postscript_len;
        proto::PostScript::decode(&file[postscript_start..file.len() - 1]).unwrap()
    }

    #[test]
    fn test_writer_compression_modes_and_postscript() {
        let values = (0..4096)
            .map(|index| format!("compressible-value-{}", index % 8))
            .collect::<Vec<_>>();
        let array = Arc::new(StringArray::from(values));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            ArrowDataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        for (compression, expected_kind, expected_block_size) in [
            (None, proto::CompressionKind::None, None),
            (
                Some(CompressionType::Zlib),
                proto::CompressionKind::Zlib,
                Some(64),
            ),
            (
                Some(CompressionType::Snappy),
                proto::CompressionKind::Snappy,
                Some(64),
            ),
            (
                Some(CompressionType::Lz4),
                proto::CompressionKind::Lz4,
                Some(64),
            ),
            (
                Some(CompressionType::Zstd),
                proto::CompressionKind::Zstd,
                Some(64),
            ),
        ] {
            let block_size = compression.map(|_| 64);
            let file = write_file(
                schema.clone(),
                Some(&batch),
                compression,
                block_size,
                usize::MAX,
            );
            let postscript = decode_postscript(&file);
            assert_eq!(postscript.compression(), expected_kind);
            assert_eq!(postscript.compression_block_size, expected_block_size);
            assert_eq!(postscript.metadata_length, Some(0));
            assert!(postscript.footer_length.unwrap() > 0);

            let reader = ArrowReaderBuilder::try_new(file).unwrap().build();
            let rows = reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(rows, vec![batch.clone()]);
        }

        let file = write_file(
            schema,
            None,
            Some(CompressionType::Zstd),
            Some(64),
            usize::MAX,
        );
        let postscript = decode_postscript(&file);
        assert_eq!(postscript.compression(), proto::CompressionKind::Zstd);
        assert_eq!(postscript.compression_block_size, Some(64));
        assert_eq!(postscript.metadata_length, Some(0));

        let reader = ArrowReaderBuilder::try_new(file).unwrap();
        assert_eq!(reader.file_metadata().number_of_rows(), 0);
        assert!(reader.file_metadata().stripe_metadatas().is_empty());
        assert!(reader
            .build()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_writer_rejects_invalid_compression_configuration() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            ArrowDataType::Int64,
            false,
        )]));

        for compression in [None, Some(CompressionType::Zstd)] {
            for block_size in [0, 1 << 23] {
                let mut file = Vec::new();
                let mut builder = ArrowWriterBuilder::new(&mut file, schema.clone())
                    .with_compression_block_size(block_size);
                if let Some(compression) = compression {
                    builder = builder.with_compression(compression);
                }
                assert!(builder.try_build().is_err());
                assert!(file.is_empty());
            }
        }

        let mut file = Vec::new();
        let result = ArrowWriterBuilder::new(&mut file, schema)
            .with_compression(CompressionType::Lzo)
            .try_build();
        assert!(result.is_err());
        assert!(file.is_empty());
    }

    #[test]
    fn test_compressed_multi_stripe_physical_offsets() {
        let data = (0..100_000).collect::<Vec<i64>>();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(data))]).unwrap();
        let file = write_file(
            batch.schema(),
            Some(&batch),
            Some(CompressionType::Zstd),
            Some(64),
            256,
        );

        let builder = ArrowReaderBuilder::try_new(file.clone()).unwrap();
        let stripes = builder.file_metadata().stripe_metadatas();
        assert!(stripes.len() > 1);
        assert_eq!(stripes[0].offset(), 3);
        for pair in stripes.windows(2) {
            let previous = &pair[0];
            let next = &pair[1];
            assert_eq!(
                next.offset(),
                previous.offset()
                    + previous.index_length()
                    + previous.data_length()
                    + previous.footer_length()
            );
        }

        for stripe in stripes {
            let footer_start = stripe.footer_offset() as usize;
            let footer_end = footer_start + stripe.footer_length() as usize;
            let mut decoded_footer = Vec::new();
            Decompressor::new(
                file.slice(footer_start..footer_end),
                builder.file_metadata().compression(),
                Vec::new(),
            )
            .read_to_end(&mut decoded_footer)
            .unwrap();
            let footer = proto::StripeFooter::decode(decoded_footer.as_slice()).unwrap();
            let stream_bytes = footer.streams.iter().map(|s| s.length()).sum::<u64>();
            assert_eq!(stream_bytes, stripe.data_length());
            assert_eq!(stripe.index_length(), 0);
        }

        let reader = builder.build();
        let rows = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let actual = concat_batches(&batch.schema(), rows.iter()).unwrap();
        assert_eq!(actual, batch);
    }
}
