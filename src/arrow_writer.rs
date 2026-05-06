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
    compression::{compress_stream, WriterCompression, DEFAULT_COMPRESSION_BLOCK_SIZE},
    error::{IoSnafu, Result, UnexpectedSnafu},
    memory::EstimateMemory,
    proto,
    writer::index::{root_column_statistics, ColumnStatsBuilder},
    writer::stripe::{StripeInformation, StripeWriter},
};

/// Construct an [`ArrowWriter`] to encode [`RecordBatch`]es into a single
/// ORC file.
pub struct ArrowWriterBuilder<W> {
    writer: W,
    schema: SchemaRef,
    batch_size: usize,
    stripe_byte_size: usize,
    compression: WriterCompression,
    compression_block_size: usize,
    row_index_stride: Option<usize>,
    bloom_filters: bool,
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
            compression: WriterCompression::None,
            compression_block_size: DEFAULT_COMPRESSION_BLOCK_SIZE as usize,
            row_index_stride: None,
            bloom_filters: false,
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

    /// Compress ORC streams and metadata with the provided writer codec.
    pub fn with_compression(mut self, compression: WriterCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Enable ORC `ZLIB` compression.
    ///
    /// ORC does not have a separate protobuf value for `GZIP`; common "gzip"
    /// writer options map to ORC `ZLIB`, so this is a naming convenience.
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

    /// Construct an [`ArrowWriter`] ready to encode [`RecordBatch`]es into
    /// an ORC file.
    pub fn try_build(mut self) -> Result<ArrowWriter<W>> {
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
        serialize_schema(&self.schema)?;

        // Required magic "ORC" bytes at start of file
        self.writer.write_all(b"ORC").context(IoSnafu)?;
        let writer = StripeWriter::new(
            self.writer,
            &self.schema,
            self.compression,
            self.compression_block_size,
            self.row_index_stride,
            self.bloom_filters,
        );
        let file_stats_builders = schema_field_stats_builders(&self.schema);
        Ok(ArrowWriter {
            writer,
            schema: self.schema,
            batch_size: self.batch_size,
            stripe_byte_size: self.stripe_byte_size,
            compression: self.compression,
            compression_block_size: self.compression_block_size,
            row_index_stride: self.row_index_stride,
            file_stats_builders,
            total_rows_written: 0,
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
    compression: WriterCompression,
    compression_block_size: usize,
    row_index_stride: Option<usize>,
    file_stats_builders: Vec<ColumnStatsBuilder>,
    total_rows_written: u64,
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

        self.total_rows_written += batch.num_rows() as u64;
        for ((array, field), stats_builder) in batch
            .columns()
            .iter()
            .zip(self.schema.fields().iter())
            .zip(self.file_stats_builders.iter_mut())
        {
            stats_builder.update_array(field.data_type(), array);
        }

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
    pub fn close(self) -> Result<()> {
        self.finish().map(|_| ())
    }

    /// Flush buffered data, write file tail metadata, and return the inner writer.
    pub fn finish(mut self) -> Result<W> {
        // Flush in-progress stripe
        if self.writer.row_count > 0 {
            self.flush_stripe()?;
        }

        let metadata = serialize_metadata(&self.written_stripes);
        let metadata = metadata.encode_to_vec();
        let metadata = compress_stream(
            bytes::Bytes::from(metadata),
            self.compression,
            self.compression_block_size,
        )?;
        let metadata_length = metadata.len() as u64;

        let file_statistics = self.file_column_statistics();
        let footer = serialize_footer(
            &self.written_stripes,
            &self.schema,
            self.row_index_stride,
            file_statistics,
        )?;
        let footer = footer.encode_to_vec();
        let footer = compress_stream(
            bytes::Bytes::from(footer),
            self.compression,
            self.compression_block_size,
        )?;
        let postscript = serialize_postscript(
            footer.len() as u64,
            metadata_length,
            self.compression,
            self.compression_block_size,
        );
        let postscript = postscript.encode_to_vec();
        let postscript_len = postscript.len() as u8;

        let mut writer = self.writer.finish();
        writer.write_all(&metadata).context(IoSnafu)?;
        writer.write_all(&footer).context(IoSnafu)?;
        writer.write_all(&postscript).context(IoSnafu)?;
        // Postscript length as last byte
        writer.write_all(&[postscript_len]).context(IoSnafu)?;

        Ok(writer)
    }

    fn file_column_statistics(&self) -> Vec<proto::ColumnStatistics> {
        let mut statistics = Vec::with_capacity(self.file_stats_builders.len() + 1);
        statistics.push(root_column_statistics(self.total_rows_written));
        statistics.extend(
            self.schema
                .fields()
                .iter()
                .zip(self.file_stats_builders.iter())
                .map(|(field, builder)| builder.finish(field.data_type())),
        );
        statistics
    }
}

fn schema_field_stats_builders(schema: &SchemaRef) -> Vec<ColumnStatsBuilder> {
    schema
        .fields()
        .iter()
        .map(|field| ColumnStatsBuilder::new(field.data_type()))
        .collect()
}

fn serialize_schema(schema: &SchemaRef) -> Result<Vec<proto::Type>> {
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
            ArrowDataType::Decimal128(precision, scale) => {
                ensure!(
                    *scale >= 0,
                    UnexpectedSnafu {
                        msg: "negative decimal scales are not supported by the ORC writer"
                    }
                );
                proto::Type {
                    kind: Some(proto::r#type::Kind::Decimal.into()),
                    precision: Some(*precision as u32),
                    scale: Some(*scale as u32),
                    ..Default::default()
                }
            }
            ArrowDataType::Date32 => proto::Type {
                kind: Some(proto::r#type::Kind::Date.into()),
                ..Default::default()
            },
            ArrowDataType::Timestamp(_, None) => proto::Type {
                kind: Some(proto::r#type::Kind::Timestamp.into()),
                ..Default::default()
            },
            ArrowDataType::Timestamp(_, Some(tz)) if tz.as_ref() == "UTC" => proto::Type {
                kind: Some(proto::r#type::Kind::TimestampInstant.into()),
                ..Default::default()
            },
            ArrowDataType::Timestamp(_, Some(_)) => {
                ensure!(
                    false,
                    UnexpectedSnafu {
                        msg: "only UTC timestamp timezones are supported by the ORC writer"
                    }
                );
                unreachable!()
            }
            // TODO: support more types
            _ => unimplemented!("unsupported datatype"),
        };
        types.push(t);
    }
    Ok(types)
}

fn serialize_footer(
    stripes: &[StripeInformation],
    schema: &SchemaRef,
    row_index_stride: Option<usize>,
    statistics: Vec<proto::ColumnStatistics>,
) -> Result<proto::Footer> {
    let body_length = stripes
        .iter()
        .map(|s| s.index_length + s.data_length + s.footer_length)
        .sum::<u64>();
    let number_of_rows = stripes.iter().map(|s| s.row_count as u64).sum::<u64>();
    let stripes = stripes.iter().map(From::from).collect();
    let types = serialize_schema(schema)?;
    Ok(proto::Footer {
        header_length: Some(3),
        content_length: Some(body_length + 3),
        stripes,
        types,
        metadata: vec![],
        number_of_rows: Some(number_of_rows),
        statistics,
        row_index_stride: row_index_stride.map(|stride| stride as u32),
        writer: Some(u32::MAX),
        encryption: None,
        calendar: None,
        software_version: None,
    })
}

fn serialize_metadata(stripes: &[StripeInformation]) -> proto::Metadata {
    proto::Metadata {
        stripe_stats: stripes
            .iter()
            .map(|stripe| proto::StripeStatistics {
                col_stats: stripe.column_statistics.clone(),
            })
            .collect(),
    }
}

fn serialize_postscript(
    footer_length: u64,
    metadata_length: u64,
    compression: WriterCompression,
    compression_block_size: usize,
) -> proto::PostScript {
    proto::PostScript {
        footer_length: Some(footer_length),
        compression: Some(compression.to_proto().into()),
        compression_block_size: (!compression.is_none()).then_some(compression_block_size as u64),
        version: vec![0, 12],
        metadata_length: Some(metadata_length),
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
            Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
            Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
            LargeStringArray, RecordBatchReader, StringArray, TimestampNanosecondArray,
        },
        buffer::NullBuffer,
        compute::concat_batches,
        datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit},
    };
    use bytes::Bytes;

    use crate::{statistics::TypeStatistics, stripe::Stripe, ArrowReaderBuilder};

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

    fn write_to_bytes(
        batch: &RecordBatch,
        gzip_compression: bool,
        row_index_stride: Option<usize>,
        bloom_filters: bool,
    ) -> Bytes {
        let mut f = vec![];
        let mut builder = ArrowWriterBuilder::new(&mut f, batch.schema());
        if gzip_compression {
            builder = builder.with_gzip_compression();
        }
        if let Some(row_index_stride) = row_index_stride {
            builder = builder.with_row_index_stride(row_index_stride);
        }
        if bloom_filters {
            builder = builder.with_bloom_filters();
        }
        let mut writer = builder.try_build().unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(f)
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
    fn test_roundtrip_write_gzip_compression() {
        let array = Arc::new(Int64Array::from((0..1024).collect::<Vec<_>>()));
        let schema = Schema::new(vec![Field::new("int64", ArrowDataType::Int64, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();

        let f = write_to_bytes(&batch, true, None, false);
        let builder = ArrowReaderBuilder::try_new(f).unwrap();
        assert!(builder.file_metadata().compression().is_some());

        let rows = builder.build().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batch, rows[0]);
    }

    #[test]
    fn test_write_row_indexes() {
        let array = Arc::new(Int64Array::from((0..12).collect::<Vec<_>>()));
        let schema = Schema::new(vec![Field::new("int64", ArrowDataType::Int64, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();

        let mut f = write_to_bytes(&batch, false, Some(5), false);
        let builder = ArrowReaderBuilder::try_new(f.clone()).unwrap();
        assert_eq!(builder.file_metadata().row_index_stride(), Some(5));

        let stripe = Stripe::new(
            &mut f,
            builder.file_metadata(),
            builder.file_metadata().root_data_type(),
            &builder.file_metadata().stripe_metadatas()[0],
        )
        .unwrap();
        let row_index = stripe.read_row_indexes(builder.file_metadata()).unwrap();
        let column_index = row_index.column(1).unwrap();

        assert_eq!(column_index.num_row_groups(), 3);
        assert_eq!(row_index.total_rows(), 12);
        assert_eq!(row_index.rows_per_group(), 5);

        let stats = column_index.row_group_stats(0).unwrap();
        assert_eq!(stats.number_of_values(), 5);
        assert!(!stats.has_null());
        match stats.type_statistics().unwrap() {
            TypeStatistics::Integer { min, max, sum } => {
                assert_eq!((*min, *max, *sum), (0, 4, Some(10)));
            }
            other => panic!("expected integer stats, got {other:?}"),
        }
    }

    #[test]
    fn test_write_bloom_filters() {
        let array = Arc::new(StringArray::from(vec!["alpha", "beta", "gamma", "delta"]));
        let schema = Schema::new(vec![Field::new("name", ArrowDataType::Utf8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();

        let mut f = write_to_bytes(&batch, false, Some(2), true);
        let builder = ArrowReaderBuilder::try_new(f.clone()).unwrap();
        let stripe = Stripe::new(
            &mut f,
            builder.file_metadata(),
            builder.file_metadata().root_data_type(),
            &builder.file_metadata().stripe_metadatas()[0],
        )
        .unwrap();
        let row_index = stripe.read_row_indexes(builder.file_metadata()).unwrap();
        let column_index = row_index.column(1).unwrap();
        let first_group = column_index.entry(0).unwrap();
        let bloom = first_group.bloom_filter.as_ref().unwrap();

        assert!(bloom.might_contain(b"alpha"));
        assert!(!bloom.might_contain(b"definitely-not-present"));
    }

    #[test]
    fn test_write_file_and_stripe_statistics() {
        let array = Arc::new(Int64Array::from((0..12).collect::<Vec<_>>()));
        let schema = Schema::new(vec![Field::new("int64", ArrowDataType::Int64, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();

        let f = write_to_bytes(&batch, false, None, false);
        let builder = ArrowReaderBuilder::try_new(f).unwrap();
        let file_stats = builder.file_metadata().column_file_statistics();
        assert_eq!(file_stats.len(), 2);
        assert_eq!(file_stats[0].number_of_values(), 12);
        assert_eq!(file_stats[1].number_of_values(), 12);
        match file_stats[1].type_statistics().unwrap() {
            TypeStatistics::Integer { min, max, sum } => {
                assert_eq!((*min, *max, *sum), (0, 11, Some(66)));
            }
            other => panic!("expected integer stats, got {other:?}"),
        }

        let stripe_stats = builder.file_metadata().stripe_metadatas()[0].column_statistics();
        assert_eq!(stripe_stats.len(), 2);
        assert_eq!(stripe_stats[0].number_of_values(), 12);
        assert_eq!(stripe_stats[1].number_of_values(), 12);
    }

    #[test]
    fn test_roundtrip_write_decimal_date_timestamp() {
        let decimal_array = Arc::new(
            Decimal128Array::from(vec![Some(12345), None, Some(-678), Some(0)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let date_array = Arc::new(Date32Array::from(vec![
            Some(19_358),
            None,
            Some(0),
            Some(-1),
        ]));
        let timestamp_array = Arc::new(TimestampNanosecondArray::from(vec![
            Some(0),
            None,
            Some(1_672_531_200_123_456_789),
            Some(-1_000_000_000),
        ]));
        let timestamp_utc_array = Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(0),
                None,
                Some(1_672_531_200_000_000_000),
                Some(1_000_000_000),
            ])
            .with_timezone("UTC"),
        );
        let schema = Schema::new(vec![
            Field::new("decimal", ArrowDataType::Decimal128(10, 2), true),
            Field::new("date", ArrowDataType::Date32, true),
            Field::new(
                "timestamp",
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "timestamp_utc",
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                decimal_array,
                date_array,
                timestamp_array,
                timestamp_utc_array,
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
