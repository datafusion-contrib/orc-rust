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

use std::marker::PhantomData;

use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{
        ArrowPrimitiveType, ArrowTimestampType, ByteArrayType, Date32Type, Decimal128Type,
        Float32Type, Float64Type, GenericBinaryType, GenericStringType, Int16Type, Int32Type,
        Int64Type, Int8Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType,
    },
};
use bytes::{BufMut, BytesMut};
use snafu::ensure;

use crate::{
    encoding::{
        boolean::BooleanEncoder,
        byte::ByteRleEncoder,
        float::FloatEncoder,
        integer::{
            rle_v2::RleV2Encoder, write_varint_zigzagged, NInt, SignedEncoding, UnsignedEncoding,
        },
        PrimitiveValueEncoder,
    },
    error::{Result, UnexpectedSnafu},
    memory::EstimateMemory,
    writer::StreamType,
};

use super::{ColumnEncoding, Stream};

/// Encodes a specific column for a stripe. Will encode to an internal memory
/// buffer until it is finished, in which case it returns the stream bytes to
/// be serialized to a writer.
pub trait ColumnStripeEncoder: EstimateMemory {
    /// Encode entire provided [`ArrayRef`] to internal buffer.
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()>;

    /// Column encoding used for streams.
    fn column_encoding(&self) -> ColumnEncoding;

    /// Emit buffered streams to be written to the writer, and reset state
    /// in preparation for next stripe.
    fn finish(&mut self) -> Vec<Stream>;
}

// TODO: simplify these generics, probably overcomplicating things here

/// Encoder for primitive ORC types (e.g. int, float). Uses a specific [`PrimitiveValueEncoder`] to
/// encode the primitive values into internal memory. When finished, outputs a DATA stream and
/// optionally a PRESENT stream.
pub struct PrimitiveColumnEncoder<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> {
    encoder: E,
    column_encoding: ColumnEncoding,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<BooleanEncoder>,
    encoded_count: usize,
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> PrimitiveColumnEncoder<T, E> {
    // TODO: encode knowledge of the ColumnEncoding as part of the type, instead of requiring it
    //       to be passed at runtime
    pub fn new(column_encoding: ColumnEncoding) -> Self {
        Self {
            encoder: E::new(),
            column_encoding,
            present: None,
            encoded_count: 0,
            _phantom: Default::default(),
        }
    }
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> EstimateMemory
    for PrimitiveColumnEncoder<T, E>
{
    fn estimate_memory_size(&self) -> usize {
        self.encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> ColumnStripeEncoder
    for PrimitiveColumnEncoder<T, E>
{
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        // TODO: return as result instead of panicking here?
        let array = array.as_primitive::<T>();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.write_one(v);
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.write_one(v);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let values = array.values();
                self.encoder.write_slice(values);
                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        self.column_encoding
    }

    fn finish(&mut self) -> Vec<Stream> {
        let bytes = self.encoder.take_inner();
        // Return mandatory Data stream and optional Present stream
        let data = Stream {
            kind: StreamType::Data,
            bytes,
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    kind: StreamType::Present,
                    bytes,
                };
                vec![data, present]
            }
            None => vec![data],
        }
    }
}

pub struct BooleanColumnEncoder {
    encoder: BooleanEncoder,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<BooleanEncoder>,
    encoded_count: usize,
}

impl BooleanColumnEncoder {
    pub fn new() -> Self {
        Self {
            encoder: BooleanEncoder::new(),
            present: None,
            encoded_count: 0,
        }
    }
}

impl EstimateMemory for BooleanColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl ColumnStripeEncoder for BooleanColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        // TODO: return as result instead of panicking here?
        let array = array.as_boolean();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.extend_boolean(v);
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.extend_boolean(v);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let values = array.values();
                self.encoder.extend_bb(values);
                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::Direct
    }

    fn finish(&mut self) -> Vec<Stream> {
        let bytes = self.encoder.finish();
        // Return mandatory Data stream and optional Present stream
        let data = Stream {
            kind: StreamType::Data,
            bytes,
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    kind: StreamType::Present,
                    bytes,
                };
                vec![data, present]
            }
            None => vec![data],
        }
    }
}

/// Direct encodes binary/strings.
pub struct GenericBinaryColumnEncoder<T: ByteArrayType>
where
    T::Offset: NInt,
{
    string_bytes: BytesMut,
    length_encoder: RleV2Encoder<T::Offset, UnsignedEncoding>,
    present: Option<BooleanEncoder>,
    encoded_count: usize,
}

impl<T: ByteArrayType> GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    pub fn new() -> Self {
        Self {
            string_bytes: BytesMut::new(),
            length_encoder: RleV2Encoder::new(),
            present: None,
            encoded_count: 0,
        }
    }
}

impl<T: ByteArrayType> EstimateMemory for GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    fn estimate_memory_size(&self) -> usize {
        self.string_bytes.len()
            + self.length_encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl<T: ByteArrayType> ColumnStripeEncoder for GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        if array.is_empty() {
            return Ok(());
        }
        // TODO: return as result instead of panicking here?
        let array = array.as_bytes::<T>();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    self.length_encoder.write_one(array.value_length(index));
                    self.string_bytes.put_slice(array.value(index).as_ref());
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    self.length_encoder.write_one(array.value_length(index));
                    self.string_bytes.put_slice(array.value(index).as_ref());
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let offsets = array.offsets();
                let first_offset = offsets[0];

                let mut length_to_copy = <T::Offset as num::Zero>::zero();
                let mut prev_offset = first_offset;
                // Derive lengths from offsets then encode them as ints
                for &offset in offsets.iter().skip(1) {
                    let length = offset - prev_offset;
                    self.length_encoder.write_one(length);
                    length_to_copy += length;
                    prev_offset = offset;
                }
                // Copy all string bytes in a single go
                // TODO: this cast to i64 to usize can be cleaned up?
                let first_offset = first_offset.as_i64() as usize;
                let end_offset = first_offset + length_to_copy.as_i64() as usize;
                let string_bytes = &array.value_data()[first_offset..end_offset];
                self.string_bytes.put_slice(string_bytes);

                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        // TODO: throwing away allocations here
        let data_bytes = std::mem::take(&mut self.string_bytes);
        let length_bytes = self.length_encoder.take_inner();
        let data = Stream {
            kind: StreamType::Data,
            bytes: data_bytes.into(),
        };
        let length = Stream {
            kind: StreamType::Length,
            bytes: length_bytes,
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    kind: StreamType::Present,
                    bytes,
                };
                vec![data, length, present]
            }
            None => vec![data, length],
        }
    }
}

pub struct DecimalColumnEncoder {
    data: BytesMut,
    scale: RleV2Encoder<i32, SignedEncoding>,
    present: Option<BooleanEncoder>,
    encoded_count: usize,
    fixed_scale: i8,
}

impl DecimalColumnEncoder {
    pub fn new(fixed_scale: i8) -> Self {
        Self {
            data: BytesMut::new(),
            scale: RleV2Encoder::new(),
            present: None,
            encoded_count: 0,
            fixed_scale,
        }
    }

    fn write_value(&mut self, value: i128) {
        write_varint_zigzagged::<i128, SignedEncoding>(&mut self.data, value);
        self.scale.write_one(self.fixed_scale as i32);
    }
}

impl EstimateMemory for DecimalColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.data.len()
            + self.scale.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl ColumnStripeEncoder for DecimalColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_primitive::<Decimal128Type>();
        match (array.nulls(), &mut self.present) {
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    self.write_value(array.value(index));
                }
            }
            (Some(null_buffer), None) => {
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    self.write_value(array.value(index));
                }
            }
            (None, _) => {
                for value in array.values() {
                    self.write_value(*value);
                }
                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        let data = Stream {
            kind: StreamType::Data,
            bytes: std::mem::take(&mut self.data).into(),
        };
        let secondary = Stream {
            kind: StreamType::Secondary,
            bytes: self.scale.take_inner(),
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let present = Stream {
                    kind: StreamType::Present,
                    bytes: present.finish(),
                };
                vec![data, secondary, present]
            }
            None => vec![data, secondary],
        }
    }
}

pub struct TimestampColumnEncoder<T: ArrowTimestampType> {
    data: RleV2Encoder<i64, SignedEncoding>,
    secondary: RleV2Encoder<i64, UnsignedEncoding>,
    present: Option<BooleanEncoder>,
    encoded_count: usize,
    _phantom: PhantomData<T>,
}

impl<T: ArrowTimestampType> TimestampColumnEncoder<T> {
    pub fn new() -> Self {
        Self {
            data: RleV2Encoder::new(),
            secondary: RleV2Encoder::new(),
            present: None,
            encoded_count: 0,
            _phantom: PhantomData,
        }
    }

    fn write_value(&mut self, value: i64) -> Result<()> {
        let nanos_since_epoch = timestamp_value_to_nanos::<T>(value);
        let (seconds_since_base, encoded_nanos) = encode_timestamp(nanos_since_epoch)?;
        self.data.write_one(seconds_since_base);
        self.secondary.write_one(encoded_nanos);
        Ok(())
    }
}

impl<T: ArrowTimestampType> EstimateMemory for TimestampColumnEncoder<T> {
    fn estimate_memory_size(&self) -> usize {
        self.data.estimate_memory_size()
            + self.secondary.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl<T: ArrowTimestampType> ColumnStripeEncoder for TimestampColumnEncoder<T> {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_primitive::<T>();
        match (array.nulls(), &mut self.present) {
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    self.write_value(array.value(index))?;
                }
            }
            (Some(null_buffer), None) => {
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    self.write_value(array.value(index))?;
                }
            }
            (None, _) => {
                for value in array.values() {
                    self.write_value(*value)?;
                }
                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        let data = Stream {
            kind: StreamType::Data,
            bytes: self.data.take_inner(),
        };
        let secondary = Stream {
            kind: StreamType::Secondary,
            bytes: self.secondary.take_inner(),
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let present = Stream {
                    kind: StreamType::Present,
                    bytes: present.finish(),
                };
                vec![data, secondary, present]
            }
            None => vec![data, secondary],
        }
    }
}

const ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH: i128 = 1_420_070_400;
const NANOSECONDS_IN_SECOND: i128 = 1_000_000_000;

fn timestamp_value_to_nanos<T: ArrowTimestampType>(value: i64) -> i128 {
    match T::UNIT {
        TimeUnit::Second => (value as i128) * 1_000_000_000,
        TimeUnit::Millisecond => (value as i128) * 1_000_000,
        TimeUnit::Microsecond => (value as i128) * 1_000,
        TimeUnit::Nanosecond => value as i128,
    }
}

fn encode_timestamp(nanos_since_epoch: i128) -> Result<(i64, i64)> {
    let seconds_since_epoch = nanos_since_epoch.div_euclid(NANOSECONDS_IN_SECOND);
    let nanos = nanos_since_epoch.rem_euclid(NANOSECONDS_IN_SECOND) as u32;
    let seconds_since_base = seconds_since_epoch - ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH;
    ensure!(
        seconds_since_base >= i64::MIN as i128 && seconds_since_base <= i64::MAX as i128,
        UnexpectedSnafu {
            msg: "timestamp seconds are out of ORC writer range"
        }
    );
    Ok((seconds_since_base as i64, encode_timestamp_nanos(nanos)))
}

fn encode_timestamp_nanos(nanos: u32) -> i64 {
    if nanos == 0 {
        return 0;
    }

    let mut stripped = nanos;
    let mut zeros = 0;
    while zeros < 8 && stripped % 10 == 0 {
        stripped /= 10;
        zeros += 1;
    }

    if zeros > 1 {
        ((stripped as i64) << 3) | ((zeros - 1) as i64)
    } else {
        (nanos as i64) << 3
    }
}

pub type FloatColumnEncoder = PrimitiveColumnEncoder<Float32Type, FloatEncoder<f32>>;
pub type DoubleColumnEncoder = PrimitiveColumnEncoder<Float64Type, FloatEncoder<f64>>;
pub type ByteColumnEncoder = PrimitiveColumnEncoder<Int8Type, ByteRleEncoder>;
pub type Int16ColumnEncoder = PrimitiveColumnEncoder<Int16Type, RleV2Encoder<i16, SignedEncoding>>;
pub type Int32ColumnEncoder = PrimitiveColumnEncoder<Int32Type, RleV2Encoder<i32, SignedEncoding>>;
pub type Int64ColumnEncoder = PrimitiveColumnEncoder<Int64Type, RleV2Encoder<i64, SignedEncoding>>;
pub type DateColumnEncoder = PrimitiveColumnEncoder<Date32Type, RleV2Encoder<i32, SignedEncoding>>;
pub type TimestampSecondColumnEncoder = TimestampColumnEncoder<TimestampSecondType>;
pub type TimestampMillisecondColumnEncoder = TimestampColumnEncoder<TimestampMillisecondType>;
pub type TimestampMicrosecondColumnEncoder = TimestampColumnEncoder<TimestampMicrosecondType>;
pub type TimestampNanosecondColumnEncoder = TimestampColumnEncoder<TimestampNanosecondType>;
pub type StringColumnEncoder = GenericBinaryColumnEncoder<GenericStringType<i32>>;
pub type LargeStringColumnEncoder = GenericBinaryColumnEncoder<GenericStringType<i64>>;
pub type BinaryColumnEncoder = GenericBinaryColumnEncoder<GenericBinaryType<i32>>;
pub type LargeBinaryColumnEncoder = GenericBinaryColumnEncoder<GenericBinaryType<i64>>;
