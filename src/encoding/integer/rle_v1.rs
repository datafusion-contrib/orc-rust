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

//! Handling decoding of Integer Run Length Encoded V1 data in ORC files

use std::{io::Read, marker::PhantomData, ops::RangeInclusive};

use bytes::{BufMut, BytesMut};
use snafu::OptionExt;

use crate::{
    encoding::{
        rle::GenericRle,
        util::{read_u8, try_read_u8},
        PrimitiveValueEncoder,
    },
    error::{OutOfSpecSnafu, Result},
    memory::EstimateMemory,
};

use super::{
    util::{read_varint_zigzagged, write_varint_zigzagged},
    EncodingSign, NInt,
};

const MIN_RUN_LENGTH: usize = 3;
const MAX_RUN_LENGTH: usize = 127 + MIN_RUN_LENGTH;
const MAX_LITERAL_LENGTH: usize = 128;
const DELAT_RANGE: RangeInclusive<i64> = -128..=127;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EncodingType {
    Run { length: usize, delta: i8 },
    Literals { length: usize },
}

impl EncodingType {
    /// Decode header byte to determine sub-encoding.
    /// Runs start with a positive byte, and literals with a negative byte.
    fn from_header<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let opt_encoding = match try_read_u8(reader)?.map(|b| b as i8) {
            Some(header) if header < 0 => {
                let length = header.unsigned_abs() as usize;
                Some(Self::Literals { length })
            }
            Some(header) => {
                let length = header as u8 as usize + 3;
                let delta = read_u8(reader)? as i8;
                Some(Self::Run { length, delta })
            }
            None => None,
        };
        Ok(opt_encoding)
    }
}

/// Decodes a stream of Integer Run Length Encoded version 1 bytes.
pub struct RleV1Decoder<N: NInt, R: Read, S: EncodingSign> {
    reader: R,
    decoded_ints: Vec<N>,
    current_head: usize,
    sign: PhantomData<S>,
}

impl<N: NInt, R: Read, S: EncodingSign> RleV1Decoder<N, R, S> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            decoded_ints: Vec::with_capacity(MAX_RUN_LENGTH),
            current_head: 0,
            sign: Default::default(),
        }
    }
}

fn read_literals<N: NInt, R: Read, S: EncodingSign>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    length: usize,
) -> Result<()> {
    for _ in 0..length {
        let lit = read_varint_zigzagged::<_, _, S>(reader)?;
        out_ints.push(lit);
    }
    Ok(())
}

fn read_run<N: NInt, R: Read, S: EncodingSign>(
    reader: &mut R,
    out_ints: &mut Vec<N>,
    length: usize,
    delta: i8,
) -> Result<()> {
    let mut base = read_varint_zigzagged::<_, _, S>(reader)?;
    // Account for base value
    let length = length - 1;
    out_ints.push(base);
    if delta < 0 {
        let delta = delta.unsigned_abs();
        let delta = N::from_u8(delta);
        for _ in 0..length {
            base = base.checked_sub(&delta).context(OutOfSpecSnafu {
                msg: "over/underflow when decoding patched base integer",
            })?;
            out_ints.push(base);
        }
    } else {
        let delta = delta as u8;
        let delta = N::from_u8(delta);
        for _ in 0..length {
            base = base.checked_add(&delta).context(OutOfSpecSnafu {
                msg: "over/underflow when decoding patched base integer",
            })?;
            out_ints.push(base);
        }
    }
    Ok(())
}

impl<N: NInt, R: Read, S: EncodingSign> GenericRle<N> for RleV1Decoder<N, R, S> {
    fn advance(&mut self, n: usize) {
        self.current_head += n;
    }

    fn available(&self) -> &[N] {
        &self.decoded_ints[self.current_head..]
    }

    fn decode_batch(&mut self) -> Result<()> {
        self.current_head = 0;
        self.decoded_ints.clear();

        match EncodingType::from_header(&mut self.reader)? {
            Some(EncodingType::Literals { length }) => {
                read_literals::<_, _, S>(&mut self.reader, &mut self.decoded_ints, length)
            }
            Some(EncodingType::Run { length, delta }) => {
                read_run::<_, _, S>(&mut self.reader, &mut self.decoded_ints, length, delta)
            }
            None => Ok(()),
        }
    }
}

/// Represents the state of the RLE V1 encoder.
///
/// The encoder can be in one of three states:
///
/// 1. `Empty`: The buffer is empty and there are no values to encode.
/// 2. `Literal`: The encoder is in literal mode, with values saved in buffer.
/// 3. `Run`: The encoder is in run mode, with a run value, delta, and length.
#[derive(Debug, Clone, Eq, PartialEq)]
enum RleV1EncodingState<N: NInt> {
    Empty,
    Literal,
    Run { value: N, delta: i8, length: usize },
}

impl<N: NInt> Default for RleV1EncodingState<N> {
    fn default() -> Self {
        Self::Empty
    }
}

/// `RleV1Encoder` is responsible for encoding a stream of integers using the Run Length Encoding (RLE) version 1 format.
pub struct RleV1Encoder<N: NInt, S: EncodingSign> {
    writer: BytesMut,
    state: RleV1EncodingState<N>,
    buffer: Vec<N>,
    sign: PhantomData<S>,
}

impl<N: NInt, S: EncodingSign> RleV1Encoder<N, S> {
    /// Processes a given value and updates the encoder state accordingly.
    ///
    /// The function handles three possible states of the encoder:
    ///
    /// 1. `RleV1EncoderState::Empty`:
    ///    - Transitions to the `Literal` state with the given value as the first element in the buffer.
    ///
    /// 2. `RleV1EncoderState::Run`:
    ///    - If the value continues the current run (i.e., it matches the expected value based on the run's delta and length),
    ///      the run length is incremented. If the run length reaches `MAX_RUN_LENGTH`, the run is written out and the state
    ///      transitions to `Empty`.
    ///    - If the value does not continue the current run, the existing run is written out and the state transitions to
    ///      `Literal` with the new value as the first element in the buffer.
    ///
    /// 3. `RleV1EncoderState::Literal`:
    ///    - The value is added to the buffer. If the buffer length reaches `MAX_LITERAL_LENGTH`, the buffer is written out
    ///      and the state transitions to `Empty`.
    ///    - If the buffer length is at least `MIN_RUN_LENGTH` and the values in the buffer form a valid run (i.e., the deltas
    ///      between consecutive values are consistent and within the allowed range), the state transitions to `Run`.
    ///    - Otherwise, the state remains `Literal`.
    ///
    fn process_value(&mut self, value: N) {
        match &mut self.state {
            RleV1EncodingState::Empty => {
                // change to literal model
                self.buffer.clear();
                self.buffer.push(value);
                self.state = RleV1EncodingState::Literal;
            }
            RleV1EncodingState::Literal => {
                let buf = &mut self.buffer;
                buf.push(value);
                let length = buf.len();
                let delta = (value - buf[length - 2]).as_i64();
                // check if can change to run model
                if length >= MIN_RUN_LENGTH
                    && DELAT_RANGE.contains(&delta)
                    && delta == (buf[length - 2] - buf[length - 3]).as_i64()
                {
                    // change to run model
                    if length > MIN_RUN_LENGTH {
                        // write the left literals
                        write_literals::<_, S>(&mut self.writer, &buf[..(length - MIN_RUN_LENGTH)]);
                    }
                    self.state = RleV1EncodingState::Run {
                        value: buf[length - MIN_RUN_LENGTH],
                        delta: delta as i8,
                        length: MIN_RUN_LENGTH,
                    }
                } else if length == MAX_LITERAL_LENGTH {
                    // reach buffer limit, write literals and change to empty state
                    write_literals::<_, S>(&mut self.writer, buf);
                    self.state = RleV1EncodingState::Empty;
                }
                // else keep literal mode
            }
            RleV1EncodingState::Run {
                value: run_value,
                delta,
                length,
            } => {
                if run_value.as_i64() + (*delta as i64) * (*length as i64) == value.as_i64() {
                    // keep run model
                    *length += 1;
                    if *length == MAX_RUN_LENGTH {
                        // reach run limit
                        write_run::<_, S>(&mut self.writer, *run_value, *delta, *length);
                        self.state = RleV1EncodingState::Empty;
                    }
                } else {
                    // write run values and change to literal model
                    write_run::<_, S>(&mut self.writer, *run_value, *delta, *length);
                    self.buffer.clear();
                    self.buffer.push(value);
                    self.state = RleV1EncodingState::Literal;
                }
            }
        }
    }

    /// Flushes the current state of the encoder, writing out any buffered values.
    ///
    /// This function handles the three possible states of the encoder:
    ///
    /// 1. `RleV1EncoderState::Empty`:
    ///    - No action is needed as there are no buffered values to write.
    ///
    /// 3. `RleV1EncoderState::Literal`:
    ///    - Writes out the buffered literal values.
    ///
    /// 2. `RleV1EncoderState::Run`:
    ///    - Writes out the current run of values.
    ///
    /// After calling this function, the encoder state will be reset to `Empty`.
    fn flush(&mut self) {
        let state = std::mem::take(&mut self.state);
        match state {
            RleV1EncodingState::Empty => {}
            RleV1EncodingState::Literal => {
                write_literals::<_, S>(&mut self.writer, &self.buffer);
            }
            RleV1EncodingState::Run {
                value,
                delta,
                length,
            } => {
                write_run::<_, S>(&mut self.writer, value, delta, length);
            }
        }
    }
}

fn write_run<N: NInt, S: EncodingSign>(writer: &mut BytesMut, value: N, delta: i8, length: usize) {
    // write header
    writer.put_u8(length as u8 - 3);
    writer.put_u8(delta as u8);
    // write run value
    write_varint_zigzagged::<_, S>(writer, value);
}

fn write_literals<N: NInt, S: EncodingSign>(writer: &mut BytesMut, buffer: &[N]) {
    // write header
    writer.put_u8(-(buffer.len() as i8) as u8);
    // write literals
    for literal in buffer {
        write_varint_zigzagged::<_, S>(writer, *literal);
    }
}

impl<N: NInt, S: EncodingSign> EstimateMemory for RleV1Encoder<N, S> {
    fn estimate_memory_size(&self) -> usize {
        self.writer.len()
    }
}

impl<N: NInt, S: EncodingSign> PrimitiveValueEncoder<N> for RleV1Encoder<N, S> {
    fn new() -> Self {
        Self {
            writer: BytesMut::new(),
            state: Default::default(),
            buffer: Vec::with_capacity(MAX_LITERAL_LENGTH),
            sign: Default::default(),
        }
    }

    fn write_one(&mut self, value: N) {
        self.process_value(value);
    }

    fn take_inner(&mut self) -> bytes::Bytes {
        self.flush();
        std::mem::take(&mut self.writer).into()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::encoding::{integer::UnsignedEncoding, PrimitiveValueDecoder};

    use super::*;

    fn test_helper(original: &[i64], encoded: &[u8]) {
        let mut encoder = RleV1Encoder::<i64, UnsignedEncoding>::new();
        encoder.write_slice(original);
        encoder.flush();
        let actual_encoded = encoder.take_inner();
        assert_eq!(actual_encoded, encoded);

        let mut decoder = RleV1Decoder::<i64, _, UnsignedEncoding>::new(Cursor::new(encoded));
        let mut actual_decoded = vec![0; original.len()];
        decoder.decode(&mut actual_decoded).unwrap();
        assert_eq!(actual_decoded, original);
    }

    #[test]
    fn test_run() -> Result<()> {
        let original = [7; 100];
        let encoded = [0x61, 0x00, 0x07];
        test_helper(&original, &encoded);

        let original = (1..=100).rev().collect::<Vec<_>>();
        let encoded = [0x61, 0xff, 0x64];
        test_helper(&original, &encoded);

        let original = (1..=150).rev().collect::<Vec<_>>();
        let encoded = [0x7f, 0xff, 0x96, 0x01, 0x11, 0xff, 0x14];
        test_helper(&original, &encoded);

        let original = [2, 4, 6, 8, 1, 3, 5, 7, 255];
        let encoded = [0x01, 0x02, 0x02, 0x01, 0x02, 0x01, 0xff, 0xff, 0x01];
        test_helper(&original, &encoded);
        Ok(())
    }

    #[test]
    fn test_literal() -> Result<()> {
        let original = vec![2, 3, 6, 7, 11];
        let encoded = [0xfb, 0x02, 0x03, 0x06, 0x07, 0xb];
        test_helper(&original, &encoded);

        let original = vec![2, 3, 6, 7, 11, 1, 2, 3, 0, 256];
        let encoded = [
            0xfb, 0x02, 0x03, 0x06, 0x07, 0x0b, 0x00, 0x01, 0x01, 0xfe, 0x00, 0x80, 0x02,
        ];
        test_helper(&original, &encoded);
        Ok(())
    }
}
