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

use std::{io::Read, marker::PhantomData};

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
const MAX_DELTA: i64 = 127;
const MIN_DELTA: i64 = -128;

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

    /// Encode header to write
    fn to_header(&self, writer: &mut BytesMut) {
        if let Self::Run { length, delta } = *self {
            writer.put_u8(length as u8 - 3);
            writer.put_u8(delta as u8);
        } else if let Self::Literals { length } = *self {
            writer.put_u8(-(length as i8) as u8);
        }
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

pub struct RleV1Encoder<N: NInt, S: EncodingSign> {
    writer: BytesMut,
    /// Literal values to encode.
    literals: [N; MAX_LITERAL_LENGTH],
    /// Represents the number of elements currently in `literals` if Literals,
    /// otherwise represents the length of the Run.
    num_literals: usize,
    /// Tracks if current Literal sequence will turn into a Run sequence due to
    /// repeated values at the end of the value sequence.
    tail_run_length: usize,
    /// If in Run sequence or not, and keeps the corresponding value.
    run_value: Option<N>,
    /// The delta value keeped now
    run_delta: i64,
    sign: PhantomData<S>,
}

impl<N: NInt, S: EncodingSign> RleV1Encoder<N, S> {
    // Algorithm adapted from:
    // https://github.com/apache/orc/blob/main/java/core/src/java/org/apache/orc/impl/RunLengthIntegerWriter.java
    fn process_value(&mut self, value: N) {
        if self.num_literals == 0 {
            self.literals[0] = value;
            self.num_literals = 1;
            self.tail_run_length = 1;
        } else if let Some(run_value) = self.run_value {
            if value.as_i64() == run_value.as_i64() + self.run_delta * self.num_literals as i64 {
                self.num_literals += 1;
                if self.num_literals == MAX_RUN_LENGTH {
                    self.flush();
                }
            } else {
                self.flush();
                self.literals[self.num_literals] = value;
                self.num_literals += 1;
                self.tail_run_length = 1;
            }
        } else {
            if self.tail_run_length == 1 {
                self.run_delta = (value - self.literals[self.num_literals - 1]).as_i64();
                if self.run_delta < MIN_DELTA || self.run_delta > MAX_DELTA {
                    self.tail_run_length = 1;
                } else {
                    self.tail_run_length = 2;
                }
            } else if value.as_i64()
                == self.literals[self.num_literals - 1].as_i64() + self.run_delta
            {
                self.tail_run_length += 1;
            } else {
                self.run_delta = (value - self.literals[self.num_literals - 1]).as_i64();
                if self.run_delta < MIN_DELTA || self.run_delta > MAX_DELTA {
                    self.tail_run_length = 1;
                } else {
                    self.tail_run_length = 2;
                }
            }
            if self.tail_run_length == MIN_RUN_LENGTH {
                if self.num_literals + 1 == MIN_RUN_LENGTH {
                    self.run_value = Some(self.literals[0]);
                    self.num_literals += 1;
                } else {
                    self.num_literals -= MIN_RUN_LENGTH - 1;
                    let run_value = self.literals[self.num_literals];
                    self.flush();
                    self.run_value = Some(run_value);
                    self.num_literals = MIN_RUN_LENGTH;
                }
            } else {
                self.literals[self.num_literals] = value;
                self.num_literals += 1;
                if self.num_literals == MAX_LITERAL_LENGTH {
                    self.flush();
                }
            }
        }
    }

    // Flush values to witer and reset state
    fn flush(&mut self) {
        if self.num_literals > 0 {
            if let Some(run_value) = self.run_value {
                let header = EncodingType::Run {
                    length: self.num_literals,
                    delta: self.run_delta as i8,
                };
                header.to_header(&mut self.writer);
                write_varint_zigzagged::<_, S>(&mut self.writer, run_value);
            } else {
                let header = EncodingType::Literals {
                    length: self.num_literals,
                };
                header.to_header(&mut self.writer);
                for i in 0..self.num_literals {
                    write_varint_zigzagged::<_, S>(&mut self.writer, self.literals[i]);
                }
            }
        }
        self.run_value = None;
        self.num_literals = 0;
        self.tail_run_length = 0;
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
            sign: Default::default(),
            literals: [N::zero(); MAX_LITERAL_LENGTH],
            num_literals: 0,
            tail_run_length: 0,
            run_value: None,
            run_delta: 0,
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
        Ok(())
    }

    #[test]
    fn test_literal() -> Result<()> {
        let original = vec![2, 3, 6, 7, 11];
        let encoded = [0xfb, 0x02, 0x03, 0x06, 0x07, 0xb];
        test_helper(&original, &encoded);
        Ok(())
    }
}
