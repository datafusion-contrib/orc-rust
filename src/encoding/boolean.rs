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

use std::io::Read;

use arrow::{
    array::BooleanBufferBuilder,
    buffer::{BooleanBuffer, NullBuffer},
};
use bytes::Bytes;

use crate::{error::Result, memory::EstimateMemory};

use super::{
    byte::{ByteRleDecoder, ByteRleEncoder},
    PrimitiveValueDecoder, PrimitiveValueEncoder,
};

pub struct BooleanDecoder<R: Read> {
    decoder: ByteRleDecoder<R>,
    data: u8,
    bits_in_data: usize,
}

impl<R: Read> BooleanDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self {
            decoder: ByteRleDecoder::new(reader),
            bits_in_data: 0,
            data: 0,
        }
    }

    pub fn value(&mut self) -> bool {
        let value = (self.data & 0x80) != 0;
        self.data <<= 1;
        self.bits_in_data -= 1;

        value
    }
}

impl<R: Read> PrimitiveValueDecoder<bool> for BooleanDecoder<R> {
    fn skip(&mut self, n: usize) -> Result<()> {
        let mut remaining_bits = n;

        // First consume from any buffered bits in `data`
        if self.bits_in_data > 0 {
            let take = remaining_bits.min(self.bits_in_data);
            // Advance by shifting left (MSB-first)
            self.data <<= take;
            self.bits_in_data -= take;
            remaining_bits -= take;
        }

        if remaining_bits == 0 {
            return Ok(());
        }

        // Skip whole bytes directly from byte RLE
        let whole_bytes = remaining_bits / 8;
        if whole_bytes > 0 {
            self.decoder.skip(whole_bytes)?;
            remaining_bits -= whole_bytes * 8;
        }

        // Skip remaining bits by decoding one more byte and positioning inside it
        if remaining_bits > 0 {
            let mut byte = [0i8; 1];
            match self.decoder.decode(&mut byte) {
                Ok(_) => {
                    self.data = (byte[0] as u8) << remaining_bits;
                    self.bits_in_data = 8 - remaining_bits;
                }
                Err(e) => {
                    // If we can't read more data, we're at the end of the stream
                    // This means we tried to skip more than available
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    // TODO: can probably implement this better
    fn decode(&mut self, out: &mut [bool]) -> Result<()> {
        for x in out.iter_mut() {
            // read more data if necessary
            if self.bits_in_data == 0 {
                let mut data = [0];
                self.decoder.decode(&mut data)?;
                self.data = data[0] as u8;
                self.bits_in_data = 8;
            }
            *x = self.value();
        }
        Ok(())
    }
}

/// ORC encodes validity starting from MSB, whilst Arrow encodes it
/// from LSB. After bytes are filled with the present bits, they are
/// further encoded via Byte RLE.
pub struct BooleanEncoder {
    // TODO: can we refactor to not need two separate buffers?
    byte_encoder: ByteRleEncoder,
    builder: BooleanBufferBuilder,
}

impl EstimateMemory for BooleanEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.builder.len() / 8
    }
}

impl BooleanEncoder {
    pub fn new() -> Self {
        Self {
            byte_encoder: ByteRleEncoder::new(),
            builder: BooleanBufferBuilder::new(8),
        }
    }

    pub fn extend(&mut self, null_buffer: &NullBuffer) {
        let bb = null_buffer.inner();
        self.extend_bb(bb);
    }

    pub fn extend_bb(&mut self, bb: &BooleanBuffer) {
        self.builder.append_buffer(bb);
    }

    /// Extend with n true bits.
    pub fn extend_present(&mut self, n: usize) {
        self.builder.append_n(n, true);
    }

    pub fn extend_boolean(&mut self, b: bool) {
        self.builder.append(b);
    }

    /// Produce ORC present stream bytes and reset internal builder.
    pub fn finish(&mut self) -> Bytes {
        // TODO: don't throw away allocation?
        let bb = self.builder.finish();
        // We use BooleanBufferBuilder so offset is 0
        let bytes = bb.values();
        // Reverse bits as ORC stores from MSB
        let bytes = bytes.iter().map(|b| b.reverse_bits()).collect::<Vec<_>>();
        for &b in bytes.as_slice() {
            self.byte_encoder.write_one(b as i8);
        }
        self.byte_encoder.take_inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let expected = vec![false; 800];
        let data = [0x61u8, 0x00];
        let data = &mut data.as_ref();
        let mut decoder = BooleanDecoder::new(data);
        let mut actual = vec![true; expected.len()];
        decoder.decode(&mut actual).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn literals() {
        let expected = vec![
            false, true, false, false, false, true, false, false, // 0b01000100
            false, true, false, false, false, true, false, true, // 0b01000101
        ];
        let data = [0xfeu8, 0b01000100, 0b01000101];
        let data = &mut data.as_ref();
        let mut decoder = BooleanDecoder::new(data);
        let mut actual = vec![true; expected.len()];
        decoder.decode(&mut actual).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn another() {
        // "For example, the byte sequence [0xff, 0x80] would be one true followed by seven false values."
        let expected = vec![true, false, false, false, false, false, false, false];
        let data = [0xff, 0x80];
        let data = &mut data.as_ref();
        let mut decoder = BooleanDecoder::new(data);
        let mut actual = vec![true; expected.len()];
        decoder.decode(&mut actual).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_skip_run() {
        // Run: 100 false values (0x61, 0x00)
        let data = [0x61u8, 0x00];
        let mut decoder = BooleanDecoder::new(data.as_ref());

        // Decode first 10 values
        let mut batch = vec![true; 10];
        decoder.decode(&mut batch).unwrap();
        assert_eq!(batch, vec![false; 10]);

        // Skip next 80 values
        decoder.skip(80).unwrap();

        // Decode last 10 values
        let mut batch = vec![true; 10];
        decoder.decode(&mut batch).unwrap();
        assert_eq!(batch, vec![false; 10]);
    }

    #[test]
    fn test_skip_all() {
        // Literal list of exactly 1 byte -> 8 bits
        let data = [0xffu8, 0x00u8];
        let mut decoder = BooleanDecoder::new(data.as_ref());

        // Skip all 8 bits
        decoder.skip(8).unwrap();

        // Next decode must error (EOF)
        let mut batch = vec![true; 1];
        let result = decoder.decode(&mut batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_skip_partial_bits() {
        // Test skipping partial bits within a byte
        let data = [0xfeu8, 0b01000100, 0b01000101]; // 16 bits of data
        let mut decoder = BooleanDecoder::new(data.as_ref());

        // Skip first 3 bits (should leave 5 bits in the first byte)
        decoder.skip(3).unwrap();

        // Decode next 5 bits should work
        let mut batch = vec![true; 5];
        decoder.decode(&mut batch).unwrap();
        // Expected: After skipping 3 bits from 0b01000100, we get 0b000100
        // Which is [false, false, true, false, false]
        assert_eq!(batch, vec![false, false, true, false, false]);
    }

    #[test]
    fn test_skip_cross_byte_boundary() {
        // Test skipping across byte boundaries
        let data = [0xfeu8, 0b01000100, 0b01000101]; // 16 bits of data
        let mut decoder = BooleanDecoder::new(data.as_ref());

        // Skip 6 bits (should consume first byte and 2 bits of second byte)
        decoder.skip(6).unwrap();

        // Decode remaining bits should work
        let mut batch = vec![true; 4];
        decoder.decode(&mut batch).unwrap();
        // Expected: 0b0001 -> [false, false, false, true]
        assert_eq!(batch, vec![false, false, false, true]);
    }

    #[test]
    fn test_skip_zero() {
        let data = [0x61u8, 0x00]; // 100 false values
        let mut decoder = BooleanDecoder::new(data.as_ref());

        // Skip 0 values should be a no-op
        decoder.skip(0).unwrap();

        // Decode should still work normally
        let mut batch = vec![true; 10];
        decoder.decode(&mut batch).unwrap();
        assert_eq!(batch, vec![false; 10]);
    }

    #[test]
    fn test_skip_exact_byte() {
        let data = [0x61u8, 0x00]; // 100 false values
        let mut decoder = BooleanDecoder::new(data.as_ref());

        // Skip exactly 8 bits (1 byte)
        decoder.skip(8).unwrap();

        // Should be able to continue decoding
        let mut batch = vec![true; 10];
        decoder.decode(&mut batch).unwrap();
        assert_eq!(batch, vec![false; 10]);
    }

    #[test]
    fn test_skip_more_than_available() {
        // Literal list of exactly 1 byte -> 8 bits
        let data = [0xffu8, 0x00u8];
        let mut decoder = BooleanDecoder::new(data.as_ref());

        // Try to skip more than available should fail
        let result = decoder.skip(9);
        assert!(result.is_err());
    }
}
