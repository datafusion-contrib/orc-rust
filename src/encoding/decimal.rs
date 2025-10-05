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

use crate::error::Result;

use super::{
    integer::{read_varint_zigzagged, SignedEncoding},
    PrimitiveValueDecoder,
};

/// Read stream of zigzag encoded varints as i128 (unbound).
pub struct UnboundedVarintStreamDecoder<R: Read> {
    reader: R,
}

impl<R: Read> UnboundedVarintStreamDecoder<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}

impl<R: Read> PrimitiveValueDecoder<i128> for UnboundedVarintStreamDecoder<R> {
    fn skip(&mut self, n: usize) -> Result<()> {
        for _ in 0..n {
            read_varint_zigzagged::<i128, _, SignedEncoding>(&mut self.reader)?;
        }
        Ok(())
    }

    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        for x in out.iter_mut() {
            *x = read_varint_zigzagged::<i128, _, SignedEncoding>(&mut self.reader)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Manually encode a few simple i128 values as zigzag varint for testing
    // Format: zigzag encode, then varint encode
    // 0 -> zigzag: 0 -> varint: [0x00]
    // 1 -> zigzag: 2 -> varint: [0x02]
    // -1 -> zigzag: 1 -> varint: [0x01]
    // 100 -> zigzag: 200 -> varint: [0xc8, 0x01]

    #[test]
    fn test_unbounded_varint_decoder_skip() -> Result<()> {
        // Test data: [0, 1, -1, 100, 200]
        // 0: 0x00
        // 1: 0x02
        // -1: 0x01
        // 100: 0xc8, 0x01 (zigzag: 200)
        // 200: 0x90, 0x03 (zigzag: 400)
        let encoded = vec![0x00, 0x02, 0x01, 0xc8, 0x01, 0x90, 0x03];
        let mut decoder = UnboundedVarintStreamDecoder::new(Cursor::new(&encoded));

        // Decode first 2 values
        let mut batch = vec![0i128; 2];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![0, 1]);

        // Skip next 2 values (-1, 100)
        decoder.skip(2)?;

        // Decode remaining value (200)
        let mut batch = vec![0i128; 1];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![200]);

        Ok(())
    }

    #[test]
    fn test_unbounded_varint_skip_all() -> Result<()> {
        // Test data: [0, 1, -1]
        let encoded = vec![0x00, 0x02, 0x01];
        let mut decoder = UnboundedVarintStreamDecoder::new(Cursor::new(&encoded));

        // Skip all 3 values
        decoder.skip(3)?;

        // Try to decode should fail (EOF)
        let mut batch = vec![0i128; 1];
        let result = decoder.decode(&mut batch);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_unbounded_varint_skip_then_decode() -> Result<()> {
        // Test data: [10, 20, 30, 40, 50]
        // 10: zigzag 20 = 0x14
        // 20: zigzag 40 = 0x28
        // 30: zigzag 60 = 0x3c
        // 40: zigzag 80 = 0x50
        // 50: zigzag 100 = 0x64
        let encoded = vec![0x14, 0x28, 0x3c, 0x50, 0x64];
        let mut decoder = UnboundedVarintStreamDecoder::new(Cursor::new(&encoded));

        // Skip first 2
        decoder.skip(2)?;

        // Decode next 2
        let mut batch = vec![0i128; 2];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![30, 40]);

        // Skip last 1
        decoder.skip(1)?;

        // Try to decode should fail (EOF)
        let mut batch = vec![0i128; 1];
        let result = decoder.decode(&mut batch);
        assert!(result.is_err());

        Ok(())
    }
}
