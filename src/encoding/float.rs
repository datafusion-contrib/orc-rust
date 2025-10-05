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

use bytemuck::{must_cast_slice, must_cast_slice_mut};
use bytes::{Bytes, BytesMut};
use snafu::ResultExt;

use crate::{
    error::{IoSnafu, Result},
    memory::EstimateMemory,
};

use super::{PrimitiveValueDecoder, PrimitiveValueEncoder};

/// Collect all the required traits we need on floats.
pub trait Float:
    num::Float + std::fmt::Debug + bytemuck::NoUninit + bytemuck::AnyBitPattern
{
}
impl Float for f32 {}
impl Float for f64 {}

pub struct FloatDecoder<F: Float, R: std::io::Read> {
    reader: R,
    phantom: std::marker::PhantomData<F>,
}

impl<F: Float, R: std::io::Read> FloatDecoder<F, R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            phantom: Default::default(),
        }
    }
}

impl<F: Float, R: std::io::Read> PrimitiveValueDecoder<F> for FloatDecoder<F, R> {
    fn skip(&mut self, n: usize) -> Result<()> {
        let bytes_to_skip = n * std::mem::size_of::<F>();
        let mut remaining = bytes_to_skip;
        // TODO: use seek instead of read to avoid copying data
        let mut buf = [0u8; 8192];

        while remaining > 0 {
            let to_read = remaining.min(buf.len());
            self.reader
                .read_exact(&mut buf[..to_read])
                .context(IoSnafu)?;
            remaining -= to_read;
        }
        Ok(())
    }

    fn decode(&mut self, out: &mut [F]) -> Result<()> {
        let bytes = must_cast_slice_mut::<F, u8>(out);
        self.reader.read_exact(bytes).context(IoSnafu)?;
        Ok(())
    }
}

/// No special run encoding for floats/doubles, they are stored as their IEEE 754 floating
/// point bit layout. This encoder simply copies incoming floats/doubles to its internal
/// byte buffer.
pub struct FloatEncoder<F: Float> {
    data: BytesMut,
    _phantom: PhantomData<F>,
}

impl<F: Float> EstimateMemory for FloatEncoder<F> {
    fn estimate_memory_size(&self) -> usize {
        self.data.len()
    }
}

impl<F: Float> PrimitiveValueEncoder<F> for FloatEncoder<F> {
    fn new() -> Self {
        Self {
            data: BytesMut::new(),
            _phantom: Default::default(),
        }
    }

    fn write_one(&mut self, value: F) {
        self.write_slice(&[value]);
    }

    fn write_slice(&mut self, values: &[F]) {
        let bytes = must_cast_slice::<F, u8>(values);
        self.data.extend_from_slice(bytes);
    }

    fn take_inner(&mut self) -> Bytes {
        std::mem::take(&mut self.data).into()
    }
}

#[cfg(test)]
mod tests {
    use std::f32::consts as f32c;
    use std::f64::consts as f64c;
    use std::io::Cursor;

    use proptest::prelude::*;

    use super::*;

    fn roundtrip_helper<F: Float>(input: &[F]) -> Result<Vec<F>> {
        let mut encoder = FloatEncoder::<F>::new();
        encoder.write_slice(input);
        let bytes = encoder.take_inner();
        let bytes = Cursor::new(bytes);

        let mut iter = FloatDecoder::<F, _>::new(bytes);
        let mut actual = vec![F::zero(); input.len()];
        iter.decode(&mut actual)?;

        Ok(actual)
    }

    fn assert_roundtrip<F: Float>(input: Vec<F>) {
        let actual = roundtrip_helper(&input).unwrap();
        assert_eq!(input, actual);
    }

    proptest! {
        #[test]
        fn roundtrip_f32(values: Vec<f32>) {
            let out = roundtrip_helper(&values)?;
            prop_assert_eq!(out, values);
        }

        #[test]
        fn roundtrip_f64(values: Vec<f64>) {
            let out = roundtrip_helper(&values)?;
            prop_assert_eq!(out, values);
        }
    }

    #[test]
    fn test_float_edge_cases() {
        assert_roundtrip::<f32>(vec![]);
        assert_roundtrip::<f64>(vec![]);

        assert_roundtrip(vec![f32c::PI]);
        assert_roundtrip(vec![f64c::PI]);

        let actual = roundtrip_helper(&[f32::NAN]).unwrap();
        assert!(actual[0].is_nan());
        let actual = roundtrip_helper(&[f64::NAN]).unwrap();
        assert!(actual[0].is_nan());
    }

    #[test]
    fn test_float_many() {
        assert_roundtrip(vec![
            f32::NEG_INFINITY,
            f32::MIN,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f32c::SQRT_2,
            f32::MAX,
            f32::INFINITY,
        ]);

        assert_roundtrip(vec![
            f64::NEG_INFINITY,
            f64::MIN,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f64c::SQRT_2,
            f64::MAX,
            f64::INFINITY,
        ]);
    }

    #[test]
    fn test_skip_f32() -> Result<()> {
        // Encode 10 f32 values: [0.0, 1.5, 3.0, 4.5, 6.0, 7.5, 9.0, 10.5, 12.0, 13.5]
        let values: Vec<f32> = (0..10).map(|i| i as f32 * 1.5).collect();
        let mut encoder = FloatEncoder::<f32>::new();
        encoder.write_slice(&values);
        let bytes = encoder.take_inner();

        let mut decoder = FloatDecoder::<f32, _>::new(Cursor::new(bytes));

        // Decode first 3 values
        let mut batch = vec![0.0f32; 3];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![0.0, 1.5, 3.0]);

        // Skip next 4 values (4.5, 6.0, 7.5, 9.0)
        decoder.skip(4)?;

        // Decode remaining 3 values (10.5, 12.0, 13.5)
        let mut batch = vec![0.0f32; 3];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![10.5, 12.0, 13.5]);

        Ok(())
    }

    #[test]
    fn test_skip_f64() -> Result<()> {
        // Encode 10 f64 values
        let values: Vec<f64> = (0..10).map(|i| i as f64 * 2.5).collect();
        let mut encoder = FloatEncoder::<f64>::new();
        encoder.write_slice(&values);
        let bytes = encoder.take_inner();

        let mut decoder = FloatDecoder::<f64, _>::new(Cursor::new(bytes));

        // Skip first 5 values
        decoder.skip(5)?;

        // Decode next 3 values
        let mut batch = vec![0.0f64; 3];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![12.5, 15.0, 17.5]);

        // Skip 1 value
        decoder.skip(1)?;

        // Decode last value
        let mut batch = vec![0.0f64; 1];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![22.5]);

        Ok(())
    }

    #[test]
    fn test_skip_all_values() -> Result<()> {
        // Test skipping all values
        let values: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mut encoder = FloatEncoder::<f32>::new();
        encoder.write_slice(&values);
        let bytes = encoder.take_inner();

        let mut decoder = FloatDecoder::<f32, _>::new(Cursor::new(bytes));

        // Skip all 5 values
        decoder.skip(5)?;

        // Try to decode should fail (EOF)
        let mut batch = vec![0.0f32; 1];
        let result = decoder.decode(&mut batch);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_skip_edge_cases() -> Result<()> {
        // Test with special float values
        let values = vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.0,
            -0.0,
            f64::MIN,
            f64::MAX,
        ];
        let mut encoder = FloatEncoder::<f64>::new();
        encoder.write_slice(&values);
        let bytes = encoder.take_inner();

        let mut decoder = FloatDecoder::<f64, _>::new(Cursor::new(bytes));

        // Skip first 3 (NAN, INF, NEG_INF)
        decoder.skip(3)?;

        // Decode next 2
        let mut batch = vec![0.0f64; 2];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![0.0, -0.0]);

        // Skip 1 (MIN)
        decoder.skip(1)?;

        // Decode last (MAX)
        let mut batch = vec![0.0f64; 1];
        decoder.decode(&mut batch)?;
        assert_eq!(batch, vec![f64::MAX]);

        Ok(())
    }
}
