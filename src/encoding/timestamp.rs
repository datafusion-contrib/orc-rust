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

use arrow::datatypes::{ArrowTimestampType, TimeUnit};
use bytes::Bytes;
use snafu::ensure;

use crate::{
    encoding::{PrimitiveValueDecoder, PrimitiveValueEncoder},
    error::{DecodeTimestampSnafu, EncodeTimestampSnafu, Result},
    memory::EstimateMemory,
};

const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;

/// Encodes Arrow timestamps into the two integer streams used by ORC timestamps.
pub struct TimestampEncoder<T: ArrowTimestampType> {
    base_from_epoch: i64,
    data: Box<dyn PrimitiveValueEncoder<i64>>,
    secondary: Box<dyn PrimitiveValueEncoder<i64>>,
    _marker: PhantomData<T>,
}

impl<T: ArrowTimestampType> TimestampEncoder<T> {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn PrimitiveValueEncoder<i64>>,
        secondary: Box<dyn PrimitiveValueEncoder<i64>>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
            _marker: PhantomData,
        }
    }

    pub fn encode(&mut self, values: &[T::Native]) -> Result<()> {
        let (units_per_second, nanoseconds_per_unit) = match T::UNIT {
            TimeUnit::Second => (1, NANOSECONDS_IN_SECOND),
            TimeUnit::Millisecond => (1_000, 1_000_000),
            TimeUnit::Microsecond => (1_000_000, 1_000),
            TimeUnit::Nanosecond => (NANOSECONDS_IN_SECOND, 1),
        };

        let mut data = Vec::with_capacity(values.len());
        let mut secondary = Vec::with_capacity(values.len());
        for &value in values {
            let mut seconds = value.div_euclid(units_per_second);
            let nanoseconds = value.rem_euclid(units_per_second) * nanoseconds_per_unit;

            // ORC-763 makes this interval immediately before the Unix epoch
            // impossible to represent without changing the decoded value.
            ensure!(
                seconds != -1 || nanoseconds <= 999_999,
                EncodeTimestampSnafu {
                    value,
                    time_unit: T::UNIT,
                }
            );
            if seconds < 0 && nanoseconds > 999_999 {
                seconds += 1;
            }
            let Some(seconds) = seconds.checked_sub(self.base_from_epoch) else {
                return EncodeTimestampSnafu {
                    value,
                    time_unit: T::UNIT,
                }
                .fail();
            };

            // The last three bits store how many trailing decimal zeros were removed.
            let encoded_nanoseconds = if nanoseconds == 0 {
                0
            } else if nanoseconds % 100 != 0 {
                nanoseconds << 3
            } else {
                let mut nanoseconds = nanoseconds / 100;
                let mut trailing = 1;
                while nanoseconds % 10 == 0 && trailing < 7 {
                    nanoseconds /= 10;
                    trailing += 1;
                }
                (nanoseconds << 3) | trailing
            };

            data.push(seconds);
            secondary.push(encoded_nanoseconds);
        }

        self.data.write_slice(&data);
        self.secondary.write_slice(&secondary);
        Ok(())
    }

    pub fn take_inner(&mut self) -> (Bytes, Bytes) {
        (self.data.take_inner(), self.secondary.take_inner())
    }
}

impl<T: ArrowTimestampType> EstimateMemory for TimestampEncoder<T> {
    fn estimate_memory_size(&self) -> usize {
        self.data.estimate_memory_size() + self.secondary.estimate_memory_size()
    }
}

pub struct TimestampDecoder<T: ArrowTimestampType> {
    base_from_epoch: i64,
    data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    _marker: PhantomData<T>,
}

impl<T: ArrowTimestampType> TimestampDecoder<T> {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
            _marker: PhantomData,
        }
    }
}

impl<T: ArrowTimestampType> PrimitiveValueDecoder<T::Native> for TimestampDecoder<T> {
    fn skip(&mut self, n: usize) -> Result<()> {
        self.data.skip(n)?;
        self.secondary.skip(n)?;
        Ok(())
    }

    fn decode(&mut self, out: &mut [T::Native]) -> Result<()> {
        // TODO: can probably optimize, reuse buffers?
        let mut data = vec![0; out.len()];
        let mut secondary = vec![0; out.len()];
        self.data.decode(&mut data)?;
        self.secondary.decode(&mut secondary)?;
        for (index, (&seconds_since_orc_base, &nanoseconds)) in
            data.iter().zip(secondary.iter()).enumerate()
        {
            out[index] =
                decode_timestamp::<T>(self.base_from_epoch, seconds_since_orc_base, nanoseconds)?;
        }
        Ok(())
    }
}

/// Arrow TimestampNanosecond type cannot represent the full datetime range of
/// the ORC Timestamp type, so this iterator provides the ability to decode the
/// raw nanoseconds without restricting it to the Arrow TimestampNanosecond range.
pub struct TimestampNanosecondAsDecimalDecoder {
    base_from_epoch: i64,
    data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
}

impl TimestampNanosecondAsDecimalDecoder {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
        }
    }
}

impl PrimitiveValueDecoder<i128> for TimestampNanosecondAsDecimalDecoder {
    fn skip(&mut self, n: usize) -> Result<()> {
        self.data.skip(n)?;
        self.secondary.skip(n)?;
        Ok(())
    }

    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        // TODO: can probably optimize, reuse buffers?
        let mut data = vec![0; out.len()];
        let mut secondary = vec![0; out.len()];
        self.data.decode(&mut data)?;
        self.secondary.decode(&mut secondary)?;
        for (index, (&seconds_since_orc_base, &nanoseconds)) in
            data.iter().zip(secondary.iter()).enumerate()
        {
            out[index] =
                decode_timestamp_as_i128(self.base_from_epoch, seconds_since_orc_base, nanoseconds);
        }
        Ok(())
    }
}

fn decode(base: i64, seconds_since_orc_base: i64, nanoseconds: i64) -> (i128, i64, u64) {
    let data = seconds_since_orc_base;
    // TODO: is this a safe cast?
    let mut nanoseconds = nanoseconds as u64;
    // Last 3 bits indicate how many trailing zeros were truncated
    let zeros = nanoseconds & 0x7;
    nanoseconds >>= 3;
    // Multiply by powers of 10 to get back the trailing zeros
    // TODO: would it be more efficient to unroll this? (if LLVM doesn't already do so)
    if zeros != 0 {
        nanoseconds *= 10_u64.pow(zeros as u32 + 1);
    }
    let seconds_since_epoch = data + base;
    // Timestamps below the UNIX epoch with nanoseconds > 999_999 need to be
    // adjusted to have 1 second subtracted due to ORC-763:
    // https://issues.apache.org/jira/browse/ORC-763
    let seconds = if seconds_since_epoch < 0 && nanoseconds > 999_999 {
        seconds_since_epoch - 1
    } else {
        seconds_since_epoch
    };
    // Convert into nanoseconds since epoch, which Arrow uses as native representation
    // of timestamps
    // The timestamp may overflow i64 as ORC encodes them as a pair of (seconds, nanoseconds)
    // while we encode them as a single i64 of nanoseconds in Arrow.
    let nanoseconds_since_epoch =
        (seconds as i128 * NANOSECONDS_IN_SECOND as i128) + (nanoseconds as i128);
    // Returning seconds & nanoseconds only for error message
    // TODO: does the error message really need those details? Can simplify by removing.
    (nanoseconds_since_epoch, seconds, nanoseconds)
}

fn decode_timestamp<T: ArrowTimestampType>(
    base: i64,
    seconds_since_orc_base: i64,
    nanoseconds: i64,
) -> Result<i64> {
    let (nanoseconds_since_epoch, seconds, nanoseconds) =
        decode(base, seconds_since_orc_base, nanoseconds);

    let nanoseconds_in_timeunit = match T::UNIT {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };

    // Error if loss of precision
    // TODO: make this configurable (e.g. can succeed but truncate)
    ensure!(
        nanoseconds_since_epoch % nanoseconds_in_timeunit == 0,
        DecodeTimestampSnafu {
            seconds,
            nanoseconds,
            to_time_unit: T::UNIT,
        }
    );

    // Convert to i64 and error if overflow
    let num_since_epoch = (nanoseconds_since_epoch / nanoseconds_in_timeunit)
        .try_into()
        .or_else(|_| {
            DecodeTimestampSnafu {
                seconds,
                nanoseconds,
                to_time_unit: T::UNIT,
            }
            .fail()
        })?;

    Ok(num_since_epoch)
}

fn decode_timestamp_as_i128(base: i64, seconds_since_orc_base: i64, nanoseconds: i64) -> i128 {
    let (nanoseconds_since_epoch, _, _) = decode(base, seconds_since_orc_base, nanoseconds);
    nanoseconds_since_epoch
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::datatypes::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };

    use crate::encoding::integer::{
        rle_v2::{RleV2Decoder, RleV2Encoder},
        SignedEncoding, UnsignedEncoding,
    };

    use super::*;

    const ORC_EPOCH_SECONDS: i64 = 1_420_070_400;

    fn encoder<T: ArrowTimestampType>() -> TimestampEncoder<T> {
        TimestampEncoder::new(
            ORC_EPOCH_SECONDS,
            Box::new(RleV2Encoder::<i64, SignedEncoding>::new()),
            Box::new(RleV2Encoder::<i64, UnsignedEncoding>::new()),
        )
    }

    fn roundtrip<T: ArrowTimestampType>(values: &[i64]) {
        let mut encoder = encoder::<T>();
        encoder.encode(values).unwrap();
        let (data, secondary) = encoder.take_inner();

        let data = Box::new(RleV2Decoder::<i64, _, SignedEncoding>::new(Cursor::new(
            data,
        )));
        let secondary = Box::new(RleV2Decoder::<i64, _, UnsignedEncoding>::new(Cursor::new(
            secondary,
        )));
        let mut decoder = TimestampDecoder::<T>::new(ORC_EPOCH_SECONDS, data, secondary);
        let mut actual = vec![0; values.len()];
        decoder.decode(&mut actual).unwrap();
        assert_eq!(values, actual);
    }

    #[test]
    fn test_timestamp_encoder_roundtrip() {
        roundtrip::<TimestampSecondType>(&[-2, -1, 0, 1, 2, 1_420_070_400]);
        roundtrip::<TimestampMillisecondType>(&[-2_001, -2_000, -1_999, -1_000, 0, 1_001, 1_234]);
        roundtrip::<TimestampMicrosecondType>(&[
            -2_000_001, -1_001_001, -1_000_000, -999_999, 0, 1_001_001, 1_234_567,
        ]);
        roundtrip::<TimestampNanosecondType>(&[
            -2_000_000_001,
            -1_001_001_001,
            -1_000_000_000,
            -999_999_999,
            0,
            1_001_001_001,
            1_234_567_890,
        ]);
    }

    #[test]
    fn test_timestamp_encoder_rejects_orc_epoch_overflow() {
        let mut encoder = encoder::<TimestampSecondType>();
        assert!(encoder.encode(&[i64::MIN]).is_err());
    }

    #[test]
    fn test_timestamp_encoder_rejects_orc_763_gap() {
        let mut milliseconds = encoder::<TimestampMillisecondType>();
        assert!(milliseconds.encode(&[-1_000]).is_ok());
        assert!(milliseconds.encode(&[-999]).is_err());
        assert!(milliseconds.encode(&[-1]).is_err());
        assert!(milliseconds.encode(&[0]).is_ok());

        let mut microseconds = encoder::<TimestampMicrosecondType>();
        assert!(microseconds.encode(&[-999_001]).is_ok());
        assert!(microseconds.encode(&[-999_000]).is_err());

        let mut nanoseconds = encoder::<TimestampNanosecondType>();
        assert!(nanoseconds.encode(&[-999_000_001]).is_ok());
        assert!(nanoseconds.encode(&[-999_000_000]).is_err());
    }
}
