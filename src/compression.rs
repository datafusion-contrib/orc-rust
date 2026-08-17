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

// Modified from https://github.com/DataEngineeringLabs/orc-format/blob/416490db0214fc51d53289253c0ee91f7fc9bc17/src/read/decompress/mod.rs
//! Related code for handling compression and decompression of ORC files.

use std::{
    borrow::Cow,
    io::{Read, Write},
};

use bytes::{Bytes, BytesMut};
use fallible_streaming_iterator::FallibleStreamingIterator;
use snafu::ResultExt;

use crate::error::{self, OrcError, Result};
use crate::proto::{self, CompressionKind};

// Spec states default is 256K
const DEFAULT_COMPRESSION_BLOCK_SIZE: u64 = 256 * 1024;

/// Bits 1..23 of the 3-byte header store the payload length.
const MAX_COMPRESSION_BLOCK_SIZE: u64 = 1 << 23;

#[derive(Clone, Copy, Debug)]
pub struct Compression {
    compression_type: CompressionType,
    /// No compression chunk will decompress to larger than this size.
    /// Use to size the scratch buffer appropriately.
    max_decompressed_block_size: usize,
}
impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({} byte max block size)",
            self.compression_type, self.max_decompressed_block_size
        )
    }
}

impl Compression {
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    pub(crate) fn from_proto(
        kind: proto::CompressionKind,
        compression_block_size: Option<u64>,
    ) -> Option<Self> {
        let max_decompressed_block_size =
            compression_block_size.unwrap_or(DEFAULT_COMPRESSION_BLOCK_SIZE) as usize;
        match kind {
            CompressionKind::None => None,
            CompressionKind::Zlib => Some(Self {
                compression_type: CompressionType::Zlib,
                max_decompressed_block_size,
            }),
            CompressionKind::Snappy => Some(Self {
                compression_type: CompressionType::Snappy,
                max_decompressed_block_size,
            }),
            CompressionKind::Lzo => Some(Self {
                compression_type: CompressionType::Lzo,
                max_decompressed_block_size,
            }),
            CompressionKind::Lz4 => Some(Self {
                compression_type: CompressionType::Lz4,
                max_decompressed_block_size,
            }),
            CompressionKind::Zstd => Some(Self {
                compression_type: CompressionType::Zstd,
                max_decompressed_block_size,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CompressionType {
    Zlib,
    Snappy,
    Lzo,
    Lz4,
    Zstd,
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl CompressionType {
    pub(crate) fn to_proto(self) -> proto::CompressionKind {
        match self {
            Self::Zlib => proto::CompressionKind::Zlib,
            Self::Snappy => proto::CompressionKind::Snappy,
            Self::Lzo => proto::CompressionKind::Lzo,
            Self::Lz4 => proto::CompressionKind::Lz4,
            Self::Zstd => proto::CompressionKind::Zstd,
        }
    }
}

/// Indicates length of block and whether it's compressed or not.
#[derive(Debug, PartialEq, Eq)]
enum CompressionHeader {
    Original(u32),
    Compressed(u32),
}

/// ORC files are compressed in blocks, with a 3 byte header at the start
/// of these blocks indicating the length of the block and whether it's
/// compressed or not.
impl CompressionHeader {
    fn decode(bytes: [u8; 3]) -> Self {
        let bytes = [bytes[0], bytes[1], bytes[2], 0];
        let length_and_flag = u32::from_le_bytes(bytes);
        let is_original = length_and_flag & 1 == 1;
        let length = length_and_flag >> 1;
        if is_original {
            Self::Original(length)
        } else {
            Self::Compressed(length)
        }
    }

    fn encode(self) -> [u8; 3] {
        let (length, is_original) = match self {
            Self::Original(length) => (length, 1),
            Self::Compressed(length) => (length, 0),
        };
        debug_assert!((length as u64) < MAX_COMPRESSION_BLOCK_SIZE);
        let encoded = (length << 1) | is_original;
        let bytes = encoded.to_le_bytes();
        [bytes[0], bytes[1], bytes[2]]
    }
}

trait DecompressorVariant: Send {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
struct Zlib;
#[derive(Debug, Clone, Copy)]
struct Zstd;
#[derive(Debug, Clone, Copy)]
struct Snappy;
#[derive(Debug, Clone, Copy)]
struct Lzo;
#[derive(Debug, Clone, Copy)]
struct Lz4 {
    max_decompressed_block_size: usize,
}

impl DecompressorVariant for Zlib {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let mut gz = flate2::read::DeflateDecoder::new(compressed_bytes);
        scratch.clear();
        gz.read_to_end(scratch).context(error::IoSnafu)?;
        Ok(())
    }
}

impl DecompressorVariant for Zstd {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let mut reader =
            zstd::Decoder::new(compressed_bytes).context(error::BuildZstdDecoderSnafu)?;
        scratch.clear();
        reader.read_to_end(scratch).context(error::IoSnafu)?;
        Ok(())
    }
}

impl DecompressorVariant for Snappy {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let len =
            snap::raw::decompress_len(compressed_bytes).context(error::BuildSnappyDecoderSnafu)?;
        scratch.resize(len, 0);
        let mut decoder = snap::raw::Decoder::new();
        decoder
            .decompress(compressed_bytes, scratch)
            .context(error::BuildSnappyDecoderSnafu)?;
        Ok(())
    }
}

impl DecompressorVariant for Lzo {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let decompressed = lzokay_native::decompress_all(compressed_bytes, None)
            .context(error::BuildLzoDecoderSnafu)?;
        // TODO: better way to utilize scratch here
        scratch.clear();
        scratch.extend(decompressed);
        Ok(())
    }
}

impl DecompressorVariant for Lz4 {
    fn decompress_block(&self, compressed_bytes: &[u8], scratch: &mut Vec<u8>) -> Result<()> {
        let decompressed =
            lz4_flex::block::decompress(compressed_bytes, self.max_decompressed_block_size)
                .context(error::BuildLz4DecoderSnafu)?;
        // TODO: better way to utilize scratch here
        scratch.clear();
        scratch.extend(decompressed);
        Ok(())
    }
}

// TODO: push this earlier so we don't check this variant each time
fn get_decompressor_variant(
    Compression {
        compression_type,
        max_decompressed_block_size,
    }: Compression,
) -> Box<dyn DecompressorVariant> {
    match compression_type {
        CompressionType::Zlib => Box::new(Zlib),
        CompressionType::Snappy => Box::new(Snappy),
        CompressionType::Lzo => Box::new(Lzo),
        CompressionType::Lz4 => Box::new(Lz4 {
            max_decompressed_block_size,
        }),
        CompressionType::Zstd => Box::new(Zstd),
    }
}

enum State {
    Original(Bytes),
    Compressed(Vec<u8>),
}

struct DecompressorIter {
    stream: BytesMut,
    current: Option<State>, // when we have compression but the value is original
    compression: Option<Box<dyn DecompressorVariant>>,
    scratch: Vec<u8>,
}

impl DecompressorIter {
    fn new(stream: Bytes, compression: Option<Compression>, scratch: Vec<u8>) -> Self {
        Self {
            stream: BytesMut::from(stream.as_ref()),
            current: None,
            compression: compression.map(get_decompressor_variant),
            scratch,
        }
    }
}

impl FallibleStreamingIterator for DecompressorIter {
    type Item = [u8];

    type Error = OrcError;

    #[inline]
    fn advance(&mut self) -> Result<(), Self::Error> {
        if self.stream.is_empty() {
            self.current = None;
            return Ok(());
        }

        match &self.compression {
            Some(compression) => {
                // TODO: take stratch from current State::Compressed for re-use
                let header = self.stream.split_to(3);
                let header = [header[0], header[1], header[2]];
                match CompressionHeader::decode(header) {
                    CompressionHeader::Original(length) => {
                        let original = self.stream.split_to(length as usize);
                        self.current = Some(State::Original(original.into()));
                    }
                    CompressionHeader::Compressed(length) => {
                        let compressed = self.stream.split_to(length as usize);
                        compression.decompress_block(&compressed, &mut self.scratch)?;
                        self.current = Some(State::Compressed(std::mem::take(&mut self.scratch)));
                    }
                };
                Ok(())
            }
            None => {
                // TODO: take stratch from current State::Compressed for re-use
                self.current = Some(State::Original(self.stream.clone().into()));
                self.stream.clear();
                Ok(())
            }
        }
    }

    #[inline]
    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref().map(|x| match x {
            State::Original(x) => x.as_ref(),
            State::Compressed(x) => x.as_ref(),
        })
    }
}

/// A [`Read`]er fulfilling the ORC specification of reading compressed data.
pub struct Decompressor {
    decompressor: DecompressorIter,
    offset: usize,
    is_first: bool,
}

impl Decompressor {
    /// Creates a new [`Decompressor`] that will use `scratch` as a temporary region.
    pub fn new(stream: Bytes, compression: Option<Compression>, scratch: Vec<u8>) -> Self {
        Self {
            decompressor: DecompressorIter::new(stream, compression, scratch),
            offset: 0,
            is_first: true,
        }
    }

    // TODO: remove need for this upstream
    pub fn empty() -> Self {
        Self {
            decompressor: DecompressorIter::new(Bytes::new(), None, vec![]),
            offset: 0,
            is_first: true,
        }
    }
}

impl std::io::Read for Decompressor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.is_first {
            self.is_first = false;
            self.decompressor.advance().unwrap();
        }
        let current = self.decompressor.get();
        let current = if let Some(current) = current {
            if current.len() == self.offset {
                self.decompressor.advance().unwrap();
                self.offset = 0;
                let current = self.decompressor.get();
                if let Some(current) = current {
                    current
                } else {
                    return Ok(0);
                }
            } else {
                &current[self.offset..]
            }
        } else {
            return Ok(0);
        };

        if current.len() >= buf.len() {
            buf.copy_from_slice(&current[..buf.len()]);
            self.offset += buf.len();
            Ok(buf.len())
        } else {
            buf[..current.len()].copy_from_slice(current);
            self.offset += current.len();
            Ok(current.len())
        }
    }
}

trait CompressorVariant: Send {
    fn compress_block(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()>;
}

struct ZlibCompressor;

impl CompressorVariant for ZlibCompressor {
    fn compress_block(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        let mut encoder =
            flate2::write::DeflateEncoder::new(output, flate2::Compression::default());
        encoder.write_all(input).context(error::IoSnafu)?;
        encoder.finish().context(error::IoSnafu)?;
        Ok(())
    }
}

struct SnappyCompressor(snap::raw::Encoder);

impl CompressorVariant for SnappyCompressor {
    fn compress_block(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.resize(snap::raw::max_compress_len(input.len()), 0);
        let written = self
            .0
            .compress(input, output)
            .context(error::CompressSnappySnafu)?;
        output.truncate(written);
        Ok(())
    }
}

struct Lz4Compressor;

impl CompressorVariant for Lz4Compressor {
    fn compress_block(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.resize(lz4_flex::block::get_maximum_output_size(input.len()), 0);
        let written =
            lz4_flex::block::compress_into(input, output).context(error::CompressLz4Snafu)?;
        output.truncate(written);
        Ok(())
    }
}

struct ZstdCompressor(zstd::bulk::Compressor<'static>);

impl CompressorVariant for ZstdCompressor {
    fn compress_block(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        output.reserve(zstd::zstd_safe::compress_bound(input.len()));
        self.0
            .compress_to_buffer(input, output)
            .context(error::IoSnafu)?;
        Ok(())
    }
}

fn get_compressor_variant(compression: CompressionType) -> Result<Box<dyn CompressorVariant>> {
    match compression {
        CompressionType::Zlib => Ok(Box::new(ZlibCompressor)),
        CompressionType::Snappy => Ok(Box::new(SnappyCompressor(snap::raw::Encoder::new()))),
        CompressionType::Lz4 => Ok(Box::new(Lz4Compressor)),
        CompressionType::Zstd => Ok(Box::new(ZstdCompressor(
            zstd::bulk::Compressor::new(0).context(error::IoSnafu)?,
        ))),
        CompressionType::Lzo => error::UnexpectedSnafu {
            msg: "LZO compression is not supported by the ORC writer",
        }
        .fail(),
    }
}

pub(crate) struct Compressor {
    compression: Option<CompressionType>,
    block_size: Option<usize>,
    compressor: Option<Box<dyn CompressorVariant>>,
    scratch: Vec<u8>,
}

impl Compressor {
    pub(crate) fn new(
        compression: Option<CompressionType>,
        block_size: Option<usize>,
    ) -> Result<Self> {
        if let Some(block_size) = block_size {
            if block_size == 0 || (block_size as u64) >= MAX_COMPRESSION_BLOCK_SIZE {
                return error::UnexpectedSnafu {
                    msg: format!(
                        "compression block size must be in 1..{}, got {}",
                        MAX_COMPRESSION_BLOCK_SIZE, block_size
                    ),
                }
                .fail();
            }
        }

        let (block_size, compressor) = match compression {
            Some(compression) => {
                let block_size = block_size.unwrap_or(DEFAULT_COMPRESSION_BLOCK_SIZE as usize);
                (Some(block_size), Some(get_compressor_variant(compression)?))
            }
            None => (None, None),
        };
        Ok(Self {
            compression,
            block_size,
            compressor,
            scratch: Vec::new(),
        })
    }

    pub(crate) fn compress<'a>(&mut self, input: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        let Some(compressor) = self.compressor.as_mut() else {
            return Ok(Cow::Borrowed(input));
        };
        let block_size = self.block_size.expect("enabled compressor has block size");
        let mut output = Vec::with_capacity(input.len());
        for block in input.chunks(block_size) {
            self.scratch.clear();
            compressor.compress_block(block, &mut self.scratch)?;

            let (payload, header) = if self.scratch.len() < block.len() {
                (
                    self.scratch.as_slice(),
                    CompressionHeader::Compressed(self.scratch.len() as u32),
                )
            } else {
                (block, CompressionHeader::Original(block.len() as u32))
            };
            output.extend_from_slice(&header.encode());
            output.extend_from_slice(payload);
        }
        Ok(Cow::Owned(output))
    }

    pub(crate) fn compression(&self) -> Option<CompressionType> {
        self.compression
    }

    pub(crate) fn block_size(&self) -> Option<usize> {
        self.block_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_header_roundtrip() {
        for (expected, bytes) in [
            (CompressionHeader::Original(5), [0x0b, 0x00, 0x00]),
            (CompressionHeader::Compressed(100_000), [0x40, 0x0d, 0x03]),
            (
                CompressionHeader::Original((MAX_COMPRESSION_BLOCK_SIZE - 1) as u32),
                [0xff, 0xff, 0xff],
            ),
        ] {
            let decoded = CompressionHeader::decode(bytes);
            assert_eq!(decoded, expected);
            assert_eq!(decoded.encode(), bytes);
        }
    }

    #[test]
    fn rejects_invalid_writer_compression() {
        assert!(Compressor::new(Some(CompressionType::Zstd), Some(0)).is_err());
        assert!(Compressor::new(
            Some(CompressionType::Zstd),
            Some(MAX_COMPRESSION_BLOCK_SIZE as usize),
        )
        .is_err());
        assert!(Compressor::new(Some(CompressionType::Lzo), None).is_err());
    }

    #[test]
    fn handles_empty_original_and_chunked_blocks() {
        let mut disabled = Compressor::new(None, None).unwrap();
        let encoded = disabled.compress(b"uncompressed").unwrap();
        assert!(matches!(encoded, Cow::Borrowed(b"uncompressed")));
        assert!(disabled.compression().is_none());
        assert!(disabled.block_size().is_none());

        let mut compressor = Compressor::new(Some(CompressionType::Zstd), Some(4)).unwrap();
        assert!(compressor.compress(&[]).unwrap().is_empty());

        for compression_type in [
            CompressionType::Zlib,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let mut compressor = Compressor::new(Some(compression_type), Some(64)).unwrap();
            let encoded = compressor.compress(b"x").unwrap();
            assert_eq!(encoded.as_ref(), [3, 0, 0, b'x']);
        }

        let encoded = compressor.compress(b"abcdefghi").unwrap();
        assert_eq!(
            encoded.as_ref(),
            [9, 0, 0, b'a', b'b', b'c', b'd', 9, 0, 0, b'e', b'f', b'g', b'h', 3, 0, 0, b'i',]
        );
    }

    #[test]
    fn roundtrips_all_writer_codecs() {
        let input = vec![0; 1024];
        for compression_type in [
            CompressionType::Zlib,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let mut compressor = Compressor::new(Some(compression_type), Some(64)).unwrap();
            let encoded = compressor.compress(&input).unwrap();
            let header = CompressionHeader::decode([encoded[0], encoded[1], encoded[2]]);
            assert!(
                matches!(header, CompressionHeader::Compressed(length) if length < 64),
                "{compression_type} unexpectedly stored a compressible block as original"
            );

            let compression = Compression::from_proto(compression_type.to_proto(), Some(64));
            let mut decoded = Vec::new();
            Decompressor::new(Bytes::from(encoded.into_owned()), compression, Vec::new())
                .read_to_end(&mut decoded)
                .unwrap();
            assert_eq!(decoded, input);
        }
    }
}
