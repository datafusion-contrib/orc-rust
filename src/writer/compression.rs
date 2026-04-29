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

//! Writer-side compression. Implements the ORC spec's per-chunk framing
//! where each chunk carries a 3-byte little-endian header encoding the
//! chunk length plus an "is original (uncompressed)" flag. When the
//! codec output is no smaller than the input, the chunk is emitted in
//! its original (uncompressed) form with the flag bit set, matching the
//! reference Java ORC writer's behaviour
//! (`org.apache.orc.impl.PhysicalFsWriter`).
//!
//! See: <https://orc.apache.org/specification/ORCv1/#compression>
//!
//! The same chunked format is consumed by [`crate::compression`] on the
//! read path.

use std::io::Write;

use snafu::ResultExt;

use crate::error::{IoSnafu, Result, SnappyEncodeSnafu, ZstdEncodeSnafu};
use crate::proto;

/// Default per-chunk compression block size, per the ORC spec ("default
/// is 256K"). Matches Java ORC's `OrcConf.BUFFER_SIZE` default.
pub const DEFAULT_COMPRESSION_BLOCK_SIZE: usize = 256 * 1024;

/// Default ZLIB level. Matches `flate2::Compression::default()` and the
/// Java ORC writer (which defaults to DEFLATE level 6).
pub const DEFAULT_ZLIB_LEVEL: u32 = 6;

/// Default ZSTD level. Matches `zstd`'s `0` (which the C library
/// translates to its "default", currently level 3) and the Java ORC
/// writer's `orc.compress.zstd.level` default of 3.
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Writer-side compression codec selection.
///
/// `None` is the default and produces byte-identical output to a
/// pre-compression-feature build of orc-rust. The other variants emit
/// per-chunk compressed streams matching the ORC v1 spec, so files
/// produced here are readable by every conformant ORC consumer
/// (including Java ORC, DuckDB, Spark, and orc-rust's own reader).
///
/// Levels are clamped to each codec's valid range. ZSTD accepts negative
/// levels (faster than level 1, lower ratio) and levels above 19 require
/// the `zstd-long` distant-match window.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Compression {
    /// No compression; writer output is byte-identical to the
    /// pre-compression-feature behaviour.
    #[default]
    None,
    /// Snappy block compression. Fast, modest ratio. The de-facto
    /// default for Hive / Trino ORC tables.
    Snappy,
    /// Raw DEFLATE (the "ZLIB" codec name in the ORC spec is a
    /// misnomer — the on-disk format is a raw DEFLATE bitstream with
    /// no zlib container, matching what the Java reader expects).
    Zlib {
        /// Compression level, 0 (fastest) – 9 (best). Default: 6.
        level: u32,
    },
    /// ZSTD compression.
    Zstd {
        /// Compression level. The `zstd` crate accepts -7 ..= 22. Levels
        /// > 19 implicitly enable long-range matching. Default: 3.
        level: i32,
    },
}

impl Compression {
    /// Convenience constructor for ZLIB at the default level (6).
    pub fn zlib() -> Self {
        Self::Zlib {
            level: DEFAULT_ZLIB_LEVEL,
        }
    }

    /// Convenience constructor for ZSTD at the default level (3).
    pub fn zstd() -> Self {
        Self::Zstd {
            level: DEFAULT_ZSTD_LEVEL,
        }
    }

    /// Map onto the wire-level [`proto::CompressionKind`] that goes into
    /// the file PostScript.
    pub(crate) fn kind(self) -> proto::CompressionKind {
        match self {
            Self::None => proto::CompressionKind::None,
            Self::Snappy => proto::CompressionKind::Snappy,
            Self::Zlib { .. } => proto::CompressionKind::Zlib,
            Self::Zstd { .. } => proto::CompressionKind::Zstd,
        }
    }

    /// Returns `true` when this codec actually wraps payloads in
    /// compression chunks (everything except [`Compression::None`]).
    pub(crate) fn is_active(self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Encode the 3-byte ORC compression chunk header.
///
/// Layout (little-endian, from the ORC v1 spec):
/// > Each compression chunk consists of a 3-byte header followed by the
/// > compressed (or original, see below) bytes. The 3-byte header is a
/// > little-endian unsigned integer. The bottom bit is set if the chunk
/// > is original (uncompressed) and clear if compressed. The remaining
/// > 23 bits encode the chunk's payload length in bytes.
///
/// `length` MUST fit in 23 bits (≤ 8 388 607). Higher-level callers
/// guarantee this by chunking the input on `block_size` boundaries.
fn write_header(out: &mut Vec<u8>, length: usize, original: bool) {
    debug_assert!(
        length < (1 << 23),
        "compression chunk length {length} exceeds 23-bit limit"
    );
    let flag = u32::from(original);
    let value = ((length as u32) << 1) | flag;
    out.push((value & 0xff) as u8);
    out.push(((value >> 8) & 0xff) as u8);
    out.push(((value >> 16) & 0xff) as u8);
}

/// Codec-specific compression of one chunk's payload. Returns the
/// compressed bytes; the caller decides whether to emit them or fall
/// back to the original payload.
fn encode_chunk(compression: Compression, chunk: &[u8]) -> Result<Vec<u8>> {
    match compression {
        Compression::None => unreachable!("encode_chunk called for Compression::None"),
        Compression::Snappy => {
            let mut encoder = snap::raw::Encoder::new();
            encoder.compress_vec(chunk).context(SnappyEncodeSnafu)
        }
        Compression::Zlib { level } => {
            let mut encoder = flate2::write::DeflateEncoder::new(
                Vec::with_capacity(chunk.len()),
                flate2::Compression::new(level.min(9)),
            );
            encoder.write_all(chunk).context(IoSnafu)?;
            encoder.finish().context(IoSnafu)
        }
        Compression::Zstd { level } => {
            zstd::stream::encode_all(chunk, level).context(ZstdEncodeSnafu)
        }
    }
}

/// Write one chunk to `out`, choosing between the compressed payload and
/// the original payload per the ORC spec rule: "if the compressed data
/// is no smaller than the original, the original is written instead and
/// the chunk is flagged as original."
fn write_chunk(compression: Compression, chunk: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let compressed = encode_chunk(compression, chunk)?;
    if compressed.len() >= chunk.len() {
        write_header(out, chunk.len(), /* original = */ true);
        out.extend_from_slice(chunk);
    } else {
        write_header(out, compressed.len(), /* original = */ false);
        out.extend_from_slice(&compressed);
    }
    Ok(())
}

/// Compress an entire stream body into a `Vec<u8>` of concatenated ORC
/// compression chunks. The input is split into `block_size`-byte
/// windows and each window is independently compressed (or emitted as
/// original) per the spec.
///
/// `block_size` MUST be > 0 and MUST fit in 23 bits — both invariants
/// are enforced by the public API in [`crate::arrow_writer`].
pub(crate) fn compress_stream(
    compression: Compression,
    block_size: usize,
    payload: &[u8],
) -> Result<Vec<u8>> {
    debug_assert!(compression.is_active());
    debug_assert!(block_size > 0 && block_size < (1 << 23));
    if payload.is_empty() {
        // ORC streams of zero length carry no chunks at all; the
        // reader's `DecompressorIter::advance` short-circuits on empty
        // input. Mirror that here so the read/write paths agree.
        return Ok(Vec::new());
    }
    // Conservative pre-allocation — every chunk gains 3 header bytes.
    let n_chunks = payload.len().div_ceil(block_size);
    let mut out = Vec::with_capacity(payload.len() + 3 * n_chunks);
    for chunk in payload.chunks(block_size) {
        write_chunk(compression, chunk, &mut out)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    /// Mirror of [`crate::compression::decode_header`] for tests, kept
    /// local so this module stays self-contained.
    fn decode_header(bytes: [u8; 3]) -> (usize, bool) {
        let v = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
        let original = v & 1 == 1;
        let length = (v >> 1) as usize;
        (length, original)
    }

    /// Iterate decoded chunks (length, is_original, payload slice) so
    /// the spec-conformance tests can walk the encoded byte stream.
    fn decode_chunks(bytes: &[u8]) -> Vec<(usize, bool, &[u8])> {
        let mut out = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            let header = [bytes[offset], bytes[offset + 1], bytes[offset + 2]];
            let (length, original) = decode_header(header);
            let payload = &bytes[offset + 3..offset + 3 + length];
            out.push((length, original, payload));
            offset += 3 + length;
        }
        assert_eq!(offset, bytes.len(), "chunks must consume all bytes");
        out
    }

    #[test]
    fn header_round_trip() {
        for (length, original) in [
            (0usize, true),
            (1, false),
            (5, true),
            (100_000, false),
            ((1 << 23) - 1, true),
        ] {
            let mut out = Vec::new();
            write_header(&mut out, length, original);
            assert_eq!(out.len(), 3, "header is always 3 bytes");
            let (decoded_len, decoded_orig) = decode_header([out[0], out[1], out[2]]);
            assert_eq!(decoded_len, length);
            assert_eq!(decoded_orig, original);
        }
    }

    /// Known-answer test — the spec reference example: 100 000 bytes
    /// compressed renders as `[0x40, 0x0d, 0x03]`. This is the same
    /// vector the reader-path test in `crate::compression` decodes, so
    /// writer + reader agree on the wire format.
    #[test]
    fn header_kat_matches_reader_decode() {
        let mut out = Vec::new();
        write_header(&mut out, 100_000, /* original = */ false);
        assert_eq!(out, vec![0x40, 0x0d, 0x03]);
    }

    /// 5 bytes uncompressed must serialise to `[0x0b, 0x00, 0x00]`,
    /// matching the existing reader unit test.
    #[test]
    fn header_kat_uncompressed_5_bytes() {
        let mut out = Vec::new();
        write_header(&mut out, 5, /* original = */ true);
        assert_eq!(out, vec![0x0b, 0x00, 0x00]);
    }

    #[test]
    fn empty_stream_emits_no_chunks() {
        let out =
            compress_stream(Compression::Snappy, DEFAULT_COMPRESSION_BLOCK_SIZE, &[]).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn snappy_roundtrip_via_reader_decoder() {
        let payload = vec![b'a'; 4096];
        let compressed = compress_stream(
            Compression::Snappy,
            DEFAULT_COMPRESSION_BLOCK_SIZE,
            &payload,
        )
        .unwrap();
        let chunks = decode_chunks(&compressed);
        assert_eq!(chunks.len(), 1, "single block fits in one chunk");
        let (_, original, body) = chunks[0];
        assert!(!original, "highly redundant payload should compress");
        let decoded = snap::raw::Decoder::new().decompress_vec(body).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn zlib_roundtrip_via_reader_decoder() {
        let payload = vec![b'x'; 8192];
        let compressed = compress_stream(
            Compression::Zlib { level: 6 },
            DEFAULT_COMPRESSION_BLOCK_SIZE,
            &payload,
        )
        .unwrap();
        let chunks = decode_chunks(&compressed);
        assert_eq!(chunks.len(), 1);
        let (_, original, body) = chunks[0];
        assert!(!original);
        let mut decoder = flate2::read::DeflateDecoder::new(body);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn zstd_roundtrip_via_reader_decoder() {
        let payload = vec![b'y'; 8192];
        let compressed = compress_stream(
            Compression::Zstd { level: 3 },
            DEFAULT_COMPRESSION_BLOCK_SIZE,
            &payload,
        )
        .unwrap();
        let chunks = decode_chunks(&compressed);
        assert_eq!(chunks.len(), 1);
        let (_, original, body) = chunks[0];
        assert!(!original);
        let decoded = zstd::stream::decode_all(body).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn zstd_high_level_round_trip() {
        let payload = vec![b'q'; 16_384];
        let compressed = compress_stream(
            Compression::Zstd { level: 19 },
            DEFAULT_COMPRESSION_BLOCK_SIZE,
            &payload,
        )
        .unwrap();
        let chunks = decode_chunks(&compressed);
        let mut reconstructed = Vec::new();
        for (_, original, body) in chunks {
            if original {
                reconstructed.extend_from_slice(body);
            } else {
                reconstructed.extend(zstd::stream::decode_all(body).unwrap());
            }
        }
        assert_eq!(reconstructed, payload);
    }

    /// Per the spec: "if the compressed data is no smaller than the
    /// original, the original is written instead and the chunk is
    /// flagged as original." Verified with a one-byte payload — every
    /// codec adds framing overhead that exceeds 1 byte.
    #[test]
    fn original_fallback_when_compression_would_expand() {
        for codec in [
            Compression::Snappy,
            Compression::Zlib { level: 6 },
            Compression::Zstd { level: 3 },
        ] {
            let payload = vec![0xABu8];
            let compressed =
                compress_stream(codec, DEFAULT_COMPRESSION_BLOCK_SIZE, &payload).unwrap();
            let (length, original) = decode_header([compressed[0], compressed[1], compressed[2]]);
            assert!(
                original,
                "codec {codec:?} must fall back to original on incompressible input"
            );
            assert_eq!(length, 1);
            assert_eq!(&compressed[3..3 + length], &payload[..]);
        }
    }

    #[test]
    fn block_size_chunks_input_into_multiple_frames() {
        let payload = vec![b'z'; 5_000];
        let compressed = compress_stream(Compression::Snappy, 1024, &payload).unwrap();
        let chunks = decode_chunks(&compressed);
        // 5000 / 1024 → ceil = 5 chunks (1024, 1024, 1024, 1024, 904).
        assert_eq!(chunks.len(), 5);
        let chunk_lens: Vec<usize> = chunks
            .iter()
            .map(|(_, original, body)| if *original { body.len() } else { 1024 })
            .collect();
        // We can't easily assert decoded chunk size when compressed
        // (snappy may have shrunk one), but every chunk's *original*
        // size must fit in [1, 1024]. Verify boundary: the first 4
        // chunks each represented exactly 1024 bytes of input.
        for (i, expected_input_size) in [1024, 1024, 1024, 1024, 904].iter().enumerate() {
            let (_, original, body) = chunks[i];
            let input_size = if original {
                body.len()
            } else {
                snap::raw::Decoder::new()
                    .decompress_vec(body)
                    .unwrap()
                    .len()
            };
            assert_eq!(
                input_size, *expected_input_size,
                "chunk {i} wraps {expected_input_size} input bytes"
            );
            let _ = chunk_lens; // silence unused
        }
    }

    /// Defence in depth — even if a future refactor accidentally feeds
    /// a `Compression::None` payload through `compress_stream`, the
    /// debug assertion will trip in tests.
    #[test]
    #[should_panic = "compression"]
    #[cfg(debug_assertions)]
    fn compress_stream_panics_on_compression_none_in_debug() {
        let _ = compress_stream(Compression::None, DEFAULT_COMPRESSION_BLOCK_SIZE, b"x");
    }
}
