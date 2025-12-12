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

//! ORC Bloom filter decoding and evaluation.
//!
//! This follows the ORC v1 spec (https://orc.apache.org/specification/ORCv1/):
//! - Stream kinds `BLOOM_FILTER` / `BLOOM_FILTER_UTF8` provide per-row-group filters.
//! - Bits are set using Murmur3 x64_128 with seed 0, deriving h1/h2 and the
//!   double-hash sequence `h1 + i*h2 (mod m)` for `numHashFunctions`.
//! - A cleared bit means the value is **definitely absent**; set bits mean
//!   **possible presence** (false positives allowed).
//!
//! Bloom filters are attached to row groups and can quickly rule out equality
//! predicates (e.g. `col = 'abc'`) before any data decoding.

use murmur3::murmur3_x64_128;

use crate::proto;

/// A Bloom filter parsed from the ORC index stream.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    num_hash_functions: u32,
    bitset: Vec<u64>,
}

impl BloomFilter {
    /// Create a Bloom filter from a decoded protobuf value.
    pub fn try_from_proto(proto: &proto::BloomFilter) -> Option<Self> {
        let num_hash_functions = proto.num_hash_functions();
        if proto.bitset.is_empty() && proto.utf8bitset.is_none() {
            return None;
        }

        let bitset = if !proto.bitset.is_empty() {
            proto.bitset.clone()
        } else {
            // utf8bitset is encoded as bytes; convert to u64 words (little-endian)
            proto
                .utf8bitset
                .as_ref()
                .map(|bytes| {
                    bytes
                        .chunks(8)
                        .map(|chunk| {
                            let mut padded = [0u8; 8];
                            for (idx, value) in chunk.iter().enumerate() {
                                padded[idx] = *value;
                            }
                            u64::from_le_bytes(padded)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        Some(Self {
            num_hash_functions: if num_hash_functions == 0 {
                // Writers are expected to set this, but default to a safe value
                3
            } else {
                num_hash_functions
            },
            bitset,
        })
    }

    /// Create a Bloom filter from raw parts (mainly for tests)
    pub fn from_parts(num_hash_functions: u32, bitset: Vec<u64>) -> Self {
        Self {
            num_hash_functions: num_hash_functions.max(1),
            bitset,
        }
    }

    /// Returns true if the value *might* be contained. False means *definitely not*.
    pub fn might_contain(&self, value: &[u8]) -> bool {
        let bit_count = self.bitset.len() * 64;
        if bit_count == 0 {
            // Defensive: no bits means we cannot use the filter
            return true;
        }

        let hash = self.hash128(value);
        let h1 = hash as u64;
        let h2 = (hash >> 64) as u64;

        for i in 0..self.num_hash_functions {
            // ORC uses the standard double-hash scheme: h1 + i*h2 (mod m)
            let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let bit_idx = (combined % (bit_count as u64)) as usize;
            if !self.test_bit(bit_idx) {
                return false;
            }
        }

        true
    }

    fn hash128(&self, value: &[u8]) -> u128 {
        // The ORC specification uses Murmur3 (64-bit) for bloom filters.
        // murmur3_x64_128 matches the Java reference implementation, where
        // the lower 64 bits are treated as h1 and the upper 64 bits as h2.
        let mut cursor = std::io::Cursor::new(value);
        murmur3_x64_128(&mut cursor, 0).unwrap_or(0)
    }

    fn test_bit(&self, bit_idx: usize) -> bool {
        let word = bit_idx / 64;
        let bit = bit_idx % 64;
        if let Some(bits) = self.bitset.get(word) {
            (bits & (1u64 << bit)) != 0
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_filter(values: &[&[u8]], bitset_words: usize, hash_funcs: u32) -> BloomFilter {
        let mut bitset = vec![0u64; bitset_words];
        let bit_count = bitset_words * 64;

        for value in values {
            let mut cursor = std::io::Cursor::new(*value);
            let hash = murmur3_x64_128(&mut cursor, 0).unwrap();
            let h1 = hash as u64;
            let h2 = (hash >> 64) as u64;
            for i in 0..hash_funcs {
                let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
                let bit_idx = (combined % (bit_count as u64)) as usize;
                bitset[bit_idx / 64] |= 1u64 << (bit_idx % 64);
            }
        }

        BloomFilter::from_parts(hash_funcs, bitset)
    }

    #[test]
    fn test_bloom_filter_hit_and_miss() {
        let filter = build_filter(&[b"abc", b"def"], 2, 3);

        assert!(filter.might_contain(b"abc"));
        assert!(!filter.might_contain(b"xyz"));
    }

    #[test]
    fn test_try_from_proto_utf8_bitset() {
        let filter = build_filter(&[b"foo"], 1, 2);

        let proto = proto::BloomFilter {
            num_hash_functions: Some(filter.num_hash_functions),
            bitset: vec![],
            utf8bitset: Some(filter.bitset.iter().flat_map(|w| w.to_le_bytes()).collect()),
        };

        let decoded = BloomFilter::try_from_proto(&proto).unwrap();
        assert!(decoded.might_contain(b"foo"));
        assert!(!decoded.might_contain(b"bar"));
    }
}
