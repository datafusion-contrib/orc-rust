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
//! - Values are hashed to a 64-bit base hash (ORC's Murmur3 hash64),
//!   split into two 32-bit hashes, and combined with `hash1 + i*hash2`
//!   for `numHashFunctions` (i starts at 1).
//! - A cleared bit means the value is **definitely absent**; set bits mean
//!   **possible presence** (false positives allowed).
//!
//! Bloom filters are attached to row groups and can quickly rule out equality
//! predicates (e.g. `col = 'abc'`) before any data decoding.

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
        // Ensure only one of bitset / utf8bitset is populated
        assert!(
            proto.bitset.is_empty() || proto.utf8bitset.is_none(),
            "Bloom filter proto has both bitset and utf8bitset populated"
        );

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

    #[cfg(test)]
    /// Create a Bloom filter from raw parts (mainly for tests)
    pub fn from_parts(num_hash_functions: u32, bitset: Vec<u64>) -> Self {
        Self {
            num_hash_functions: num_hash_functions.max(1),
            bitset,
        }
    }

    /// Set bits for the provided 64-bit hash using ORC's double-hash scheme.
    pub fn add_hash(&mut self, hash64: u64) {
        let bit_count = self.bitset.len() * 64;
        if bit_count == 0 {
            return;
        }

        let hash1 = hash64 as u32 as i32;
        let hash2 = (hash64 >> 32) as u32 as i32;

        for i in 1..=self.num_hash_functions {
            let mut combined = hash1.wrapping_add((i as i32).wrapping_mul(hash2));
            if combined < 0 {
                combined = !combined;
            }
            let bit_idx = ((combined as u32 as u64) % (bit_count as u64)) as usize;
            self.bitset[bit_idx / 64] |= 1u64 << (bit_idx % 64);
        }
    }

    /// Returns true if the hash *might* be contained. False means *definitely not*.
    pub fn test_hash(&self, hash64: u64) -> bool {
        let bit_count = self.bitset.len() * 64;
        if bit_count == 0 {
            // Defensive: no bits means we cannot use the filter
            return true;
        }

        let hash1 = hash64 as u32 as i32;
        let hash2 = (hash64 >> 32) as u32 as i32;

        // Mirror ORC Java BloomFilter.addHash/testHash:
        // split 64-bit hash into two signed 32-bit ints, combine with i=1..k,
        // flip negative results, then modulo by bit count.
        for i in 1..=self.num_hash_functions {
            let mut combined = hash1.wrapping_add((i as i32).wrapping_mul(hash2));
            if combined < 0 {
                combined = !combined;
            }
            let bit_idx = ((combined as u32 as u64) % (bit_count as u64)) as usize;
            let word = bit_idx / 64;
            let bit = bit_idx % 64;
            let mask = 1u64 << bit;
            if self
                .bitset
                .get(word)
                .map_or(true, |bits| (bits & mask) == 0)
            {
                return false;
            }
        }

        true
    }

    pub(crate) fn hash_bytes(value: &[u8]) -> u64 {
        // ORC Java uses Murmur3.hash64 with a fixed seed (104729).
        murmur3_64_orc(value)
    }

    pub(crate) fn hash_long(value: i64) -> u64 {
        // Thomas Wang's 64-bit mix function, matching Java's signed long operations.
        let mut key = value;
        key = (!key).wrapping_add(key.wrapping_shl(21));
        key ^= key >> 24;
        key = key
            .wrapping_add(key.wrapping_shl(3))
            .wrapping_add(key.wrapping_shl(8));
        key ^= key >> 14;
        key = key
            .wrapping_add(key.wrapping_shl(2))
            .wrapping_add(key.wrapping_shl(4));
        key ^= key >> 28;
        key = key.wrapping_add(key.wrapping_shl(31));
        key as u64
    }
}

/// ORC's Murmur3 hash64 implementation (seed = 104729), used for Bloom filters.
fn murmur3_64_orc(bytes: &[u8]) -> u64 {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    const R1: u32 = 31;
    const R2: u32 = 27;
    const M: u64 = 5;
    const N1: u64 = 1_390_208_809;
    const SEED: u64 = 104_729;

    let mut h1 = SEED;
    let nblocks = bytes.len() / 8;

    for i in 0..nblocks {
        let start = i * 8;
        let mut k1 =
            u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap()).wrapping_mul(C1);
        k1 = k1.rotate_left(R1);
        k1 = k1.wrapping_mul(C2);

        h1 ^= k1;
        h1 = h1.rotate_left(R2);
        h1 = h1.wrapping_mul(M).wrapping_add(N1);
    }

    let mut k1 = 0u64;
    let tail = &bytes[nblocks * 8..];
    if tail.len() >= 7 {
        k1 ^= (tail[6] as u64) << 48;
    }
    if tail.len() >= 6 {
        k1 ^= (tail[5] as u64) << 40;
    }
    if tail.len() >= 5 {
        k1 ^= (tail[4] as u64) << 32;
    }
    if tail.len() >= 4 {
        k1 ^= (tail[3] as u64) << 24;
    }
    if tail.len() >= 3 {
        k1 ^= (tail[2] as u64) << 16;
    }
    if tail.len() >= 2 {
        k1 ^= (tail[1] as u64) << 8;
    }
    if !tail.is_empty() {
        k1 ^= tail[0] as u64;
    }

    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(R1);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= bytes.len() as u64;
    fmix64(h1)
}

/// Finalization mix function for Murmur3 64-bit.
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_filter(values: &[&[u8]], bitset_words: usize, hash_funcs: u32) -> BloomFilter {
        let mut filter = BloomFilter::from_parts(hash_funcs, vec![0u64; bitset_words]);
        for value in values {
            let hash64 = BloomFilter::hash_bytes(value);
            filter.add_hash(hash64);
        }
        filter
    }

    #[test]
    fn test_bloom_filter_hit_and_miss() {
        let filter = build_filter(&[b"abc", b"def"], 2, 3);

        let abc = BloomFilter::hash_bytes(b"abc");
        let xyz = BloomFilter::hash_bytes(b"xyz");
        assert!(filter.test_hash(abc));
        assert!(!filter.test_hash(xyz));
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
        let foo = BloomFilter::hash_bytes(b"foo");
        let bar = BloomFilter::hash_bytes(b"bar");
        assert!(decoded.test_hash(foo));
        assert!(!decoded.test_hash(bar));
    }

    #[test]
    fn test_might_contain_hash64() {
        let value = 42i64;
        let hash64 = BloomFilter::hash_long(value);

        let num_hash_functions = 3;
        let mut filter = BloomFilter::from_parts(num_hash_functions, vec![0u64; 2]);
        filter.add_hash(hash64);
        assert!(filter.test_hash(hash64));
        assert!(!filter.test_hash(BloomFilter::hash_long(value + 1)));
    }
}
