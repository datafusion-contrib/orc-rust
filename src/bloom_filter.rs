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

//! Bloom Filter support for ORC files
//!
//! This module provides Bloom Filter parsing and querying functionality.
//! Bloom Filters are used to optimize equality queries by quickly determining
//! if a value might exist in a row group, allowing entire row groups to be
//! skipped when the filter indicates the value is definitely not present.

use crate::error::{Result, UnexpectedSnafu};
use crate::predicate::PredicateValue;

/// A Bloom Filter for a single row group
///
/// According to ORC spec:
/// - Bloom Filters use Murmur3 64-bit hash for strings/binary
/// - For numeric types, use Thomas Wang's 64-bit integer hash
/// - Each row group has its own Bloom Filter
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Number of hash functions used (k)
    num_hash_functions: u32,
    /// Bit set for non-UTF8 values (fixed64 array)
    bitset: Vec<u64>,
    /// Bit set for UTF8 strings (bytes)
    utf8_bitset: Option<Vec<u8>>,
}

impl BloomFilter {
    /// Create a new Bloom Filter from protobuf message
    pub fn from_proto(proto: &crate::proto::BloomFilter) -> Result<Self> {
        let num_hash_functions = proto.num_hash_functions.unwrap_or(0);
        let bitset = proto.bitset.clone();
        let utf8_bitset = proto.utf8bitset.clone();

        Ok(Self {
            num_hash_functions,
            bitset,
            utf8_bitset,
        })
    }

    /// Check if a value might be present in the Bloom Filter
    ///
    /// Returns `true` if the value might be present (could be false positive),
    /// `false` if the value is definitely not present.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to check
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Value might be present
    /// * `Ok(false)` - Value is definitely not present
    /// * `Err` - If the filter is invalid or value type is unsupported
    pub fn might_contain(&self, value: &PredicateValue) -> Result<bool> {
        if self.num_hash_functions == 0 {
            // Empty filter, can't determine
            return Ok(true);
        }

        match value {
            PredicateValue::Utf8(Some(s)) => self.might_contain_string(s),
            PredicateValue::Int8(Some(v)) => self.might_contain_int64(*v as i64),
            PredicateValue::Int16(Some(v)) => self.might_contain_int64(*v as i64),
            PredicateValue::Int32(Some(v)) => self.might_contain_int64(*v as i64),
            PredicateValue::Int64(Some(v)) => self.might_contain_int64(*v),
            PredicateValue::Float32(Some(v)) => self.might_contain_int64(v.to_bits() as i64),
            PredicateValue::Float64(Some(v)) => self.might_contain_int64(v.to_bits() as i64),
            PredicateValue::Boolean(Some(v)) => self.might_contain_int64(if *v { 1 } else { 0 }),
            _ => {
                // NULL values or unsupported types
                Ok(true)
            }
        }
    }

    /// Check if a string value might be present
    fn might_contain_string(&self, value: &str) -> Result<bool> {
        if let Some(ref utf8_bitset) = self.utf8_bitset {
            // Use UTF8 bitset
            self.check_utf8_bitset(utf8_bitset, value.as_bytes())
        } else if !self.bitset.is_empty() {
            // Fall back to regular bitset with string hash
            self.check_bitset(&self.bitset, murmur3_64(value.as_bytes()))
        } else {
            // No bitset available
            Ok(true)
        }
    }

    /// Check if an integer value might be present
    fn might_contain_int64(&self, value: i64) -> Result<bool> {
        if self.bitset.is_empty() {
            return Ok(true);
        }

        // Use Thomas Wang's 64-bit integer hash
        let hash = thomas_wang_hash64(value);
        self.check_bitset(&self.bitset, hash)
    }

    /// Check if all hash positions are set in the bitset
    fn check_bitset(&self, bitset: &[u64], hash: u64) -> Result<bool> {
        let num_bits = bitset.len() * 64;
        if num_bits == 0 {
            return Ok(true);
        }

        // Generate k hash values from the base hash
        for i in 0..self.num_hash_functions {
            let hash_value = hash.wrapping_add(i as u64).wrapping_mul(0x9e3779b97f4a7c15);
            let bit_pos = (hash_value % (num_bits as u64)) as usize;
            let word_idx = bit_pos / 64;
            let bit_idx = bit_pos % 64;

            if word_idx >= bitset.len() {
                return Err(UnexpectedSnafu {
                    msg: format!(
                        "Bloom filter bit position out of bounds: {} >= {}",
                        word_idx,
                        bitset.len()
                    ),
                }
                .build());
            }

            let bit = (bitset[word_idx] >> bit_idx) & 1;
            if bit == 0 {
                // At least one hash position is not set, value is definitely not present
                return Ok(false);
            }
        }

        // All hash positions are set, value might be present
        Ok(true)
    }

    /// Check UTF8 bitset (byte array)
    fn check_utf8_bitset(&self, bitset: &[u8], value: &[u8]) -> Result<bool> {
        let num_bits = bitset.len() * 8;
        if num_bits == 0 {
            return Ok(true);
        }

        // Use Murmur3 hash for strings
        let hash = murmur3_64(value);

        // Generate k hash values from the base hash
        for i in 0..self.num_hash_functions {
            let hash_value = hash.wrapping_add(i as u64).wrapping_mul(0x9e3779b97f4a7c15);
            let bit_pos = (hash_value % (num_bits as u64)) as usize;
            let byte_idx = bit_pos / 8;
            let bit_idx = bit_pos % 8;

            if byte_idx >= bitset.len() {
                return Err(UnexpectedSnafu {
                    msg: format!(
                        "Bloom filter UTF8 bit position out of bounds: {} >= {}",
                        byte_idx,
                        bitset.len()
                    ),
                }
                .build());
            }

            let bit = (bitset[byte_idx] >> bit_idx) & 1;
            if bit == 0 {
                // At least one hash position is not set, value is definitely not present
                return Ok(false);
            }
        }

        // All hash positions are set, value might be present
        Ok(true)
    }
}

/// Thomas Wang's 64-bit integer hash function
///
/// This is used for numeric types in ORC Bloom Filters.
fn thomas_wang_hash64(key: i64) -> u64 {
    let mut key = key as u64;
    key = (!key).wrapping_add(key << 21);
    key = key ^ (key >> 24);
    key = (key.wrapping_add(key << 3)).wrapping_add(key << 8);
    key = key ^ (key >> 14);
    key = (key.wrapping_add(key << 2)).wrapping_add(key << 4);
    key = key ^ (key >> 28);
    key = key.wrapping_add(key << 31);
    key
}

/// Murmur3 64-bit hash function
///
/// This is used for string and binary types in ORC Bloom Filters.
#[allow(unused_assignments)]
fn murmur3_64(data: &[u8]) -> u64 {
    const C1: u64 = 0x87c37b91_114253d5;
    const C2: u64 = 0x4cf5ad43_2745937f;
    const R1: u32 = 27;
    const R2: u32 = 33;
    const M: u64 = 5;
    const N: u64 = 0x52dce729;

    let mut h1: u64 = 0;
    let mut h2: u64 = 0;

    let mut i = 0;
    while i + 16 <= data.len() {
        let mut k1 = u64::from_le_bytes([
            data[i],
            data[i + 1],
            data[i + 2],
            data[i + 3],
            data[i + 4],
            data[i + 5],
            data[i + 6],
            data[i + 7],
        ]);
        let mut k2 = u64::from_le_bytes([
            data[i + 8],
            data[i + 9],
            data[i + 10],
            data[i + 11],
            data[i + 12],
            data[i + 13],
            data[i + 14],
            data[i + 15],
        ]);

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(R1);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(R2);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(M).wrapping_add(N);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(R2);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(R1);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(M).wrapping_add(N);

        i += 16;
    }

    // Handle remaining bytes
    if i < data.len() {
        let remaining = data.len() - i;
        let mut k1 = if remaining >= 8 {
            let val = u64::from_le_bytes([
                data[i],
                data[i + 1],
                data[i + 2],
                data[i + 3],
                data[i + 4],
                data[i + 5],
                data[i + 6],
                data[i + 7],
            ]);
            i += 8;
            val
        } else {
            let mut bytes = [0u8; 8];
            bytes[..remaining].copy_from_slice(&data[i..(remaining + i)]);
            i = data.len();
            u64::from_le_bytes(bytes)
        };

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(R1);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        if i < data.len() {
            let remaining = data.len() - i;
            let mut bytes = [0u8; 8];
            bytes[..remaining].copy_from_slice(&data[i..(remaining + i)]);
            let mut k2 = u64::from_le_bytes(bytes);
            k2 = k2.wrapping_mul(C2);
            k2 = k2.rotate_left(R2);
            k2 = k2.wrapping_mul(C1);
            h2 ^= k2;
        }
    }

    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;

    let h1_orig = h1;
    let h2_orig = h2;
    h1 = h1_orig.wrapping_add(h2_orig);
    h2 = h2_orig.wrapping_add(h1_orig);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    let h1_orig = h1;
    let h2_orig = h2;
    h1 = h1_orig.wrapping_add(h2_orig);
    h2 = h2_orig.wrapping_add(h1);

    h1
}

/// Finalization mix function for Murmur3
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thomas_wang_hash() {
        // Test that hash function produces consistent results
        let hash1 = thomas_wang_hash64(12345);
        let hash2 = thomas_wang_hash64(12345);
        assert_eq!(hash1, hash2);

        // Test different values produce different hashes
        let hash3 = thomas_wang_hash64(12346);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_murmur3_hash() {
        // Test that hash function produces consistent results
        let data = b"hello world";
        let hash1 = murmur3_64(data);
        let hash2 = murmur3_64(data);
        assert_eq!(hash1, hash2);

        // Test different values produce different hashes
        let hash3 = murmur3_64(b"hello worl");
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_bloom_filter_empty() {
        let proto = crate::proto::BloomFilter {
            num_hash_functions: None,
            bitset: vec![],
            utf8bitset: None,
        };

        let filter = BloomFilter::from_proto(&proto).unwrap();
        // Empty filter should return true (can't determine)
        assert!(filter
            .might_contain(&PredicateValue::Int32(Some(42)))
            .unwrap());
    }

    #[test]
    fn test_bloom_filter_int64() {
        // Test Bloom Filter with integer values
        // Since Bloom Filter construction is deterministic, we can properly test it

        // Create a Bloom Filter that contains value 42
        let value = 42i64;
        let num_hash_functions = 3;
        let num_bits = 64; // Use one u64 word (64 bits)
        let hash = thomas_wang_hash64(value);

        // Calculate which bits should be set for this value
        let mut bitset = vec![0u64; 1]; // One word
        for i in 0..num_hash_functions {
            let hash_value = hash.wrapping_add(i as u64).wrapping_mul(0x9e3779b97f4a7c15);
            let bit_pos = (hash_value % (num_bits as u64)) as usize;
            let word_idx = bit_pos / 64;
            let bit_idx = bit_pos % 64;
            bitset[word_idx] |= 1u64 << bit_idx;
        }

        let proto = crate::proto::BloomFilter {
            num_hash_functions: Some(num_hash_functions),
            bitset,
            utf8bitset: None,
        };

        let filter = BloomFilter::from_proto(&proto).unwrap();

        // Value 42 should be found (we constructed the filter with it)
        assert!(filter
            .might_contain(&PredicateValue::Int64(Some(value)))
            .unwrap());

        // Value 999 should likely not be found (unless there's a false positive)
        // We test that the function doesn't crash and returns a boolean
        let _result = filter
            .might_contain(&PredicateValue::Int64(Some(999)))
            .unwrap();
        // Function successfully returned a boolean value (no panic)
    }

    #[test]
    fn test_bloom_filter_string() {
        // Test Bloom Filter with string values
        // Since Bloom Filter construction is deterministic, we can properly test it

        let value = "test_string";
        let num_hash_functions = 3;
        let num_bits = 64; // Use 8 bytes (64 bits)
        let hash = murmur3_64(value.as_bytes());

        // Calculate which bits should be set for this value
        let mut bitset = vec![0u8; 8]; // 8 bytes = 64 bits
        for i in 0..num_hash_functions {
            let hash_value = hash.wrapping_add(i as u64).wrapping_mul(0x9e3779b97f4a7c15);
            let bit_pos = (hash_value % (num_bits as u64)) as usize;
            let byte_idx = bit_pos / 8;
            let bit_idx = bit_pos % 8;
            bitset[byte_idx] |= 1u8 << bit_idx;
        }

        let proto = crate::proto::BloomFilter {
            num_hash_functions: Some(num_hash_functions),
            bitset: vec![], // Empty for UTF8 bitset
            utf8bitset: Some(bitset),
        };

        let filter = BloomFilter::from_proto(&proto).unwrap();

        // Value "test_string" should be found (we constructed the filter with it)
        assert!(filter
            .might_contain(&PredicateValue::Utf8(Some(value.to_string())))
            .unwrap());

        // Value "different_string" should likely not be found (unless there's a false positive)
        let _result = filter
            .might_contain(&PredicateValue::Utf8(Some("different_string".to_string())))
            .unwrap();
        // Function successfully returned a boolean value (no panic)
    }

    #[test]
    fn test_bloom_filter_deterministic() {
        // Test that Bloom Filter behavior is deterministic
        // Same value should always produce the same hash and bit positions

        let value = 12345i64;
        let num_hash_functions = 3;
        let num_bits = 64;
        let hash1 = thomas_wang_hash64(value);
        let hash2 = thomas_wang_hash64(value);

        // Hash should be deterministic
        assert_eq!(hash1, hash2);

        // Calculate bit positions for both hashes (should be identical)
        let mut positions1 = Vec::new();
        let mut positions2 = Vec::new();
        for i in 0..num_hash_functions {
            let hash_value1 = hash1
                .wrapping_add(i as u64)
                .wrapping_mul(0x9e3779b97f4a7c15);
            let hash_value2 = hash2
                .wrapping_add(i as u64)
                .wrapping_mul(0x9e3779b97f4a7c15);
            let bit_pos1 = (hash_value1 % (num_bits as u64)) as usize;
            let bit_pos2 = (hash_value2 % (num_bits as u64)) as usize;
            positions1.push(bit_pos1);
            positions2.push(bit_pos2);
        }

        // Bit positions should be identical
        assert_eq!(positions1, positions2);
    }
}
