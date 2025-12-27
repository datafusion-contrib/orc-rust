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

//! Inspect bloom filters in ORC files.
//!
//! Bloom filters are probabilistic data structures that can quickly determine
//! if a value is definitely NOT present in a row group. This is useful for
//! predicate pushdown optimization.

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use orc_rust::bloom_filter::BloomFilter;
use orc_rust::compression::Decompressor;
use orc_rust::proto::{stream::Kind, BloomFilterIndex};
use orc_rust::reader::metadata::read_metadata;
use orc_rust::reader::ChunkReader;
use orc_rust::schema::RootDataType;
use orc_rust::stripe::StripeMetadata;
use prost::Message;

#[derive(Debug, Parser)]
#[command(about = "Inspect bloom filters in ORC files")]
pub struct Args {
    /// Path to the ORC file
    file: PathBuf,

    /// Column name to inspect (show all columns if not specified)
    #[arg(short, long)]
    column: Option<String>,

    /// Test if a value might be contained in the bloom filter
    #[arg(short, long)]
    test: Option<String>,
}

/// Read bloom filter streams from a stripe
fn read_bloom_filters<R: ChunkReader>(
    reader: &R,
    stripe: &StripeMetadata,
    compression: Option<orc_rust::compression::Compression>,
    root_type: &RootDataType,
) -> Result<HashMap<usize, Vec<BloomFilter>>> {
    // Read stripe footer to get stream information
    let footer_bytes = reader
        .get_bytes(stripe.footer_offset(), stripe.footer_length())
        .context("reading stripe footer")?;

    let mut decompressed = Vec::new();
    Decompressor::new(footer_bytes, compression, vec![])
        .read_to_end(&mut decompressed)
        .context("decompressing stripe footer")?;

    let footer = orc_rust::proto::StripeFooter::decode(decompressed.as_slice())
        .context("decoding footer")?;

    // Find bloom filter streams and their offsets
    let mut bloom_filters: HashMap<usize, Vec<BloomFilter>> = HashMap::new();
    let mut stream_offset = stripe.offset();

    for stream in &footer.streams {
        let length = stream.length();
        let column_id = stream.column() as usize;
        let kind = stream.kind();

        // Check if this column is in the schema
        let is_valid_column = root_type.contains_column_index(column_id);

        if is_valid_column && (kind == Kind::BloomFilter || kind == Kind::BloomFilterUtf8) {
            // Read and decode bloom filter
            let data = reader
                .get_bytes(stream_offset, length)
                .context("reading bloom filter stream")?;

            let mut decompressed = Vec::new();
            Decompressor::new(data, compression, vec![])
                .read_to_end(&mut decompressed)
                .context("decompressing bloom filter")?;

            let bloom_index = BloomFilterIndex::decode(decompressed.as_slice())
                .context("decoding bloom filter index")?;

            let filters: Vec<BloomFilter> = bloom_index
                .bloom_filter
                .iter()
                .filter_map(BloomFilter::try_from_proto)
                .collect();

            if !filters.is_empty() {
                bloom_filters.insert(column_id, filters);
            }
        }

        stream_offset += length;
    }

    Ok(bloom_filters)
}

/// Get column name by index
fn get_column_name(root_type: &RootDataType, column_index: usize) -> Option<String> {
    for child in root_type.children() {
        if child.data_type().column_index() == column_index {
            return Some(child.name().to_string());
        }
    }
    None
}

/// Find column index by name
fn find_column_index(root_type: &RootDataType, name: &str) -> Option<usize> {
    for child in root_type.children() {
        if child.name() == name {
            return Some(child.data_type().column_index());
        }
    }
    None
}

pub fn run(args: Args) -> Result<()> {
    let mut file = File::open(&args.file)
        .with_context(|| format!("failed to open {:?}", args.file.display()))?;
    let metadata = read_metadata(&mut file)?;

    println!("File: {}", args.file.display());
    println!("Stripes: {}", metadata.stripe_metadatas().len());

    // If a specific column is requested, validate it exists
    let filter_column_index = if let Some(ref col_name) = args.column {
        let idx = find_column_index(metadata.root_data_type(), col_name).ok_or_else(|| {
            let available = metadata
                .root_data_type()
                .children()
                .iter()
                .map(|c| c.name().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::anyhow!(
                "column '{}' not found. Available columns: {}",
                col_name,
                available
            )
        })?;
        Some(idx)
    } else {
        None
    };

    // Collect bloom filter info for all stripes
    let mut all_bloom_info: Vec<(usize, HashMap<usize, Vec<BloomFilter>>)> = Vec::new();
    let mut columns_with_bloom: HashMap<usize, String> = HashMap::new();

    for (stripe_idx, stripe_meta) in metadata.stripe_metadatas().iter().enumerate() {
        let bloom_filters = read_bloom_filters(
            &file,
            stripe_meta,
            metadata.compression(),
            metadata.root_data_type(),
        )?;

        // Track which columns have bloom filters
        for &col_idx in bloom_filters.keys() {
            if let std::collections::hash_map::Entry::Vacant(e) = columns_with_bloom.entry(col_idx)
            {
                if let Some(name) = get_column_name(metadata.root_data_type(), col_idx) {
                    e.insert(name);
                }
            }
        }

        all_bloom_info.push((stripe_idx, bloom_filters));
    }

    if columns_with_bloom.is_empty() {
        println!("\nNo bloom filters found in this file.");
        return Ok(());
    }

    // Print summary of columns with bloom filters
    println!("\nColumns with Bloom Filters:");
    let mut sorted_columns: Vec<_> = columns_with_bloom.iter().collect();
    sorted_columns.sort_by_key(|(idx, _)| *idx);

    for (col_idx, col_name) in &sorted_columns {
        // Get sample filter info from first stripe
        if let Some((_, bloom_map)) = all_bloom_info.first() {
            if let Some(filters) = bloom_map.get(col_idx) {
                if let Some(first_filter) = filters.first() {
                    println!(
                        "  Column {} ({}): {} row groups, {} hash functions, {} bits/filter",
                        col_idx,
                        col_name,
                        filters.len(),
                        first_filter.num_hash_functions(),
                        first_filter.bit_count()
                    );
                }
            }
        }
    }

    // If we should filter by column or test a value, show detailed output
    if filter_column_index.is_some() || args.test.is_some() {
        println!();

        for (stripe_idx, bloom_filters) in &all_bloom_info {
            // Filter by column if specified
            let cols_to_show: Vec<_> = if let Some(col_idx) = filter_column_index {
                bloom_filters
                    .iter()
                    .filter(|(&k, _)| k == col_idx)
                    .collect()
            } else {
                bloom_filters.iter().collect()
            };

            if cols_to_show.is_empty() {
                continue;
            }

            println!("Stripe {}:", stripe_idx);

            for (&col_idx, filters) in cols_to_show {
                let col_name = columns_with_bloom
                    .get(&col_idx)
                    .map(|s| s.as_str())
                    .unwrap_or("unknown");

                println!("  Column {} ({}):", col_idx, col_name);

                for (rg_idx, filter) in filters.iter().enumerate() {
                    let mut info = format!(
                        "    Row group {}: {} words, {} bits",
                        rg_idx,
                        filter.word_count(),
                        filter.bit_count()
                    );

                    // Test value if specified
                    if let Some(ref test_value) = args.test {
                        let might_contain = filter.might_contain(test_value.as_bytes());
                        info.push_str(&format!(
                            ", might_contain(\"{}\") = {}",
                            test_value, might_contain
                        ));
                    }

                    println!("{}", info);
                }
            }
        }
    }

    Ok(())
}
