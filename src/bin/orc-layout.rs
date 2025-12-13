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

//! Emit a JSON description of the physical layout of an ORC file.
//!
//! Useful for inspecting stripe offsets, stream kinds/sizes, and column encodings
//! to debug writer output or validate round trips.

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use orc_rust::compression::{Compression, Decompressor};
use orc_rust::proto::{column_encoding, stream::Kind, StripeFooter};
use orc_rust::reader::metadata::{read_metadata, FileMetadata};
use orc_rust::reader::ChunkReader;
use orc_rust::stripe::StripeMetadata;
use prost::Message;
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(author, version, about = "Print ORC stripe and stream layout as JSON")]
struct Args {
    /// Path to the ORC file
    file: PathBuf,
}

#[derive(Serialize)]
struct Layout {
    file: String,
    format_version: String,
    compression: Option<String>,
    rows: u64,
    stripes: Vec<StripeLayout>,
}

#[derive(Serialize)]
struct StripeLayout {
    index: usize,
    offset: u64,
    index_length: u64,
    data_length: u64,
    footer_length: u64,
    rows: u64,
    streams: Vec<StreamLayout>,
    encodings: Vec<ColumnEncodingLayout>,
}

#[derive(Serialize)]
struct StreamLayout {
    column: u32,
    kind: String,
    length: u64,
    offset: u64,
}

#[derive(Serialize)]
struct ColumnEncodingLayout {
    column: usize,
    kind: String,
    dictionary_size: Option<u32>,
}

fn read_stripe_footer<R: ChunkReader>(
    reader: &R,
    stripe: &StripeMetadata,
    compression: Option<Compression>,
) -> Result<StripeFooter> {
    let footer_bytes = reader
        .get_bytes(stripe.footer_offset(), stripe.footer_length())
        .context("reading stripe footer")?;
    let mut buffer = Vec::new();
    Decompressor::new(footer_bytes, compression, vec![])
        .read_to_end(&mut buffer)
        .context("decompressing stripe footer")?;
    StripeFooter::decode(buffer.as_slice()).context("decoding stripe footer")
}

fn kind_to_str(kind: Kind) -> &'static str {
    match kind {
        Kind::Present => "PRESENT",
        Kind::Data => "DATA",
        Kind::Length => "LENGTH",
        Kind::DictionaryData => "DICTIONARY_DATA",
        Kind::Secondary => "SECONDARY",
        Kind::RowIndex => "ROW_INDEX",
        Kind::BloomFilter => "BLOOM_FILTER",
        Kind::BloomFilterUtf8 => "BLOOM_FILTER_UTF8",
        Kind::DictionaryCount => "DICTIONARY_COUNT",
        Kind::EncryptedIndex => "ENCRYPTED_INDEX",
        Kind::EncryptedData => "ENCRYPTED_DATA",
        Kind::StripeStatistics => "STRIPE_STATISTICS",
        Kind::FileStatistics => "FILE_STATISTICS",
    }
}

fn encoding_to_str(kind: column_encoding::Kind) -> &'static str {
    match kind {
        column_encoding::Kind::Direct => "DIRECT",
        column_encoding::Kind::Dictionary => "DICTIONARY",
        column_encoding::Kind::DirectV2 => "DIRECT_V2",
        column_encoding::Kind::DictionaryV2 => "DICTIONARY_V2",
    }
}

fn build_stripe_layout<R: ChunkReader>(
    reader: &R,
    metadata: &FileMetadata,
    stripe_idx: usize,
    stripe: &StripeMetadata,
) -> Result<StripeLayout> {
    let footer = read_stripe_footer(reader, stripe, metadata.compression())?;

    let mut offset = stripe.offset();
    let streams = footer
        .streams
        .iter()
        .map(|s| {
            let stream = StreamLayout {
                column: s.column(),
                kind: kind_to_str(s.kind()).to_string(),
                length: s.length(),
                offset,
            };
            offset += s.length();
            stream
        })
        .collect();

    let encodings = footer
        .columns
        .iter()
        .enumerate()
        .map(|(idx, enc)| ColumnEncodingLayout {
            column: idx,
            kind: encoding_to_str(enc.kind()).to_string(),
            dictionary_size: enc.dictionary_size,
        })
        .collect();

    Ok(StripeLayout {
        index: stripe_idx,
        offset: stripe.offset(),
        index_length: stripe.index_length(),
        data_length: stripe.data_length(),
        footer_length: stripe.footer_length(),
        rows: stripe.number_of_rows(),
        streams,
        encodings,
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut file = File::open(&args.file)
        .with_context(|| format!("failed to open {:?}", args.file.display()))?;
    let metadata = read_metadata(&mut file)?;

    let stripes = metadata
        .stripe_metadatas()
        .iter()
        .enumerate()
        .map(|(idx, stripe)| build_stripe_layout(&file, &metadata, idx, stripe))
        .collect::<Result<Vec<_>>>()?;

    let layout = Layout {
        file: args.file.display().to_string(),
        format_version: metadata.file_format_version().to_string(),
        compression: metadata.compression().map(|c| c.to_string()),
        rows: metadata.number_of_rows(),
        stripes,
    };

    serde_json::to_writer_pretty(std::io::stdout(), &layout).context("writing layout")?;
    Ok(())
}
