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

//! Print the schema and metadata of an ORC file.

use std::{fs::File, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use orc_rust::reader::metadata::read_metadata;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Print the schema and metadata of an ORC file"
)]
struct Args {
    /// Path to the ORC file
    file: PathBuf,
    /// Include stripe offsets and row counts
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut file = File::open(&args.file)
        .with_context(|| format!("failed to open {:?}", args.file.display()))?;
    let metadata = read_metadata(&mut file)?;

    println!("File: {}", args.file.display());
    println!("Format version: {}", metadata.file_format_version());
    println!(
        "Compression: {}",
        metadata
            .compression()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "None".to_string())
    );
    if let Some(stride) = metadata.row_index_stride() {
        println!("Row index stride: {stride}");
    } else {
        println!("Row index stride: None");
    }
    println!("Rows: {}", metadata.number_of_rows());
    println!("Stripes: {}", metadata.stripe_metadatas().len());
    println!();
    println!("Schema:\n{}", metadata.root_data_type());

    if args.verbose {
        println!("\nStripe layout:");
        for (idx, stripe) in metadata.stripe_metadatas().iter().enumerate() {
            println!("Stripe {idx}:");
            println!("  offset: {}", stripe.offset());
            println!("  index length: {}", stripe.index_length());
            println!("  data length: {}", stripe.data_length());
            println!("  footer length: {}", stripe.footer_length());
            println!("  rows: {}", stripe.number_of_rows());
        }
    }

    Ok(())
}
