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

//! Display file metadata, schema, and basic information.
//!
//! This subcommand unifies the functionality of the former `orc-metadata`,
//! `orc-schema`, and `orc-rowcount` commands.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use orc_rust::reader::metadata::read_metadata;
use orc_rust::stripe::Stripe;

#[derive(Debug, Parser)]
#[command(about = "Display file metadata, schema, and basic information")]
pub struct Args {
    /// Path(s) to the ORC file(s)
    #[arg(required = true)]
    files: Vec<PathBuf>,

    /// Include stripe layout details (offsets, lengths, rows)
    #[arg(short, long)]
    verbose: bool,

    /// Only display the row count for each file
    #[arg(long)]
    row_count_only: bool,
}

pub fn run(args: Args) -> Result<()> {
    // If row-count-only mode, just print counts for all files
    if args.row_count_only {
        for path in &args.files {
            let mut file =
                File::open(path).with_context(|| format!("failed to open {:?}", path.display()))?;
            let metadata = read_metadata(&mut file)?;
            println!("{}: {}", path.display(), metadata.number_of_rows());
        }
        return Ok(());
    }

    // For normal mode, process each file with full info
    for (idx, path) in args.files.iter().enumerate() {
        if idx > 0 {
            println!("\n---\n");
        }
        print_file_info(path, args.verbose)?;
    }

    Ok(())
}

fn print_file_info(path: &PathBuf, verbose: bool) -> Result<()> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {:?}", path.display()))?;
    let metadata = Arc::new(read_metadata(&mut file)?);

    println!("File: {}", path.display());
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

    if verbose {
        println!("\nStripe layout:");
        for (idx, stripe_meta) in metadata.stripe_metadatas().iter().enumerate() {
            let stripe = Stripe::new(&mut file, &metadata, metadata.root_data_type(), stripe_meta)?;
            println!("Stripe {idx}:");
            println!("  offset: {}", stripe_meta.offset());
            println!("  index length: {}", stripe_meta.index_length());
            println!("  data length: {}", stripe_meta.data_length());
            println!("  footer length: {}", stripe_meta.footer_length());
            println!("  rows: {}", stripe.number_of_rows());
            println!(
                "  writer timezone: {}",
                stripe
                    .writer_tz()
                    .map(|tz| tz.to_string())
                    .unwrap_or_else(|| "None".to_string())
            );
        }
    }

    Ok(())
}
