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

//! Inspect row indexes for a specific ORC column.
//!
//! Row indexes carry per-row-group statistics and positions; this tool surfaces
//! them for debugging predicate pushdown and verifying writer-produced indexes.

use std::fs::File;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use orc_rust::reader::metadata::read_metadata;
use orc_rust::schema::{DataType, RootDataType};
use orc_rust::stripe::Stripe;

use crate::common::format_stats;

#[derive(Debug, Parser)]
#[command(about = "Print row group index information for an ORC column")]
pub struct Args {
    /// Path to the ORC file
    file: PathBuf,
    /// Column name to inspect (top-level columns only)
    column: String,
}

fn find_column<'a>(root: &'a RootDataType, name: &str) -> Option<(usize, &'a DataType, &'a str)> {
    root.children()
        .iter()
        .find(|c| c.name() == name)
        .map(|col| (col.data_type().column_index(), col.data_type(), col.name()))
}

pub fn run(args: Args) -> Result<()> {
    let mut file = File::open(&args.file)
        .with_context(|| format!("failed to open {:?}", args.file.display()))?;
    let metadata = read_metadata(&mut file)?;

    let Some((column_index, data_type, name)) =
        find_column(metadata.root_data_type(), &args.column)
    else {
        let available = metadata
            .root_data_type()
            .children()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(anyhow!(
            "column '{}' not found. Available columns: {available}",
            args.column
        ));
    };

    println!(
        "File: {} | Column: {} (index {})",
        args.file.display(),
        name,
        column_index
    );
    println!("Type: {data_type}");
    println!("Stripes: {}", metadata.stripe_metadatas().len());

    for (stripe_idx, stripe_meta) in metadata.stripe_metadatas().iter().enumerate() {
        let stripe = Stripe::new(&mut file, &metadata, metadata.root_data_type(), stripe_meta)?;
        let row_index = stripe.read_row_indexes(&metadata)?;

        let Some(col_index) = row_index.column(column_index) else {
            println!("Stripe {stripe_idx}: no row index for column");
            continue;
        };

        if col_index.num_row_groups() == 0 {
            println!("Stripe {stripe_idx}: no row groups recorded");
            continue;
        }

        println!(
            "Stripe {stripe_idx}: rows_per_group={} total_rows={}",
            col_index.rows_per_group(),
            row_index.total_rows()
        );
        for (row_group_idx, entry) in col_index.entries().enumerate() {
            let start = row_group_idx * col_index.rows_per_group();
            let end = (start + col_index.rows_per_group()).min(row_index.total_rows());
            print!("  Row group {row_group_idx} rows [{start},{end})");
            if let Some(stats) = &entry.statistics {
                println!(" -> {}", format_stats(stats));
            } else {
                println!(" -> no statistics");
            }
        }
    }

    Ok(())
}
