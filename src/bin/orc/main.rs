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

//! Unified ORC CLI tool with subcommands for inspecting and exporting ORC files.

use anyhow::Result;
use clap::{Parser, Subcommand};

mod bloom;
mod common;
mod export;
mod index;
mod info;
mod layout;
mod stats;

#[derive(Parser)]
#[command(name = "orc")]
#[command(author, version, about = "ORC file inspection and export tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Display file metadata, schema, and basic information
    Info(info::Args),
    /// Export ORC data to CSV or JSON format
    Export(export::Args),
    /// Print column and stripe statistics
    Stats(stats::Args),
    /// Print physical layout (stripes, streams, encodings) as JSON
    Layout(layout::Args),
    /// Print row group index information for a specific column
    Index(index::Args),
    /// Inspect bloom filters in ORC files
    Bloom(bloom::Args),
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Info(args) => info::run(args),
        Commands::Export(args) => export::run(args),
        Commands::Stats(args) => stats::run(args),
        Commands::Layout(args) => layout::run(args),
        Commands::Index(args) => index::run(args),
        Commands::Bloom(args) => bloom::run(args),
    }
}
