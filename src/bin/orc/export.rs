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

//! Export ORC data to CSV or JSON format.
//!
//! This subcommand unifies the functionality of the former `orc-export`
//! and `orc-read` commands.

use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::datatypes::DataType;
use bytes::Bytes;
use clap::Parser;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::metadata::read_metadata;
use orc_rust::reader::ChunkReader;
use orc_rust::ArrowReaderBuilder;

use crate::common::{create_csv_writer, create_json_writer, OutputFormat, OutputWriter};

#[derive(Debug, Parser)]
#[command(about = "Export ORC data to CSV or JSON format")]
pub struct Args {
    /// Path to an ORC file or "-" to read from stdin
    file: String,

    /// Output file. If not provided, output goes to stdout
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Output format
    #[arg(value_enum, short, long, default_value_t = OutputFormat::Csv)]
    format: OutputFormat,

    /// Export only the first N records (0 = all)
    #[arg(short, long, value_name = "N", default_value_t = 0)]
    num_rows: usize,

    /// Export only the specified columns (comma-separated list)
    #[arg(short, long, value_delimiter = ',')]
    columns: Option<Vec<String>>,

    /// Batch size to use when reading
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,
}

pub fn run(args: Args) -> Result<()> {
    // Prepare output writer
    let output: Box<dyn io::Write> = if let Some(ref path) = args.output {
        Box::new(File::create(path).with_context(|| format!("creating {}", path.display()))?)
    } else {
        Box::new(io::stdout().lock())
    };

    if args.file == "-" {
        // Read from stdin
        let mut buf = Vec::new();
        io::stdin().read_to_end(&mut buf).context("reading stdin")?;
        let bytes = Bytes::from(buf);
        run_export(bytes, &args, output)
    } else {
        // Read from file
        let file = File::open(&args.file).with_context(|| format!("opening {}", args.file))?;
        run_export(file, &args, output)
    }
}

fn run_export<R: ChunkReader>(
    mut source: R,
    args: &Args,
    output: Box<dyn io::Write>,
) -> Result<()> {
    // Build projection mask if columns are specified
    let projection = if args.columns.is_some() {
        // Need to read metadata to build projection
        let metadata = read_metadata(&mut source)?;

        let cols: Vec<usize> = metadata
            .root_data_type()
            .children()
            .iter()
            .enumerate()
            .filter(|(_, nc)| {
                // Filter out unsupported types for CSV/JSON export
                match nc.data_type().to_arrow_data_type() {
                    DataType::Binary => false,
                    DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                        matches!(args.format, OutputFormat::Csv)
                    }
                    _ => {
                        if let Some(ref cols) = args.columns {
                            cols.iter().any(|c| nc.name().eq(c))
                        } else {
                            true
                        }
                    }
                }
            })
            .map(|(i, _)| i)
            .collect();

        Some(ProjectionMask::roots(metadata.root_data_type(), cols))
    } else {
        None
    };

    // Build reader
    let mut builder = ArrowReaderBuilder::try_new(source)?.with_batch_size(args.batch_size);

    if let Some(proj) = projection {
        builder = builder.with_projection(proj);
    }

    let reader = builder.build();

    // Create appropriate writer based on format
    let mut writer: OutputWriter<Box<dyn io::Write>, _> = match args.format {
        OutputFormat::Json => OutputWriter::Json(create_json_writer(output)),
        OutputFormat::Csv => OutputWriter::Csv(create_csv_writer(output)),
    };

    // Read and write data
    let mut remaining = if args.num_rows == 0 {
        usize::MAX
    } else {
        args.num_rows
    };

    for batch in reader {
        if remaining == 0 {
            break;
        }
        let mut batch = batch?;
        if remaining < batch.num_rows() {
            batch = batch.slice(0, remaining);
        }
        writer.write(&batch)?;
        remaining = remaining.saturating_sub(batch.num_rows());
    }

    writer.finish().context("closing writer")?;
    Ok(())
}
