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

//! Stream an ORC file to stdout as CSV or JSON lines.
//!
//! This is a thin wrapper around `ArrowReaderBuilder` so that CLI behavior mirrors
//! library reads (projection/predicate defaults, batch sizing, etc).

use std::fs::File;
use std::io::{self, Read};

use anyhow::{Context, Result};
use arrow::{csv, error::ArrowError, json, record_batch::RecordBatch};
use bytes::Bytes;
use clap::Parser;
use orc_rust::reader::ChunkReader;
use orc_rust::ArrowReaderBuilder;

#[derive(Debug, Parser)]
#[command(author, version, about = "Read ORC data and print to stdout")]
struct Args {
    /// Path to an ORC file or "-" to read from stdin
    file: String,
    /// Number of records to read (0 = all)
    #[arg(short, long, default_value_t = 0)]
    num_records: usize,
    /// Output as JSON lines instead of CSV
    #[arg(short, long)]
    json: bool,
    /// Batch size to use when reading
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,
}

#[allow(clippy::large_enum_variant)]
enum OutputWriter<W: io::Write, F: json::writer::JsonFormat> {
    Csv(csv::Writer<W>),
    Json(json::Writer<W, F>),
}

impl<W, F> OutputWriter<W, F>
where
    W: io::Write,
    F: json::writer::JsonFormat,
{
    fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        match self {
            OutputWriter::Csv(w) => w.write(batch),
            OutputWriter::Json(w) => w.write(batch),
        }
    }

    fn finish(&mut self) -> Result<(), ArrowError> {
        match self {
            OutputWriter::Csv(_) => Ok(()),
            OutputWriter::Json(w) => w.finish(),
        }
    }
}

fn run_reader<R: ChunkReader>(
    source: R,
    args: &Args,
    mut writer: OutputWriter<impl io::Write, impl json::writer::JsonFormat>,
) -> Result<()> {
    let reader = ArrowReaderBuilder::try_new(source)?
        .with_batch_size(args.batch_size)
        .build();

    let mut remaining = if args.num_records == 0 {
        usize::MAX
    } else {
        args.num_records
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

fn main() -> Result<()> {
    let args = Args::parse();

    let stdout = io::stdout();
    let handle = stdout.lock();

    if args.file == "-" {
        let mut buf = Vec::new();
        io::stdin().read_to_end(&mut buf).context("reading stdin")?;
        let bytes = Bytes::from(buf);
        let writer: OutputWriter<_, json::writer::LineDelimited> = if args.json {
            OutputWriter::Json(
                json::WriterBuilder::new().build::<_, json::writer::LineDelimited>(handle),
            )
        } else {
            OutputWriter::Csv(csv::WriterBuilder::new().with_header(true).build(handle))
        };
        run_reader(bytes, &args, writer)
    } else {
        let file = File::open(&args.file).with_context(|| format!("opening {}", args.file))?;
        let writer: OutputWriter<_, json::writer::LineDelimited> = if args.json {
            OutputWriter::Json(
                json::WriterBuilder::new().build::<_, json::writer::LineDelimited>(handle),
            )
        } else {
            OutputWriter::Csv(csv::WriterBuilder::new().with_header(true).build(handle))
        };
        run_reader(file, &args, writer)
    }
}
