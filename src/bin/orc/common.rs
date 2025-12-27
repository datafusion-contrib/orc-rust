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

//! Common utilities shared across ORC CLI subcommands.

use std::io;

use arrow::{csv, error::ArrowError, json, record_batch::RecordBatch};
use orc_rust::statistics::{ColumnStatistics, TypeStatistics};

/// Output format for data export.
#[derive(Clone, Debug, PartialEq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Output data in CSV format
    Csv,
    /// Output data in JSON lines format
    Json,
}

/// Unified writer that can output to CSV or JSON format.
#[allow(clippy::large_enum_variant)]
pub enum OutputWriter<W: io::Write, F: json::writer::JsonFormat> {
    Csv(csv::Writer<W>),
    Json(json::Writer<W, F>),
}

impl<W, F> OutputWriter<W, F>
where
    W: io::Write,
    F: json::writer::JsonFormat,
{
    /// Write a record batch to the output.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        match self {
            OutputWriter::Csv(w) => w.write(batch),
            OutputWriter::Json(w) => w.write(batch),
        }
    }

    /// Finish writing and flush any remaining data.
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        match self {
            OutputWriter::Csv(_) => Ok(()),
            OutputWriter::Json(w) => w.finish(),
        }
    }
}

/// Create a CSV writer with headers.
pub fn create_csv_writer<W: io::Write>(writer: W) -> csv::Writer<W> {
    csv::WriterBuilder::new().with_header(true).build(writer)
}

/// Create a JSON lines writer.
pub fn create_json_writer<W: io::Write>(writer: W) -> json::Writer<W, json::writer::LineDelimited> {
    json::WriterBuilder::new().build::<_, json::writer::LineDelimited>(writer)
}

/// Format column statistics into a human-readable string.
pub fn format_stats(stats: &ColumnStatistics) -> String {
    let mut parts = vec![format!("values={}", stats.number_of_values())];
    if stats.has_null() {
        parts.push("has_nulls=true".to_string());
    }
    if let Some(ts) = stats.type_statistics() {
        match ts {
            TypeStatistics::Integer { min, max, .. } => {
                parts.push(format!("min={min}"));
                parts.push(format!("max={max}"));
            }
            TypeStatistics::Double { min, max, .. } => {
                parts.push(format!("min={min}"));
                parts.push(format!("max={max}"));
            }
            TypeStatistics::String { min, max, .. } => {
                parts.push(format!("min={min}"));
                parts.push(format!("max={max}"));
            }
            TypeStatistics::Bucket { true_count } => {
                parts.push(format!("true_count={true_count}"));
            }
            TypeStatistics::Decimal { min, max, .. } => {
                parts.push(format!("min={min}"));
                parts.push(format!("max={max}"));
            }
            TypeStatistics::Date { min, max } => {
                parts.push(format!("min={min}"));
                parts.push(format!("max={max}"));
            }
            TypeStatistics::Binary { sum } => {
                parts.push(format!("total_bytes={sum}"));
            }
            TypeStatistics::Timestamp { min, max, .. } => {
                parts.push(format!("min={min}"));
                parts.push(format!("max={max}"));
            }
            TypeStatistics::Collection {
                min_children,
                max_children,
                total_children,
            } => {
                parts.push(format!("min_children={min_children}"));
                parts.push(format!("max_children={max_children}"));
                parts.push(format!("total_children={total_children}"));
            }
        }
    }
    parts.join(", ")
}
