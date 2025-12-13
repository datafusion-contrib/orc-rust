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

//! Return the number of rows in one or more ORC files.
//!
//! Uses metadata only (no row decoding), so it is fast even on large files.

use std::{fs::File, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use orc_rust::reader::metadata::read_metadata;

#[derive(Debug, Parser)]
#[command(author, version, about = "Return the number of rows in ORC files")]
struct Args {
    /// List of ORC files to inspect
    #[arg(required = true)]
    files: Vec<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    for path in args.files {
        let mut file =
            File::open(&path).with_context(|| format!("failed to open {:?}", path.display()))?;
        let metadata = read_metadata(&mut file)?;
        println!("{}: {}", path.display(), metadata.number_of_rows());
    }

    Ok(())
}
