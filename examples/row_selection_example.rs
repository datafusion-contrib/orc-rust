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
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.

//! Example demonstrating how to use RowSelection to skip rows when reading ORC files

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::arrow_writer::ArrowWriterBuilder;
use orc_rust::row_selection::{RowSelection, RowSelector};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Create a sample ORC file with 100 rows
    println!("Creating sample ORC file...");
    let file_path = "/tmp/row_selection_example.orc";
    create_sample_orc_file(file_path)?;

    // Step 2: Read the file without row selection (baseline)
    println!("\n=== Reading all rows (no selection) ===");
    let file = File::open(file_path)?;
    let reader = ArrowReaderBuilder::try_new(file)?.build();
    let mut total_rows = 0;
    for batch in reader {
        let batch = batch?;
        total_rows += batch.num_rows();
    }
    println!("Total rows read: {}", total_rows);

    // Step 3: Read with row selection - skip first 30 rows, select next 40, skip rest
    println!("\n=== Reading with row selection ===");
    let file = File::open(file_path)?;
    
    // Create a selection: skip 30, select 40, skip 30
    let selection = vec![
        RowSelector::skip(30),
        RowSelector::select(40),
        RowSelector::skip(30),
    ]
    .into();
    
    let reader = ArrowReaderBuilder::try_new(file)?
        .with_row_selection(selection)
        .build();
    
    let mut selected_rows = 0;
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch?;
        selected_rows += batch.num_rows();
        batches.push(batch);
    }
    
    println!("Total rows selected: {}", selected_rows);
    println!("Expected: 40, Actual: {}", selected_rows);
    
    // Display some of the selected data
    if let Some(first_batch) = batches.first() {
        let id_col = first_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_col = first_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        
        println!("\nFirst 5 selected rows:");
        for i in 0..5.min(first_batch.num_rows()) {
            println!(
                "  id: {}, name: {}",
                id_col.value(i),
                name_col.value(i)
            );
        }
    }

    // Step 4: Read with multiple non-consecutive selections
    println!("\n=== Reading with multiple selections ===");
    let file = File::open(file_path)?;
    
    // Select rows 10-20 and 60-70
    let selection = RowSelection::from_consecutive_ranges(
        vec![10..20, 60..70].into_iter(),
        100,
    );
    
    let reader = ArrowReaderBuilder::try_new(file)?
        .with_row_selection(selection)
        .build();
    
    let mut selected_rows = 0;
    for batch in reader {
        let batch = batch?;
        selected_rows += batch.num_rows();
    }
    
    println!("Total rows selected: {}", selected_rows);
    println!("Expected: 20 (10 from each range)");

    println!("\n✓ Row selection example completed successfully!");
    
    Ok(())
}

fn create_sample_orc_file(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create 100 rows
    let ids: ArrayRef = Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>()));
    let names: ArrayRef = Arc::new(StringArray::from(
        (0..100)
            .map(|i| format!("name_{}", i))
            .collect::<Vec<String>>(),
    ));

    let batch = RecordBatch::try_new(schema.clone(), vec![ids, names])?;

    let file = File::create(path)?;
    let mut writer = ArrowWriterBuilder::new(file, schema).try_build()?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

