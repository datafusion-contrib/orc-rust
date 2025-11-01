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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use crate::array_decoder::NaiveStripeDecoder;
use crate::error::Result;
use crate::predicate::Predicate;
use crate::projection::ProjectionMask;
use crate::reader::metadata::{read_metadata, FileMetadata};
use crate::reader::ChunkReader;
use crate::row_group_filter::evaluate_predicate;
use crate::row_selection::RowSelection;
use crate::schema::{ArrowSchemaOptions, RootDataType, TimestampPrecision};
use crate::stripe::{Stripe, StripeMetadata};

const DEFAULT_BATCH_SIZE: usize = 8192;

pub struct ArrowReaderBuilder<R> {
    pub(crate) reader: R,
    pub(crate) file_metadata: Arc<FileMetadata>,
    pub(crate) batch_size: usize,
    pub(crate) projection: ProjectionMask,
    pub(crate) schema_ref: Option<SchemaRef>,
    pub(crate) file_byte_range: Option<Range<usize>>,
    pub(crate) row_selection: Option<RowSelection>,
    pub(crate) timestamp_precision: TimestampPrecision,
    pub(crate) predicate: Option<Predicate>,
}

impl<R> ArrowReaderBuilder<R> {
    pub(crate) fn new(reader: R, file_metadata: Arc<FileMetadata>) -> Self {
        Self {
            reader,
            file_metadata,
            batch_size: DEFAULT_BATCH_SIZE,
            projection: ProjectionMask::all(),
            schema_ref: None,
            file_byte_range: None,
            row_selection: None,
            timestamp_precision: TimestampPrecision::default(),
            predicate: None,
        }
    }

    pub fn file_metadata(&self) -> &FileMetadata {
        &self.file_metadata
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_projection(mut self, projection: ProjectionMask) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema_ref = Some(schema);
        self
    }

    /// Specifies a range of file bytes that will read the strips offset within this range
    pub fn with_file_byte_range(mut self, range: Range<usize>) -> Self {
        self.file_byte_range = Some(range);
        self
    }

    /// Set a [`RowSelection`] to filter rows
    ///
    /// The [`RowSelection`] specifies which rows should be decoded from the ORC file.
    /// This can be used to skip rows that don't match predicates, reducing I/O and
    /// improving query performance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::fs::File;
    /// # use orc_rust::arrow_reader::ArrowReaderBuilder;
    /// # use orc_rust::row_selection::{RowSelection, RowSelector};
    /// let file = File::open("data.orc").unwrap();
    /// let selection = vec![
    ///     RowSelector::skip(100),
    ///     RowSelector::select(50),
    /// ].into();
    /// let reader = ArrowReaderBuilder::try_new(file)
    ///     .unwrap()
    ///     .with_row_selection(selection)
    ///     .build();
    /// ```
    pub fn with_row_selection(mut self, row_selection: RowSelection) -> Self {
        self.row_selection = Some(row_selection);
        self
    }

    /// Sets the timestamp precision for reading timestamp columns.
    ///
    /// By default, timestamps are read as Nanosecond precision.
    /// Use this method to switch to Microsecond precision if needed for compatibility.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::fs::File;
    /// # use orc_rust::arrow_reader::ArrowReaderBuilder;
    /// # use orc_rust::schema::TimestampPrecision;
    /// let file = File::open("/path/to/file.orc").unwrap();
    /// let reader = ArrowReaderBuilder::try_new(file)
    ///     .unwrap()
    ///     .with_timestamp_precision(TimestampPrecision::Microsecond)
    ///     .build();
    /// ```
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.timestamp_precision = precision;
        self
    }

    /// Set a predicate for row group filtering
    ///
    /// The predicate will be evaluated against row group statistics to automatically
    /// generate a [`RowSelection`] that skips filtered row groups. This provides
    /// efficient predicate pushdown based on ORC row indexes.
    ///
    /// The predicate is evaluated lazily when each stripe is read, using the row group
    /// statistics from the stripe's index section.
    ///
    /// If both `with_predicate()` and `with_row_selection()` are called, the results
    /// are combined using logical AND (both conditions must be satisfied).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::fs::File;
    /// # use orc_rust::{ArrowReaderBuilder, Predicate, PredicateValue};
    /// let file = File::open("data.orc").unwrap();
    ///
    /// // Filter: age >= 18
    /// let predicate = Predicate::gte("age", PredicateValue::Int32(Some(18)));
    ///
    /// let reader = ArrowReaderBuilder::try_new(file)
    ///     .unwrap()
    ///     .with_predicate(predicate)
    ///     .build();
    /// ```
    ///
    /// # Notes
    ///
    /// - Predicate evaluation requires row indexes to be present in the ORC file
    /// - If row indexes are missing, the predicate is ignored (all row groups are kept)
    /// - Only primitive columns have row indexes; predicates on compound types may be limited
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Returns the currently computed schema
    ///
    /// Unless [`with_schema`](Self::with_schema) was called, this is computed dynamically
    /// based on the current projection and the underlying file format.
    pub fn schema(&self) -> SchemaRef {
        let projected_data_type = self
            .file_metadata
            .root_data_type()
            .project(&self.projection);
        let metadata = self
            .file_metadata
            .user_custom_metadata()
            .iter()
            .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).to_string()))
            .collect::<HashMap<_, _>>();
        self.schema_ref.clone().unwrap_or_else(|| {
            let options =
                ArrowSchemaOptions::new().with_timestamp_precision(self.timestamp_precision);
            Arc::new(projected_data_type.create_arrow_schema_with_options(&metadata, options))
        })
    }
}

impl<R: ChunkReader> ArrowReaderBuilder<R> {
    pub fn try_new(mut reader: R) -> Result<Self> {
        let file_metadata = Arc::new(read_metadata(&mut reader)?);
        Ok(Self::new(reader, file_metadata))
    }

    pub fn build(self) -> ArrowReader<R> {
        let schema_ref = self.schema();
        let projected_data_type = self
            .file_metadata
            .root_data_type()
            .project(&self.projection);
        let projected_data_type_clone = projected_data_type.clone();
        let cursor = Cursor {
            reader: self.reader,
            file_metadata: self.file_metadata,
            projected_data_type,
            stripe_index: 0,
            file_byte_range: self.file_byte_range,
        };
        ArrowReader {
            cursor,
            schema_ref,
            current_stripe: None,
            batch_size: self.batch_size,
            row_selection: self.row_selection,
            predicate: self.predicate,
            projected_data_type: projected_data_type_clone,
        }
    }
}

pub struct ArrowReader<R> {
    cursor: Cursor<R>,
    schema_ref: SchemaRef,
    current_stripe: Option<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>>,
    batch_size: usize,
    row_selection: Option<RowSelection>,
    predicate: Option<Predicate>,
    projected_data_type: RootDataType,
}

impl<R> ArrowReader<R> {
    pub fn total_row_count(&self) -> u64 {
        self.cursor.file_metadata.number_of_rows()
    }
}

impl<R: ChunkReader> ArrowReader<R> {
    fn try_advance_stripe(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let stripe = self.cursor.next().transpose()?;
        match stripe {
            Some(stripe) => {
                let stripe_rows = stripe.number_of_rows();

                // Evaluate predicate if present
                let mut stripe_selection: Option<RowSelection> = None;
                if let Some(ref predicate) = self.predicate {
                    // Try to read row indexes for this stripe
                    match stripe.read_row_indexes(&self.cursor.file_metadata) {
                        Ok(row_index) => {
                            // Evaluate predicate against row group statistics
                            match evaluate_predicate(
                                predicate,
                                &row_index,
                                &self.projected_data_type,
                            ) {
                                Ok(row_group_filter) => {
                                    // Generate RowSelection from filter results
                                    let rows_per_group = self
                                        .cursor
                                        .file_metadata
                                        .row_index_stride()
                                        .unwrap_or(10_000);
                                    stripe_selection = Some(RowSelection::from_row_group_filter(
                                        &row_group_filter,
                                        rows_per_group,
                                        stripe_rows,
                                    ));
                                }
                                Err(_) => {
                                    // Predicate evaluation failed (e.g., column not found)
                                    // Keep all rows (maybe)
                                    stripe_selection = Some(RowSelection::select_all(stripe_rows));
                                }
                            }
                        }
                        Err(_) => {
                            // Row indexes not available, keep all rows (maybe)
                            stripe_selection = Some(RowSelection::select_all(stripe_rows));
                        }
                    }
                }

                // Combine with existing row_selection if present
                let mut final_selection = stripe_selection;
                if let Some(ref mut existing_selection) = self.row_selection {
                    if existing_selection.row_count() > 0 {
                        let existing_for_stripe = existing_selection.split_off(stripe_rows);
                        final_selection = match final_selection {
                            Some(predicate_selection) => {
                                // Both predicate and manual selection: combine with AND
                                Some(existing_for_stripe.and_then(&predicate_selection))
                            }
                            None => Some(existing_for_stripe),
                        };
                    }
                }

                let decoder = NaiveStripeDecoder::new_with_selection(
                    stripe,
                    self.schema_ref.clone(),
                    self.batch_size,
                    final_selection,
                )?;
                self.current_stripe = Some(Box::new(decoder));
                self.next().transpose()
            }
            None => Ok(None),
        }
    }
}

impl<R: ChunkReader> RecordBatchReader for ArrowReader<R> {
    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
    }
}

impl<R: ChunkReader> Iterator for ArrowReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_stripe.as_mut() {
            Some(stripe) => {
                match stripe
                    .next()
                    .map(|batch| batch.map_err(|err| ArrowError::ExternalError(Box::new(err))))
                {
                    Some(rb) => Some(rb),
                    None => self.try_advance_stripe().transpose(),
                }
            }
            None => self.try_advance_stripe().transpose(),
        }
    }
}

pub(crate) struct Cursor<R> {
    pub reader: R,
    pub file_metadata: Arc<FileMetadata>,
    pub projected_data_type: RootDataType,
    pub stripe_index: usize,
    pub file_byte_range: Option<Range<usize>>,
}

impl<R: ChunkReader> Cursor<R> {
    fn get_stripe_metadatas(&self) -> Vec<StripeMetadata> {
        if let Some(range) = self.file_byte_range.clone() {
            self.file_metadata
                .stripe_metadatas()
                .iter()
                .filter(|info| {
                    let offset = info.offset() as usize;
                    range.contains(&offset)
                })
                .map(|info| info.to_owned())
                .collect::<Vec<_>>()
        } else {
            self.file_metadata.stripe_metadatas().to_vec()
        }
    }
}

impl<R: ChunkReader> Iterator for Cursor<R> {
    type Item = Result<Stripe>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_stripe_metadatas()
            .get(self.stripe_index)
            .map(|info| {
                let stripe = Stripe::new(
                    &mut self.reader,
                    &self.file_metadata,
                    &self.projected_data_type.clone(),
                    info,
                );
                self.stripe_index += 1;
                stripe
            })
    }
}
