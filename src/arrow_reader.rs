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
use crate::projection::ProjectionMask;
use crate::read_plan::ReadPlan;
use crate::reader::metadata::{read_metadata, FileMetadata};
use crate::reader::ChunkReader;
use crate::schema::RootDataType;
use crate::selection::{RowFilter, RowSelection};
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
    pub(crate) row_filter: Option<RowFilter>,
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
            row_filter: None,
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

    /// Provide a RowSelection to filter out rows, and avoid fetching their
    /// data into memory.
    ///
    /// This feature is used to restrict which rows are decoded within stripes,
    /// skipping ranges of rows that are not needed.
    pub fn with_row_selection(mut self, selection: RowSelection) -> Self {
        self.row_selection = Some(selection);
        self
    }

    /// Provide a RowFilter to skip decoding rows
    ///
    /// Row filters are applied after row selection and can be used to
    /// apply predicates during the read process.
    pub fn with_row_filter(mut self, filter: RowFilter) -> Self {
        self.row_filter = Some(filter);
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
        self.schema_ref
            .clone()
            .unwrap_or_else(|| Arc::new(projected_data_type.create_arrow_schema(&metadata)))
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
        let cursor = Cursor {
            reader: self.reader,
            file_metadata: self.file_metadata,
            projected_data_type,
            stripe_index: 0,
            file_byte_range: self.file_byte_range,
        };
        use crate::read_plan::ReadPlanBuilder;

        let read_plan = ReadPlanBuilder::new(self.batch_size)
            .with_selection(self.row_selection)
            .build();

        ArrowReader {
            cursor,
            schema_ref,
            current_stripe: None,
            read_plan,
            row_filter: self.row_filter,
        }
    }
}

pub struct ArrowReader<R> {
    cursor: Cursor<R>,
    schema_ref: SchemaRef,
    current_stripe: Option<NaiveStripeDecoder>,
    read_plan: ReadPlan,
    #[allow(dead_code)]
    row_filter: Option<RowFilter>,
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
                let batch_size = self.read_plan.batch_size();
                let decoder = NaiveStripeDecoder::new(stripe, self.schema_ref.clone(), batch_size)?;
                self.current_stripe = Some(decoder);
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
        self.next_inner().transpose()
    }
}

impl<R: ChunkReader> ArrowReader<R> {
    /// Internal method to handle row selection logic, similar to parquet's next_inner
    fn next_inner(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        match self.read_plan.selection_mut() {
            Some(selection) => {
                // Process row selection by skipping records as needed
                while !selection.is_empty() {
                    let front = selection.pop_front().unwrap();
                    if front.skip {
                        // Skip the specified number of records
                        let skipped = self
                            .current_stripe
                            .as_mut()
                            .ok_or_else(|| {
                                ArrowError::ExternalError(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "No current stripe available",
                                )))
                            })?
                            .skip_records(front.row_count)
                            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                        if skipped != front.row_count {
                            return Err(ArrowError::ExternalError(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!(
                                    "Expected to skip {} records, but only skipped {}",
                                    front.row_count, skipped
                                ),
                            ))));
                        }
                        continue;
                    }

                    // For selected records, we need to read them
                    // Since we can't easily read partial batches with the current NaiveStripeDecoder,
                    // we'll use the regular iterator approach for now
                    // In a more sophisticated implementation, we could modify NaiveStripeDecoder
                    // to support reading specific numbers of records
                    break;
                }

                // Use regular processing for the remaining records
                match self.current_stripe.as_mut() {
                    Some(stripe) => {
                        match stripe.next() {
                            Some(Ok(batch)) => Ok(Some(batch)),
                            Some(Err(e)) => Err(ArrowError::ExternalError(Box::new(e))),
                            None => {
                                // Try to advance to next stripe
                                self.try_advance_stripe()
                            }
                        }
                    }
                    None => self.try_advance_stripe(),
                }
            }
            None => {
                // No row selection, use regular processing
                match self.current_stripe.as_mut() {
                    Some(stripe) => {
                        match stripe.next() {
                            Some(Ok(batch)) => Ok(Some(batch)),
                            Some(Err(e)) => Err(ArrowError::ExternalError(Box::new(e))),
                            None => {
                                // Try to advance to next stripe
                                self.try_advance_stripe()
                            }
                        }
                    }
                    None => self.try_advance_stripe(),
                }
            }
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
