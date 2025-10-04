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
use crate::reader::metadata::{read_metadata, FileMetadata};
use crate::reader::ChunkReader;
use crate::row_selection::RowSelection;
use crate::schema::RootDataType;
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
        ArrowReader {
            cursor,
            schema_ref,
            current_stripe: None,
            batch_size: self.batch_size,
            row_selection: self.row_selection,
        }
    }
}

pub struct ArrowReader<R> {
    cursor: Cursor<R>,
    schema_ref: SchemaRef,
    current_stripe: Option<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>>,
    batch_size: usize,
    row_selection: Option<RowSelection>,
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
                // Split off the row selection for this stripe
                let stripe_rows = stripe.number_of_rows();
                let selection = self.row_selection.as_mut().and_then(|s| {
                    if s.row_count() > 0 {
                        Some(s.split_off(stripe_rows))
                    } else {
                        None
                    }
                });

                let decoder = NaiveStripeDecoder::new_with_selection(
                    stripe,
                    self.schema_ref.clone(),
                    self.batch_size,
                    selection,
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
