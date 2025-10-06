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

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StructArray},
    buffer::NullBuffer,
    datatypes::Fields,
};
use snafu::ResultExt;

use crate::error::Result;
use crate::stripe::Stripe;
use crate::{column::Column, error::ArrowSnafu};

use super::{array_decoder_factory, derive_present_vec, ArrayBatchDecoder, PresentDecoder};

pub struct StructArrayDecoder {
    fields: Fields,
    decoders: Vec<Box<dyn ArrayBatchDecoder>>,
    present: Option<PresentDecoder>,
}

impl StructArrayDecoder {
    pub fn new(column: &Column, fields: Fields, stripe: &Stripe) -> Result<Self> {
        let present = PresentDecoder::from_stripe(stripe, column);

        let decoders = column
            .children()
            .iter()
            .zip(fields.iter())
            .map(|(child, field)| array_decoder_factory(child, field.data_type(), stripe))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            decoders,
            present,
            fields,
        })
    }
}

impl ArrayBatchDecoder for StructArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;

        let child_arrays = self
            .decoders
            .iter_mut()
            .map(|child| child.next_batch(batch_size, present.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        let null_buffer = present;
        let array = StructArray::try_new(self.fields.clone(), child_arrays, null_buffer)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        use super::derive_present_vec;

        // Derive the combined present buffer like in next_batch
        let present = derive_present_vec(&mut self.present, parent_present, n).transpose()?;

        // Skip values in all child decoders
        // Pass the present buffer to children so they know which values to skip
        for decoder in &mut self.decoders {
            decoder.skip_values(n, present.as_ref())?;
        }

        Ok(())
    }
}
