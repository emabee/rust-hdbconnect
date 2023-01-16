use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::util;
use crate::{HdbError, HdbResult, HdbValue};
use serde_db::ser::to_params;

// Implementation of the PARAMETERS part.
//
// Contains rows of input parameters.
// The argument count of the part defines how many rows of parameters are included.
#[derive(Debug)]
#[allow(clippy::new_without_default)]
pub struct ParameterRows<'a>(Vec<ParameterRow<'a>>);
impl<'a> ParameterRows<'a> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> ParameterRows<'a> {
        ParameterRows(Vec::<ParameterRow>::new())
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push_hdb_values(
        &mut self,
        hdb_parameters: Vec<HdbValue<'a>>,
        descriptors: &ParameterDescriptors,
    ) -> HdbResult<()> {
        self.0.push(ParameterRow::new(hdb_parameters, descriptors)?);
        Ok(())
    }

    #[cfg(feature = "sync")]
    pub(crate) fn sync_emit(
        &self,
        descriptors: &ParameterDescriptors,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        for row in &self.0 {
            row.sync_emit(descriptors, w)?;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub(crate) async fn async_emit<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        descriptors: &ParameterDescriptors,
        w: &mut W,
    ) -> std::io::Result<()> {
        for row in &self.0 {
            row.async_emit(descriptors, w).await?;
        }
        Ok(())
    }

    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn size(&self, descriptors: &ParameterDescriptors) -> std::io::Result<usize> {
        let mut size = 0;
        for row in &self.0 {
            size += row.size(descriptors)?;
        }
        Ok(size)
    }
}

impl ParameterRows<'static> {
    pub fn push<T: serde::ser::Serialize>(
        &mut self,
        input: &T,
        descriptors: &ParameterDescriptors,
    ) -> HdbResult<()> {
        self.0.push(ParameterRow::new(
            to_params(input, &mut descriptors.iter_in())?,
            descriptors,
        )?);
        Ok(())
    }
}

// A single row of parameters.
#[derive(Default, Debug)]
pub struct ParameterRow<'a>(Vec<HdbValue<'a>>);

impl<'a> ParameterRow<'a> {
    // Constructor, fails if the provided `HdbValue`s are not compatible with the in-descriptors.
    fn new(
        hdb_parameters: Vec<HdbValue<'a>>,
        descriptors: &ParameterDescriptors,
    ) -> HdbResult<ParameterRow<'a>> {
        let mut in_descriptors = descriptors.iter_in();
        for hdb_value in &hdb_parameters {
            if let Some(descriptor) = in_descriptors.next() {
                if !hdb_value.is_null() {
                    descriptor
                        .type_id()
                        .matches_value_type(hdb_value.type_id_for_emit(descriptor.type_id())?)?;
                }
            } else {
                return Err(HdbError::Impl("ParameterRow::new(): Not enough metadata"));
            }
        }
        Ok(ParameterRow(hdb_parameters))
    }

    fn size(&self, descriptors: &ParameterDescriptors) -> std::io::Result<usize> {
        let mut size = 0;
        let mut in_descriptors = descriptors.iter_in();
        for value in &(self.0) {
            if let Some(descriptor) = in_descriptors.next() {
                size += value.size(descriptor.type_id())?;
            } else {
                return Err(util::io_error(
                    "ParameterRow::size(): Not enough metadata".to_string(),
                ));
            }
        }

        Ok(size)
    }

    #[cfg(feature = "sync")]
    fn sync_emit(
        &self,
        descriptors: &ParameterDescriptors,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        let mut data_pos = 0_i32;
        let mut in_descriptors = descriptors.iter_in();
        for value in &(self.0) {
            // emit the value
            if let Some(descriptor) = in_descriptors.next() {
                value.sync_emit(&mut data_pos, descriptor, w)?;
            } else {
                return Err(util::io_error(
                    "ParameterRow::emit(): Not enough metadata".to_string(),
                ));
            }
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    async fn async_emit<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        descriptors: &ParameterDescriptors,
        w: &mut W,
    ) -> std::io::Result<()> {
        let mut data_pos = 0_i32;
        let mut in_descriptors = descriptors.iter_in();
        for value in &(self.0) {
            // emit the value
            if let Some(descriptor) = in_descriptors.next() {
                value.async_emit(&mut data_pos, descriptor, w).await?;
            } else {
                return Err(util::io_error(
                    "ParameterRow::emit(): Not enough metadata".to_string(),
                ));
            }
        }
        Ok(())
    }
}
