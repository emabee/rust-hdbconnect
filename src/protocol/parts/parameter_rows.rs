use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::{HdbError, HdbResult, HdbValue};
use serde_db::ser::to_params;
use std::io::Write;

// Implementation of the PARAMETERS part.
//
// Contains rows of input parameters.
// The argument count of the part defines how many rows of parameters are included.
#[derive(Debug)]
pub(crate) struct ParameterRows<'a>(Vec<ParameterRow<'a>>);
impl<'a> ParameterRows<'a> {
    pub fn new() -> ParameterRows<'a> {
        ParameterRows(Vec::<ParameterRow>::new())
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn push_hdb_values(
        &mut self,
        hdb_parameters: Vec<HdbValue<'a>>,
        descriptors: &ParameterDescriptors,
    ) -> HdbResult<()> {
        self.0
            .push(ParameterRow::new(hdb_parameters, &descriptors)?);
        Ok(())
    }

    pub(crate) fn emit<T: Write>(
        &self,
        descriptors: &ParameterDescriptors,
        w: &mut T,
    ) -> HdbResult<()> {
        for row in &self.0 {
            row.emit(descriptors, w)?;
        }
        Ok(())
    }

    pub(crate) fn count(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn size(&self, descriptors: &ParameterDescriptors) -> HdbResult<usize> {
        let mut size = 0;
        for row in &self.0 {
            size += row.size(descriptors)?;
        }
        Ok(size)
    }
}

impl ParameterRows<'static> {
    pub(crate) fn push<T: serde::ser::Serialize>(
        &mut self,
        input: &T,
        descriptors: &ParameterDescriptors,
    ) -> HdbResult<()> {
        self.0.push(ParameterRow::new(
            to_params(input, &mut descriptors.iter_in())?,
            &descriptors,
        )?);
        Ok(())
    }
}

// A single row of parameters.
#[derive(Default, Debug)]
struct ParameterRow<'a>(Vec<HdbValue<'a>>);

impl<'a> ParameterRow<'a> {
    // Constructor, fails if the provided `HdbValue`s are not compatible with the in-descriptors.
    pub fn new(
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
                return Err(HdbError::Impl(
                    "ParameterRow::new(): Not enough metadata".to_string(),
                ));
            }
        }
        Ok(ParameterRow(hdb_parameters))
    }

    fn size(&self, descriptors: &ParameterDescriptors) -> HdbResult<usize> {
        let mut size = 0;
        let mut in_descriptors = descriptors.iter_in();
        for value in &(self.0) {
            match in_descriptors.next() {
                Some(descriptor) => {
                    size += value.size(descriptor.type_id())?;
                }
                None => {
                    return Err(HdbError::Impl(
                        "ParameterRow::size(): Not enough metadata".to_string(),
                    ));
                }
            }
        }

        Ok(size)
    }

    fn emit<T: Write>(&self, descriptors: &ParameterDescriptors, w: &mut T) -> HdbResult<()> {
        let mut data_pos = 0_i32;
        let mut in_descriptors = descriptors.iter_in();
        for value in &(self.0) {
            // emit the value
            match in_descriptors.next() {
                Some(descriptor) => {
                    value.emit(&mut data_pos, descriptor, w)?;
                }
                None => {
                    return Err(HdbError::Impl(
                        "ParameterRow::emit(): Not enough metadata".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}
