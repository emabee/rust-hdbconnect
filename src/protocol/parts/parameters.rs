use super::hdb_value::HdbValue;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::{HdbError, HdbResult};

use std::io;

// A single row of parameters.
#[derive(Default, Debug, Clone)]
pub(crate) struct ParameterRow(Vec<HdbValue>);

impl ParameterRow {
    /// Constructor.
    ///
    /// Fails if the provided `HdbValue`s are not compatible with the parameter descriptors.
    pub fn new(
        hdb_values: Vec<HdbValue>,
        descriptors: &ParameterDescriptors,
    ) -> HdbResult<ParameterRow> {
        let mut in_descriptors = descriptors.iter_in();
        for hdb_value in &hdb_values {
            if let Some(descriptor) = in_descriptors.next() {
                descriptor
                    .type_id()
                    .matches_value_type(hdb_value.type_id_for_emit(descriptor.type_id())?)?;
            } else {
                return Err(HdbError::Impl(
                    "ParameterRow::new(): Not enough metadata".to_string(),
                ));
            }
        }
        Ok(ParameterRow(hdb_values))
    }

    pub(crate) fn size(&self, descriptors: &ParameterDescriptors) -> HdbResult<usize> {
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

    pub(crate) fn emit<T: io::Write>(
        &self,
        descriptors: &ParameterDescriptors,
        w: &mut T,
    ) -> HdbResult<()> {
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

        // BLOBs only emitted their header, the data now
        for value in &(self.0) {
            if let HdbValue::BLOB(ref blob) = *value {
                w.write_all(blob.ref_to_bytes()?)?;
            }
        }
        Ok(())
    }
}

// A part that contains input parameters.
//
// The argument count of the part defines how many rows of parameters are
// included.
#[derive(Clone, Debug)]
pub(crate) struct Parameters {
    rows: Vec<ParameterRow>,
}
impl Parameters {
    pub fn new(rows: Vec<ParameterRow>) -> Parameters {
        Parameters { rows }
    }

    pub(crate) fn emit<T: io::Write>(
        &self,
        descriptors: &ParameterDescriptors,
        w: &mut T,
    ) -> HdbResult<()> {
        for row in &self.rows {
            row.emit(descriptors, w)?;
        }
        Ok(())
    }

    pub(crate) fn count(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn size(&self, descriptors: &ParameterDescriptors) -> HdbResult<usize> {
        let mut size = 0;
        for row in &self.rows {
            size += row.size(descriptors)?;
        }
        Ok(size)
    }
}
