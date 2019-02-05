use super::hdb_value::HdbValue;
use crate::protocol::parts::parameter_descriptor::{ParameterDescriptor, ParameterDirection};
use crate::{HdbError, HdbResult};

use std::io;

// A single row of parameters.
#[derive(Default, Debug, Clone)]
pub(crate) struct ParameterRow(Vec<HdbValue>);

impl ParameterRow {
    /// Constructor.
    pub fn new(
        hdb_values: Vec<HdbValue>,
        descriptors: &[ParameterDescriptor],
    ) -> HdbResult<ParameterRow> {
        let mut iter = descriptors.iter();
        for value in &(hdb_values) {
            // find next IN or INOUT descriptor:: FIXME
            let mut o_descriptor: Option<&ParameterDescriptor> = None;
            while let Some(descr) = iter.next() {
                match descr.direction() {
                    ParameterDirection::OUT => {}
                    ParameterDirection::IN | ParameterDirection::INOUT => {
                        o_descriptor = Some(descr);
                        break;
                    }
                }
            }
            match o_descriptor {
                Some(descriptor) => {
                    descriptor
                        .type_id()
                        .matches_value_type(value.type_id_for_emit(descriptor.type_id())?)?;
                }
                None => {
                    return Err(HdbError::Impl(
                        "ParameterRow::new(): Not enough metadata".to_string(),
                    ));
                }
            }
        }

        Ok(ParameterRow(hdb_values))
    }

    pub(crate) fn size(&self, descriptors: &[ParameterDescriptor]) -> HdbResult<usize> {
        let mut size = 0;
        let mut iter = descriptors.iter();
        for value in &(self.0) {
            // find next IN or INOUT descriptor
            let mut o_descriptor: Option<&ParameterDescriptor> = None;
            while let Some(descr) = iter.next() {
                match descr.direction() {
                    ParameterDirection::OUT => {}
                    ParameterDirection::IN | ParameterDirection::INOUT => {
                        o_descriptor = Some(descr);
                        break;
                    }
                }
            }
            match o_descriptor {
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
        descriptors: &[ParameterDescriptor],
        w: &mut T,
    ) -> HdbResult<()> {
        let mut data_pos = 0_i32;
        let mut iter = descriptors.iter();
        for value in &(self.0) {
            // find next IN or INOUT descriptor
            let mut o_descriptor: Option<&ParameterDescriptor> = None;
            while let Some(descr) = iter.next() {
                match descr.direction() {
                    ParameterDirection::OUT => {}
                    ParameterDirection::IN | ParameterDirection::INOUT => {
                        o_descriptor = Some(descr);
                        break;
                    }
                }
            }
            // emit the value
            match o_descriptor {
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
        par_md: &[ParameterDescriptor],
        w: &mut T,
    ) -> HdbResult<()> {
        for row in &self.rows {
            row.emit(par_md, w)?;
        }
        Ok(())
    }

    pub(crate) fn count(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn size(&self, descriptors: &[ParameterDescriptor]) -> HdbResult<usize> {
        let mut size = 0;
        for row in &self.rows {
            size += row.size(descriptors)?;
        }
        Ok(size)
    }
}
