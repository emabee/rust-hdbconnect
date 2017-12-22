use protocol::lowlevel::util;
use super::PrtResult;
use super::typed_value::TypedValue;
use super::typed_value::size as typed_value_size;
use super::typed_value::serialize as typed_value_serialize;

use std::io;

/// A single row of parameters; batches can consist of many such rows
#[derive(Default, Debug, Clone)]
pub struct ParameterRow {
    pub values: Vec<TypedValue>,
}
impl ParameterRow {
    pub fn new(vtv: Vec<TypedValue>) -> ParameterRow {
        ParameterRow { values: vtv }
    }

    pub fn size(&self) -> PrtResult<usize> {
        let mut size = 0;
        for value in &(self.values) {
            size += typed_value_size(value)?;
        }
        Ok(size)
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        // FIXME must it be 1?
        let mut data_pos = 0_i32;
        // serialize the values (LOBs only serialize their header, the data follow below)
        for value in &(self.values) {
            typed_value_serialize(value, &mut data_pos, w)?;
        }

        // serialize LOB data
        for value in &(self.values) {
            match *value {
                TypedValue::BLOB(ref blob) | TypedValue::N_BLOB(Some(ref blob)) => {
                    util::serialize_bytes(blob.ref_to_bytes()?, w)?
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// A PARAMETERS part contains input parameters.
/// The argument count of the part defines how many rows of parameters are included.
#[derive(Clone, Debug)]
pub struct Parameters {
    rows: Vec<ParameterRow>,
}
impl Parameters {
    pub fn new(rows: Vec<ParameterRow>) -> Parameters {
        Parameters { rows: rows }
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        for row in &self.rows {
            row.serialize(w)?;
        }
        Ok(())
    }

    pub fn count(&self) -> usize {
        self.rows.len()
    }

    pub fn size(&self) -> PrtResult<usize> {
        let mut size = 0;
        for row in &self.rows {
            size += row.size()?;
        }
        Ok(size)
    }
}
