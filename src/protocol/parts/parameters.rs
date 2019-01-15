use super::hdb_value::HdbValue;
use crate::HdbResult;

use std::io;

// A single row of parameters.
#[derive(Default, Debug, Clone)]
pub(crate) struct ParameterRow(Vec<HdbValue>);

impl ParameterRow {
    /// Constructor.
    pub fn new(vec: Vec<HdbValue>) -> ParameterRow {
        ParameterRow(vec)
    }

    pub(crate) fn size(&self) -> HdbResult<usize> {
        let mut size = 0;
        for value in &(self.0) {
            size += value.size()?;
        }
        Ok(size)
    }

    pub(crate) fn emit<T: io::Write>(&self, w: &mut T) -> HdbResult<()> {
        let mut data_pos = 0_i32;
        // emit the values (LOBs only emit their header, the data follow below)
        for value in &(self.0) {
            value.emit(&mut data_pos, w)?;
        }

        // emit LOB data
        for value in &(self.0) {
            match *value {
                HdbValue::BLOB(ref blob) | HdbValue::N_BLOB(Some(ref blob)) => {
                    w.write(blob.ref_to_bytes()?)?;
                }
                _ => {}
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

    pub(crate) fn emit<T: io::Write>(&self, w: &mut T) -> HdbResult<()> {
        for row in &self.rows {
            row.emit(w)?;
        }
        Ok(())
    }

    pub(crate) fn count(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn size(&self) -> HdbResult<usize> {
        let mut size = 0;
        for row in &self.rows {
            size += row.size()?;
        }
        Ok(size)
    }
}
