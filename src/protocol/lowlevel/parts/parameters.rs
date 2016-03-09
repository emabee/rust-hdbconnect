use super::PrtResult;
use super::typed_value::TypedValue;
use super::typed_value::size as typed_value_size;
use super::typed_value::serialize as typed_value_serialize;

use std::io;

/// A single row of parameters; batches can consist of many such rows
#[derive(Debug,Clone)]
pub struct ParameterRow {
    pub values: Vec<TypedValue>,
}
impl ParameterRow {
    pub fn new() -> ParameterRow {
        ParameterRow { values: Vec::<TypedValue>::new() }
    }
    pub fn push(&mut self, val: TypedValue) {
        self.values.push(val)
    }

    pub fn size(&self) -> PrtResult<usize> {
        let mut size = 0;
        for value in &self.values {
            size += try!(typed_value_size(value));
        }
        Ok(size)
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        for value in &self.values {
            try!(typed_value_serialize(value,w));
        }
        Ok(())
    }
}


/// A PARAMETERS part contains input parameters.
/// The argument count of the part defines how many rows of parameters are included.
#[derive(Clone,Debug)]
pub struct Parameters {
    rows: Vec<ParameterRow>,
}
impl Parameters {
    pub fn new(rows: Vec<ParameterRow>) -> Parameters {
        Parameters { rows: rows }
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        for ref row in &self.rows {
            try!(row.serialize(w));
        }
        Ok(())
    }

    pub fn count(&self) -> usize {
        self.rows.len()
    }

    pub fn size(&self) -> PrtResult<usize> {
        let mut size = 0;
        for ref row in &self.rows {
            size += try!(row.size());
        }
        Ok(size)
    }
}
