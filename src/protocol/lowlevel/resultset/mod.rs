// pub use self::resde::{
//     RsDeserializer,
//     from_resultset,
// };
// pub use self::rs_error::{RsError, RsErrorCode, RsResult};
//

mod deserialize;
mod rs_error;

use super::typed_value::*;
use super::resultset_metadata::*;

use serde;
use std::io;

#[derive(Debug,Clone)]
pub struct ResultSet {
    pub rows: Vec<Row>,
    pub metadata: ResultSetMetadata,
}
impl ResultSet {
    pub fn size(&self) -> usize {
        let mut size = 0;
        for row in &self.rows {
            size += row.size();
        }
        size
    }
    pub fn parse(count: i32, rsm: &ResultSetMetadata, rdr: &mut io::BufRead) -> io::Result<ResultSet> {
        let no_of_cols = rsm.count();
        let mut result = ResultSet {rows: Vec::<Row>::new(), metadata: (*rsm).clone()};  // FIXME get rid of clone()
        for r in 0..count {
            let mut row = Row{values: Vec::<TypedValue>::new()};
            for c in 0..no_of_cols {
                let typecode = rsm.fields.get(c as usize).unwrap().value_type;
                trace!("Parsing row {}, column {}, typecode {}.",r,c,typecode);
                let value = try!(TypedValue::parse_value(typecode,rdr));
                trace!("Found value {:?}", value);
                row.values.push(value);
            }
            result.rows.push(row);
        }
        Ok(result)
    }

    pub fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        self.metadata.get_fieldname(field_idx)
    }

    pub fn get_value(&self, row: usize, column: usize) -> Option<&TypedValue> {
        match self.rows.get(row) {
            Some(row) => row.values.get(column),
            None => None,
        }
    }

    // pub fn no_of_rows(&self) -> usize {
    //     self.rows.len()
    // }
    //
    // pub fn no_of_cols(&self) -> usize {
    //     self.metadata.fields.len()
    // }

    /// Translates a generic result set into a given type
    pub fn as_table<T>(self) -> io::Result<T>
      where T: serde::de::Deserialize
    {
        trace!("ResultSet::as_table()");
        let mut deserializer = self::deserialize::RsDeserializer::new(self);
        serde::de::Deserialize::deserialize(&mut deserializer).map_err(|e|{io::Error::from(e)})
    }
}


#[derive(Debug,Clone)]
pub struct Row {
    pub values: Vec<TypedValue>,
}
impl Row{
    pub fn size(&self) -> usize {
        let mut size = 0;
        for value in &self.values {
            size += value.size();
        }
        size
    }
}
