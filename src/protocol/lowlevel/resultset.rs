use super::typed_value::*;
use super::resultset_metadata::*;

use std::io::Result as IoResult;
use std::io::BufRead;


#[derive(Debug)]
pub struct ResultSet {
    rows: Vec<Row>,
    metadata: ResultSetMetadata,
}
impl ResultSet {
    pub fn size(&self) -> usize {
        let mut size = 0;
        for row in &self.rows {
            size += row.size();
        }
        size
    }
    pub fn parse(count: i32, rsm: &ResultSetMetadata, rdr: &mut BufRead) -> IoResult<ResultSet> {
        let no_of_cols = rsm.count();
        let mut result = ResultSet {rows: Vec::<Row>::new(), metadata: (*rsm).clone()};
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
}


#[derive(Debug)]
pub struct Row {
    values: Vec<TypedValue>,
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
