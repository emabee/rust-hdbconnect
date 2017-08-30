use {HdbError, HdbResult};
use rs_serde::de::RowDeserializer;
use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
use protocol::lowlevel::parts::typed_value::TypedValue;
use serde;
use std::fmt;
use std::vec;
use std::sync::Arc;

/// A single line of a ResultSet.
#[derive(Debug,Clone)]
pub struct Row {
    metadata: Arc<ResultSetMetadata>,
    values: Vec<TypedValue>,
}

pub fn new_row(metadata: Arc<ResultSetMetadata>, values: Vec<TypedValue>) -> Row {
    Row {
        metadata: metadata,
        values: values,
    }
}

impl Row {
    /// Returns a clone of the ith value.
    pub fn get(&self, i: usize) -> HdbResult<TypedValue> {
        self.values
            .get(i)
            .map(|tv| tv.clone())
            .ok_or(HdbError::UsageError("element with index {} does not exist".to_owned()))
    }

    /// Returns the length of the row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Removes and returns the last value.
    pub fn pop(&mut self) -> Option<TypedValue> {
        self.values.pop()
    }

    /// Returns a reference to the last value.
    pub fn last(&self) -> Option<&TypedValue> {
        self.values.last()
    }

    /// Returns the name of the column at the specified index
    pub fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        self.metadata.get_fieldname(field_idx)
    }

    /// Reverses the order of the values
    pub fn reverse_values(&mut self) {
        self.values.reverse()
    }

    /// Converts the row into a struct, a tuple, or (if applicable) into a plain rust value.
    pub fn into_typed<'de, T>(self) -> HdbResult<T>
        where T: serde::de::Deserialize<'de>
    {
        trace!("ResultSet::into_typed()");
        Ok(serde::de::Deserialize::deserialize(&mut RowDeserializer::new(self))?)
    }
}

impl IntoIterator for Row {
    type Item = TypedValue;
    type IntoIter = vec::IntoIter<TypedValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl fmt::Display for Row {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for value in &self.values {
            fmt::Display::fmt(&value, fmt).unwrap(); // write the value
            write!(fmt, ", ").unwrap();
        }
        Ok(())
    }
}
