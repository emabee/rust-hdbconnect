use serde;
use serde_db::de::{DbValue, DeserializableRow};
use std::fmt;
use std::vec;
use std::sync::Arc;

use {HdbError, HdbResult};
use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
use protocol::lowlevel::parts::typed_value::TypedValue;

/// A generic implementation of a single line of a `ResultSet`.
#[derive(Clone, Debug)]
pub struct Row {
    metadata: Arc<ResultSetMetadata>,
    values: Vec<TypedValue>,
}

impl Row {
    /// Factory for row.
    pub fn new(metadata: Arc<ResultSetMetadata>, values: Vec<TypedValue>) -> Row {
        Row {
            metadata: metadata,
            values: values,
        }
    }

    /// Returns the length of the row.
    pub fn len(&self) -> usize {
        trace!("Row::len()");
        self.values.len()
    }

    /// Returns true if the row contains no value.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Removes and returns the last value.
    pub fn pop(&mut self) -> Option<TypedValue> {
        trace!("Row::pop()");
        self.values.pop()
    }

    /// Returns the name of the column at the specified index
    pub fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        trace!("Row::get_fieldname()");
        self.metadata.get_fieldname(field_idx)
    }

    /// Reverses the order of the values
    pub fn reverse_values(&mut self) {
        trace!("Row::reverse()");
        self.values.reverse()
    }

    /// Returns a clone of the ith value.
    pub fn cloned_value(&self, i: usize) -> HdbResult<TypedValue> {
        trace!("Row::cloned_value()");
        self.values.get(i)
            .cloned()
            .ok_or_else(|| HdbError::UsageError("element with index {} does not exist".to_owned()))
    }

    /// Pops and converts the last field into a plain rust value.
    pub fn pop_into<'de, T>(&mut self) -> Result<T, <Row as DeserializableRow>::E>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::pop_into()");
        Ok(DbValue::into_typed(DeserializableRow::pop(self).unwrap())?)
    }

    /// Converts a copy of the field into a plain rust value.
    pub fn field_as<'de, T>(&self, i: usize) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::field_as()");
        Ok(DbValue::into_typed(self.cloned_value(i)?)?)
    }

    /// Converts the Row into a rust value.
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::into_typed()");
        Ok(DeserializableRow::into_typed(self)?)
    }
}

impl IntoIterator for Row {
    type Item = TypedValue;
    type IntoIter = vec::IntoIter<TypedValue>;

    fn into_iter(self) -> Self::IntoIter {
        trace!("<Row as IntoIterator>::into_iter()");
        self.values.into_iter()
    }
}

impl fmt::Display for Row {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for v in &self.values {
            fmt::Display::fmt(&v, fmt)?;
            write!(fmt, "")?;
        }
        Ok(())
    }
}
