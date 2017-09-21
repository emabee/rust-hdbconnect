
use std::fmt;
use std::vec;
use std::sync::Arc;
use super::db_value::DbValue;
use super::rs_metadata::RsMetadata;
use super::deser_row::DeserializableRow;
use super::deserialization_error::DeserError;


/// A generic implementation of a single line of a ResultSet.
#[derive(Debug,Clone)]
pub struct Row<MD: RsMetadata, TV: DbValue> {
    metadata: Arc<MD>,
    values: Vec<TV>,
}

/// Factory for row.
pub fn new_row<MD: RsMetadata, TV: DbValue>(metadata: Arc<MD>, values: Vec<TV>) -> Row<MD, TV> {
    Row {
        metadata: metadata,
        values: values,
    }
}

impl<MD: RsMetadata, TV: DbValue> DeserializableRow for Row<MD, TV> {
    type V = TV;
    type E = DeserError;

    /// Returns a clone of the ith value.
    fn get(&self, i: usize) -> Result<&TV, Self::E> {
        self.values
            .get(i)
            .map(|tv| tv.clone())
            .ok_or(DeserError::UnknownField("element with index {} does not exist".to_owned()))
    }


    /// Returns the length of the row.
    fn len(&self) -> usize {
        self.values.len()
    }

    /// Removes and returns the last value.
    fn pop(&mut self) -> Option<TV> {
        self.values.pop()
    }

    /// Returns a reference to the last value.
    fn last(&self) -> Option<&TV> {
        self.values.last()
    }

    /// Returns the name of the column at the specified index
    fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        self.metadata.get_fieldname(field_idx)
    }

    /// Reverses the order of the values
    fn reverse_values(&mut self) {
        self.values.reverse()
    }
}

impl<MD: RsMetadata, TV: DbValue> IntoIterator for Row<MD, TV> {
    type Item = TV;
    type IntoIter = vec::IntoIter<TV>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<MD: RsMetadata, TV: DbValue> fmt::Display for Row<MD, TV>
    where TV: fmt::Display
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for value in &self.values {
            fmt::Display::fmt(&value, fmt).unwrap(); // write the value
            write!(fmt, ", ").unwrap();
        }
        Ok(())
    }
}
