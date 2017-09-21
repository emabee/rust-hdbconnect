use rs_serde::de::RowDeserializer;
use serde;
use std::convert::From;
use std::fmt;
use std::marker::Sized;
use super::db_value::DbValue;
use rs_serde::de::deserialization_error::DeserError;

/// A minimal interface for the Row type to support the deserialization.
pub trait DeserializableRow: fmt::Debug + Sized {
    /// The error type used by the database driver.
    type E: From<DeserError> + Sized;
    /// The value type used by the database driver.
    type V: DbValue + fmt::Debug;

    /// Returns a clone of the ith value.
    fn get(&self, i: usize) -> Result<&Self::V, Self::E>;

    /// Returns the length of the row.
    fn len(&self) -> usize;

    /// Removes and returns the last value.
    fn pop(&mut self) -> Option<Self::V>;

    /// Returns a reference to the last value.
    fn last(&self) -> Option<&Self::V>;

    /// Returns the name of the column at the specified index
    fn get_fieldname(&self, field_idx: usize) -> Option<&String>;

    /// Reverses the order of the values
    fn reverse_values(&mut self);

    /// Converts the row into a struct, a tuple, or (if applicable) into a plain rust value.
    fn into_typed<'de, T>(self) -> Result<T, Self::E>
        where T: serde::de::Deserialize<'de>
    {
        trace!("DeserializableRow::into_typed()");
        Ok(serde::de::Deserialize::deserialize(&mut RowDeserializer::new(self))?)
    }
}
