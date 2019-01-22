use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::types::{BLob, CLob, NCLob};
use crate::{HdbError, HdbResult};

use serde;
use serde_db::de::{ConversionError, DbValue, DeserializableRow};
use std::fmt;
use std::mem;
use std::sync::Arc;
use std::vec;

/// A single line of a `ResultSet`, consisting of the contained `HdbValue`s and
/// a reference to the metadata.
///
/// `Row` has several methods that support an efficient data transfer into your own data structures.
///
/// You also can access individual values with `row[idx]`, or iterate over the values (with
/// `row.iter()` or `for value in row {...}`).
#[derive(Clone, Debug)]
pub struct Row {
    metadata: Arc<ResultSetMetadata>,
    values: Vec<HdbValue>,
}

impl Row {
    /// Factory for row.
    pub(crate) fn new(metadata: Arc<ResultSetMetadata>, values: Vec<HdbValue>) -> Row {
        Row { metadata, values }
    }

    /// Converts the entire Row into a rust value.
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::into_typed()");
        Ok(DeserializableRow::into_typed(self)?)
    }

    /// Iterate over the values.
    pub fn iter(&self) -> std::slice::Iter<HdbValue> {
        self.values.iter()
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
    pub fn pop(&mut self) -> Option<HdbValue> {
        trace!("Row::pop()");
        self.values.pop()
    }

    /// Pops and converts the last field into a plain rust value.
    pub fn pop_into<'de, T>(&mut self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::pop_into()");
        Ok(DbValue::into_typed(DeserializableRow::pop(self).unwrap())?)
    }

    /// Swaps out a field and converts it into a plain rust value.
    pub fn field_into<'de, T>(&mut self, i: usize) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::field_into()");
        if let HdbValue::NOTHING = self.values[i] {
            Err(HdbError::Usage(
                "Row::field_into() called on Null value".to_owned(),
            ))
        } else {
            let mut tmp = HdbValue::NOTHING;
            mem::swap(&mut self.values[i], &mut tmp);
            Ok(DbValue::into_typed(tmp)?)
        }
    }

    /// Swaps out a field and converts it into an Option of a plain rust value.
    pub fn field_into_option<'de, T>(&mut self, i: usize) -> HdbResult<Option<T>>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::field_into()");
        if self.values[i].is_null() {
            Ok(None)
        } else {
            let mut tmp = HdbValue::NOTHING;
            mem::swap(&mut self.values[i], &mut tmp);
            Ok(Some(DbValue::into_typed(tmp)?))
        }
    }

    /// Swaps out a field and converts it into a CLOB.
    pub fn field_into_nclob(&mut self, i: usize) -> HdbResult<NCLob> {
        trace!("Row::field_into_nclob()");
        let mut tmp = HdbValue::NOTHING;
        mem::swap(&mut self.values[i], &mut tmp);

        match tmp {
            HdbValue::NCLOB(nclob) | HdbValue::N_NCLOB(Some(nclob)) => Ok(nclob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "The value {:?} cannot be converted into a CLOB",
                tv
            )))),
        }
    }

    /// Swaps out a field and converts it into a CLOB.
    pub fn field_into_clob(&mut self, i: usize) -> HdbResult<CLob> {
        trace!("Row::field_into_clob()");
        let mut tmp = HdbValue::NOTHING;
        mem::swap(&mut self.values[i], &mut tmp);

        match tmp {
            HdbValue::CLOB(clob) | HdbValue::N_CLOB(Some(clob)) => Ok(clob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "The value {:?} cannot be converted into a CLOB",
                tv
            )))),
        }
    }

    /// Swaps out a field and converts it into a BLob.
    pub fn field_into_blob(&mut self, i: usize) -> HdbResult<BLob> {
        trace!("Row::field_into_blob()");
        let mut tmp = HdbValue::NOTHING;
        mem::swap(&mut self.values[i], &mut tmp);

        match tmp {
            HdbValue::BLOB(blob) | HdbValue::N_BLOB(Some(blob)) => Ok(blob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "The value {:?} cannot be converted into a BLOB",
                tv
            )))),
        }
    }

    /// Reverses the order of the values
    pub(crate) fn reverse_values(&mut self) {
        trace!("Row::reverse()");
        self.values.reverse()
    }

    /// Returns the metadata.
    pub fn metadata(&self) -> &ResultSetMetadata {
        trace!("Row::metadata()");
        &(self.metadata)
    }
}

impl std::ops::Index<usize> for Row {
    type Output = HdbValue;
    fn index(&self, idx: usize) -> &HdbValue {
        &self.values[idx]
    }
}

impl IntoIterator for Row {
    type Item = HdbValue;
    type IntoIter = vec::IntoIter<HdbValue>;

    fn into_iter(self) -> Self::IntoIter {
        trace!("<Row as IntoIterator>::into_iter()");
        self.values.into_iter()
    }
}

impl fmt::Display for Row {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for v in &self.values {
            write!(fmt, "{}, ", &v)?;
        }
        Ok(())
    }
}
