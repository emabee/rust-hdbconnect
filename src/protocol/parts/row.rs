use protocol::parts::hdb_value::HdbValue;
use protocol::parts::resultset_metadata::ResultSetMetadata;
use types::{BLob, CLob, NCLob};
use {HdbError, HdbResult};

use serde;
use serde_db::de::{ConversionError, DbValue, DeserializableRow};
use std::fmt;
use std::mem;
use std::sync::Arc;
use std::vec;

/// A generic implementation of a single line of a `ResultSet`.
#[derive(Clone, Debug)]
pub struct Row {
    metadata: Arc<ResultSetMetadata>,
    values: Vec<HdbValue>,
}

impl Row {
    /// Factory for row.
    pub fn new(metadata: Arc<ResultSetMetadata>, values: Vec<HdbValue>) -> Row {
        Row { metadata, values }
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

    /// Returns the name of the i'th column
    pub fn get_fieldname(&self, i: usize) -> HdbResult<&String> {
        trace!("Row::get_fieldname()");
        self.metadata.displayname(i)
    }

    /// Reverses the order of the values
    pub fn reverse_values(&mut self) {
        trace!("Row::reverse()");
        self.values.reverse()
    }

    /// Returns a clone of the ith value.
    pub fn cloned_value(&self, i: usize) -> HdbResult<HdbValue> {
        trace!("Row::cloned_value()");
        self.values
            .get(i)
            .cloned()
            .ok_or_else(|| HdbError::Usage("element with index {} does not exist".to_owned()))
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
