use crate::conn_core::AmConnCore;
use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::resultset::AmRsCore;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::{HdbError, HdbResult};

use serde;
use serde_db::de::DeserializableRow;
use std::fmt;
use std::sync::Arc;

/// A single line of a `ResultSet`, consisting of the contained `HdbValue`s and
/// a reference to the metadata.
///
/// `Row` has several methods that support an efficient data transfer into your own data structures.
///
/// You also can access individual values with `row[idx]`, or iterate over the values (with
/// `row.iter()` or `for value in row {...}`).
#[derive(Debug)]
pub struct Row {
    metadata: Arc<ResultSetMetadata>,
    value_iter: <Vec<HdbValue<'static>> as IntoIterator>::IntoIter,
}

impl Row {
    /// Factory for row.
    pub(crate) fn new(metadata: Arc<ResultSetMetadata>, values: Vec<HdbValue<'static>>) -> Row {
        Row {
            metadata,
            value_iter: values.into_iter(),
        }
    }

    /// Converts the entire Row into a rust value.
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::into_typed()");
        Ok(DeserializableRow::into_typed(self)?)
    }

    /// Removes and returns the next value.
    pub fn next_value(&mut self) -> Option<HdbValue<'static>> {
        self.value_iter.next()
    }

    /// Returns the length of the row.
    pub fn len(&self) -> usize {
        trace!("Row::len()");
        self.value_iter.len()
    }

    /// Returns true if the row contains no value.
    pub fn is_empty(&self) -> bool {
        self.value_iter.as_slice().is_empty()
    }

    /// Converts itself in the single contained value.
    ///
    /// Fails if the row is empty or has more than one value.
    pub fn into_single_value(mut self) -> HdbResult<HdbValue<'static>> {
        if self.len() > 1 {
            Err(HdbError::Usage("Row has more than one field".to_owned()))
        } else {
            self.next_value()
                .ok_or_else(|| HdbError::Usage("Row is empty".to_owned()))
        }
    }

    /// Returns the metadata.
    pub fn metadata(&self) -> &ResultSetMetadata {
        trace!("Row::metadata()");
        &(self.metadata)
    }

    pub(crate) fn number_of_fields(&self) -> usize {
        self.metadata.number_of_fields()
    }

    pub(crate) fn parse(
        md: std::sync::Arc<ResultSetMetadata>,
        o_am_rscore: &Option<AmRsCore>,
        am_conn_core: &AmConnCore,
        rdr: &mut dyn std::io::BufRead,
    ) -> HdbResult<Row> {
        let no_of_cols = md.number_of_fields();
        let mut values = Vec::<HdbValue>::new();
        for c in 0..no_of_cols {
            let type_id = md.type_id(c)?;
            let nullable = md.nullable(c)?;
            let scale = md.scale(c)?;
            trace!(
                "Parsing column {}, {}{:?}",
                c,
                if nullable { "Nullable " } else { "" },
                type_id,
            );
            let value = HdbValue::parse_from_reply(
                type_id,
                scale,
                nullable,
                am_conn_core,
                o_am_rscore,
                rdr,
            )?;
            values.push(value);
        }
        let row = Row::new(md, values);
        Ok(row)
    }
}

/// Support indexing.
impl std::ops::Index<usize> for Row {
    type Output = HdbValue<'static>;
    fn index(&self, idx: usize) -> &HdbValue<'static> {
        &self.value_iter.as_slice()[idx]
    }
}

/// Row is an iterator with item = HdbValue.
impl Iterator for Row {
    type Item = HdbValue<'static>;
    fn next(&mut self) -> Option<HdbValue<'static>> {
        self.next_value()
    }
}

impl fmt::Display for Row {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for v in self.value_iter.as_slice() {
            write!(fmt, "{}, ", &v)?;
        }
        Ok(())
    }
}
