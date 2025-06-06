use crate::{
    HdbResult,
    base::{OAM, RsCore},
    conn::AmConnCore,
    protocol::parts::{HdbValue, ResultSetMetadata},
    usage_err,
};
use serde_db::de::DeserializableRow;
use std::sync::Arc;

/// A single line of a `ResultSet`, consisting of the contained `HdbValue`s and
/// a reference to the metadata.
///
/// `Row` has several methods that support an efficient data transfer into your own data structures.
///
/// You also can access individual values with `row[idx]`, or iterate over the values (with
/// `row.iter()` or `for value in row {...}`).
pub struct Row {
    metadata: Arc<ResultSetMetadata>,
    value_iter: <Vec<HdbValue<'static>> as IntoIterator>::IntoIter,
}

impl Row {
    /// Factory for row.
    pub(crate) fn new(metadata: Arc<ResultSetMetadata>, values: Vec<HdbValue<'static>>) -> Self {
        Self {
            metadata,
            value_iter: values.into_iter(),
        }
    }

    /// Converts the entire Row into a rust value.
    ///
    /// # Errors
    ///
    /// `HdbError::Deserialization` if deserialization into the target type is not possible.
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Row::into_typed()");
        Ok(DeserializableRow::try_into(self)?)
    }

    /// Removes and returns the next value.
    pub fn next_value(&mut self) -> Option<HdbValue<'static>> {
        self.value_iter.next()
    }

    /// Conveniently combines `next_value()` and the value's `try_into()`.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if there is no more element.
    ///
    /// `HdbError::Deserialization` if deserialization into the target type is not possible.
    pub fn next_try_into<'de, T>(&mut self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        self.next_value()
            .ok_or_else(|| usage_err!("no more value"))?
            .try_into()
    }

    /// Returns the length of the row.
    #[must_use]
    pub fn len(&self) -> usize {
        trace!("Row::len()");
        self.value_iter.len()
    }

    /// Returns true if the row contains no value.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.value_iter.as_slice().is_empty()
    }

    /// Converts itself in the single contained value.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the row is empty or has more than one value.
    pub fn into_single_value(mut self) -> HdbResult<HdbValue<'static>> {
        if self.len() > 1 {
            Err(usage_err!("Row has more than one field"))
        } else {
            Ok(self
                .next_value()
                .ok_or_else(|| usage_err!("Row is empty"))?)
        }
    }

    /// Returns the metadata.
    #[must_use]
    pub fn metadata(&self) -> &ResultSetMetadata {
        trace!("Row::metadata()");
        &(self.metadata)
    }

    #[allow(clippy::ref_option)]
    #[cfg(feature = "sync")]
    pub(crate) fn parse_sync(
        md: Arc<ResultSetMetadata>,
        o_am_rscore: &OAM<RsCore>,
        am_conn_core: &AmConnCore,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Self> {
        let mut values = Vec::<HdbValue>::new();

        let md0 = Arc::as_ref(&md);

        // for col_idx in 0..md.len() {
        for col_md in &**md0 {
            let value = HdbValue::parse_sync(
                col_md.type_id(),
                col_md.is_array_type(),
                col_md.scale(),
                col_md.is_nullable(),
                am_conn_core,
                o_am_rscore,
                rdr,
            )?;
            values.push(value);
        }
        let row = Self::new(md, values);
        Ok(row)
    }

    #[allow(clippy::ref_option)]
    #[cfg(feature = "async")]
    pub(crate) async fn parse_async(
        md: Arc<ResultSetMetadata>,
        o_am_rscore: &OAM<RsCore>,
        am_conn_core: &AmConnCore,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Self> {
        let mut values = Vec::<HdbValue>::new();

        let md0 = Arc::as_ref(&md);

        // for col_idx in 0..md.len() {
        for col_md in &**md0 {
            let value = HdbValue::parse_async(
                col_md.type_id(),
                col_md.is_array_type(),
                col_md.scale(),
                col_md.is_nullable(),
                am_conn_core,
                o_am_rscore,
                rdr,
            )
            .await?;
            values.push(value);
        }
        let row = Self::new(md, values);
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

impl std::ops::IndexMut<usize> for Row {
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.value_iter.as_mut_slice()[idx]
    }
}

/// Row is an iterator with item `HdbValue`.
impl Iterator for Row {
    type Item = HdbValue<'static>;
    fn next(&mut self) -> Option<HdbValue<'static>> {
        self.next_value()
    }
}

impl std::fmt::Display for Row {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        for v in self.value_iter.as_slice() {
            write!(fmt, "{v}, ")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Row {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (v, md) in self.value_iter.as_slice().iter().zip(self.metadata.iter()) {
            write!(fmt, "{v:?}:[{}], ", md.type_id())?;
        }
        Ok(())
    }
}
