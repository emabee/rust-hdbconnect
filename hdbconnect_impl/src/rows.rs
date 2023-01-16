use crate::{HdbResult, HdbValue, ResultSetMetadata, Row};
use std::sync::Arc;

/// Representation of a `ResultSet` that is fully loaded.
///
/// Since serde is completely sync, we cannot use asynchronous fetching during deserialization,
/// which makes it necessary to fetch all data before we call serde.
#[derive(Debug)]
pub struct Rows {
    pub(crate) metadata: Arc<ResultSetMetadata>,
    pub(crate) number_of_rows: usize,
    pub(crate) row_iter: <Vec<Row> as IntoIterator>::IntoIter,
}
impl Rows {
    #[cfg(feature = "sync")]
    pub(crate) fn new(metadata: Arc<ResultSetMetadata>, mut rows: Vec<Row>) -> HdbResult<Rows> {
        let number_of_rows = rows.len();

        let lob_field_indices: Vec<usize> = metadata
            .iter()
            .enumerate()
            .filter_map(|(n, fmd)| if fmd.is_lob() { Some(n) } else { None })
            .collect();
        for idx in lob_field_indices {
            for row in &mut rows {
                match row[idx] {
                    HdbValue::BLOB(ref mut blob) => blob.sync_load_complete()?,
                    HdbValue::CLOB(ref mut clob) => clob.sync_load_complete()?,
                    HdbValue::NCLOB(ref mut nclob) => nclob.sync_load_complete()?,
                    _ => {}
                }
            }
        }

        let row_iter = rows.into_iter();
        Ok(Rows {
            metadata,
            number_of_rows,
            row_iter,
        })
    }

    #[cfg(feature = "async")]
    pub(crate) async fn new(
        metadata: Arc<ResultSetMetadata>,
        mut rows: Vec<Row>,
    ) -> HdbResult<Rows> {
        let number_of_rows = rows.len();
        let lob_field_indices: Vec<usize> = metadata
            .iter()
            .enumerate()
            .filter_map(|(n, fmd)| if fmd.is_lob() { Some(n) } else { None })
            .collect();
        for idx in lob_field_indices {
            for row in &mut rows {
                match row[idx] {
                    HdbValue::BLOB(ref mut blob) => blob.async_load_complete().await?,
                    HdbValue::CLOB(ref mut clob) => clob.async_load_complete().await?,
                    HdbValue::NCLOB(ref mut nclob) => nclob.async_load_complete().await?,

                    _ => {}
                }
            }
        }

        let row_iter = rows.into_iter();
        Ok(Rows {
            metadata,
            number_of_rows,
            row_iter,
        })
    }
}

impl Iterator for Rows {
    type Item = Row;
    fn next(&mut self) -> Option<Row> {
        self.row_iter.next()
    }
}
