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
    pub(crate) fn new_sync(
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
                    HdbValue::SYNC_BLOB(ref mut blob) => blob.load_complete()?,
                    HdbValue::SYNC_CLOB(ref mut clob) => clob.load_complete()?,
                    HdbValue::SYNC_NCLOB(ref mut nclob) => nclob.load_complete()?,
                    _ => {}
                }
            }
        }

        Ok(Rows {
            metadata,
            number_of_rows,
            row_iter: rows.into_iter(),
        })
    }

    #[cfg(feature = "async")]
    pub(crate) async fn new_async(
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
                    HdbValue::ASYNC_BLOB(ref mut blob) => blob.load_complete().await?,
                    HdbValue::ASYNC_CLOB(ref mut clob) => clob.load_complete().await?,
                    HdbValue::ASYNC_NCLOB(ref mut nclob) => nclob.load_complete().await?,
                    _ => {}
                }
            }
        }

        Ok(Rows {
            metadata,
            number_of_rows,
            row_iter: rows.into_iter(),
        })
    }
}

impl Iterator for Rows {
    type Item = Row;
    fn next(&mut self) -> Option<Row> {
        self.row_iter.next()
    }
}
