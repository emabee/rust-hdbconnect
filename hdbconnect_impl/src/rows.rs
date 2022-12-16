use crate::{ResultSetMetadata, Row};
use std::sync::Arc;

#[derive(Debug)]
pub struct Rows {
    pub(crate) metadata: Arc<ResultSetMetadata>,
    pub(crate) number_of_rows: usize,
    pub(crate) row_iter: <Vec<Row> as IntoIterator>::IntoIter,
}
impl Rows {
    pub(crate) fn new(
        metadata: Arc<ResultSetMetadata>,
        number_of_rows: usize,
        row_iter: <Vec<Row> as IntoIterator>::IntoIter,
    ) -> Rows {
        Rows {
            metadata,
            number_of_rows,
            row_iter,
        }
    }
}
impl Iterator for Rows {
    type Item = Row;
    fn next(&mut self) -> Option<Row> {
        self.row_iter.next()
    }
}
