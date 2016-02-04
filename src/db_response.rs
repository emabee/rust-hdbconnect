use {DbcError,DbcResult};
use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::rows_affected::{RowsAffected,VecRowsAffected};

use std::fmt;

#[derive(Debug)]
pub enum DbResponse {
    ResultSet(ResultSet),
    RowsAffected(VecRowsAffected)
}

impl DbResponse {
    pub fn as_resultset(self) -> DbcResult<ResultSet> {
        match self {
            DbResponse::RowsAffected(_) => {
                Err(DbcError::EvaluationError(String::from("The call returned a RowsAffected, not a ResultSet")))
            },
            DbResponse::ResultSet(rs) => Ok(rs),
        }
    }
    pub fn as_rows_affected(self) -> DbcResult<Vec<RowsAffected>> {
        match self {
            DbResponse::RowsAffected(v) => Ok(v.0),
            DbResponse::ResultSet(_) => {
                Err(DbcError::EvaluationError(String::from("The call returned a ResultSet, not a VecRowsAffected")))
            },
        }
    }
}

impl fmt::Display for DbResponse {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DbResponse::ResultSet(ref result_set) => fmt::Display::fmt(&result_set, fmt),
            DbResponse::RowsAffected(ref vec_rows_affected) => fmt::Display::fmt(&vec_rows_affected, fmt),
        }
    }
}
