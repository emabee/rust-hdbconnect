use {DbcError,DbcResult};
use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::rows_affected::RowsAffected;


pub enum DbResponse {
    ResultSet(ResultSet),
    RowsAffected(Vec<RowsAffected>)
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
            DbResponse::RowsAffected(v) => Ok(v),
            DbResponse::ResultSet(_) => {
                Err(DbcError::EvaluationError(String::from("The call returned a ResultSet, not a RowsAffected")))
            },
        }
    }
}
