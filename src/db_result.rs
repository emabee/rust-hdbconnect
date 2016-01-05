use {DbcError,DbcResult};
use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::rows_affected::RowsAffected;


pub enum DbResult {
    ResultSet(ResultSet),
    RowsAffected(Vec<RowsAffected>)
}

impl DbResult {
    pub fn as_resultset(self) -> DbcResult<ResultSet> {
        match self {
            DbResult::RowsAffected(_) => {
                Err(DbcError::EvaluationError(String::from("The call returned a RowsAffected, not a ResultSet")))
            },
            DbResult::ResultSet(rs) => Ok(rs),
        }
    }
    pub fn as_rows_affected(self) -> DbcResult<Vec<RowsAffected>> {
        match self {
            DbResult::RowsAffected(v) => Ok(v),
            DbResult::ResultSet(_) => {
                Err(DbcError::EvaluationError(String::from("The call returned a ResultSet, not a RowsAffected")))
            },
        }
    }
}
