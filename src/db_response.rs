use {DbcError,DbcResult};
use protocol::lowlevel::parts::resultset::ResultSet;
use std::fmt;

const ERR_1: &'static str = "The call returned a single object, but not a ResultSet";
const ERR_2: &'static str = "The call returned a single object, but not a number of affected rows";
const ERR_3: &'static str = "The call returned a single number of affected rows, but it was non-zero";
const ERR_4: &'static str = "Multiple return values exist, conversion to a single one not possible";

#[derive(Debug)]
pub enum DbResponse {
    ResultSet(ResultSet),
    AffectedRows(usize),
}

#[derive(Debug)]
pub struct DbResponses(Vec<DbResponse>);

impl DbResponses {
    /// Turns itself into a single resultset, if that can be done without loss
    pub fn as_resultset(mut self) -> DbcResult<ResultSet> {
        match self.0.len() {
            1 => match self.0.remove(0) {
                    DbResponse::ResultSet(rs) => Ok(rs),
                    _ => Err(DbcError::EvaluationError(ERR_1)),
            },
            _ => Err(DbcError::EvaluationError(ERR_4)),
        }
    }

    /// Turns itself into a single RowsAffected, if that can be done without loss
    pub fn as_row_count(mut self) -> DbcResult<usize> {
        match self.0.len() {
            1 => match self.0.remove(0) {
                    DbResponse::AffectedRows(count) => Ok(count),
                    _ => Err(DbcError::EvaluationError(ERR_2)),
            },
            _ => Err(DbcError::EvaluationError(ERR_4)),
        }
    }

    /// Turns itself into a single RowsAffected, if that can be done without loss
    pub fn as_success(mut self) -> DbcResult<()> {
        match self.0.len() {
            1 => match self.0.remove(0) {
                    DbResponse::AffectedRows(count) => match count {
                        0 => Ok(()),
                        _ => Err(DbcError::EvaluationError(ERR_3)),
                    },
                    _ => Err(DbcError::EvaluationError(ERR_2)),
            },
            _ => Err(DbcError::EvaluationError(ERR_4)),
        }
    }
}

impl fmt::Display for DbResponses {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let v = &(self.0);
        for ref sdr in v {
            match **sdr {
                DbResponse::ResultSet(ref result_set) => try!(fmt::Display::fmt(&result_set, fmt)),
                DbResponse::AffectedRows(count) => try!(fmt::Display::fmt(&count, fmt)),
            }
        }
        Ok(())
    }
}

pub mod factory {
    use protocol::lowlevel::parts::resultset::ResultSet;
    use protocol::lowlevel::parts::rows_affected::{RowsAffected,VecRowsAffected};
    use super::{DbResponse,DbResponses};

    pub fn from_resultset(rs: ResultSet) -> DbResponses {
        let mut v = Vec::<DbResponse>::new();
        v.push(DbResponse::ResultSet(rs));
        DbResponses(v)
    }

    pub fn from_rows_affected(vra: VecRowsAffected) -> DbResponses {
        let mut v = Vec::<DbResponse>::new();
        for ra in vra.0 {
            match ra {
                RowsAffected::Success(count) => v.push(DbResponse::AffectedRows(count as usize)),
                RowsAffected::SuccessNoInfo => panic!("from_rows_affected() encountered a RowsAffected::SuccessNoInfo"),
        }}
        DbResponses(v)
    }
}
