use {DbcError, DbcResult};
use protocol::lowlevel::parts::output_parameters::OutputParameters;
use protocol::lowlevel::parts::resultset::ResultSet;
use std::fmt;

const ERR_1: &'static str = "Wrong call to as_resultset()";
const ERR_2: &'static str = "Wrong call to as_affected_rows()";
const ERR_3: &'static str = "Wrong call to as_success()";
const ERR_4: &'static str = "Wrong call to get_success()";
const ERR_5: &'static str = "Wrong call to get_resultset()";
const ERR_6: &'static str = "Wrong call to get_output_parameters()";


/// Represents the database response to a command.
///
#[derive(Debug)]
pub enum DbResponse {
    SingleReturnValue(DbReturnValue),
    MultipleReturnValues(Vec<DbReturnValue>),
}

#[derive(Debug)]
pub enum DbReturnValue {
    ResultSet(ResultSet),
    AffectedRows(Vec<usize>),
    OutputParameters(OutputParameters),
    Success,
}

impl DbResponse {
    /// Turns itself into a single resultset, if that can be done without loss
    pub fn as_resultset(self) -> DbcResult<ResultSet> {
        match self {
            DbResponse::SingleReturnValue(DbReturnValue::ResultSet(rs)) => Ok(rs),
            _ => Err(DbcError::EvaluationError(ERR_1)),
        }
    }

    /// Turns itself into a single RowsAffected, if that can be done without loss
    pub fn as_affected_rows(self) -> DbcResult<Vec<usize>> {
        match self {
            DbResponse::SingleReturnValue(DbReturnValue::AffectedRows(array)) => Ok(array),
            _ => Err(DbcError::EvaluationError(ERR_2)),
        }
    }

    /// Returns simply (), if that can be done without loss
    pub fn as_success(self) -> DbcResult<()> {
        match self {
            DbResponse::SingleReturnValue(DbReturnValue::Success) => Ok(()),
            _ => Err(DbcError::EvaluationError(ERR_3)),
        }
    }

    /// returns the latest object as true, if that is a success
    pub fn get_success(&mut self) -> DbcResult<()> {
        if let DbResponse::MultipleReturnValues(ref mut vec) = *self {
            match vec.iter().rposition(|x: &DbReturnValue| {
                match *x {
                    DbReturnValue::AffectedRows(ref vec) => {
                        if vec.len() == 1 && vec.get(0) == Some(&0) { true } else { false }
                    }
                    DbReturnValue::Success => true,
                    _ => false,
                }
            }) {
                Some(idx) => {
                    vec.remove(idx);
                    Ok(())
                }
                None => Err(DbcError::EvaluationError("No Success found in DbResponse")),
            }
        } else {
            Err(DbcError::EvaluationError(ERR_4))
        }
    }

    /// returns the latest object as ResultSet, if it is one
    pub fn get_resultset(&mut self) -> DbcResult<ResultSet> {
        if let DbResponse::MultipleReturnValues(ref mut vec) = *self {
            if let Some(DbReturnValue::ResultSet(mut rs)) = vec.pop() {
                try!(rs.fetch_all());
                return Ok(rs);
            }
        }
        Err(DbcError::EvaluationError(ERR_5))
    }

    /// returns the latest object as ResultSet, if it is one
    pub fn get_output_parameters(&mut self) -> DbcResult<OutputParameters> {
        if let DbResponse::MultipleReturnValues(ref mut vec) = *self {
            if let Some(DbReturnValue::OutputParameters(op)) = vec.pop() {
                return Ok(op);
            }
        }
        Err(DbcError::EvaluationError(ERR_6))
    }
}

impl fmt::Display for DbResponse {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DbResponse::SingleReturnValue(ref dbretval) => {
                try!(fmt::Display::fmt(dbretval, fmt));
            }
            DbResponse::MultipleReturnValues(ref vec) => {
                for dbretval in vec {
                    try!(fmt::Display::fmt(dbretval, fmt));
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for DbReturnValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DbReturnValue::AffectedRows(ref vec) => try!(fmt::Debug::fmt(vec, fmt)),
            DbReturnValue::OutputParameters(ref op) => try!(fmt::Display::fmt(op, fmt)),
            DbReturnValue::ResultSet(ref rs) => try!(fmt::Display::fmt(rs, fmt)),
            DbReturnValue::Success => try!(fmt::Display::fmt("Success", fmt)),
        }
        Ok(())
    }
}

pub mod factory {
    use super::{DbResponse, DbReturnValue};
    use {DbcError, DbcResult};
    use protocol::lowlevel::parts::resultset::ResultSet;
    use protocol::lowlevel::parts::rows_affected::RowsAffected;
    use protocol::lowlevel::parts::output_parameters::OutputParameters;

    #[derive(Debug)]
    pub enum InternalReturnValue {
        ResultSet(ResultSet),
        AffectedRows(Vec<RowsAffected>),
        OutputParameters(OutputParameters),
    }

    pub fn resultset(mut int_return_values: Vec<InternalReturnValue>) -> DbcResult<DbResponse> {
        if int_return_values.len() > 1 {
            return Err(DbcError::EvaluationError("Only a single ResultSet was expected"));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::ResultSet(rs)) => Ok(DbResponse::SingleReturnValue(DbReturnValue::ResultSet(rs))),
            None => return Err(DbcError::EvaluationError("Nothing found, but a single Resultset was expected")),
            _ => return Err(DbcError::EvaluationError("Wrong DbReturnValue, a single Resultset was expected")),
        }
    }

    pub fn rows_affected(mut int_return_values: Vec<InternalReturnValue>) -> DbcResult<DbResponse> {
        if int_return_values.len() > 1 {
            return Err(DbcError::EvaluationError("Only a single AffectedRows was expected"));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::AffectedRows(vec_ra)) => {
                let mut vec_i = Vec::<usize>::new();
                for ra in vec_ra {
                    match ra {
                        RowsAffected::Count(i) => vec_i.push(i),
                        RowsAffected::SuccessNoInfo => vec_i.push(0),
                        RowsAffected::ExecutionFailed => {
                            return Err(DbcError::EvaluationError("Found unexpected returnvalue ExecutionFailed"));
                        }
                    }
                }
                Ok(DbResponse::SingleReturnValue(DbReturnValue::AffectedRows(vec_i)))
            }
            Some(InternalReturnValue::OutputParameters(_)) => {
                return Err(DbcError::EvaluationError("Found OutputParameters, but a single AffectedRows was expected"))
            }
            Some(InternalReturnValue::ResultSet(_)) => {
                return Err(DbcError::EvaluationError("Found ResultSet, but a single AffectedRows was expected"))
            }
            None => return Err(DbcError::EvaluationError("Nothing found, but a single AffectedRows was expected")),
        }
    }

    pub fn success(mut int_return_values: Vec<InternalReturnValue>) -> DbcResult<DbResponse> {
        if int_return_values.len() > 1 {
            return Err(DbcError::EvaluationError("found multiple InternalReturnValues, but only a single Success \
                                                  was expected"));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::AffectedRows(mut vec_ra)) => {
                if vec_ra.len() != 1 {
                    return Err(DbcError::EvaluationError("found no or multiple affected-row-counts, but only a \
                                                          single Success was expected"));
                }
                match vec_ra.pop().unwrap() {
                    RowsAffected::Count(i) if i > 0 => {
                        Err(DbcError::EvaluationError("found an affected-row-count > 0, but only a single Success \
                                                       was expected"))
                    }
                    RowsAffected::ExecutionFailed => {
                        Err(DbcError::EvaluationError("Found unexpected returnvalue ExecutionFailed"))
                    }
                    _ => Ok(DbResponse::SingleReturnValue(DbReturnValue::Success)),
                }
            }
            Some(InternalReturnValue::OutputParameters(_)) => {
                Err(DbcError::EvaluationError("Found OutputParameters, but a single Success was expected"))
            }
            Some(InternalReturnValue::ResultSet(_)) => {
                Err(DbcError::EvaluationError("Found ResultSet, but a single Success was expected"))
            }
            None => Err(DbcError::EvaluationError("Nothing found, but a single Success was expected")),
        }
    }

    pub fn multiple_return_values(mut int_return_values: Vec<InternalReturnValue>) -> DbcResult<DbResponse> {
        let mut vec_dbrv = Vec::<DbReturnValue>::new();
        int_return_values.reverse();
        for irv in int_return_values {
            match irv {
                InternalReturnValue::AffectedRows(vec_ra) => {
                    let mut vec_i = Vec::<usize>::new();
                    for ra in vec_ra {
                        match ra {
                            RowsAffected::Count(i) => vec_i.push(i),
                            RowsAffected::SuccessNoInfo => vec_i.push(0),
                            RowsAffected::ExecutionFailed => {
                                return Err(DbcError::EvaluationError("Found unexpected returnvalue 'ExecutionFailed'"));
                            }
                        }
                    }
                    vec_dbrv.push(DbReturnValue::AffectedRows(vec_i));
                }
                InternalReturnValue::OutputParameters(op) => {
                    vec_dbrv.push(DbReturnValue::OutputParameters(op));
                }
                InternalReturnValue::ResultSet(rs) => {
                    vec_dbrv.push(DbReturnValue::ResultSet(rs));
                }
            }
        }
        Ok(DbResponse::MultipleReturnValues(vec_dbrv))
    }
}
