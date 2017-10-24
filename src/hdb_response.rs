use {HdbError, HdbResult};
use protocol::lowlevel::parts::output_parameters::OutputParameters;
use protocol::lowlevel::parts::resultset::ResultSet;
use std::fmt;

const ERR_1: &'static str = "Wrong call to as_resultset()";
const ERR_2: &'static str = "Wrong call to as_affected_rows()";
const ERR_3: &'static str = "Wrong call to as_success()";
const ERR_4: &'static str = "Wrong call to get_success()";
const ERR_5: &'static str = "Wrong call to get_resultset()";
const ERR_6: &'static str = "Wrong call to get_output_parameters()";


/// Represents all possible non-error responses to a database command.
///
#[derive(Debug)]
pub enum HdbResponse {
    /// Most commands return a single return value which can easily be evaluated here.
    SingleReturnValue(HdbReturnValue),
    /// Some commands return multiple return values of same or different types.
    MultipleReturnValues(Vec<HdbReturnValue>),
}

/// An enum that describes a single database return value.
#[derive(Debug)]
pub enum HdbReturnValue {
    /// A resultset of a query.
    ResultSet(ResultSet),
    /// A list of numbers of affected rows.
    AffectedRows(Vec<usize>),
    /// Values of output parameters of a procedure call.
    OutputParameters(OutputParameters),
    /// Indication that a db call was successful.
    Success,
}

impl HdbResponse {
    /// Turns itself into a single resultset.
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn as_resultset(self) -> HdbResult<ResultSet> {
        match self {
            HdbResponse::SingleReturnValue(HdbReturnValue::ResultSet(rs)) => Ok(rs),
            _ => Err(HdbError::EvaluationError(ERR_1)),
        }
    }

    /// Turns itself into a Vector of numbers (each number representing a number of affected rows).
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn as_affected_rows(self) -> HdbResult<Vec<usize>> {
        match self {
            HdbResponse::SingleReturnValue(HdbReturnValue::AffectedRows(array)) => Ok(array),
            _ => Err(HdbError::EvaluationError(ERR_2)),
        }
    }

    /// Turns itself into (), if the statement had returned successfully.
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn as_success(self) -> HdbResult<()> {
        match self {
            HdbResponse::SingleReturnValue(HdbReturnValue::Success) => Ok(()),
            _ => Err(HdbError::EvaluationError(ERR_3)),
        }
    }

    /// Pops and returns the latest object as (), if that is a success.
    pub fn get_success(&mut self) -> HdbResult<()> {
        if let HdbResponse::MultipleReturnValues(ref mut vec) = *self {
            match vec.iter().rposition(|x: &HdbReturnValue| match *x {
                HdbReturnValue::AffectedRows(ref vec) => vec.len() == 1 && vec.get(0) == Some(&0), 
                HdbReturnValue::Success => true,
                _ => false,
            }) {
                Some(idx) => {
                    vec.remove(idx);
                    Ok(())
                }
                None => Err(HdbError::EvaluationError("No Success found in HdbResponse")),
            }
        } else {
            Err(HdbError::EvaluationError(ERR_4))
        }
    }

    /// Pops and returns the latest object as ResultSet, if it is one.
    pub fn get_resultset(&mut self) -> HdbResult<ResultSet> {
        if let HdbResponse::MultipleReturnValues(ref mut vec) = *self {
            if let Some(HdbReturnValue::ResultSet(mut rs)) = vec.pop() {
                rs.fetch_all()?;
                return Ok(rs);
            }
        }
        Err(HdbError::EvaluationError(ERR_5))
    }

    /// Pops and returns the latest object as OutputParameters, if it is one.
    pub fn get_output_parameters(&mut self) -> HdbResult<OutputParameters> {
        if let HdbResponse::MultipleReturnValues(ref mut vec) = *self {
            if let Some(HdbReturnValue::OutputParameters(op)) = vec.pop() {
                return Ok(op);
            }
        }
        Err(HdbError::EvaluationError(ERR_6))
    }
}

impl fmt::Display for HdbResponse {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HdbResponse::SingleReturnValue(ref dbretval) => {
                fmt::Display::fmt(dbretval, fmt)?;
            }
            HdbResponse::MultipleReturnValues(ref vec) => {
                for dbretval in vec {
                    fmt::Display::fmt(dbretval, fmt)?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for HdbReturnValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HdbReturnValue::AffectedRows(ref vec) => fmt::Debug::fmt(vec, fmt)?,
            HdbReturnValue::OutputParameters(ref op) => fmt::Display::fmt(op, fmt)?,
            HdbReturnValue::ResultSet(ref rs) => fmt::Display::fmt(rs, fmt)?,
            HdbReturnValue::Success => fmt::Display::fmt("Success", fmt)?,
        }
        Ok(())
    }
}

pub mod factory {
    use super::{HdbResponse, HdbReturnValue};
    use {HdbError, HdbResult};
    use protocol::lowlevel::parts::resultset::ResultSet;
    use protocol::lowlevel::parts::rows_affected::RowsAffected;
    use protocol::lowlevel::parts::output_parameters::OutputParameters;

    #[derive(Debug)]
    pub enum InternalReturnValue {
        ResultSet(ResultSet),
        AffectedRows(Vec<RowsAffected>),
        OutputParameters(OutputParameters),
    }

    pub fn resultset(mut int_return_values: Vec<InternalReturnValue>) -> HdbResult<HdbResponse> {
        if int_return_values.len() > 1 {
            return Err(HdbError::EvaluationError("Only a single ResultSet was expected"));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::ResultSet(rs)) => {
                Ok(HdbResponse::SingleReturnValue(HdbReturnValue::ResultSet(rs)))
            }
            None => {
                Err(HdbError::EvaluationError("Nothing found, but a single Resultset was expected"))
            }
            _ => {
                Err(
                    HdbError::EvaluationError("Wrong HdbReturnValue, a single Resultset was expected"),
                )
            }
        }
    }

    pub fn rows_affected(mut int_return_values: Vec<InternalReturnValue>)
                         -> HdbResult<HdbResponse> {
        if int_return_values.len() > 1 {
            return Err(HdbError::EvaluationError("Only a single AffectedRows was expected"));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::AffectedRows(vec_ra)) => {
                let mut vec_i = Vec::<usize>::new();
                for ra in vec_ra {
                    match ra {
                        RowsAffected::Count(i) => vec_i.push(i),
                        RowsAffected::SuccessNoInfo => vec_i.push(0),
                        RowsAffected::ExecutionFailed => {
                            return Err(HdbError::EvaluationError(
                                "Found unexpected returnvalue ExecutionFailed",
                            ));
                        }
                    }
                }
                Ok(HdbResponse::SingleReturnValue(HdbReturnValue::AffectedRows(vec_i)))
            }
            Some(InternalReturnValue::OutputParameters(_)) => {
                Err(HdbError::EvaluationError(
                    "Found OutputParameters, but a single AffectedRows was expected",
                ))
            }
            Some(InternalReturnValue::ResultSet(_)) => {
                Err(
                    HdbError::EvaluationError("Found ResultSet, but a single AffectedRows was expected"),
                )
            }
            None => {
                Err(
                    HdbError::EvaluationError("Nothing found, but a single AffectedRows was expected"),
                )
            }
        }
    }

    pub fn success(mut int_return_values: Vec<InternalReturnValue>) -> HdbResult<HdbResponse> {
        if int_return_values.len() > 1 {
            return Err(HdbError::EvaluationError(
                "found multiple InternalReturnValues, but only a single Success was expected",
            ));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::AffectedRows(mut vec_ra)) => {
                if vec_ra.len() != 1 {
                    return Err(HdbError::EvaluationError(
                        "found no or multiple affected-row-counts, but only a single Success \
                         was expected",
                    ));
                }
                match vec_ra.pop().unwrap() {
                    RowsAffected::Count(i) if i > 0 => {
                        Err(HdbError::EvaluationError(
                            "found an affected-row-count > 0, but only a single Success was \
                             expected",
                        ))
                    }
                    RowsAffected::ExecutionFailed => {
                        Err(
                            HdbError::EvaluationError("Found unexpected returnvalue ExecutionFailed"),
                        )
                    }
                    _ => Ok(HdbResponse::SingleReturnValue(HdbReturnValue::Success)),
                }
            }
            Some(InternalReturnValue::OutputParameters(_)) => {
                Err(HdbError::EvaluationError(
                    "Found OutputParameters, but a single Success was expected",
                ))
            }
            Some(InternalReturnValue::ResultSet(_)) => {
                Err(HdbError::EvaluationError("Found ResultSet, but a single Success was expected"))
            }
            None => {
                Err(HdbError::EvaluationError("Nothing found, but a single Success was expected"))
            }
        }
    }

    pub fn multiple_return_values(mut int_return_values: Vec<InternalReturnValue>)
                                  -> HdbResult<HdbResponse> {
        let mut vec_dbrv = Vec::<HdbReturnValue>::new();
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
                                return Err(HdbError::EvaluationError(
                                    "Found unexpected returnvalue 'ExecutionFailed'",
                                ));
                            }
                        }
                    }
                    vec_dbrv.push(HdbReturnValue::AffectedRows(vec_i));
                }
                InternalReturnValue::OutputParameters(op) => {
                    vec_dbrv.push(HdbReturnValue::OutputParameters(op));
                }
                InternalReturnValue::ResultSet(rs) => {
                    vec_dbrv.push(HdbReturnValue::ResultSet(rs));
                }
            }
        }
        Ok(HdbResponse::MultipleReturnValues(vec_dbrv))
    }
}
