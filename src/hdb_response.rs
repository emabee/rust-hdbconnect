use {HdbError, HdbResult};
use protocol::lowlevel::parts::output_parameters::OutputParameters;
use hdb_return_value::HdbReturnValue;
use protocol::lowlevel::parts::resultset::ResultSet;
use std::fmt;

/// Represents all possible non-error responses to a database command.
///
#[derive(Debug)]
pub struct HdbResponse(Vec<HdbReturnValue>);

impl HdbResponse {
    /// Turns itself into a single resultset.
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn into_resultset(self) -> HdbResult<ResultSet> {
        self.into_single_retval()?.into_resultset()
    }

    /// Turns itself into a Vector of numbers (each number representing a number of affected rows).
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn into_affected_rows(self) -> HdbResult<Vec<usize>> {
        self.into_single_retval()?.into_affected_rows()
    }

    /// Turns itself into a Vector of numbers (each number representing a number of affected rows).
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn into_output_parameters(self) -> HdbResult<OutputParameters> {
        self.into_single_retval()?.into_output_parameters()
    }

    /// Turns itself into (), if the statement had returned successfully.
    ///
    /// If this cannot be done without loss of information, an error is returned.
    pub fn into_success(self) -> HdbResult<()> {
        self.into_single_retval()?.into_success()
    }

    /// Turns itself into a single return value, if there is one any only one.
    pub fn into_single_retval(mut self) -> HdbResult<HdbReturnValue> {
        if self.0.len() > 1 {
            Err(HdbError::EvaluationError(
                "Not a single HdbReturnValue".to_string(),
            ))
        } else {
            self.0.pop().ok_or_else(|| {
                HdbError::EvaluationError(
                    "expected a single HdbReturnValue, found none".to_string(),
                )
            })
        }
    }

    /// Returns () if a successful execution was signaled by the database explicitly,
    /// or an error otherwise.
    pub fn get_success(&mut self) -> HdbResult<()> {
        if let Some(i) = self.find_success() {
            return self.0.remove(i).into_success();
        }
        Err(self.get_err("success"))
    }
    fn find_success(&self) -> Option<usize> {
        for (i, rt) in self.0.iter().enumerate().rev() {
            if rt.is_success() {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `ResultSet`, or an error if there is none.
    pub fn get_resultset(&mut self) -> HdbResult<ResultSet> {
        if let Some(i) = self.find_resultset() {
            return self.0.remove(i).into_resultset();
        }
        Err(self.get_err("resultset"))
    }
    fn find_resultset(&self) -> Option<usize> {
        for (i, rt) in self.0.iter().enumerate().rev() {
            if let HdbReturnValue::ResultSet(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next set of affected rows counters, or an error if there is none.
    pub fn get_affected_rows(&mut self) -> HdbResult<Vec<usize>> {
        if let Some(i) = self.find_affected_rows() {
            return self.0.remove(i).into_affected_rows();
        }
        Err(self.get_err("affected_rows"))
    }
    fn find_affected_rows(&self) -> Option<usize> {
        for (i, rt) in self.0.iter().enumerate().rev() {
            if let HdbReturnValue::AffectedRows(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    /// Returns the next `OutputParameters`, or an error if there is none.
    pub fn get_output_parameters(&mut self) -> HdbResult<OutputParameters> {
        if let Some(i) = self.find_output_parameters() {
            return self.0.remove(i).into_output_parameters();
        }
        Err(self.get_err("output_parameters"))
    }
    fn find_output_parameters(&self) -> Option<usize> {
        for (i, rt) in self.0.iter().enumerate().rev() {
            if let HdbReturnValue::OutputParameters(_) = *rt {
                return Some(i);
            }
        }
        None
    }

    fn get_err(&self, type_s: &str) -> HdbError {
        let mut errmsg = String::new();
        errmsg.push_str("No ");
        errmsg.push_str(type_s);
        errmsg.push_str(" found in this HdbResponse [");
        for rt in &self.0 {
            errmsg.push_str(match *rt {
                HdbReturnValue::ResultSet(_) => "ResultSet, ",
                HdbReturnValue::AffectedRows(_) => "AffectedRows, ",
                HdbReturnValue::OutputParameters(_) => "OutputParameters, ",
                HdbReturnValue::Success => "Success, ",
                HdbReturnValue::XaTransactionIds(_) => "XaTransactionIds, ",
            });
        }
        errmsg.push_str("]");
        HdbError::EvaluationError(errmsg)
    }
}

impl fmt::Display for HdbResponse {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt("HdbResponse [", fmt)?;
        for dbretval in &self.0 {
            fmt::Display::fmt(dbretval, fmt)?;
        }
        fmt::Display::fmt("]", fmt)?;
        Ok(())
    }
}

pub mod factory {
    use dist_tx::tm::XaTransactionId;
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
        XaTransactionIds(Vec<XaTransactionId>),
    }

    pub fn resultset(mut int_return_values: Vec<InternalReturnValue>) -> HdbResult<HdbResponse> {
        if int_return_values.len() > 1 {
            return Err(HdbError::InternalEvaluationError(
                "Only a single ResultSet was expected",
            ));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::ResultSet(rs)) => {
                Ok(HdbResponse(vec![HdbReturnValue::ResultSet(rs)]))
            }
            None => Err(HdbError::InternalEvaluationError(
                "Nothing found, but a single Resultset was expected",
            )),
            _ => Err(HdbError::InternalEvaluationError(
                "Wrong HdbReturnValue, a single Resultset was expected",
            )),
        }
    }

    pub fn rows_affected(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        if int_return_values.len() > 1 {
            return Err(HdbError::InternalEvaluationError(
                "Only a single AffectedRows was expected",
            ));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::AffectedRows(vec_ra)) => {
                let mut vec_i = Vec::<usize>::new();
                for ra in vec_ra {
                    match ra {
                        RowsAffected::Count(i) => vec_i.push(i),
                        RowsAffected::SuccessNoInfo => vec_i.push(0),
                        RowsAffected::ExecutionFailed => {
                            return Err(HdbError::InternalEvaluationError(
                                "Found unexpected returnvalue ExecutionFailed",
                            ));
                        }
                    }
                }
                Ok(HdbResponse(vec![HdbReturnValue::AffectedRows(vec_i)]))
            }
            Some(InternalReturnValue::OutputParameters(_)) => {
                Err(HdbError::InternalEvaluationError(
                    "Found OutputParameters, but a single AffectedRows was expected",
                ))
            }
            Some(InternalReturnValue::ResultSet(_)) => Err(HdbError::InternalEvaluationError(
                "Found ResultSet, but a single AffectedRows was expected",
            )),
            Some(InternalReturnValue::XaTransactionIds(_)) => {
                Err(HdbError::InternalEvaluationError(
                    "Found XaTransactionIds, but a single AffectedRows was expected",
                ))
            }
            None => Err(HdbError::InternalEvaluationError(
                "Nothing found, but a single AffectedRows was expected",
            )),
        }
    }

    pub fn success(mut int_return_values: Vec<InternalReturnValue>) -> HdbResult<HdbResponse> {
        if int_return_values.is_empty() {
            return Ok(HdbResponse(vec![HdbReturnValue::Success]));
        } else if int_return_values.len() > 1 {
            return Err(HdbError::InternalEvaluationError(
                "found multiple InternalReturnValues, but only a single Success was expected",
            ));
        }
        match int_return_values.pop() {
            Some(InternalReturnValue::AffectedRows(mut vec_ra)) => {
                if vec_ra.len() != 1 {
                    return Err(HdbError::InternalEvaluationError(
                        "found no or multiple affected-row-counts, but only a single Success was \
                         expected",
                    ));
                }
                match vec_ra.pop().unwrap() {
                    RowsAffected::Count(i) => if i > 0 {
                        Err(HdbError::InternalEvaluationError(
                            "found an affected-row-count > 0, but only a single Success was \
                             expected",
                        ))
                    } else {
                        Ok(HdbResponse(vec![HdbReturnValue::Success]))
                    },
                    RowsAffected::SuccessNoInfo => Ok(HdbResponse(vec![HdbReturnValue::Success])),
                    RowsAffected::ExecutionFailed => Err(HdbError::InternalEvaluationError(
                        "Found unexpected returnvalue ExecutionFailed",
                    )),
                }
            }
            Some(InternalReturnValue::OutputParameters(_)) => {
                Err(HdbError::InternalEvaluationError(
                    "Found OutputParameters, but a single Success was expected",
                ))
            }
            Some(InternalReturnValue::ResultSet(_)) => Err(HdbError::InternalEvaluationError(
                "Found ResultSet, but a single Success was expected",
            )),
            Some(InternalReturnValue::XaTransactionIds(_)) => {
                Err(HdbError::InternalEvaluationError(
                    "Found XaTransactionIds, but a single Success was expected",
                ))
            }
            None => Err(HdbError::InternalEvaluationError(
                "Nothing found, but a single Success was expected",
            )),
        }
    }

    pub fn multiple_return_values(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
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
                                return Err(HdbError::InternalEvaluationError(
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
                InternalReturnValue::XaTransactionIds(v_xatid) => {
                    vec_dbrv.push(HdbReturnValue::XaTransactionIds(v_xatid));
                }
            }
        }
        Ok(HdbResponse(vec_dbrv))
    }

    // that's really bad: reply type XAControl can have either a list of xatids,
    // or just be a success.
    pub fn xa_transaction_ids(
        mut int_return_values: Vec<InternalReturnValue>,
    ) -> HdbResult<HdbResponse> {
        if int_return_values.len() > 1 {
            return Err(HdbError::InternalEvaluationError(
                "Only a single InternalReturnValue was expected",
            ));
        }

        match int_return_values.pop() {
            Some(InternalReturnValue::XaTransactionIds(v_xatid)) => {
                Ok(HdbResponse(vec![HdbReturnValue::XaTransactionIds(v_xatid)]))
            }
            Some(InternalReturnValue::AffectedRows(mut vec_ra)) => {
                if vec_ra.len() != 1 {
                    return Err(HdbError::InternalEvaluationError(
                        "found no or multiple affected-row-counts, but only a single Success was \
                         expected",
                    ));
                }
                match vec_ra.pop().unwrap() {
                    RowsAffected::Count(i) => if i > 0 {
                        Err(HdbError::InternalEvaluationError(
                            "found an affected-row-count > 0, but only a single Success was \
                             expected",
                        ))
                    } else {
                        Ok(HdbResponse(vec![HdbReturnValue::Success]))
                    },
                    RowsAffected::SuccessNoInfo => Ok(HdbResponse(vec![HdbReturnValue::Success])),
                    RowsAffected::ExecutionFailed => Err(HdbError::InternalEvaluationError(
                        "Found unexpected returnvalue ExecutionFailed",
                    )),
                }
            }
            Some(InternalReturnValue::OutputParameters(_)) => {
                Err(HdbError::InternalEvaluationError(
                    "Found OutputParameters, but a single Success was expected",
                ))
            }
            Some(InternalReturnValue::ResultSet(_)) => Err(HdbError::InternalEvaluationError(
                "Found ResultSet, but a single Success was expected",
            )),
            None => Ok(HdbResponse(vec![HdbReturnValue::Success])),
        }
    }
}
