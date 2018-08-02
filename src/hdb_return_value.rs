use dist_tx::tm::XaTransactionId;
use protocol::parts::output_parameters::OutputParameters;
use protocol::parts::resultset::ResultSet;
use std::fmt;
use {HdbError, HdbResult};

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
    /// A list of `XaTransactionId`s.
    XaTransactionIds(Vec<XaTransactionId>),
}
impl HdbReturnValue {
    /// Turns itself into a single resultset.
    ///
    /// If this cannot be done without loss of information, an error is
    /// returned.
    pub fn into_resultset(self) -> HdbResult<ResultSet> {
        match self {
            HdbReturnValue::ResultSet(rs) => Ok(rs),
            _ => Err(HdbError::Evaluation(
                "HdbReturnValue::into_resultset(): not  a ResultSet".to_string(),
            )),
        }
    }

    /// Turns itself into a Vector of numbers (each number representing a
    /// number of affected rows).
    ///
    /// If this cannot be done without loss of information, an error is
    /// returned.
    pub fn into_affected_rows(self) -> HdbResult<Vec<usize>> {
        match self {
            HdbReturnValue::AffectedRows(array) => Ok(array),
            _ => Err(HdbError::Evaluation(
                "Wrong call to HdbReturnValue::into_affected_rows(): not AffectedRows".to_string(),
            )),
        }
    }

    /// Turns itself into a Vector of numbers (each number representing a
    /// number of affected rows).
    ///
    /// If this cannot be done without loss of information, an error is
    /// returned.
    pub fn into_output_parameters(self) -> HdbResult<OutputParameters> {
        match self {
            HdbReturnValue::OutputParameters(op) => Ok(op),
            _ => Err(HdbError::Evaluation(
                "Wrong call to HdbReturnValue::into_output_parameters(): not OutputParameters"
                    .to_string(),
            )),
        }
    }

    /// Turns itself into (), if the statement had returned successfully.
    ///
    /// If this cannot be done without loss of information, an error is
    /// returned.
    pub fn into_success(self) -> HdbResult<()> {
        match self {
            HdbReturnValue::Success => Ok(()),
            HdbReturnValue::AffectedRows(_) => if self.is_success() {
                Ok(())
            } else {
                Err(HdbError::Evaluation(
                    "Wrong call to HdbReturnValue::into_success(): non-zero AffectRows".to_string(),
                ))
            },
            HdbReturnValue::OutputParameters(_) => Err(HdbError::Evaluation(
                "Wrong call to HdbReturnValue::into_success(): is OutputParameters".to_string(),
            )),
            HdbReturnValue::ResultSet(_) => Err(HdbError::Evaluation(
                "Wrong call to HdbReturnValue::into_success(): is a ResultSet".to_string(),
            )),
            HdbReturnValue::XaTransactionIds(_) => Err(HdbError::Evaluation(
                "Wrong call to HdbReturnValue::into_success(): is a list of XaTransactionIds"
                    .to_string(),
            )),
        }
    }

    /// Returns true if the statement had returned successfully.
    pub fn is_success(&self) -> bool {
        match *self {
            HdbReturnValue::Success => true,
            HdbReturnValue::AffectedRows(ref vec) => vec.len() == 1 && vec.get(0) == Some(&0),
            _ => false,
        }
    }
}

impl fmt::Display for HdbReturnValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HdbReturnValue::AffectedRows(ref vec) => {
                fmt::Display::fmt("AffectedRows ", fmt)?;
                fmt::Debug::fmt(vec, fmt)?;
                fmt::Display::fmt(",\n", fmt)?
            }
            HdbReturnValue::OutputParameters(ref op) => {
                fmt::Display::fmt("OutputParameters [", fmt)?;
                fmt::Display::fmt(op, fmt)?;
                fmt::Display::fmt("],\n", fmt)?
            }
            HdbReturnValue::ResultSet(ref rs) => {
                fmt::Display::fmt("ResultSet [", fmt)?;
                fmt::Display::fmt(rs, fmt)?;
                fmt::Display::fmt("],\n", fmt)?
            }
            HdbReturnValue::Success => fmt::Display::fmt("Success,\n", fmt)?,
            HdbReturnValue::XaTransactionIds(_) => fmt::Display::fmt("XaTransactionIds,\n", fmt)?,
        }
        Ok(())
    }
}
