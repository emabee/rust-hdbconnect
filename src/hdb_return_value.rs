use crate::protocol::parts::output_parameters::OutputParameters;
use crate::protocol::parts::resultset::ResultSet;
use crate::{HdbErrorKind, HdbResult};
use dist_tx::tm::XaTransactionId;
use std::fmt;

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
            _ => Err(HdbErrorKind::Evaluation.into()),
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
            _ => Err(HdbErrorKind::Evaluation.into()),
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
            _ => Err(HdbErrorKind::Evaluation.into()),
        }
    }

    /// Turns itself into (), if the statement had returned successfully.
    ///
    /// If this cannot be done without loss of information, an error is
    /// returned.
    pub fn into_success(self) -> HdbResult<()> {
        match self {
            HdbReturnValue::Success => Ok(()),
            HdbReturnValue::AffectedRows(_) => {
                if self.is_success() {
                    Ok(())
                } else {
                    Err(HdbErrorKind::Evaluation.into())
                }
            }
            HdbReturnValue::OutputParameters(_)
            | HdbReturnValue::ResultSet(_)
            | HdbReturnValue::XaTransactionIds(_) => Err(HdbErrorKind::Evaluation.into()),
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
            HdbReturnValue::AffectedRows(ref vec) => writeln!(fmt, "AffectedRows {:?},", vec),
            HdbReturnValue::OutputParameters(ref op) => writeln!(fmt, "OutputParameters [{}],", op),
            HdbReturnValue::ResultSet(ref rs) => writeln!(fmt, "ResultSet [{}],", rs),
            HdbReturnValue::Success => writeln!(fmt, "Success,"),
            HdbReturnValue::XaTransactionIds(_) => writeln!(fmt, "XaTransactionIds,<"),
        }
    }
}
