use crate::protocol::parts::OutputParameters;
use crate::{HdbError, HdbResult};

#[cfg(feature = "dist_tx")]
use dist_tx::XaTransactionId;

/// An enum that describes a single database return value.
#[derive(Debug)]
pub enum HdbReturnValue {
    /// A resultset of a query.
    ResultSet(crate::sync::ResultSet),
    /// A list of numbers of affected rows.
    AffectedRows(Vec<usize>),
    /// Values of output parameters of a procedure call.
    OutputParameters(OutputParameters),
    /// Indication that a db call was successful.
    Success,
    /// A list of `XaTransactionId`s.
    #[cfg(feature = "dist_tx")]
    XaTransactionIds(Vec<XaTransactionId>),
}
impl HdbReturnValue {
    /// Turns itself into a single resultset.
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` for other variants than `HdbReturnValue::ResultSet`.
    pub fn into_resultset(self) -> HdbResult<crate::sync::ResultSet> {
        match self {
            Self::ResultSet(rs) => Ok(rs),
            _ => Err(HdbError::Evaluation("Not a HdbReturnValue::ResultSet")),
        }
    }

    /// Turns itself into a Vector of numbers (each number representing a
    /// number of affected rows).
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` for other variants than `HdbReturnValue::AffectedRows`.
    pub fn into_affected_rows(self) -> HdbResult<Vec<usize>> {
        match self {
            Self::AffectedRows(array) => Ok(array),
            _ => Err(HdbError::Evaluation("Not a HdbReturnValue::AffectedRows")),
        }
    }

    /// Turns itself into a Vector of numbers (each number representing a
    /// number of affected rows).
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` for other variants than `HdbReturnValue::OutputParameters`.
    pub fn into_output_parameters(self) -> HdbResult<OutputParameters> {
        match self {
            Self::OutputParameters(op) => Ok(op),
            _ => Err(HdbError::Evaluation(
                "Not a HdbReturnValue::OutputParameters",
            )),
        }
    }

    /// Turns itself into (), if the statement had returned successfully.
    ///
    /// # Errors
    ///
    /// `HdbError::Evaluation` for other variants of `HdbReturnValue`.
    pub fn into_success(self) -> HdbResult<()> {
        match self {
            Self::Success => Ok(()),
            Self::AffectedRows(_) => {
                if self.is_success() {
                    Ok(())
                } else {
                    Err(HdbError::Evaluation(
                        "HdbReturnValue::AffectedRows contained value > 0",
                    ))
                }
            }
            _ => Err(HdbError::Evaluation(
                "Not a HdbReturnValue::AffectedRows or ::Success",
            )),
        }
    }

    /// Returns true if the statement had returned successfully.
    #[must_use]
    pub fn is_success(&self) -> bool {
        match *self {
            Self::Success => true,
            Self::AffectedRows(ref vec) => vec.len() == 1 && vec.first() == Some(&0),
            _ => false,
        }
    }
}

impl std::fmt::Display for HdbReturnValue {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::AffectedRows(ref vec) => writeln!(fmt, "AffectedRows {vec:?},"),
            Self::OutputParameters(ref op) => writeln!(fmt, "OutputParameters [{op}],"),
            Self::ResultSet(ref rs) => writeln!(fmt, "ResultSet [{rs}],"),
            Self::Success => writeln!(fmt, "Success,"),
            #[cfg(feature = "dist_tx")]
            Self::XaTransactionIds(_) => writeln!(fmt, "XaTransactionIds,<"),
        }
    }
}
