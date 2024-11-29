use std::vec::IntoIter;

use crate::{HdbResult, ServerError};
use byteorder::{LittleEndian, ReadBytesExt};

/// Describes the success of a command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionResult {
    /// Number of rows that were affected by the successful execution.
    RowsAffected(usize),
    /// Command was successful.
    SuccessNoInfo, // -2
    /// Execution failed with given `ServerError`.
    Failure(Option<ServerError>), // -3
    /// `ServerError` was reported without matching execution failure
    ExtraFailure(ServerError),
}
impl ExecutionResult {
    /// True if it is an instance of `Self::Failure`.
    #[must_use]
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failure(_))
    }
    /// True if it is an instance of `Self::RowsAffected`.
    #[must_use]
    pub fn is_rows_affected(&self) -> bool {
        matches!(self, Self::RowsAffected(_))
    }
}

impl std::fmt::Display for ExecutionResult {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::RowsAffected(count) => writeln!(fmt, "Number of affected rows: {count}, ")?,
            Self::SuccessNoInfo => writeln!(
                fmt,
                "Command successfully executed but number of affected rows cannot be determined"
            )?,
            Self::Failure(Some(ref server_error)) => writeln!(
                fmt,
                "Execution of statement or processing of row has failed with {server_error:?}",
            )?,
            Self::Failure(None) => writeln!(
                fmt,
                "Execution of statement or processing of row has failed"
            )?,
            Self::ExtraFailure(ref server_error) => {
                writeln!(fmt, "Extra server error was reported: {server_error:?}",)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
/// A list of execution results.
pub struct ExecutionResults(Vec<ExecutionResult>);
impl std::fmt::Display for ExecutionResults {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        for execution_result in &self.0 {
            std::fmt::Display::fmt(&execution_result, fmt)?;
        }
        Ok(())
    }
}
impl ExecutionResults {
    pub(crate) fn parse(count: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let mut vec = Vec::<ExecutionResult>::with_capacity(count);
        for _ in 0..count {
            match rdr.read_i32::<LittleEndian>()? {
                -2 => vec.push(ExecutionResult::SuccessNoInfo),
                -3 => vec.push(ExecutionResult::Failure(None)),
                #[allow(clippy::cast_sign_loss)]
                i => vec.push(ExecutionResult::RowsAffected(i as usize)),
            }
        }
        Ok(Self(vec))
    }

    pub(crate) fn mix_in_server_errors(&mut self, mut err_iter: IntoIter<ServerError>) {
        for execution_result in &mut self.0 {
            if let ExecutionResult::Failure(_) = *execution_result {
                *execution_result = ExecutionResult::Failure(err_iter.next());
            };
        }
        for e in err_iter {
            warn!(
                "Reply::handle_db_error(): \
                 found more server_errors than instances of ExecutionResult::Failure"
            );
            self.0.push(ExecutionResult::Failure(Some(e)));
        }
    }
}

impl std::iter::IntoIterator for ExecutionResults {
    type Item = ExecutionResult;
    type IntoIter = std::vec::IntoIter<ExecutionResult>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
impl<I: std::slice::SliceIndex<[ExecutionResult]>> std::ops::Index<I> for ExecutionResults {
    type Output = I::Output;
    fn index(&self, index: I) -> &Self::Output {
        &self.0[index]
    }
}
