use crate::protocol::parts::server_error::ServerError;
use crate::HdbResult;

use byteorder::{LittleEndian, ReadBytesExt};
use std::fmt;
use std::io;

/// Describes the success of a command.
#[derive(Debug)]
pub enum ExecutionResult {
    /// Number of rows that were affected by the successful execution.
    RowsAffected(usize),
    /// Command was successful.
    SuccessNoInfo, // -2
    /// Execution failed with given ServerError.
    Failure(Option<ServerError>), // -3
}
impl ExecutionResult {
    pub(crate) fn parse<T: io::BufRead>(
        count: i32,
        rdr: &mut T,
    ) -> HdbResult<Vec<ExecutionResult>> {
        let mut vec = Vec::<ExecutionResult>::with_capacity(count as usize);
        for _ in 0..count {
            match rdr.read_i32::<LittleEndian>()? {
                -2 => vec.push(ExecutionResult::SuccessNoInfo),
                -3 => vec.push(ExecutionResult::Failure(None)),
                i => vec.push(ExecutionResult::RowsAffected(i as usize)),
            }
        }
        Ok(vec)
    }
    /// True if it is an instance of ExecutionResult::Failure.
    pub fn is_failure(&self) -> bool {
        match self {
            ExecutionResult::Failure(_) => true,
            _ => false,
        }
    }
    /// True if it is an instance of ExecutionResult::RowsAffected.
    pub fn is_rows_affected(&self) -> bool {
        match self {
            ExecutionResult::RowsAffected(_) => true,
            _ => false,
        }
    }
}

impl fmt::Display for ExecutionResult {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ExecutionResult::RowsAffected(count) => {
                writeln!(fmt, "Number of affected rows: {}, ", count)?
            }
            ExecutionResult::SuccessNoInfo => writeln!(
                fmt,
                "Command successfully executed but number of affected rows cannot be determined"
            )?,
            ExecutionResult::Failure(Some(ref server_error)) => writeln!(
                fmt,
                "Execution of statement or processing of row has failed with {:?}",
                server_error
            )?,
            ExecutionResult::Failure(None) => writeln!(
                fmt,
                "Execution of statement or processing of row has failed"
            )?,
        }
        Ok(())
    }
}
