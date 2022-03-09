use crate::protocol::parts::server_error::ServerError;
use byteorder::{LittleEndian, ReadBytesExt};
use std::fmt;

/// Describes the success of a command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionResult {
    /// Number of rows that were affected by the successful execution.
    RowsAffected(usize),
    /// Command was successful.
    SuccessNoInfo, // -2
    /// Execution failed with given ServerError.
    Failure(Option<ServerError>), // -3
}
impl ExecutionResult {
    pub(crate) fn parse_sync(
        count: usize,
        rdr: &mut dyn std::io::Read,
    ) -> std::io::Result<Vec<Self>> {
        let mut vec = Vec::<Self>::with_capacity(count);
        for _ in 0..count {
            match rdr.read_i32::<LittleEndian>()? {
                -2 => vec.push(Self::SuccessNoInfo),
                -3 => vec.push(Self::Failure(None)),
                #[allow(clippy::cast_sign_loss)]
                i => vec.push(Self::RowsAffected(i as usize)),
            }
        }
        Ok(vec)
    }
    /// True if it is an instance of `Self::Failure`.
    pub fn is_failure(&self) -> bool {
        match self {
            Self::Failure(_) => true,
            _ => false,
        }
    }
    /// True if it is an instance of `Self::RowsAffected`.
    pub fn is_rows_affected(&self) -> bool {
        match self {
            Self::RowsAffected(_) => true,
            _ => false,
        }
    }
}

impl fmt::Display for ExecutionResult {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::RowsAffected(count) => writeln!(fmt, "Number of affected rows: {}, ", count)?,
            Self::SuccessNoInfo => writeln!(
                fmt,
                "Command successfully executed but number of affected rows cannot be determined"
            )?,
            Self::Failure(Some(ref server_error)) => writeln!(
                fmt,
                "Execution of statement or processing of row has failed with {:?}",
                server_error
            )?,
            Self::Failure(None) => writeln!(
                fmt,
                "Execution of statement or processing of row has failed"
            )?,
        }
        Ok(())
    }
}
