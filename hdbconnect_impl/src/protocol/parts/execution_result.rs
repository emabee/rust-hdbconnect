use crate::{HdbResult, ServerError};
// #[cfg(feature = "sync")]
use byteorder::{LittleEndian, ReadBytesExt};

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
    // #[cfg(feature = "sync")]
    pub(crate) fn parse_sync(count: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Vec<Self>> {
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

    // #[cfg(feature = "async")]
    // pub(crate) async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    //     count: usize,
    //     rdr: &mut R,
    // ) -> HdbResult<Vec<Self>> {
    //     let mut vec = Vec::<Self>::with_capacity(count);
    //     for _ in 0..count {
    //         match rdr.read_i32_le().await? {
    //             -2 => vec.push(Self::SuccessNoInfo),
    //             -3 => vec.push(Self::Failure(None)),
    //             #[allow(clippy::cast_sign_loss)]
    //             i => vec.push(Self::RowsAffected(i as usize)),
    //         }
    //     }
    //     Ok(vec)
    // }

    /// True if it is an instance of `Self::Failure`.
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failure(_))
    }
    /// True if it is an instance of `Self::RowsAffected`.
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
        }
        Ok(())
    }
}
