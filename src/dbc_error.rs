use rs_serde::deser_error::DeserError;
use protocol::protocol_error::PrtError;

use std::error;
use std::fmt;
use std::io;
use std::result;

/// This type represents all possible errors that can occur in hdbconnect
#[derive(Debug)]
pub enum DbcError {
    /// Error occured in deserialization of data into an application-defined structure
    DeserializationError(DeserError),
    /// Error occured in evaluation of a response from the DB
    EvaluationError(String),
    /// IO error occured in communication setup
    IoError(io::Error),
    /// Error occured in communication with the database
    ProtocolError(PrtError),
}

pub type DbcResult<T> = result::Result<T, DbcError>;

impl error::Error for DbcError {
    fn description(&self) -> &str {
        match *self {
            DbcError::DeserializationError(ref error) => error.description(),
            DbcError::IoError(ref error) => error.description(),
            DbcError::ProtocolError(ref error) => error.description(),
            DbcError::EvaluationError(ref s) => s,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            DbcError::DeserializationError(ref error) => Some(error),
            DbcError::IoError(ref error) => Some(error),
            DbcError::ProtocolError(ref error) => Some(error),
            DbcError::EvaluationError(_) => None,
        }
    }
}

impl fmt::Display for DbcError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DbcError::DeserializationError(ref error) => write!(fmt, "{:?}", error),
            DbcError::IoError(ref error) => write!(fmt, "{:?}", error),
            DbcError::ProtocolError(ref error) => write!(fmt, "{:?}", error),
            DbcError::EvaluationError(ref s) => write!(fmt, "{:?}",s),
        }
    }
}

impl From<DeserError> for DbcError {
    fn from(error: DeserError) -> DbcError { DbcError::DeserializationError(error) }
}

impl From<PrtError> for DbcError {
    fn from(error: PrtError) -> DbcError { DbcError::ProtocolError(error) }
}

impl From<io::Error> for DbcError {
    fn from(error: io::Error) -> DbcError { DbcError::IoError(error)  }
}
