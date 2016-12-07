use rs_serde::de::DeserError;
use rs_serde::ser::SerializationError;
use protocol::protocol_error::PrtError;

use std::error;
use std::fmt;
use std::io;
use std::result;

/// An abbreviation of <code>Result&lt;T, HdbError&gt;</code>.
///
/// Just for convenience.
pub type HdbResult<T> = result::Result<T, HdbError>;



/// Represents all possible errors that can occur in hdbconnect.
#[derive(Debug)]
pub enum HdbError {
    /// Error occured in deserialization of data into an application-defined structure.
    DeserializationError(DeserError),

    /// Error occured in evaluation of a response from the DB.
    EvaluationError(&'static str),

    /// IO error occured in communication setup.
    IoError(io::Error),

    /// Error occured in communication with the database.
    ProtocolError(PrtError),

    /// Error occured in serialization of rust data into values for the database.
    SerializationError(SerializationError),

    /// Error due to wrong usage of API.
    UsageError(&'static str),
}

impl error::Error for HdbError {
    fn description(&self) -> &str {
        match *self {
            HdbError::DeserializationError(ref error) => error.description(),
            HdbError::IoError(ref error) => error.description(),
            HdbError::ProtocolError(ref error) => error.description(),
            HdbError::EvaluationError(ref s) => s,
            HdbError::SerializationError(ref error) => error.description(),
            HdbError::UsageError(ref s) => s,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            HdbError::DeserializationError(ref error) => Some(error),
            HdbError::IoError(ref error) => Some(error),
            HdbError::ProtocolError(ref error) => Some(error),
            HdbError::EvaluationError(_) => None,
            HdbError::SerializationError(ref error) => Some(error),
            HdbError::UsageError(_) => None,
        }
    }
}

impl fmt::Display for HdbError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HdbError::DeserializationError(ref error) => write!(fmt, "{:?}", error),
            HdbError::IoError(ref error) => write!(fmt, "{:?}", error),
            HdbError::ProtocolError(ref error) => write!(fmt, "{:?}", error),
            HdbError::EvaluationError(ref s) => write!(fmt, "{:?}", s),
            HdbError::SerializationError(ref error) => write!(fmt, "{:?}", error),
            HdbError::UsageError(ref s) => write!(fmt, "{:?}", s),
        }
    }
}

impl From<DeserError> for HdbError {
    fn from(error: DeserError) -> HdbError {
        HdbError::DeserializationError(error)
    }
}

impl From<SerializationError> for HdbError {
    fn from(error: SerializationError) -> HdbError {
        HdbError::SerializationError(error)
    }
}

impl From<PrtError> for HdbError {
    fn from(error: PrtError) -> HdbError {
        HdbError::ProtocolError(error)
    }
}

impl From<io::Error> for HdbError {
    fn from(error: io::Error) -> HdbError {
        HdbError::IoError(error)
    }
}
