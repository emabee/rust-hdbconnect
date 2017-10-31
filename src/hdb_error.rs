use serde_db::de::{ConversionError, DeserializationError};
use serde_db::ser::SerializationError;
use protocol::protocol_error::PrtError;
use protocol::lowlevel::conn_core::ConnectionCore;
use protocol::lowlevel::parts::resultset::ResultSetCore;

use std::error::{self, Error};
use std::fmt;
use std::io;
use std::result;
use std::sync;

/// An abbreviation of <code>Result&lt;T, `HdbError`&gt;</code>.
///
/// Just for convenience.
pub type HdbResult<T> = result::Result<T, HdbError>;



/// Represents all possible errors that can occur in hdbconnect.
#[derive(Debug)]
pub enum HdbError {
    /// Conversion of single db value to rust type failed.
    ConversionError(ConversionError),

    /// Error occured in deserialization of data structures into an application-defined structure.
    DeserializationError(DeserializationError),

    /// Error occured in evaluation of a response from the DB.
    EvaluationError(&'static str),

    /// Format error occured in communication setup.
    FmtError(fmt::Error),

    /// IO error occured in communication setup.
    IoError(io::Error),

    /// Error occured in communication with the database.
    ProtocolError(PrtError),

    /// Error occured in serialization of rust data into values for the database.
    SerializationError(SerializationError),

    /// Error due to wrong usage of API.
    UsageError(String),

    /// Error occured in thread synchronization.
    PoisonError(String),
}

impl error::Error for HdbError {
    fn description(&self) -> &str {
        match *self {
            HdbError::ConversionError(_) => "Conversion of database type to rust type failed",
            HdbError::DeserializationError(ref error) => error.description(),
            HdbError::IoError(ref error) => error.description(),
            HdbError::FmtError(ref error) => error.description(),
            HdbError::ProtocolError(ref error) => error.description(),
            HdbError::EvaluationError(s) => s,
            HdbError::SerializationError(ref error) => error.description(),
            HdbError::UsageError(ref s) => s,
            HdbError::PoisonError(ref s) => s,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            HdbError::ConversionError(ref error) => Some(error),
            HdbError::DeserializationError(ref error) => Some(error),
            HdbError::IoError(ref error) => Some(error),
            HdbError::FmtError(ref error) => Some(error),
            HdbError::ProtocolError(ref error) => Some(error),
            HdbError::SerializationError(ref error) => Some(error),
            HdbError::UsageError(_) | HdbError::PoisonError(_) | HdbError::EvaluationError(_) => {
                None
            }
        }
    }
}

impl fmt::Display for HdbError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HdbError::ConversionError(ref e) => write!(fmt, "{}", e),
            HdbError::DeserializationError(ref error) => write!(fmt, "{:?}", error),
            HdbError::IoError(ref error) => write!(fmt, "{:?}", error),
            HdbError::FmtError(ref error) => write!(fmt, "{:?}", error),
            HdbError::ProtocolError(ref error) => write!(fmt, "{:?}", error),
            HdbError::EvaluationError(s) => write!(fmt, "{:?}", s),
            HdbError::SerializationError(ref error) => write!(fmt, "{:?}", error),
            HdbError::UsageError(ref s) => write!(fmt, "{:?}", s),
            HdbError::PoisonError(ref s) => write!(fmt, "{:?}", s),
        }
    }
}

impl From<ConversionError> for HdbError {
    fn from(error: ConversionError) -> HdbError {
        HdbError::ConversionError(error)
    }
}

impl From<DeserializationError> for HdbError {
    fn from(error: DeserializationError) -> HdbError {
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

impl From<String> for HdbError {
    fn from(s: String) -> HdbError {
        HdbError::UsageError(s)
    }
}

impl From<io::Error> for HdbError {
    fn from(error: io::Error) -> HdbError {
        HdbError::IoError(error)
    }
}

impl From<fmt::Error> for HdbError {
    fn from(error: fmt::Error) -> HdbError {
        HdbError::FmtError(error)
    }
}

impl<'a> From<sync::PoisonError<sync::MutexGuard<'a, ConnectionCore>>> for HdbError {
    fn from(error: sync::PoisonError<sync::MutexGuard<'a, ConnectionCore>>) -> HdbError {
        HdbError::PoisonError(error.description().to_owned())
    }
}

impl<'a> From<sync::PoisonError<sync::MutexGuard<'a, ResultSetCore>>> for HdbError {
    fn from(error: sync::PoisonError<sync::MutexGuard<'a, ResultSetCore>>) -> HdbError {
        HdbError::PoisonError(error.description().to_owned())
    }
}
