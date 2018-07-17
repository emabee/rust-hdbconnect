use serde_db::de::{ConversionError, DeserializationError};
use serde_db::ser::SerializationError;
use protocol::lowlevel::cesu8::Cesu8DecodingError;
use protocol::lowlevel::conn_core::ConnectionCore;
use protocol::lowlevel::parts::resultset::ResultSetCore;
use protocol::lowlevel::parts::server_error::ServerError;

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
    // FIXME subsume into Deserialization?? -> has to be done in serde_db!
    /// Conversion of single db value to rust type failed.
    Conversion(ConversionError),

    /// Error occured in deserialization of data structures into an application-defined
    /// structure.
    Deserialization(DeserializationError),

    /// Database server responded with an error.
    DbError(ServerError),

    /// Database server responded with an error.
    MultipleDbErrors(Vec<ServerError>),

    /// Some error occured while reading CESU-8.
    Cesu8(Cesu8DecodingError),

    /// Error occured while evaluating a HdbResponse object.
    Evaluation(String),

    /// Missing or wrong implementation of HANA's wire protocol.
    Impl(String),

    /// IO error occured in communication with the database.
    Io(io::Error),

    /// Error occured in thread synchronization.
    Poison(String),

    /// Error occured in serialization of rust data into values for the
    /// database.
    Serialization(SerializationError),

    /// Error due to wrong usage of API.
    Usage(String),
}

impl HdbError {
    #[doc(hidden)]
    pub fn impl_<S: AsRef<str>>(s: S) -> HdbError {
        HdbError::Impl(s.as_ref().to_owned())
    }
    #[doc(hidden)]
    pub fn usage_<S: AsRef<str>>(s: S) -> HdbError {
        HdbError::Usage(s.as_ref().to_owned())
    }
}

impl error::Error for HdbError {
    fn description(&self) -> &str {
        match *self {
            HdbError::DbError(_) => "Error from database server",
            HdbError::MultipleDbErrors(_) => "Multiple errors from database server",
            HdbError::Conversion(_) => "Conversion of database type to rust type failed",
            HdbError::Deserialization(ref e) => e.description(),
            HdbError::Cesu8(ref e) => e.description(),
            HdbError::Io(ref e) => e.description(),
            HdbError::Serialization(ref e) => e.description(),
            HdbError::Impl(ref s)
            | HdbError::Evaluation(ref s)
            | HdbError::Usage(ref s)
            | HdbError::Poison(ref s) => s,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            HdbError::Cesu8(ref e) => Some(e),
            HdbError::Conversion(ref error) => Some(error),
            HdbError::Deserialization(ref error) => Some(error),
            HdbError::Io(ref error) => Some(error),
            HdbError::Serialization(ref error) => Some(error),
            HdbError::Impl(_)
            | HdbError::DbError(_)
            | HdbError::MultipleDbErrors(_)
            | HdbError::Usage(_)
            | HdbError::Poison(_)
            | HdbError::Evaluation(_) => None,
        }
    }
}

impl fmt::Display for HdbError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HdbError::Cesu8(ref e) => write!(fmt, "{}", e),
            HdbError::Conversion(ref e) => write!(fmt, "{}", e),
            HdbError::Deserialization(ref error) => write!(fmt, "{:?}", error),
            HdbError::Io(ref error) => write!(fmt, "{:?}", error),
            HdbError::Impl(ref error) => write!(fmt, "{:?}", error),
            HdbError::Serialization(ref error) => write!(fmt, "{:?}", error),
            HdbError::Evaluation(ref s) | HdbError::Usage(ref s) | HdbError::Poison(ref s) => {
                write!(fmt, "{:?}", s)
            }
            HdbError::DbError(ref se) => write!(fmt, "{:?}", se),
            HdbError::MultipleDbErrors(ref vec) => write!(fmt, "{:?}", vec[0]),
        }
    }
}

impl From<ConversionError> for HdbError {
    fn from(error: ConversionError) -> HdbError {
        HdbError::Conversion(error)
    }
}

impl From<DeserializationError> for HdbError {
    fn from(error: DeserializationError) -> HdbError {
        HdbError::Deserialization(error)
    }
}

impl From<SerializationError> for HdbError {
    fn from(error: SerializationError) -> HdbError {
        HdbError::Serialization(error)
    }
}

impl From<String> for HdbError {
    fn from(s: String) -> HdbError {
        HdbError::Usage(s)
    }
}

impl From<io::Error> for HdbError {
    fn from(error: io::Error) -> HdbError {
        HdbError::Io(error)
    }
}

impl From<fmt::Error> for HdbError {
    fn from(error: fmt::Error) -> HdbError {
        HdbError::Usage(error.description().to_owned())
    }
}

impl From<Cesu8DecodingError> for HdbError {
    fn from(error: Cesu8DecodingError) -> HdbError {
        HdbError::Cesu8(error)
    }
}

impl<'a> From<sync::PoisonError<sync::MutexGuard<'a, ConnectionCore>>> for HdbError {
    fn from(error: sync::PoisonError<sync::MutexGuard<'a, ConnectionCore>>) -> HdbError {
        HdbError::Poison(error.description().to_owned())
    }
}

impl<'a> From<sync::PoisonError<sync::MutexGuard<'a, ResultSetCore>>> for HdbError {
    fn from(error: sync::PoisonError<sync::MutexGuard<'a, ResultSetCore>>) -> HdbError {
        HdbError::Poison(error.description().to_owned())
    }
}
