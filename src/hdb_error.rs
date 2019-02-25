use crate::protocol::parts::execution_result::ExecutionResult;
use crate::protocol::parts::server_error::ServerError;
use cesu8::Cesu8DecodingError;
use serde_db::de::{ConversionError, DeserializationError};
use serde_db::ser::SerializationError;
use std::error::{self, Error};
use std::fmt;
use std::result;
use std::sync;

/// Abbreviation of `Result<T, HdbError>`.
pub type HdbResult<T> = result::Result<T, HdbError>;

/// Represents all possible errors that can occur in hdbconnect.
#[derive(Debug)]
pub enum HdbError {
    // FIXME subsume into Deserialization?? -> has to be done in serde_db!
    /// Conversion of single db value to rust type failed.
    Conversion(ConversionError),

    /// Error occured in deserialization of data structures into an application-defined structure.
    Deserialization(DeserializationError),

    /// Database server responded with an error.
    DbError(ServerError),

    /// Database server has a severe issue.
    DbIssue(String),

    /// Database server responded with at least one error.
    MixedResults(Vec<ExecutionResult>),

    /// Some error occured while reading CESU-8.
    Cesu8(Cesu8DecodingError),

    /// Error occured while evaluating a HdbResponse object.
    Evaluation(String),

    /// Missing or wrong implementation of HANA's wire protocol.
    Impl(String),

    /// IO error occured in communication with the database.
    Io(std::io::Error),

    /// Error occured in thread synchronization.
    Poison(String),

    /// Error occured in serialization of rust data into values for the
    /// database.
    Serialization(SerializationError),

    /// Error due to wrong usage of API.
    Usage(String),
}

impl HdbError {
    /// Return the contained server_error, if any.
    ///
    /// This method helps in case you need programmatic access to e.g. the error code.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection,HdbError, HdbResult};
    /// # use hdbconnect::IntoConnectParams;
    /// # fn main() -> HdbResult<()> {
    ///     # let hdb_result: HdbResult<()> = Err(HdbError::Usage("test".to_string()));
    ///     # let mut connection = Connection::new("".into_connect_params()?)?;
    ///     if let Err(hdberror) = hdb_result {
    ///         if let Some(server_error) = hdberror.server_error() {
    ///             let sys_m_error_code: (i32, String, String) = connection
    ///                 .query(&format!(
    ///                     "select * from SYS.M_ERROR_CODES where code = {}",
    ///                     server_error.code()
    ///                 ))?.try_into()?;
    ///             println!("sys_m_error_code: {:?}", sys_m_error_code);
    ///         }
    ///     }
    ///     # Ok(())
    /// # }
    /// ```
    pub fn server_error(&self) -> Option<&ServerError> {
        match self {
            HdbError::DbError(server_error) => Some(&server_error),
            _ => None,
        }
    }
}
// Factory methods
impl HdbError {
    pub(crate) fn impl_<S: AsRef<str>>(s: S) -> HdbError {
        HdbError::Impl(s.as_ref().to_owned())
    }
    pub(crate) fn usage_<S: AsRef<str>>(s: S) -> HdbError {
        HdbError::Usage(s.as_ref().to_owned())
    }
}

impl error::Error for HdbError {
    fn description(&self) -> &str {
        match *self {
            HdbError::DbError(_) => "Error from database server",
            HdbError::DbIssue(_) => "Issue on database server",
            HdbError::MixedResults(_) => "Database server responded with at least one error",
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
            HdbError::DbError(ref server_error) => Some(server_error),
            HdbError::Impl(_)
            | HdbError::DbIssue(_)
            | HdbError::MixedResults(_)
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
            HdbError::Evaluation(ref s)
            | HdbError::Usage(ref s)
            | HdbError::Poison(ref s)
            | HdbError::DbIssue(ref s) => write!(fmt, "{:?}", s),
            HdbError::DbError(ref se) => write!(fmt, "{:?}", se),
            HdbError::MixedResults(ref vec_rows_affected) => {
                write!(fmt, "MixedResults[")?;
                let mut first = true;
                for ra in vec_rows_affected {
                    if first {
                        first = false;
                    } else {
                        write!(fmt, ", ")?;
                    };
                    match ra {
                        ExecutionResult::Failure(Some(err)) => {
                            write!(fmt, "Failure({:?})", err)?;
                        }
                        ExecutionResult::Failure(None) => {
                            write!(fmt, "Failure()")?;
                        }
                        ra => write!(fmt, "{:?}", ra)?,
                    }
                }
                write!(fmt, "]")?;
                Ok(())
            }
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

impl From<std::io::Error> for HdbError {
    fn from(error: std::io::Error) -> HdbError {
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

impl<'a, T> From<sync::PoisonError<sync::MutexGuard<'a, T>>> for HdbError {
    fn from(error: sync::PoisonError<sync::MutexGuard<'a, T>>) -> HdbError {
        HdbError::Poison(error.description().to_owned())
    }
}
