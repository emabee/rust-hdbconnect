use crate::protocol::parts::{ExecutionResult, ServerError};
// use std::backtrace::Backtrace;
use thiserror::Error;

/// A list specifying categories of [`HdbError`](crate::HdbError).
///
#[derive(Error, Debug)] //Copy, Clone, Eq, PartialEq,
#[non_exhaustive]
pub enum HdbError {
    /// Deserialization of a `ResultSet`, a `Row`, a single `HdbValue`,
    /// or an `OutputParameter` failed (methods `try_into()`).
    #[error("Error occured in deserialization")]
    Deserialization {
        /// The causing Error.
        #[from]
        source: serde_db::de::DeserializationError,
        // backtrace: Backtrace,
    },

    /// Serialization of a `ParameterDescriptor` or a `ParameterRow` failed.
    #[error("Error occured in serialization")]
    Serialization {
        /// The causing Error.
        #[from]
        source: serde_db::ser::SerializationError,
        // backtrace: Backtrace,
    },

    /// Some error occured while decoding CESU-8. This indicates a server issue!
    #[error("Some error occured while decoding CESU-8")]
    Cesu8,

    /// Decoding CESU-8 failed, original bytes are available.
    #[error("Some error occured while decoding CESU-8")]
    Cesu8AsBytes {
        bytes: Vec<u8>,
        // backtrace: Backtrace,
    },

    /// Erroneous Connection Parameters, e.g. from a malformed connection URL.
    #[error("Erroneous Connection Parameters")]
    ConnParams {
        /// The causing Error.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        // backtrace: Backtrace,
    },

    /// Database server responded with an error;
    /// the contained `ServerError` describes the conrete reason.
    #[error("Database server responded with an error")]
    DbError {
        /// The causing Error.
        #[from]
        source: ServerError,
        // backtrace: Backtrace,
    },

    /// Decompression
    #[error("Decompression failed")]
    Decompression {
        /// The causing Error.
        #[from]
        source: lz4_flex::block::DecompressError,
        // backtrace: Backtrace,
    },

    /// TLS set up failed because the server name was not valid.
    #[error("TLS setup failed because the server name was not valid")]
    TlsServerName,

    /// TLS protocol error.
    #[error(
        "TLS set up failed, after setting up the TCP connection; is the database prepared for TLS?"
    )]
    TlsProtocol {
        /// The causing Error.
        #[from]
        source: rustls::Error,
    },

    /// Error occured while evaluating an `HdbResponse` or an `HdbReturnValue`.
    #[error("Error occured while evaluating a HdbResponse or an HdbReturnValue")]
    Evaluation(&'static str),

    /// Database server responded with at least one error.
    #[error("Database server responded with at least one error")]
    ExecutionResults(Vec<ExecutionResult>),

    /// Implementation error.
    #[error("Implementation error: {}", _0)]
    Impl(&'static str),

    /// Implementation error.
    #[error("Implementation error: {}", _0)]
    ImplDetailed(String),

    /// Error occured in thread synchronization.
    #[cfg(feature = "sync")]
    #[error("Error occured in thread synchronization")]
    Poison,

    /// An error occurred on the server that requires the session to be terminated.
    #[error("An error occurred on the server that requires the session to be terminated")]
    SessionClosingTransactionError,

    /// Error occured in communication with the database.
    #[error(transparent)]
    Io {
        /// The causing Error.
        #[from]
        source: std::io::Error,
        // backtrace: Backtrace,
    },

    /// Error caused by wrong usage.
    #[error("Wrong usage: {}", _0)]
    Usage(&'static str),

    /// Error caused by wrong usage.
    #[error("Wrong usage: {}", _0)]
    UsageDetailed(String),
}

/// Abbreviation of `Result<T, HdbError>`.
pub type HdbResult<T> = std::result::Result<T, HdbError>;

impl HdbError {
    /// Returns the contained `ServerError`, if any.
    ///
    /// This method helps in case you need programmatic access to e.g. the error code.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection, HdbError, HdbResult};
    /// # use hdbconnect::IntoConnectParams;
    /// # fn main() -> HdbResult<()> {
    ///     # let hdb_result: HdbResult<()> = Err(HdbError::Usage("test"));
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
            Self::DbError {
                source: server_error,
            } => Some(server_error),
            _ => None,
        }
    }

    /// Reveal the inner error
    pub fn inner(&self) -> Option<&dyn std::error::Error> {
        match self {
            Self::Deserialization { source } => Some(source),
            Self::Serialization { source } => Some(source),
            Self::ConnParams { source } => Some(&**source),
            Self::DbError { source } => Some(source),
            Self::TlsProtocol { source } => Some(source),
            Self::Io { source } => Some(source),
            _ => None,
        }
    }

    pub(crate) fn conversion_error_into_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Cesu8AsBytes { bytes } => Some(bytes),
            _ => None,
        }
    }

    pub(crate) fn conn_params(error: Box<dyn std::error::Error + Send + Sync + 'static>) -> Self {
        Self::ConnParams { source: error }
    }
}

#[cfg(feature = "sync")]
impl<G> From<std::sync::PoisonError<G>> for HdbError {
    fn from(_error: std::sync::PoisonError<G>) -> Self {
        Self::Poison
    }
}
