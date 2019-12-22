use crate::protocol::parts::execution_result::ExecutionResult;
use crate::protocol::parts::server_error::ServerError;
use failure::{Backtrace, Context, Fail};

/// A list specifying categories of [`HdbError`](struct.HdbError.html).
///
/// This list may grow over time and it is not recommended to exhaustively
/// match against it.
#[derive(Clone, Eq, PartialEq, Debug, Fail)] //Copy
pub enum HdbErrorKind {
    /// Error occured in deserialization.
    #[fail(display = "Error occured in deserialization.")]
    Deserialization,

    /// Error occured in serialization.
    #[fail(display = "Error occured in serialization.")]
    Serialization,

    /// Database server responded with an error.
    #[fail(display = "Database server responded with an error.")]
    DbError(ServerError),

    /// An error occurred that requires the session to be terminated.
    #[fail(display = "An error occurred that requires the session to be terminated.")]
    SessionClosingTransactionError,

    /// Database server responded with at least one error.
    #[fail(display = "Database server responded with at least one error.")]
    ExecutionResults(Vec<ExecutionResult>),

    /// Some error occured while reading CESU-8.
    #[fail(display = "Some error occured while reading CESU-8.")]
    Cesu8,

    /// Error occured while evaluating a HdbResponse or an HdbReturnValue.
    #[fail(display = "Error occured while evaluating a HdbResponse or an HdbReturnValue.")]
    Evaluation,

    /// Error occured while streaming a LOB.
    #[fail(display = "Error occured while streaming a LOB.")]
    LobStreaming,

    /// Implementation error.
    #[fail(display = "Implementation error: {}", _0)]
    Impl(&'static str),

    /// Implementation error.
    #[fail(display = "Implementation error: {}", _0)]
    ImplDetailed(String),

    /// Error occured in thread synchronization.
    #[fail(display = "Error occured in thread synchronization.")]
    Poison,

    /// Error caused by wrong usage.
    #[fail(display = "Wrong usage: {}", _0)]
    Usage(&'static str),

    /// Error caused by wrong usage.
    #[fail(display = "Wrong usage: {}", _0)]
    UsageDetailed(String),

    /// Error occured in communication with the database.
    #[fail(display = "Error occured in communication with the database")]
    Database,

    /// Erroneous Connection Parameters.
    #[fail(display = "Erroneous Connection Parameters")]
    ConnParams,
}

/// Abbreviation of `Result<T, HdbError>`.
pub type HdbResult<T> = std::result::Result<T, HdbError>;

/// Represents all possible errors that can occur in hdbconnect.
#[derive(Debug)]
pub struct HdbError {
    inner: Context<HdbErrorKind>,
}

impl HdbError {
    pub(crate) fn imp(s: &'static str) -> HdbError {
        HdbErrorKind::Impl(s).into()
    }

    pub(crate) fn imp_detailed(s: String) -> HdbError {
        HdbErrorKind::ImplDetailed(s).into()
    }

    /// Return the contained server_error, if any.
    ///
    /// This method helps in case you need programmatic access to e.g. the error code.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection,HdbErrorKind, HdbResult};
    /// # use hdbconnect::IntoConnectParams;
    /// # fn main() -> Result<(),failure::Error> {
    ///     # let hdb_result: HdbResult<()> = Err(HdbErrorKind::Usage("test").into());
    ///     # let mut connection = Connection::try_new("".into_connect_params()?)?;
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
        match self.inner.get_context() {
            HdbErrorKind::DbError(server_error) => Some(&server_error),
            _ => None,
        }
    }

    /// Get access to the error context.
    pub fn kind(&self) -> HdbErrorKind {
        self.inner.get_context().clone()
    }
}

impl Fail for HdbError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl std::fmt::Display for HdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl<'a, T> From<std::sync::PoisonError<std::sync::MutexGuard<'a, T>>> for HdbError {
    fn from(_error: std::sync::PoisonError<std::sync::MutexGuard<'a, T>>) -> HdbError {
        HdbError {
            inner: Context::new(HdbErrorKind::Poison),
        }
    }
}
impl From<HdbErrorKind> for HdbError {
    fn from(kind: HdbErrorKind) -> HdbError {
        HdbError {
            inner: Context::new(kind),
        }
    }
}
impl From<Context<HdbErrorKind>> for HdbError {
    fn from(inner: Context<HdbErrorKind>) -> HdbError {
        HdbError { inner }
    }
}
