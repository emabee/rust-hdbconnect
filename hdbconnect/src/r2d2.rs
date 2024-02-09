//! Connection Pooling with r2d2.

use crate::{
    ConnectParams, Connection, ConnectionConfiguration, HdbError, HdbResult, IntoConnectParams,
};
use log::trace;

/// Implementation of r2d2's
/// [`ManageConnection`](https://docs.rs/r2d2/*/r2d2/trait.ManageConnection.html).
///
/// ## Example
///
/// ```rust,no_run
/// use hdbconnect::{
///     ConnectionConfiguration, ConnectParams, ConnectionManager, IntoConnectParamsBuilder
/// };
///
/// # use hdbconnect::HdbResult;
/// # fn foo() -> HdbResult<()> {
/// let pool = r2d2::Pool::builder()
///     .max_size(15)
///     .build(ConnectionManager::with_configuration(
///         "hdbsql://abcd123:2222"
///             .into_connect_params_builder()?
///             .with_dbuser("MEIER")
///             .with_password("schlau"),
///         ConnectionConfiguration::default().with_auto_commit(false),
///     )?).unwrap();
///
/// let conn = pool.get().unwrap();
/// conn.query("select 1 from dummy")?;
/// # Ok(())}
/// ```
///
#[derive(Debug)]
pub struct ConnectionManager {
    connect_params: ConnectParams,
    connect_config: ConnectionConfiguration,
}
impl ConnectionManager {
    /// Creates a new `ConnectionManager`.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if not enough or inconsistent information was provided
    pub fn new<P: IntoConnectParams>(p: P) -> HdbResult<Self> {
        Ok(Self {
            connect_params: p.into_connect_params()?,
            connect_config: ConnectionConfiguration::default(),
        })
    }
    /// Creates a new `ConnectionManager` with provided configuration.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if not enough or inconsistent information was provided
    pub fn with_configuration<P: IntoConnectParams>(
        p: P,
        c: ConnectionConfiguration,
    ) -> HdbResult<Self> {
        Ok(Self {
            connect_params: p.into_connect_params()?,
            connect_config: c,
        })
    }
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = HdbError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        trace!("ConnectionManager::connect()");
        Connection::with_configuration(&self.connect_params, &self.connect_config)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        trace!("ConnectionManager::is_valid()");
        conn.query("SELECT 'IsConnectionStillAlive' from dummy")
            .map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        trace!("ConnectionManager::has_broken()");
        conn.is_broken().unwrap_or(false)
    }
}
