//! Connection Pooling with bb8.

use crate::{
    ConnectParams, Connection, ConnectionConfiguration, HdbError, HdbResult, IntoConnectParams,
};
use async_trait::async_trait;
use bb8::ManageConnection;
use log::trace;
/// Implementation of
/// [`bb8::ManageConnection`](https://docs.rs/bb8/latest/bb8/trait.ManageConnection.html#).
///
/// ## Example
///
/// ```rust,no_run
/// use bb8::Pool;
/// use hdbconnect_async::{
///     ConnectionConfiguration, ConnectParams, ConnectionManager, IntoConnectParamsBuilder
/// };
///
/// # use hdbconnect_async::HdbResult;
/// # async fn foo() -> HdbResult<()> {
/// let pool = Pool::builder()
///     .max_size(15)
///     .build(ConnectionManager::with_configuration(
///         "hdbsql://abcd123:2222"
///             .into_connect_params_builder()?
///             .with_dbuser("MEIER")
///             .with_password("schlau"),
///         ConnectionConfiguration::default().with_auto_commit(false),
///     )?)
///     .await
///     .unwrap();
///
/// let conn = pool.get().await.unwrap();
/// conn.query("select 1 from dummy").await?;
/// # Ok(())}
/// ```
///
#[derive(Debug, Clone)]
pub struct ConnectionManager {
    connect_params: ConnectParams,
    connect_config: ConnectionConfiguration,
}
impl ConnectionManager {
    /// Creates a new `ConnectionManager` with default configuration.
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

#[async_trait]
impl ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = HdbError;

    #[doc = r" Attempts to create a new connection."]
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        trace!("ConnectionManager::connect()");
        Connection::with_configuration(&self.connect_params, &self.connect_config).await
    }

    #[doc = r" Determines if the connection is still connected to the database."]
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        trace!("ConnectionManager::is_valid()");
        if conn.is_broken().await {
            Err(HdbError::ConnectionBroken { source: None })
        } else {
            Ok(())
        }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
