use crate::{
    ConnectParams, Connection, ConnectionConfiguration, HdbError, HdbResult, IntoConnectParams,
};
use rocket_db_pools::{figment::Figment, Pool};

/// An implementation of rocket's
/// [`Pool`](https://docs.rs/rocket_db_pools/0.1.0/rocket_db_pools/trait.Pool.html) trait.
///
/// ## Example
///
/// ```rust,no_run
/// use hdbconnect_async::{
///     ConnectParams, ConnectionConfiguration, HanaPoolForRocket, IntoConnectParamsBuilder
/// };
/// use rocket_db_pools::Pool;
///
/// # use hdbconnect_async::HdbResult;
/// # async fn foo() -> HdbResult<()> {
/// let pool = HanaPoolForRocket::with_configuration(
///     "hdbsql://abcd123:2222"
///         .into_connect_params_builder()?
///         .with_dbuser("MEIER")
///         .with_password("schlau"),
///     ConnectionConfiguration::default()
///         .with_auto_commit(false),
/// )?;
///
/// let conn = pool.get().await.unwrap();
/// conn.query("select 1 from dummy").await.unwrap();
/// # Ok(())}
/// ```
///
#[derive(Debug, Clone)]
pub struct HanaPoolForRocket {
    connect_params: ConnectParams,
    connect_config: ConnectionConfiguration,
}
impl HanaPoolForRocket {
    /// Creates a new `HanaPoolForRocket` with default configuration.
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

    /// Creates a new `HanaPoolForRocket` with provided configuration.
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

#[rocket::async_trait]
impl Pool for HanaPoolForRocket {
    type Connection = Connection;
    type Error = HdbError;

    async fn init(figment: &Figment) -> HdbResult<Self> {
        let connect_params =
            figment
                .extract::<ConnectParams>()
                .map_err(|e| HdbError::ConnParams {
                    source: Box::new(e),
                })?;
        let connect_config = figment.extract::<ConnectionConfiguration>().map_err(|_| {
            HdbError::Usage(std::borrow::Cow::from("Incorrect ConnectionConfiguration"))
        })?;
        let pool = Self {
            connect_params,
            connect_config,
        };
        // try getting a connection to ensure it works
        pool.get().await.map(|_| pool)
    }

    async fn get(&self) -> HdbResult<Connection> {
        Connection::with_configuration(&self.connect_params, &self.connect_config).await
    }

    async fn close(&self) {}
}
