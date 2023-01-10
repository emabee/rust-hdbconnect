use crate::{ConnectParams, Connection, HdbError, HdbResult};
use hdbconnect_impl::IntoConnectParams;
use rocket_db_pools::{figment::Figment, Pool};

/// FIXME
#[derive(Debug, Clone)]
pub struct HanaPoolForRocket {
    connect_params: ConnectParams,
}
impl HanaPoolForRocket {
    /// Creates a new `HanaPoolForRocket`.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if not enough or inconsistent information was provided
    pub fn new<P: IntoConnectParams>(p: P) -> HdbResult<Self> {
        Ok(Self {
            connect_params: p.into_connect_params()?,
        })
    }
}

#[rocket::async_trait]
impl Pool for HanaPoolForRocket {
    type Connection = Connection;
    type Error = HdbError;

    async fn init(figment: &Figment) -> Result<Self, HdbError> {
        let connect_params =
            figment
                .extract::<ConnectParams>()
                .map_err(|e| HdbError::ConnParams {
                    source: Box::new(e),
                })?;
        Ok(Self { connect_params })
    }

    async fn get(&self) -> Result<Connection, HdbError> {
        Connection::new(&self.connect_params).await
    }
}
