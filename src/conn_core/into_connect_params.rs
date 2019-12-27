use crate::conn_core::connect_params::ConnectParams;
use crate::conn_core::connect_params_builder::ConnectParamsBuilder;
use crate::hdb_error::{HdbErrorKind, HdbResult};
use failure::ResultExt;
use url::Url;

/// A trait implemented by types that can be converted into a `ConnectParams`.
pub trait IntoConnectParams {
    /// Converts the value of `self` into a `ConnectParams`.
    fn into_connect_params(self) -> HdbResult<ConnectParams>;
}

impl IntoConnectParams for ConnectParams {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        Ok(self)
    }
}

impl IntoConnectParams for &ConnectParams {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        Ok(self.clone())
    }
}

impl IntoConnectParams for ConnectParamsBuilder {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        self.build()
    }
}

impl IntoConnectParams for &ConnectParamsBuilder {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        self.build()
    }
}

impl<'a> IntoConnectParams for &'a str {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        Url::parse(self)
            .context(HdbErrorKind::ConnParams)?
            .into_connect_params()
    }
}

impl IntoConnectParams for String {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        self.as_str().into_connect_params()
    }
}

impl IntoConnectParams for Url {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        let builder = ConnectParamsBuilder::from_url(&self)?;
        Ok(builder.into_connect_params()?)
    }
}
