use crate::{ConnectParamsBuilder, HdbError, HdbResult};
use url::Url;

/// A trait implemented by types that can be converted into a `ConnectParamsBuilder`.
///
/// Example:
/// ```rust
///     use hdbconnect::IntoConnectParamsBuilder;
///
///     let cp_builder = "hdbsql://MEIER:schLau@abcd123:2222"
///         .into_connect_params_builder()
///         .unwrap();
///
///     assert_eq!("abcd123", cp_builder.get_hostname().unwrap());
/// ```
pub trait IntoConnectParamsBuilder {
    /// Converts the value of `self` into a `ConnectParamsBuilder`.
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder>;
}

impl IntoConnectParamsBuilder for ConnectParamsBuilder {
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder> {
        Ok(self)
    }
}

impl<'a> IntoConnectParamsBuilder for &'a str {
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder> {
        Url::parse(self)
            .map_err(|e| HdbError::conn_params(Box::new(e)))?
            .into_connect_params_builder()
    }
}

impl IntoConnectParamsBuilder for String {
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder> {
        self.as_str().into_connect_params_builder()
    }
}

impl IntoConnectParamsBuilder for Url {
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder> {
        Ok(ConnectParamsBuilder::from_url(&self)?)
    }
}
