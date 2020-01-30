use crate::conn::connect_params::ServerCerts;
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
        let mut builder = ConnectParamsBuilder::new();
        if let Some(host) = self.host_str() {
            builder.hostname(host);
        }

        if let Some(port) = self.port() {
            builder.port(port);
        }

        let dbuser = self.username();
        if !dbuser.is_empty() {
            builder.dbuser(dbuser);
        }

        if let Some(password) = self.password() {
            builder.password(password);
        }

        match self.scheme() {
            "hdbsql" | "hdbsqls" => {}
            _ => {
                return Err(HdbError::Usage(
                    "Unknown protocol, only 'hdbsql' and 'hdbsqls' are supported",
                ));
            }
        }

        let mut server_certs = Vec::<ServerCerts>::new();
        let mut clientlocale = None;

        for (name, value) in self.query_pairs() {
            match name.as_ref() {
                "client_locale" => clientlocale = Some(value.to_string()),
                "client_locale_from_env" => {
                    clientlocale = std::env::var("LANG").ok();
                }
                "tls_certificate_dir" => {
                    server_certs.push(ServerCerts::Directory(value.to_string()));
                }
                "tls_certificate_env" => {
                    server_certs.push(ServerCerts::Environment(value.to_string()));
                }
                "use_mozillas_root_certificates" => {
                    server_certs.push(ServerCerts::RootCertificates);
                }
                _ => log::warn!("option {} not supported", name),
            }
        }

        if let Some(cl) = clientlocale {
            builder.clientlocale(cl);
        }

        for cert in server_certs {
            builder.tls_with(cert);
        }

        Ok(builder)
    }
}
