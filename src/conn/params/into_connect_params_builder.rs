use super::connect_params::ServerCerts;
use super::connect_params_builder::ConnectParamsBuilder;
use super::cp_url;
use crate::{HdbError, HdbResult};
use url::Url;

/// A trait implemented by types that can be converted into a `ConnectParamsBuilder`.
///
/// # Example
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
    ///
    /// # Errors
    /// `HdbError::Usage` if wrong information was provided
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
        self.host_str().as_ref().map(|host| builder.hostname(host));
        self.port().as_ref().map(|port| builder.port(*port));

        let dbuser = self.username();
        if !dbuser.is_empty() {
            builder.dbuser(dbuser);
        }
        self.password().as_ref().map(|pw| builder.password(pw));

        let use_tls = match self.scheme() {
            "hdbsql" => false,
            "hdbsqls" => true,
            _ => {
                return Err(HdbError::Usage(
                    "Unknown protocol, only 'hdbsql' and 'hdbsqls' are supported",
                ));
            }
        };

        let mut server_certs = Vec::<ServerCerts>::new();

        for (name, value) in self.query_pairs() {
            match name.as_ref() {
                cp_url::OPTION_CLIENT_LOCALE => {
                    builder.clientlocale(value.to_string());
                }
                cp_url::OPTION_CLIENT_LOCALE_FROM_ENV => {
                    std::env::var(value.to_string())
                        .ok()
                        .map(|s| builder.clientlocale(s));
                }
                cp_url::OPTION_CERT_DIR => {
                    server_certs.push(ServerCerts::Directory(value.to_string()));
                }
                cp_url::OPTION_CERT_ENV => {
                    server_certs.push(ServerCerts::Environment(value.to_string()));
                }
                cp_url::OPTION_CERT_MOZILLA => {
                    server_certs.push(ServerCerts::RootCertificates);
                }
                cp_url::OPTION_INSECURE_NO_CHECK => {
                    server_certs.push(ServerCerts::None);
                }
                cp_url::OPTION_NONBLOCKING => {
                    #[cfg(feature = "alpha_nonblocking")]
                    builder.use_nonblocking();

                    #[cfg(not(feature = "alpha_nonblocking"))]
                    return Err(HdbError::UsageDetailed(format!(
                        "url option {} requires feature alpha_nonblocking",
                        cp_url::OPTION_NONBLOCKING
                    )));
                }
                _ => log::warn!("option {} not supported", name),
            }
        }

        if use_tls && server_certs.is_empty() {
            return Err(HdbError::Usage(
                "Using 'hdbsqls' requires one of the url-options 'tls_certificate_dir', \
                'tls_certificate_env', 'tls_certificate_direct', \
                'use_mozillas_root_certificates', or 'insecure_omit_server_certificate_check'",
            ));
        }
        for cert in server_certs {
            builder.tls_with(cert);
        }

        Ok(builder)
    }
}
