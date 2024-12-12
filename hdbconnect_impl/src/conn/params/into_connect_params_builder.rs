use super::cp_url::UrlOpt;
use crate::{
    url::{HDBSQL, HDBSQLS},
    ConnectParamsBuilder, HdbError, HdbResult, ServerCerts,
};
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

impl IntoConnectParamsBuilder for &str {
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
        self.host_str().map(|host| builder.hostname(host));
        self.port().map(|port| builder.port(port));

        let dbuser = self.username();
        if !dbuser.is_empty() {
            builder.dbuser(dbuser);
        }
        self.password().map(|pw| builder.password(pw));

        // authoritative switch between protocols:
        let use_tls = match self.scheme() {
            HDBSQL => false,
            HDBSQLS => true,
            _ => {
                error!("unknown scheme: {}, from {}", self.scheme(), self);
                return Err(HdbError::Usage(
                    "Unknown protocol, only 'hdbsql' and 'hdbsqls' are supported",
                ));
            }
        };

        let mut insecure_option = false;
        let mut server_certs = Vec::<ServerCerts>::new();

        for (name, value) in self.query_pairs() {
            match UrlOpt::from(name.as_ref()) {
                Some(UrlOpt::ClientLocale) => {
                    builder.clientlocale(&value);
                }
                Some(UrlOpt::ClientLocaleFromEnv) => {
                    std::env::var(value.to_string())
                        .ok()
                        .map(|s| builder.clientlocale(s));
                }
                Some(UrlOpt::TlsCertificateDir) => {
                    server_certs.push(ServerCerts::Directory(value.to_string()));
                }
                Some(UrlOpt::TlsCertificateEnv) => {
                    server_certs.push(ServerCerts::Environment(value.to_string()));
                }
                Some(UrlOpt::TlsCertificateMozilla) => {
                    server_certs.push(ServerCerts::RootCertificates);
                }
                Some(UrlOpt::InsecureOmitServerCheck) => {
                    insecure_option = true;
                }
                Some(UrlOpt::Database) => {
                    builder.dbname(&value);
                }
                Some(UrlOpt::NetworkGroup) => {
                    builder.network_group(&value);
                }
                Some(UrlOpt::NoCompression) => {
                    builder.always_uncompressed(true);
                }
                None => {
                    return Err(HdbError::UsageDetailed(format!(
                        "option '{name}' not supported",
                    )));
                }
            }
        }

        if use_tls {
            if insecure_option {
                if !server_certs.is_empty() {
                    return Err(HdbError::Usage(
                        "Use either the url-options 'tls_certificate_dir', 'tls_certificate_env', \
                        'tls_certificate_direct' and 'use_mozillas_root_certificates' \
                        to specify the access to the server certificate,\
                        or use 'insecure_omit_server_certificate_check' to not verify the server's \
                        identity, which is not recommended in most situations",
                    ));
                }
                builder.tls_without_server_verification();
            } else {
                if server_certs.is_empty() {
                    return Err(HdbError::Usage(
                        "Using 'hdbsqls' requires at least one of the url-options \
                        'tls_certificate_dir', 'tls_certificate_env', 'tls_certificate_direct', \
                        'use_mozillas_root_certificates', or 'insecure_omit_server_certificate_check'",
                    ));
                }
                for cert in server_certs {
                    builder.tls_with(cert);
                }
            }
        } else if insecure_option || !server_certs.is_empty() {
            return Err(HdbError::Usage(
                "Using 'hdbsql' is not possible with any of the url-options \
                    'tls_certificate_dir', 'tls_certificate_env', 'tls_certificate_direct', \
                    'use_mozillas_root_certificates', or 'insecure_omit_server_certificate_check'; \
                    consider using 'hdbsqls' instead",
            ));
        }

        Ok(builder)
    }
}
