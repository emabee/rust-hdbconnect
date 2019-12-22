//! Connection parameters
use crate::conn_core::connect_params_builder::ConnectParamsBuilder;
use crate::{HdbErrorKind, HdbResult};
use failure::ResultExt;
use secstr::SecStr;
use std::env;
use std::fs;
use std::path::Path;
use url::Url;

/// An immutable struct with all information necessary to open a new connection
/// to a HANA database.
///
/// An instance of `ConnectParams` can be created in various ways:
///
/// ## Using the [builder](struct.ConnectParams.html#method.builder)
/// The builder allows specifying all necessary details programmatically.
///
/// ### Example with TLS
///
/// ```rust,ignore
/// # // norun cannot be used because the snippet requires feature tls
/// use hdbconnect::{ConnectParams, ServerCerts};
/// # fn read_certificate() -> String {String::from("can't do that")};
/// let certificate: String = read_certificate();
/// let connect_params = ConnectParams::builder()
///    .hostname("the_host")
///    .port(2222)
///    .dbuser("my_user")
///    .password("my_passwd")
///    .tls_with(ServerCerts::Direct(certificate))
///    .build()
///    .unwrap();
/// ```
///  
/// ## Using a URL
///
/// The URL is supposed to have the form
///
/// ```text
/// <scheme>://<username>:<password>@<host>:<port>[<options>]
/// ```
/// where
/// > `<scheme>` = `hdbsql` | `hdbsqls`  
/// > `<username>` = the name of the DB user to log on  
/// > `<password>` = the password of the DB user  
/// > `<host>` = the host where HANA can be found  
/// > `<port>` = the port at which HANA can be found on `<host>`  
/// > `<options>` = `?<key>[=<value>][{&<key>[=<value>}]]`
///
/// Special option keys are:
/// > `client_locale=<value>` specifies the client locale  
/// > `client_locale_from_env` (no value): lets the driver read the client's locale from the
///    environment variabe LANG  
/// > `tls_certificate_dir=<value>`: points to a folder with pem files that contain
///   certificates; all pem files in that folder are evaluated  
/// > `tls_certificate_env=<value>`: denotes an environment variable that contains
///   certificates
/// > `use_mozillas_root_certificates` (no value): use the root certificates from
///   [`https://mkcert.org/`](https://mkcert.org/)
///
///
/// The client locale is used in language-dependent handling within the SAP HANA
/// database calculation engine.
///
/// ### Example
///
/// ```
/// use hdbconnect::IntoConnectParams;
/// let conn_params = "hdbsql://my_user:my_passwd@the_host:2222"
///     .into_connect_params()
///     .unwrap();
/// ```
///
/// ## Reading a URL from a file
///
/// The shortcut [`ConnectParams::from_file`](struct.ConnectParams.html#method.from_file)
/// reads a URL from a file and converts it into an instance of `ConnectParams`.
///
#[derive(Clone, Debug)]
pub struct ConnectParams {
    host: String,
    addr: String,
    dbuser: String,
    password: SecStr,
    clientlocale: Option<String>,
    #[cfg(feature = "tls")]
    server_certs: Vec<ServerCerts>,
}
impl ConnectParams {
    #[cfg(not(feature = "tls"))]
    pub(crate) fn new(
        host: String,
        addr: String,
        dbuser: String,
        password: SecStr,
        clientlocale: Option<String>,
    ) -> ConnectParams {
        ConnectParams {
            host,
            addr,
            dbuser,
            password,
            clientlocale,
        }
    }
    #[cfg(feature = "tls")]
    pub(crate) fn new(
        host: String,
        addr: String,
        dbuser: String,
        password: SecStr,
        clientlocale: Option<String>,
        server_certs: Vec<ServerCerts>,
    ) -> ConnectParams {
        ConnectParams {
            host,
            addr,
            dbuser,
            password,
            clientlocale,
            server_certs,
        }
    }

    /// Returns a new builder for ConnectParams.
    pub fn builder() -> ConnectParamsBuilder {
        ConnectParamsBuilder::new()
    }

    /// Reads a url from the given file and converts it into `ConnectParams`.
    pub fn from_file<P: AsRef<Path>>(path: P) -> HdbResult<ConnectParams> {
        fs::read_to_string(path)
            .context(HdbErrorKind::ConnParams)?
            .into_connect_params()
    }

    /// The ServerCerts.
    #[cfg(feature = "tls")]
    pub fn server_certs(&self) -> &Vec<ServerCerts> {
        &self.server_certs
    }

    /// The host.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The socket address.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Whether TLS or a plain TCP connection is to be used.
    pub fn use_tls(&self) -> bool {
        #[cfg(feature = "tls")]
        return !self.server_certs.is_empty();

        #[cfg(not(feature = "tls"))]
        return false;
    }

    /// The database user.
    pub fn dbuser(&self) -> &String {
        &self.dbuser
    }

    /// The password.
    pub fn password(&self) -> &SecStr {
        &self.password
    }

    /// The client locale.
    pub fn clientlocale(&self) -> Option<&String> {
        self.clientlocale.as_ref()
    }
}

impl std::fmt::Display for ConnectParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ConnectParams {{ addr: {}, dbuser: {}, clientlocale: {:?} }}",
            self.addr, self.dbuser, self.clientlocale,
        )
    }
}

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

impl IntoConnectParams for ConnectParamsBuilder {
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
        let host: String = match self.host_str() {
            Some("") | None => return Err(HdbErrorKind::Usage("host is missing").into()),
            Some(host) => host.to_string(),
        };

        let port: u16 = match self.port() {
            Some(p) => p,
            None => return Err(HdbErrorKind::Usage("port is missing").into()),
        };

        let dbuser: String = match self.username() {
            "" => return Err(HdbErrorKind::Usage("dbuser is missing").into()),
            s => s.to_string(),
        };

        let password = SecStr::from(match self.password() {
            None => return Err(HdbErrorKind::Usage("password is missing").into()),
            Some(s) => s.to_string(),
        });

        #[cfg(feature = "tls")]
        let use_tls = match self.scheme() {
            "hdbsql" => false,
            "hdbsqls" => true,
            _ => {
                return Err(HdbErrorKind::Usage(
                    "Unknown protocol, only 'hdbsql' and 'hdbsqls' are supported",
                )
                .into());
            }
        };
        #[cfg(not(feature = "tls"))]
        {
            if self.scheme() != "hdbsql" {
                return Err(HdbErrorKind::Usage(
                    "Unknown protocol, only 'hdbsql' is supported; \
                     for 'hdbsqls' the feature 'tls' must be used when compiling hdbconnect",
                )
                .into());
            }
        }

        #[cfg(feature = "tls")]
        let mut server_certs = Vec::<ServerCerts>::new();
        let mut clientlocale = None;

        for (name, value) in self.query_pairs() {
            match name.as_ref() {
                "client_locale" => clientlocale = Some(value.to_string()),
                "client_locale_from_env" => {
                    clientlocale = env::var("LANG").ok();
                }
                #[cfg(feature = "tls")]
                "tls_certificate_dir" => {
                    server_certs.push(ServerCerts::Directory(value.to_string()));
                }
                #[cfg(feature = "tls")]
                "tls_certificate_env" => {
                    server_certs.push(ServerCerts::Environment(value.to_string()));
                }
                #[cfg(feature = "tls")]
                "use_mozillas_root_certificates" => {
                    server_certs.push(ServerCerts::RootCertificates);
                }
                _ => log::warn!("option {} not supported", name),
            }
        }

        #[cfg(feature = "tls")]
        {
            if use_tls && server_certs.is_empty() {
                return Err(HdbErrorKind::Usage(
                    "protocol 'hdbsqls' requires certificates, but none are specified",
                )
                .into());
            }
        }

        Ok(ConnectParams {
            addr: format!("{}:{}", host, port),
            host,
            dbuser,
            password,
            clientlocale,
            #[cfg(feature = "tls")]
            server_certs,
        })
    }
}

/// Expresses where Server Certificates are read from.
#[cfg(feature = "tls")]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ServerCerts {
    /// Server Certificates are read from files in the specified folder.
    Directory(String),
    /// Server Certificates are read from the specified environment variable.
    Environment(String),
    /// The Server Certificate is given directly.
    Direct(String),
    /// Defines that the server roots from https://mkcert.org/ should be added to the
    /// trust store for TLS.
    RootCertificates,
}

#[cfg(test)]
mod tests {
    use super::IntoConnectParams;
    #[cfg(feature = "tls")]
    use super::ServerCerts;

    #[test]
    fn test_params_from_url() {
        {
            let params = "hdbsql://meier:schLau@abcd123:2222"
                .into_connect_params()
                .unwrap();

            assert_eq!("meier", params.dbuser());
            assert_eq!(b"schLau", params.password().unsecure());
            assert_eq!("abcd123:2222", params.addr());
            assert_eq!(None, params.clientlocale);
            #[cfg(feature = "tls")]
            assert!(params.server_certs().is_empty());
        }
        {
            let params = "hdbsql://meier:schLau@abcd123:2222\
                          ?client_locale=CL1\
                          &tls_certificate_dir=TCD\
                          &use_mozillas_root_certificates"
                .into_connect_params()
                .unwrap();

            assert_eq!("meier", params.dbuser());
            assert_eq!(b"schLau", params.password().unsecure());
            assert_eq!(Some("CL1".to_string()), params.clientlocale);
            #[cfg(feature = "tls")]
            assert_eq!(
                ServerCerts::RootCertificates,
                *params.server_certs().get(0).unwrap()
            );
        }
    }

    #[test]
    fn test_errors() {
        assert!("hdbsql://schLau@abcd123:2222"
            .into_connect_params()
            .is_err());
        assert!("hdbsql://meier@abcd123:2222".into_connect_params().is_err());
        assert!("hdbsql://meier:schLau@:2222".into_connect_params().is_err());
        assert!("hdbsql://meier:schLau@abcd123"
            .into_connect_params()
            .is_err());
    }
}
