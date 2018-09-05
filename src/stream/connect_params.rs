//! Connection parameters
use rustls::ClientConfig;
use secstr::SecStr;
use std::env;
use std::fmt;
use std::io;
use std::sync::Arc;
use url::Url;
use webpki::DNSNameRef;
use {HdbError, HdbResult};

/// An immutable struct with all information necessary to open a new connection
/// to a HANA database.
///
/// An instance of `ConnectParams` can be created either programmatically with
/// the builder, or implicitly using the trait `IntoConnectParams` and its
/// implementations.
///
/// # Example
///
/// ```
/// use hdbconnect::IntoConnectParams;
/// let conn_params = "hdbsql://my_user:my_passwd@the_host:2222"
///     .into_connect_params()
///     .unwrap();
/// ```
#[derive(Clone)]
pub struct ConnectParams {
    host: String,
    addr: String,
    dbuser: String,
    password: SecStr,
    clientlocale: Option<String>,
    tls_config: Option<Arc<ClientConfig>>,
    options: Vec<(String, String)>,
}
impl ConnectParams {
    // /// Returns a new builder for ConnectParams.
    // pub fn builder() -> ConnectParamsBuilder {
    //     ConnectParamsBuilder::new()
    // }

    /// The socket address.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The tls_configuration.
    pub fn tls_config(&self) -> io::Result<Arc<ClientConfig>> {
        match self.tls_config {
            Some(ref acc) => Ok(Arc::clone(acc)),
            None => Err(io::Error::from(
                io::ErrorKind::Other,
                // "No tls config available for plain connections".to_owned(),
            )),
        }
    }

    /// The dns_name_ref.
    pub fn dns_name_ref(&self) -> DNSNameRef {
        DNSNameRef::try_from_ascii_str(&self.host).unwrap()
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
    pub fn clientlocale(&self) -> &Option<String> {
        &self.clientlocale
    }

    /// Options to be passed to HANA.
    pub fn options(&self) -> &[(String, String)] {
        &self.options
    }
}

impl fmt::Debug for ConnectParams {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl<'a> IntoConnectParams for &'a str {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        match Url::parse(self) {
            Ok(url) => url.into_connect_params(),
            Err(_) => Err(HdbError::Usage("url parse error".to_owned())),
        }
    }
}

impl IntoConnectParams for String {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        self.as_str().into_connect_params()
    }
}

impl IntoConnectParams for Url {
    fn into_connect_params(self) -> HdbResult<ConnectParams> {
        let tls = match self.scheme() {
            "hdbsql" => false,
            "hdbsqls" => true,
            s => return Err(HdbError::Usage(format!("Unknown protocol {}", s))),
        };

        let host: String = match self.host_str() {
            Some("") | None => return Err(HdbError::Usage("host is missing".to_owned())),
            Some(host) => host.to_string(),
        };

        let port: u16 = match self.port() {
            Some(p) => p,
            None => return Err(HdbError::Usage("port is missing".to_owned())),
        };

        let dbuser: String = match self.username() {
            "" => return Err(HdbError::Usage("dbuser is missing".to_owned())),
            s => s.to_string(),
        };

        let password = SecStr::from(match self.password() {
            None => return Err(HdbError::Usage("password is missing".to_owned())),
            Some(s) => s.to_string(),
        });

        let tls_config = if tls {
            Some(Arc::new(ClientConfig::new()))
        } else {
            None
        };

        let mut clientlocale = None;
        let mut options = Vec::<(String, String)>::new();
        for (name, value) in self.query_pairs() {
            match name.as_ref() {
                "client_locale" => clientlocale = Some(value.to_string()),
                "client_locale_from_env" => {
                    clientlocale = match env::var("LANG") {
                        Ok(l) => Some(l),
                        Err(_) => None,
                    };
                }
                _ => options.push((name.to_string(), value.to_string())),
            }
        }

        Ok(ConnectParams {
            addr: format!("{}:{}", host, port),
            host,
            dbuser,
            password,
            clientlocale,
            options,
            tls_config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::IntoConnectParams;

    #[test]
    fn test_params_from_url() {
        let params = "hdbsql://meier:schLau@abcd123:2222"
            .into_connect_params()
            .unwrap();

        assert_eq!("meier", params.dbuser());
        assert_eq!(b"schLau", params.password().unsecure());
        assert_eq!("abcd123:2222", params.addr());
    }

    #[test]
    fn test_errors() {
        assert!(
            "hdbsql://schLau@abcd123:2222"
                .into_connect_params()
                .is_err()
        );
        assert!("hdbsql://meier@abcd123:2222".into_connect_params().is_err());
        assert!("hdbsql://meier:schLau@:2222".into_connect_params().is_err());
        assert!(
            "hdbsql://meier:schLau@abcd123"
                .into_connect_params()
                .is_err()
        );
    }
}
