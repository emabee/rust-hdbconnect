//! Connection parameters
use secstr::SecStr;
use std::env;
use std::fmt;
use std::fs;
use std::mem;
use std::path::Path;
use url::Url;
use {HdbError, HdbResult};

/// An immutable struct with all information necessary to open a new connection
/// to a HANA database.
///
/// An instance of `ConnectParams` can be created from a url (represented as `String` or as `Url`)
/// either using the trait `IntoConnectParams` and its implementations, or with the shortcut
/// `ConnectParams::from_file`.
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
/// > `<options>` = `?<key> = <value> [{&<key> = <value>}]`
///
/// Special option keys are:
/// > `client_locale`: `<value>` is used to specify the client's locale  
/// > `client_locale_from_env`: if `<value>` is 1, the client's locale is read
///   from the environment variabe LANG  
/// > `tls_certificate_dir`: the `<value>` points to a folder with pem files that contain
///   the server's certificates; all pem files in that folder are evaluated  
/// > `tls_certificate_env`: the `<value>` denotes the environment variable that contains
///   the server's certificate
///
/// The client locale is used in language-dependent handling within the SAP HANA
/// database calculation engine.
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
    #[cfg(feature = "tls")]
    use_tls: bool,
    host: String,
    addr: String,
    dbuser: String,
    password: SecStr,
    clientlocale: Option<String>,
    #[cfg(feature = "tls")]
    server_certs: ServerCerts,
    options: Vec<(String, String)>,
}
impl ConnectParams {
    /// Returns a new builder for ConnectParams.
    pub fn builder() -> ConnectParamsBuilder {
        ConnectParamsBuilder::new()
    }

    /// Reads a url from the given file and converts it into `ConnectParams`.
    pub fn from_file<P: AsRef<Path>>(path: P) -> HdbResult<ConnectParams> {
        fs::read_to_string(path)?.into_connect_params()
    }

    /// The ServerCerts.
    #[cfg(feature = "tls")]
    pub fn server_certs(&self) -> &ServerCerts {
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
        return self.use_tls;

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

        #[cfg(feature = "tls")]
        let use_tls = match self.scheme() {
            "hdbsql" => false,
            "hdbsqls" => true,
            s => {
                return Err(HdbError::Usage(format!(
                    "Unknown protocol '{}'; only 'hdbsql' and 'hdbsqls' are supported",
                    s
                )))
            }
        };

        #[cfg(not(feature = "tls"))]
        {
            if self.scheme() != "hdbsql" {
                return Err(HdbError::Usage(format!(
                    "Unknown protocol '{}'; only 'hdbsql' is supported; \
                     for 'hdbsqls' the feature 'tls' must be used when compiling hdbconnect",
                    self.scheme()
                )));
            }
        }

        #[cfg(feature = "tls")]
        let mut server_certs = ServerCerts::None;
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
                #[cfg(feature = "tls")]
                "tls_certificate_dir" => server_certs = ServerCerts::Directory(value.to_string()),
                #[cfg(feature = "tls")]
                "tls_certificate_env" => server_certs = ServerCerts::Environment(value.to_string()),
                _ => options.push((name.to_string(), value.to_string())),
            }
        }

        Ok(ConnectParams {
            #[cfg(feature = "tls")]
            use_tls,
            addr: format!("{}:{}", host, port),
            host,
            dbuser,
            password,
            clientlocale,
            #[cfg(feature = "tls")]
            server_certs,
            options,
        })
    }
}

/// A builder for `ConnectParams`.
///
/// # Example
///
/// ```
/// use hdbconnect::ConnectParams;
/// let connect_params = ConnectParams::builder()
///     .hostname("abcd123")
///     .port(2222)
///     .dbuser("MEIER")
///     .password("schlau")
///     .build()
///     .unwrap();
/// ```
#[derive(Clone, Debug, Default)]
pub struct ConnectParamsBuilder {
    hostname: Option<String>,
    port: Option<u16>,
    dbuser: Option<String>,
    password: Option<String>,
    clientlocale: Option<String>,
    #[cfg(feature = "tls")]
    server_certs: ServerCerts,
    options: Vec<(String, String)>,
}

impl ConnectParamsBuilder {
    /// Creates a new builder.
    pub fn new() -> ConnectParamsBuilder {
        ConnectParamsBuilder {
            hostname: None,
            port: None,
            dbuser: None,
            password: None,
            clientlocale: None,
            #[cfg(feature = "tls")]
            server_certs: ServerCerts::None,
            options: vec![],
        }
    }

    /// Sets the hostname.
    pub fn hostname<H: AsRef<str>>(&mut self, hostname: H) -> &mut ConnectParamsBuilder {
        self.hostname = Some(hostname.as_ref().to_owned());
        self
    }

    /// Sets the port.
    pub fn port(&mut self, port: u16) -> &mut ConnectParamsBuilder {
        self.port = Some(port);
        self
    }

    /// Sets the database user.
    pub fn dbuser<D: AsRef<str>>(&mut self, dbuser: D) -> &mut ConnectParamsBuilder {
        self.dbuser = Some(dbuser.as_ref().to_owned());
        self
    }

    /// Sets the password.
    pub fn password<P: AsRef<str>>(&mut self, pw: P) -> &mut ConnectParamsBuilder {
        self.password = Some(pw.as_ref().to_owned());
        self
    }

    /// Sets the client locale.
    pub fn clientlocale<P: AsRef<str>>(&mut self, cl: P) -> &mut ConnectParamsBuilder {
        self.clientlocale = Some(cl.as_ref().to_owned());
        self
    }

    /// Sets the client locale from the value of the environment variable LANG
    pub fn clientlocale_from_env_lang(&mut self) -> &mut ConnectParamsBuilder {
        self.clientlocale = match env::var("LANG") {
            Ok(l) => Some(l),
            Err(_) => None,
        };

        self
    }

    /// Enforces the usage of TLS for the connection to the database and requires that
    /// the server's certificate is provided in a directory
    #[cfg(feature = "tls")]
    pub fn use_certificate_dir(&mut self, certificate_dir: String) -> &mut ConnectParamsBuilder {
        self.server_certs = ServerCerts::Directory(certificate_dir);
        self
    }

    /// Enforces the usage of TLS for the connection to the database and requires that
    /// the server's certificate is provided via the specified environment variable
    #[cfg(feature = "tls")]
    pub fn use_certificate_env(&mut self, certificate_env: String) -> &mut ConnectParamsBuilder {
        self.server_certs = ServerCerts::Environment(certificate_env);
        self
    }

    /// Adds a runtime parameter.
    pub fn option<'a>(&'a mut self, name: &str, value: &str) -> &'a mut ConnectParamsBuilder {
        self.options.push((name.to_string(), value.to_string()));
        self
    }

    /// Constructs a `ConnectParams` from the builder.
    pub fn build(&mut self) -> HdbResult<ConnectParams> {
        Ok(ConnectParams {
            host: match self.hostname {
                Some(ref s) => s.clone(),
                None => return Err(HdbError::Usage("hostname is missing".to_owned())),
            },
            addr: format!(
                "{}:{}",
                match self.hostname {
                    Some(ref s) => s,
                    None => return Err(HdbError::Usage("hostname is missing".to_owned())),
                },
                match self.port {
                    Some(ref p) => *p,
                    None => return Err(HdbError::Usage("port is missing".to_owned())),
                }
            ),
            dbuser: match self.dbuser {
                Some(_) => self.dbuser.take().unwrap(),
                None => return Err(HdbError::Usage("dbuser is missing".to_owned())),
            },
            password: match self.password {
                Some(_) => SecStr::from(self.password.take().unwrap()),
                None => return Err(HdbError::Usage("password is missing".to_owned())),
            },
            clientlocale: match self.clientlocale {
                Some(_) => Some(self.clientlocale.take().unwrap()),
                None => None,
            },
            options: mem::replace(&mut self.options, vec![]),

            #[cfg(feature = "tls")]
            use_tls: self.server_certs.is_some(),

            #[cfg(feature = "tls")]
            server_certs: self.server_certs.clone(),
        })
    }
}

#[cfg(feature = "tls")]
#[derive(Clone, Debug)]
pub enum ServerCerts {
    Directory(String),
    Environment(String),
    None,
}
#[cfg(feature = "tls")]
impl ServerCerts {
    pub fn is_some(&self) -> bool {
        match self {
            ServerCerts::None => false,
            _ => true,
        }
    }
}
#[cfg(feature = "tls")]
impl Default for ServerCerts {
    fn default() -> ServerCerts {
        ServerCerts::None
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
