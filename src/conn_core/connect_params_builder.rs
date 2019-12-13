use crate::conn_core::connect_params::ConnectParams;
#[cfg(feature = "tls")]
use crate::conn_core::connect_params::ServerCerts;
use crate::{HdbError, HdbResult};
use secstr::SecStr;

/// A builder for `ConnectParams`.
///
/// Note that some methods are not included by default, but require to build `hdbconnect` with
/// the respective feature(s).
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
    password: Option<SecStr>,
    clientlocale: Option<String>,
    #[cfg(feature = "tls")]
    server_certs: Vec<ServerCerts>,
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
            server_certs: Default::default(),
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
        self.password = Some(SecStr::new(pw.as_ref().as_bytes().to_vec()));
        self
    }

    /// Sets the client locale.
    pub fn clientlocale<P: AsRef<str>>(&mut self, cl: P) -> &mut ConnectParamsBuilder {
        self.clientlocale = Some(cl.as_ref().to_owned());
        self
    }

    /// Sets the client locale from the value of the environment variable LANG
    pub fn clientlocale_from_env_lang(&mut self) -> &mut ConnectParamsBuilder {
        self.clientlocale = match std::env::var("LANG") {
            Ok(l) => Some(l),
            Err(_) => None,
        };

        self
    }

    /// Makes the driver use TLS for the connection to the database.
    ///
    /// Requires that the server's certificate is provided with one of the
    /// enum variants of [`ServerCerts`](enum.ServerCerts.html).
    ///
    /// If needed, you can call this function multiple times with different `ServerCert` variants.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{ConnectParams,ServerCerts};
    /// # let string_with_certificate = String::new();
    /// let mut conn_params = ConnectParams::builder()
    ///    // ...more settings required...
    ///    .tls_with(ServerCerts::Direct(string_with_certificate))
    ///    .build();
    /// ```
    ///
    /// This method is only available with feature `tls`.
    #[cfg(feature = "tls")]
    pub fn tls_with(&mut self, server_certs: ServerCerts) -> &mut ConnectParamsBuilder {
        self.server_certs.push(server_certs);
        self
    }

    /// Adds a runtime parameter.
    pub fn option(&mut self, name: &str, value: &str) -> &mut ConnectParamsBuilder {
        self.options.push((name.to_string(), value.to_string()));
        self
    }

    /// Constructs a `ConnectParams` from the builder.
    pub fn build(&self) -> HdbResult<ConnectParams> {
        let host = match self.hostname {
            Some(ref s) => s.clone(),
            None => return Err(HdbError::Usage("hostname is missing".to_owned())),
        };

        let addr = format!(
            "{}:{}",
            host,
            match self.port {
                Some(p) => p,
                None => return Err(HdbError::Usage("port is missing".to_owned())),
            }
        );
        let dbuser = match self.dbuser {
            Some(ref s) => s.clone(),
            None => return Err(HdbError::Usage("dbuser is missing".to_owned())),
        };
        let password = match self.password {
            Some(ref secstr) => secstr.clone(),
            None => return Err(HdbError::Usage("password is missing".to_owned())),
        };

        #[cfg(feature = "tls")]
        let conn_params = ConnectParams::new(
            host,
            addr,
            dbuser,
            password,
            self.clientlocale.clone(),
            self.server_certs.clone(),
        );

        #[cfg(not(feature = "tls"))]
        let conn_params =
            ConnectParams::new(host, addr, dbuser, password, self.clientlocale.clone());

        Ok(conn_params)
    }
}

#[cfg(test)]
mod test {
    use super::ConnectParamsBuilder;
    #[cfg(feature = "tls")]
    use super::ServerCerts;

    #[test]
    fn test_connect_params_builder() {
        {
            let params = ConnectParamsBuilder::new()
                .hostname("abcd123")
                .port(2222)
                .dbuser("MEIER")
                .password("schLau")
                .build()
                .unwrap();
            assert_eq!("MEIER", params.dbuser());
            assert_eq!(b"schLau", params.password().unsecure());
            assert_eq!("abcd123:2222", params.addr());
            assert_eq!(None, params.clientlocale());
            #[cfg(feature = "tls")]
            assert!(params.server_certs().is_empty());
        }
        {
            let mut builder = ConnectParamsBuilder::new();
            builder
                .hostname("abcd123")
                .port(2222)
                .dbuser("MEIER")
                .password("schLau")
                .clientlocale("CL1");
            #[cfg(feature = "tls")]
            builder.tls_with(crate::ServerCerts::Directory("TCD".to_string()));
            #[cfg(feature = "tls")]
            builder.tls_with(crate::ServerCerts::RootCertificates);

            let params = builder.build().unwrap();
            assert_eq!("MEIER", params.dbuser());
            assert_eq!(b"schLau", params.password().unsecure());
            assert_eq!(Some(&"CL1".to_string()), params.clientlocale());
            #[cfg(feature = "tls")]
            assert_eq!(
                ServerCerts::RootCertificates,
                *params.server_certs().get(0).unwrap()
            );
        }
    }
}
