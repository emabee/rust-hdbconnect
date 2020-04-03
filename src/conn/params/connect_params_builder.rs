use crate::{ConnectParams, HdbError, HdbResult, IntoConnectParamsBuilder, ServerCerts};
use secstr::SecStr;

/// A builder for `ConnectParams`.
///
/// # Example
///
/// ```rust
/// use hdbconnect::ConnectParams;
///
/// let connect_params = ConnectParams::builder()
///     .hostname("abcd123")
///     .port(2222)
///     .dbuser("MEIER")
///     .password("schlau")
///     .build()
///     .unwrap();
/// ```
///
/// ## Instantiating a `ConnectParamsBuilder` from a URL
///
/// The URL is supposed to have the same form as for `ConnectParams`
/// (i.e. `<scheme>://<username>:<password>@<host>:<port>[<options>]`,
/// see [Using a URL](struct.ConnectParams.html#using-a-url)),
/// but only scheme and host
/// are mandatory.
///
/// ### Example
///
/// ```rust
/// use hdbconnect::IntoConnectParamsBuilder;
///
/// let conn_params = "hdbsql://abcd123:2222"
///     .into_connect_params_builder()
///     .unwrap()
///     .dbuser("MEIER")
///     .password("schlau")
///     .build()
///     .unwrap();
/// ```
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(into = "String")]
pub struct ConnectParamsBuilder {
    hostname: Option<String>,
    port: Option<u16>,
    dbuser: Option<String>,
    #[serde(skip)]
    password: Option<SecStr>,
    clientlocale: Option<String>,
    server_certs: Vec<ServerCerts>,
    options: Vec<(String, String)>,
    #[cfg(feature = "alpha_nonblocking")]
    use_nonblocking: bool,
}

impl ConnectParamsBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self {
            hostname: None,
            port: None,
            dbuser: None,
            password: None,
            clientlocale: None,
            server_certs: Vec::<ServerCerts>::default(),
            options: vec![],
            #[cfg(feature = "alpha_nonblocking")]
            use_nonblocking: false,
        }
    }

    /// Sets the hostname.
    pub fn hostname<H: AsRef<str>>(&mut self, hostname: H) -> &mut Self {
        self.hostname = Some(hostname.as_ref().to_owned());
        self
    }

    /// Sets the port.
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port);
        self
    }

    /// Sets the database user.
    pub fn dbuser<D: AsRef<str>>(&mut self, dbuser: D) -> &mut Self {
        self.dbuser = Some(dbuser.as_ref().to_owned());
        self
    }

    /// Sets the password.
    pub fn password<P: AsRef<str>>(&mut self, pw: P) -> &mut Self {
        self.password = Some(SecStr::new(pw.as_ref().as_bytes().to_vec()));
        self
    }

    /// Unsets the password.
    pub fn unset_password(&mut self) -> &mut Self {
        self.password = None;
        self
    }

    /// Sets the client locale.
    pub fn clientlocale<P: AsRef<str>>(&mut self, cl: P) -> &mut Self {
        self.clientlocale = Some(cl.as_ref().to_owned());
        self
    }

    /// Sets the client locale from the value of the environment variable LANG
    pub fn clientlocale_from_env_lang(&mut self) -> &mut Self {
        self.clientlocale = match std::env::var("LANG") {
            Ok(l) => Some(l),
            Err(_) => None,
        };

        self
    }

    /// Sets the client locale.
    #[cfg(feature = "alpha_nonblocking")]
    pub fn use_nonblocking(&mut self) -> &mut Self {
        self.use_nonblocking = true;
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
    pub fn tls_with(&mut self, server_certs: ServerCerts) -> &mut Self {
        self.server_certs.push(server_certs);
        self
    }

    /// Adds a runtime parameter.
    pub fn option(&mut self, name: &str, value: &str) -> &mut Self {
        self.options.push((name.to_string(), value.to_string()));
        self
    }

    /// Constructs a `ConnectParams` from the builder.
    ///
    /// # Errors
    /// `HdbError::Usage` if the builder was not yet configured to
    /// create a meaningful `ConnectParams`
    pub fn build(&self) -> HdbResult<ConnectParams> {
        let host = match self.hostname {
            Some(ref s) => s.clone(),
            None => return Err(HdbError::Usage("hostname is missing")),
        };

        let addr = format!(
            "{}:{}",
            host,
            match self.port {
                Some(p) => p,
                None => return Err(HdbError::Usage("port is missing")),
            }
        );
        let dbuser = match self.dbuser {
            Some(ref s) => s.clone(),
            None => return Err(HdbError::Usage("dbuser is missing")),
        };
        let password = match self.password {
            Some(ref secstr) => secstr.clone(),
            None => return Err(HdbError::Usage("password is missing")),
        };

        Ok(ConnectParams::new(
            host,
            addr,
            dbuser,
            password,
            self.clientlocale.clone(),
            self.server_certs.clone(),
            #[cfg(feature = "alpha_nonblocking")]
            self.use_nonblocking,
        ))
    }

    /// Returns the url for this connection
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the builder was not yet configured to
    /// build a correct url
    pub fn to_url(&self) -> HdbResult<String> {
        if let Some(dbuser) = &self.dbuser {
            if let Some(hostname) = &self.hostname {
                if let Some(port) = &self.port {
                    return Ok(format!(
                        "{}://{}@{}:{}{}",
                        self.get_protocol_name(),
                        dbuser,
                        hostname,
                        port,
                        self.get_options_as_parameters()
                    ));
                }
            }
        }

        Err(HdbError::Usage("missing data. not possible to build url"))
    }

    fn get_protocol_name(&self) -> &str {
        if self.server_certs.is_empty() {
            "hdbsql"
        } else {
            "hdbsqls"
        }
    }

    fn get_options_as_parameters(&self) -> String {
        let mut result = String::with_capacity(200);
        for (index, s) in self
            .options
            .iter()
            .map(|(k, v)| {
                if v.is_empty() {
                    k.clone()
                } else {
                    format!("{}={}", k, v)
                }
            })
            .chain(self.server_certs.iter().map(ToString::to_string))
            .chain(
                self.clientlocale
                    .iter()
                    .map(|l| format!("{}={}", super::cp_url::OPTION_CLIENT_LOCALE, l)),
            )
            .enumerate()
        {
            let prefix = if index == 0 { "?" } else { "&" };
            result.push_str(&format!("{}{}", prefix, s));
        }
        result
    }

    /// Getter
    pub fn get_hostname(&self) -> Option<&str> {
        self.hostname.as_deref()
    }

    /// Getter
    pub fn get_dbuser(&self) -> Option<&str> {
        self.dbuser.as_deref()
    }

    /// Getter
    pub fn get_password(&self) -> Option<&SecStr> {
        self.password.as_ref()
    }

    /// Getter
    pub fn get_port(&self) -> Option<u16> {
        self.port
    }

    /// Getter
    pub fn get_clientlocale(&self) -> Option<&str> {
        self.clientlocale.as_deref()
    }

    /// Getter
    pub fn get_server_certs(&self) -> &Vec<ServerCerts> {
        &self.server_certs
    }

    /// Getter
    pub fn get_options(&self) -> &Vec<(String, String)> {
        &self.options
    }
}

impl<'de> serde::de::Deserialize<'de> for ConnectParamsBuilder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let visitor = Visitor();
        deserializer.deserialize_str(visitor)
    }
}

struct Visitor();
impl<'de> serde::de::Visitor<'de> for Visitor {
    type Value = ConnectParamsBuilder;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a String in the form of a url")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        IntoConnectParamsBuilder::into_connect_params_builder(v).map_err(E::custom)
    }
}

impl Into<String> for ConnectParamsBuilder {
    fn into(mut self) -> String {
        self.unset_password();
        self.to_url().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::super::into_connect_params_builder::IntoConnectParamsBuilder;
    use super::ConnectParamsBuilder;
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
            assert!(params.server_certs().is_empty());
        }
        {
            let mut builder = ConnectParamsBuilder::new();
            builder
                .hostname("abcd123")
                .port(2222)
                .dbuser("MEIER")
                .password("schLau")
                .clientlocale("de_DE");
            builder.tls_with(crate::ServerCerts::Directory("TCD".to_string()));
            builder.tls_with(crate::ServerCerts::RootCertificates);

            let params = builder.build().unwrap();
            assert_eq!("MEIER", params.dbuser());
            assert_eq!(b"schLau", params.password().unsecure());
            assert_eq!(Some("de_DE"), params.clientlocale());
            assert_eq!(
                ServerCerts::Directory("TCD".to_string()),
                *params.server_certs().get(0).unwrap()
            );
            assert_eq!(
                ServerCerts::RootCertificates,
                *params.server_certs().get(1).unwrap()
            );
        }
        {
            let builder = "hdbsql://MEIER:schLau@abcd123:2222"
                .into_connect_params_builder()
                .unwrap();
            assert_eq!("MEIER", builder.get_dbuser().unwrap());
            assert_eq!(b"schLau", builder.get_password().unwrap().unsecure());
            assert_eq!("abcd123", builder.get_hostname().unwrap());
            assert_eq!(2222, builder.get_port().unwrap());
            assert_eq!(None, builder.get_clientlocale());
            assert!(builder.get_server_certs().is_empty());
        }
    }

    #[test]
    fn serde_test() {
        #[derive(Serialize, Deserialize, Debug)]
        struct Data {
            x: ConnectParamsBuilder,
        }

        let mut data = Data {
            x: ConnectParamsBuilder::new(),
        };
        data.x
            .hostname("abcd123")
            .port(2222)
            .dbuser("MEIER")
            .password("schLau")
            .clientlocale("de_DE")
            .tls_with(crate::ServerCerts::Directory("TCD".to_string()))
            .tls_with(crate::ServerCerts::RootCertificates);

        let serialized = serde_json::to_string(&data).unwrap();
        assert_eq!(
            r#"{"x":"hdbsqls://MEIER@abcd123:2222?tls_certificate_dir=TCD&use_mozillas_root_certificates&client_locale=de_DE"}"#,
            serialized
        );

        let deserialized: Data = serde_json::from_str(&serialized).unwrap();
        assert_ne!(data.x, deserialized.x);
        data.x.unset_password();
        assert_eq!(data.x, deserialized.x);
    }
}
