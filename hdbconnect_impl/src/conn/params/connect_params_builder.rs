use super::{cp_url::format_as_url, tls::Tls};
use crate::{
    ConnectParams, HdbResult, IntoConnectParamsBuilder, ServerCerts, conn::Compression, usage_err,
};
use secstr::SecUtf8;

/// A builder for `ConnectParams`.
///
/// # Instantiating a `ConnectParamsBuilder` programmatically
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
/// # Instantiating a `ConnectParamsBuilder` from a URL
///
/// See module [`url`](crate::url) for details about the supported URLs.
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
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(into = "String")]
pub struct ConnectParamsBuilder {
    hostname: Option<String>,
    port: Option<u16>,
    dbuser: Option<String>,
    #[serde(skip)]
    password: Option<SecUtf8>,
    dbname: Option<String>,
    network_group: Option<String>,
    clientlocale: Option<String>,
    compression: Compression,
    tls: Tls,
}

impl ConnectParamsBuilder {
    /// Creates a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new builder based on the given URL.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` when parsing or evaluating the url failed.
    pub fn from(url: &str) -> HdbResult<Self> {
        url.into_connect_params_builder()
    }

    /// Sets the hostname.
    pub fn hostname<H: AsRef<str>>(&mut self, hostname: H) -> &mut Self {
        self.hostname = Some(hostname.as_ref().to_owned());
        self
    }

    /// Sets the hostname.
    #[must_use]
    pub fn with_hostname<H: AsRef<str>>(mut self, hostname: H) -> Self {
        self.hostname = Some(hostname.as_ref().to_owned());
        self
    }

    /// Sets the port.
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port);
        self
    }

    /// Sets the port.
    #[must_use]
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Sets the database user.
    pub fn dbuser<D: AsRef<str>>(&mut self, dbuser: D) -> &mut Self {
        self.dbuser = Some(dbuser.as_ref().to_owned());
        self
    }

    /// Sets the database user.
    #[must_use]
    pub fn with_dbuser<D: AsRef<str>>(mut self, dbuser: D) -> Self {
        self.dbuser = Some(dbuser.as_ref().to_owned());
        self
    }

    /// Sets the password.
    pub fn password<P: AsRef<str>>(&mut self, pw: P) -> &mut Self {
        self.password = Some(SecUtf8::from(pw.as_ref()));
        self
    }

    /// Sets the password.
    #[must_use]
    pub fn with_password<P: AsRef<str>>(mut self, pw: P) -> Self {
        self.password = Some(SecUtf8::from(pw.as_ref()));
        self
    }

    /// Unsets the password.
    pub fn unset_password(&mut self) -> &mut Self {
        self.password = None;
        self
    }

    /// Whether TLS or a plain TCP connection is to be used.
    #[must_use]
    pub fn is_tls(&self) -> bool {
        !matches!(self.tls, Tls::Off)
    }

    /// Sets the database name.
    ///
    /// This allows specifying host and port of the system DB
    /// and getting automatically redirected and connected to the specified tenant database.
    pub fn dbname<D: AsRef<str>>(&mut self, dbname: D) -> &mut Self {
        self.dbname = Some(dbname.as_ref().to_owned());
        self
    }

    /// Sets the database name.
    ///
    /// This allows specifying host and port of the system DB
    /// and getting automatically redirected and connected to the specified tenant database.
    #[must_use]
    pub fn with_dbname<D: AsRef<str>>(mut self, dbname: D) -> Self {
        self.dbname = Some(dbname.as_ref().to_owned());
        self
    }

    /// Sets the network group.
    pub fn network_group<D: AsRef<str>>(&mut self, network_group: D) -> &mut Self {
        self.network_group = Some(network_group.as_ref().to_owned());
        self
    }

    /// Sets the client locale.
    pub fn clientlocale<P: AsRef<str>>(&mut self, cl: P) -> &mut Self {
        self.clientlocale = Some(cl.as_ref().to_owned());
        self
    }

    /// Sets the client locale from the value of the environment variable LANG
    pub fn clientlocale_from_env_lang(&mut self) -> &mut Self {
        self.clientlocale = std::env::var("LANG").ok();
        self
    }

    /// Switch off compression (for debugging purposes?)
    ///
    /// By default, compression is supported, like with `always_uncompressed(false)`
    pub fn always_uncompressed(&mut self, uncompressed: bool) -> &mut Self {
        self.compression = if uncompressed {
            Compression::Off
        } else {
            Compression::Always
        };
        self
    }

    /// Makes the driver use TLS for the connection to the database.
    ///
    /// Requires that the server's certificate is provided with one of the
    /// enum variants of [`ServerCerts`](crate::ServerCerts).
    ///
    /// If needed, you can call this function multiple times with different `ServerCert` variants.
    pub fn tls_with(&mut self, server_certs: ServerCerts) -> &mut Self {
        match self.tls {
            Tls::Off | Tls::Insecure => {
                self.tls = Tls::Secure(vec![]);
            }
            Tls::Secure(_) => {}
        }
        if let Tls::Secure(ref mut v) = self.tls {
            v.push(server_certs);
        }
        self
    }

    /// Makes the driver use TLS for the connection to the database.
    ///
    /// Requires that the server's certificate is provided with one of the
    /// enum variants of [`ServerCerts`](crate::ServerCerts).
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
    ///    .with_tls_with(ServerCerts::Direct(string_with_certificate))
    ///    .build();
    /// ```
    #[must_use]
    pub fn with_tls_with(mut self, server_certs: ServerCerts) -> Self {
        match self.tls {
            Tls::Off | Tls::Insecure => {
                self.tls = Tls::Secure(vec![]);
            }
            Tls::Secure(_) => {}
        }
        if let Tls::Secure(ref mut v) = self.tls {
            v.push(server_certs);
        }
        self
    }

    /// Makes the driver use TLS for the connection to the database, but
    /// hazardously without verifying the server's certificate.
    /// Erases all already configured server certs.
    pub fn tls_without_server_verification(&mut self) -> &mut Self {
        self.tls = Tls::Insecure;
        self
    }

    /// Decides whether the driver use TLS for the connection to the database, or not.
    /// If yes, it chooses the hazardous option without verifying the server's certificate.
    /// Erases all already configured server certs.
    #[must_use]
    pub fn with_tls_without_server_verification(mut self, with_tls: bool) -> Self {
        self.tls = if with_tls { Tls::Insecure } else { Tls::Off };
        self
    }

    /// Constructs a `ConnectParams` from the builder.
    ///
    /// # Errors
    /// `HdbError::Usage` if the builder was not yet configured to
    /// create a meaningful `ConnectParams`
    pub fn build(&self) -> HdbResult<ConnectParams> {
        let host = self
            .hostname
            .clone()
            .ok_or_else(|| usage_err!("hostname is missing"))?;

        let port = self.port.ok_or_else(|| usage_err!("port is missing"))?;

        let dbuser: String = self
            .dbuser
            .clone()
            .ok_or_else(|| usage_err!("dbuser is missing"))?;

        let password = self
            .password
            .clone()
            .ok_or_else(|| usage_err!("password is missing"))?;

        Ok(ConnectParams::new(
            host,
            port,
            dbuser,
            password,
            self.dbname.clone(),
            self.network_group.clone(),
            self.clientlocale.clone(),
            self.compression,
            self.tls.clone(),
        ))
    }

    /// Returns the url for this connection, without the password.
    #[must_use]
    pub fn to_url_without_password(&self) -> String {
        self.to_string()
    }

    /// Returns the url for this connection _with_ the password.
    #[must_use]
    pub fn to_url_with_password(&self) -> String {
        let user_string = if let Some(pw) = self.get_password() {
            format!(
                "{}:{}",
                self.dbuser.as_deref().unwrap_or("<user_not_given>"),
                pw.unsecure()
            )
        } else {
            self.dbuser.as_deref().unwrap_or("").to_string()
        };
        format_as_url(
            &format!(
                "{}:{}",
                self.hostname.as_deref().unwrap_or(""),
                self.port.unwrap_or_default()
            ),
            &user_string,
            self.dbname.as_deref(),
            self.network_group.as_deref(),
            &self.tls,
            self.clientlocale.as_deref(),
            self.compression,
        )
    }

    /// Returns the configured hostname.
    #[must_use]
    pub fn get_hostname(&self) -> Option<&str> {
        self.hostname.as_deref()
    }

    /// Returns the configured database user.
    #[must_use]
    pub fn get_dbuser(&self) -> Option<&str> {
        self.dbuser.as_deref()
    }

    /// Returns the configured password.
    #[must_use]
    pub fn get_password(&self) -> Option<&SecUtf8> {
        self.password.as_ref()
    }

    /// Returns the configured port number.
    #[must_use]
    pub fn get_port(&self) -> Option<u16> {
        self.port
    }

    /// Returns the configured client locale.
    #[must_use]
    pub fn get_clientlocale(&self) -> Option<&str> {
        self.clientlocale.as_deref()
    }

    /// Returns the configured database name.
    #[must_use]
    pub fn get_dbname(&self) -> Option<&str> {
        self.dbname.as_deref()
    }

    /// Returns the configured network group.
    #[must_use]
    pub fn get_networkgroup(&self) -> Option<&str> {
        self.network_group.as_deref()
    }

    /// Returns the configured variants for validating the server certificate.
    #[must_use]
    pub fn get_server_certs(&self) -> Option<&Vec<ServerCerts>> {
        match self.tls {
            Tls::Secure(ref sc) => Some(sc),
            _ => None,
        }
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
impl serde::de::Visitor<'_> for Visitor {
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

impl From<ConnectParamsBuilder> for String {
    fn from(mut cpb: ConnectParamsBuilder) -> String {
        cpb.unset_password();
        cpb.to_string()
    }
}

impl std::fmt::Display for ConnectParamsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            format_as_url(
                &format!(
                    "{}:{}",
                    self.hostname.as_deref().unwrap_or(""),
                    self.port.unwrap_or_default()
                ),
                self.dbuser.as_deref().unwrap_or(""),
                self.dbname.as_deref(),
                self.network_group.as_deref(),
                &self.tls,
                self.clientlocale.as_deref(),
                self.compression,
            )
        )
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
            assert_eq!("schLau", params.password().unsecure());
            assert_eq!("abcd123:2222", params.addr());
            assert_eq!(None, params.clientlocale());
            assert!(params.server_certs().is_none());
            assert!(!params.is_tls());
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
            assert_eq!("schLau", params.password().unsecure());
            assert_eq!(Some("de_DE"), params.clientlocale());
            assert_eq!(
                ServerCerts::Directory("TCD".to_string()),
                *params.server_certs().unwrap().first().unwrap()
            );
            assert_eq!(
                ServerCerts::RootCertificates,
                *params.server_certs().unwrap().get(1).unwrap()
            );
        }
        {
            let builder = "hdbsql://MEIER:schLau@abcd123:2222"
                .into_connect_params_builder()
                .unwrap();
            assert_eq!("MEIER", builder.get_dbuser().unwrap());
            assert_eq!("schLau", builder.get_password().unwrap().unsecure());
            assert_eq!("abcd123", builder.get_hostname().unwrap());
            assert_eq!(2222, builder.get_port().unwrap());
            assert_eq!(None, builder.get_clientlocale());
            assert!(builder.get_server_certs().is_none());
            assert!(!builder.is_tls());
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
