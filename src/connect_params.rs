//! Connection parameters
use {HdbError, HdbResult};
use url::{self, Url};
use std::mem;


/// Immutable struct with all information necessary to open a new connection to a HANA database.
#[derive(Clone, Debug, Deserialize)]
pub struct ConnectParams {
    hostname: String,
    port: u16,
    dbuser: String,
    password: String,
    options: Vec<(String, String)>,
}

impl ConnectParams {
    /// Returns a new builder.
    pub fn builder() -> ConnectParamsBuilder {
        ConnectParamsBuilder::new()
    }

    /// The target host.
    pub fn hostname(&self) -> &String {
        &self.hostname
    }

    /// The target port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The database user.
    pub fn dbuser(&self) -> &str {
        &self.dbuser
    }

    /// The password.
    pub fn password(&self) -> &str {
        &self.password
    }

    /// Options to be passed to HANA.
    pub fn options(&self) -> &[(String, String)] {
        &self.options
    }
}

/// A builder for `ConnectParams`.
#[derive(Clone, Debug, Deserialize)]
pub struct ConnectParamsBuilder {
    hostname: Option<String>,
    port: Option<u16>,
    dbuser: Option<String>,
    password: Option<String>,
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
            options: vec![],
        }
    }

    /// Sets the hostname.
    pub fn hostname<'a, H: AsRef<str>>(&'a mut self, hostname: H) -> &'a mut ConnectParamsBuilder {
        self.hostname = Some(hostname.as_ref().to_owned());
        self
    }

    /// Sets the port.
    pub fn port<'a>(&'a mut self, port: u16) -> &'a mut ConnectParamsBuilder {
        self.port = Some(port);
        self
    }

    /// Sets the database user.
    pub fn dbuser<'a, D: AsRef<str>>(&'a mut self, dbuser: D) -> &'a mut ConnectParamsBuilder {
        self.dbuser = Some(dbuser.as_ref().to_owned());
        self
    }

    /// Sets the password.
    pub fn password<'a, P: AsRef<str>>(&'a mut self, pw: P) -> &'a mut ConnectParamsBuilder {
        self.password = Some(pw.as_ref().to_owned());
        self
    }

    /// Adds a runtime parameter.
    pub fn option<'a>(&'a mut self, name: &str, value: &str) -> &'a mut ConnectParamsBuilder {
        self.options.push((name.to_string(), value.to_string()));
        self
    }


    /// Constructs a `ConnectParams` from the builder.
    pub fn build<'a>(&'a mut self) -> HdbResult<ConnectParams> {
        Ok(ConnectParams {
            hostname: match self.hostname {
                Some(ref s) => s.clone(),
                None => return Err(HdbError::UsageError("hostname is missing".to_owned())),
            },
            port: match self.port {
                Some(p) => p,
                None => return Err(HdbError::UsageError("port is missing".to_owned())),
            },
            dbuser: match self.dbuser {
                Some(_) => self.dbuser.take().unwrap(),
                None => return Err(HdbError::UsageError("dbuser is missing".to_owned())),
            },
            password: match self.password {
                Some(_) => self.password.take().unwrap(),
                None => return Err(HdbError::UsageError("password is missing".to_owned())),
            },
            options: mem::replace(&mut self.options, vec![]),
        })
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
            Err(_) => Err(HdbError::UsageError("url parse error".to_owned())),
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
        let Url { host, port, user, path: url::Path { query: options, .. }, .. } = self;

        let mut builder = ConnectParams::builder();

        if let Some(port) = port {
            builder.port(port);
        }

        if let Some(info) = user {
            builder.dbuser(&info.user);
            if let Some(pass) = info.pass.as_ref().map(|p| &**p) {
                builder.password(pass);
            }
        }

        // if !path.is_empty() {
        //     // path contains the leading /
        //     builder.database(&path[1..]);
        // }

        for (name, value) in options {
            builder.option(&name, &value);
        }

        let host = url::decode_component(&host).map_err(|_| HdbError::UsageError("decode error".to_owned()))?;
        builder.hostname(host);
        builder.build()
    }
}


/// A trait implemented by types that can be converted into a `ConnectParamsBuilder`.
pub trait IntoConnectParamsBuilder {
    /// Converts the value of `self` into a `ConnectParams`.
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder>;
}

impl IntoConnectParamsBuilder for ConnectParamsBuilder {
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder> {
        Ok(self)
    }
}

impl<'a> IntoConnectParamsBuilder for &'a str {
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder> {
        match Url::parse(self) {
            Ok(url) => url.into_connect_params_builder(),
            Err(_) => Err(HdbError::UsageError("url parse error".to_owned())),
        }
    }
}

impl IntoConnectParamsBuilder for String {
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder> {
        self.as_str().into_connect_params_builder()
    }
}

impl IntoConnectParamsBuilder for Url {
    fn into_connect_params_builder(self) -> HdbResult<ConnectParamsBuilder> {
        let Url { host, port, user, path: url::Path { query: options, .. }, .. } = self;

        let mut builder = ConnectParams::builder();

        if let Some(port) = port {
            builder.port(port);
        }

        if let Some(info) = user {
            builder.dbuser(&info.user);
            if let Some(pass) = info.pass.as_ref().map(|p| &**p) {
                builder.password(pass);
            }
        }

        // if !path.is_empty() {
        //     // path contains the leading /
        //     builder.database(&path[1..]);
        // }

        for (name, value) in options {
            builder.option(&name, &value);
        }

        let host = url::decode_component(&host).map_err(|_| HdbError::UsageError("decode error".to_owned()))?;
        builder.hostname(host);
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use {ConnectParams, ConnectParamsBuilder};
    use connect_params::{IntoConnectParams, IntoConnectParamsBuilder};

    #[test]
    fn test_oneliner() {
        let connect_params: ConnectParams = ConnectParams::builder()
            .hostname("abcd123")
            .port(2222)
            .dbuser("MEIER")
            .password("schlau")
            .build()
            .unwrap();
        assert_eq!("abcd123", connect_params.hostname());
        assert_eq!("MEIER", connect_params.dbuser());
        assert_eq!(2222, connect_params.port());
    }

    #[test]
    fn test_reuse_builder() {
        let mut cp_builder: ConnectParamsBuilder = ConnectParams::builder();
        cp_builder.hostname("abcd123")
                  .port(2222);
        let params1: ConnectParams = cp_builder.dbuser("MEIER")
                                               .password("schlau")
                                               .build()
                                               .unwrap();
        let params2: ConnectParams = cp_builder.dbuser("HALLODRI")
                                               .password("kannnix")
                                               .build()
                                               .unwrap();

        assert_eq!("MEIER", params1.dbuser());
        assert_eq!("schlau", params1.password());
        assert_eq!("HALLODRI", params2.dbuser());
        assert_eq!("kannnix", params2.password());
    }

    #[test]
    fn test_params_from_url() {
        let params = "hdbsql://meier:schLau@abcd123:2222".into_connect_params().unwrap();

        assert_eq!("meier", params.dbuser());
        assert_eq!("schLau", params.password());
        assert_eq!("abcd123", params.hostname());
        assert_eq!(2222, params.port());
    }
    #[test]
    fn test_builder_from_url() {
        let params = "hdbsql://meier:schLau@abcd123:2222"
            .into_connect_params_builder()
            .unwrap()
            .password("GanzArgSchlau")
            .build()
            .unwrap();

        assert_eq!("meier", params.dbuser());
        assert_eq!("GanzArgSchlau", params.password());
        assert_eq!("abcd123", params.hostname());
        assert_eq!(2222, params.port());
    }
}
