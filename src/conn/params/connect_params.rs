//! Connection parameters
use super::cp_url;
use crate::{ConnectParamsBuilder, HdbError, HdbResult, IntoConnectParams};
use rustls::ClientConfig;
use secstr::SecStr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// An immutable struct with all information necessary to open a new connection
/// to a HANA database.
///
/// An instance of `ConnectParams` can be created in various ways:
///
/// ## Using the [builder](struct.ConnectParams.html#method.builder)
/// This is the most flexible way for instantiating `ConnectParams`.
/// The builder can be instantiated empty or from a minimal URL,
/// and allows specifying all necessary details programmatically.
///
/// ### Example
///
/// ```rust,norun
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
/// > `insecure_omit_server_certificate_check` (no value): lets the driver omit the validation of
///   the server's identity. Don't use this option in productive setups!
///
///
/// The client locale is used in language-dependent handling within the SAP HANA
/// database calculation engine.
///
/// ### Example
///
/// ```rust
/// use hdbconnect::IntoConnectParams;
/// let conn_params = "hdbsql://my_user:my_passwd@the_host:2222"
///     .into_connect_params()
///     .unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct ConnectParams {
    host: String,
    addr: String,
    dbuser: String,
    password: SecStr,
    clientlocale: Option<String>,
    server_certs: Vec<ServerCerts>,
    #[cfg(feature = "alpha_nonblocking")]
    use_nonblocking: bool,
}
impl ConnectParams {
    pub(crate) fn new(
        host: String,
        addr: String,
        dbuser: String,
        password: SecStr,
        clientlocale: Option<String>,
        server_certs: Vec<ServerCerts>,
        #[cfg(feature = "alpha_nonblocking")] use_nonblocking: bool,
    ) -> Self {
        Self {
            host,
            addr,
            dbuser,
            password,
            clientlocale,
            server_certs,
            #[cfg(feature = "alpha_nonblocking")]
            use_nonblocking,
        }
    }

    /// Returns a new builder for `ConnectParams`.
    pub fn builder() -> ConnectParamsBuilder {
        ConnectParamsBuilder::new()
    }

    /// Reads a url from the given file and converts it into `ConnectParams`.
    ///
    /// # Errors
    /// `HdbError::ConnParams`
    pub fn from_file<P: AsRef<Path>>(path: P) -> HdbResult<Self> {
        std::fs::read_to_string(path)
            .map_err(|e| HdbError::ConnParams {
                source: Box::new(e),
            })?
            .into_connect_params()
    }

    /// The `ServerCerts`.
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
        !self.server_certs.is_empty()
    }

    #[cfg(feature = "alpha_nonblocking")]
    pub(crate) fn use_nonblocking(&self) -> bool {
        self.use_nonblocking
    }

    /// The database user.
    pub fn dbuser(&self) -> &str {
        self.dbuser.as_str()
    }

    /// The password.
    pub fn password(&self) -> &SecStr {
        &self.password
    }

    /// The client locale.
    pub fn clientlocale(&self) -> Option<&str> {
        self.clientlocale.as_deref()
    }

    pub(crate) fn rustls_clientconfig(&self) -> std::io::Result<ClientConfig> {
        let mut config = ClientConfig::new();
        for server_cert in self.server_certs() {
            match server_cert {
                ServerCerts::None => {
                    config
                        .dangerous()
                        .set_certificate_verifier(Arc::new(NoCertificateVerification {}));
                }
                ServerCerts::RootCertificates => {
                    config
                        .root_store
                        .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
                }
                ServerCerts::Direct(pem) => {
                    let mut cursor = std::io::Cursor::new(pem);
                    let (n_ok, n_err) = config
                        .root_store
                        .add_pem_file(&mut cursor)
                        .unwrap_or((0, 0));
                    if n_ok == 0 {
                        info!("None of the directly provided server certificates was accepted");
                    } else if n_err > 0 {
                        info!("Not all directly provided server certificates were accepted");
                    }
                }
                ServerCerts::Environment(env_var) => match std::env::var(env_var) {
                    Ok(value) => {
                        let mut cursor = std::io::Cursor::new(value);
                        let (n_ok, n_err) = config
                            .root_store
                            .add_pem_file(&mut cursor)
                            .unwrap_or((0, 0));
                        if n_ok == 0 {
                            info!("None of the env-provided server certificates was accepted");
                        } else if n_err > 0 {
                            info!("Not all env-provided server certificates were accepted");
                        }
                    }
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("Environment variable {} not found, reason: {}", env_var, e),
                        ));
                    }
                },
                ServerCerts::Directory(trust_anchor_dir) => {
                    #[allow(clippy::filter_map)]
                    let trust_anchor_files: Vec<PathBuf> = std::fs::read_dir(trust_anchor_dir)?
                        .filter_map(Result::ok)
                        .filter(|dir_entry| {
                            dir_entry.file_type().is_ok()
                                && dir_entry.file_type().unwrap().is_file()
                        })
                        .filter(|dir_entry| {
                            let path = dir_entry.path();
                            let ext = path.extension();
                            Some(AsRef::<std::ffi::OsStr>::as_ref("pem")) == ext
                        })
                        .map(|dir_entry| dir_entry.path())
                        .collect();
                    let mut t_ok = 0;
                    let mut t_err = 0;
                    for trust_anchor_file in trust_anchor_files {
                        trace!("Trying trust anchor file {:?}", trust_anchor_file);
                        let mut rd =
                            std::io::BufReader::new(std::fs::File::open(trust_anchor_file)?);
                        let (n_ok, n_err) =
                            config.root_store.add_pem_file(&mut rd).map_err(|_| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "server certificates in directory could not be parsed",
                                )
                            })?;
                        t_ok += n_ok;
                        t_err += n_err;
                    }
                    if t_ok == 0 {
                        warn!("None of the server certificates in the directory was accepted");
                    } else if t_err > 0 {
                        warn!("Not all server certificates in the directory were accepted");
                    }
                }
            }
        }
        Ok(config)
    }

    fn option_string(&self) -> String {
        if self.server_certs.is_empty() && self.clientlocale.is_none() {
            String::from("")
        } else {
            let mut s = String::with_capacity(200);

            let it = self.server_certs.iter().map(ServerCerts::to_string);
            let it = it.chain(
                self.clientlocale
                    .iter()
                    .map(|cl| format!("{}={}", cp_url::OPTION_CLIENT_LOCALE, cl)),
            );
            #[cfg(feature = "alpha_nonblocking")]
            let it = it.chain(
                {
                    if self.use_nonblocking {
                        Some(cp_url::OPTION_NONBLOCKING.to_string())
                    } else {
                        None
                    }
                }
                .into_iter(),
            );

            for (i, assignment) in it.enumerate() {
                if i > 0 {
                    s.push('&');
                }
                s.push_str(&assignment);
            }
            s
        }
    }
}

impl std::fmt::Display for ConnectParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let option_string = self.option_string();
        write!(
            f,
            "hdbsql{}://{}@{}{}{}",
            if self.use_tls() { "s" } else { "" },
            self.dbuser,
            self.addr,
            if option_string.is_empty() { "" } else { "?" },
            option_string
        )
    }
}

/// Expresses where Certificates for TLS are read from.
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
    /// Defines that the server's identity is not validated. Don't use this
    /// option in productive setups!
    None,
}
impl std::fmt::Display for ServerCerts {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Directory(s) => write!(f, "{}={}", cp_url::OPTION_CERT_DIR, s),
            Self::Environment(s) => write!(f, "{}={}", cp_url::OPTION_CERT_ENV, s),
            Self::Direct(_s) => write!(f, "{}=<...>", cp_url::OPTION_CERT_DIRECT),
            Self::RootCertificates => write!(f, "{}", cp_url::OPTION_CERT_MOZILLA),
            Self::None => write!(f, "{}", cp_url::OPTION_INSECURE_NO_CHECK),
        }
    }
}

struct NoCertificateVerification {}
impl rustls::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _roots: &rustls::RootCertStore,
        _presented_certs: &[rustls::Certificate],
        _dns_name: webpki::DNSNameRef<'_>,
        _ocsp: &[u8],
    ) -> Result<rustls::ServerCertVerified, rustls::TLSError> {
        Ok(rustls::ServerCertVerified::assertion())
    }
}

#[cfg(test)]
mod tests {
    use super::IntoConnectParams;
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
            assert!(params.server_certs().is_empty());
        }
        {
            let params = "hdbsqls://meier:schLau@abcd123:2222\
                          ?client_locale=CL1\
                          &tls_certificate_dir=TCD\
                          &use_mozillas_root_certificates"
                .into_connect_params()
                .unwrap();

            assert_eq!("meier", params.dbuser());
            assert_eq!(b"schLau", params.password().unsecure());
            assert_eq!(Some("CL1".to_string()), params.clientlocale);
            assert_eq!(
                ServerCerts::Directory("TCD".to_string()),
                *params.server_certs().get(0).unwrap()
            );
            assert_eq!(
                ServerCerts::RootCertificates,
                *params.server_certs().get(1).unwrap()
            );
            assert_eq!(
                params.to_string(),
                "hdbsqls://meier@abcd123:2222\
                ?tls_certificate_dir=TCD\
                &use_mozillas_root_certificates&client_locale=CL1"
                    .to_owned() // no password
            )
        }
        {
            let params = "hdbsqls://meier:schLau@abcd123:2222\
                          ?insecure_omit_server_certificate_check"
                .into_connect_params()
                .unwrap();

            assert_eq!("meier", params.dbuser());
            assert_eq!(b"schLau", params.password().unsecure());
            assert_eq!(ServerCerts::None, *params.server_certs().get(0).unwrap());
            assert_eq!(
                params.to_string(),
                "hdbsqls://meier@abcd123:2222?insecure_omit_server_certificate_check".to_owned() // no password
            )
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
