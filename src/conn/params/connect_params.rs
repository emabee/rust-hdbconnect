//! Connection parameters
use super::cp_url::format_as_url;
use crate::{ConnectParamsBuilder, HdbError, HdbResult, IntoConnectParams};
use rustls::ClientConfig;
use secstr::SecUtf8;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// An immutable struct with all information necessary to open a new connection
/// to a HANA database.
///
/// # Instantiating a `ConnectParams` using the `ConnectParamsBuilder`
///
/// See [`ConnectParamsBuilder`](crate::ConnectParamsBuilder) for details.
///
/// ```rust,no_run
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
/// # Instantiating a `ConnectParams` from a URL
///
/// See module [`url`](crate::url) for details about the supported URLs.
///
/// ```rust
/// use hdbconnect::IntoConnectParams;
/// let conn_params = "hdbsql://my_user:my_passwd@the_host:2222"
///     .into_connect_params()
///     .unwrap();
/// ```
///
/// # Redirects
///
/// `hdbconnect` supports redirects.
/// You can connect to an MDC tenant database by specifying the host and port of the
/// system database, and the name of the database to which you want to be connected
/// with url parameter "db" or with [`ConnectParamsBuilder::dbname`].
///
#[derive(Clone, Debug)]
pub struct ConnectParams {
    host: String,
    addr: String,
    dbuser: String,
    dbname: Option<String>,
    network_group: Option<String>,
    password: SecUtf8,
    clientlocale: Option<String>,
    server_certs: Vec<ServerCerts>,
    #[cfg(feature = "alpha_nonblocking")]
    use_nonblocking: bool,
}
impl ConnectParams {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        host: String,
        port: u16,
        dbuser: String,
        password: SecUtf8,
        dbname: Option<String>,
        network_group: Option<String>,
        clientlocale: Option<String>,
        server_certs: Vec<ServerCerts>,
        #[cfg(feature = "alpha_nonblocking")] use_nonblocking: bool,
    ) -> Self {
        Self {
            addr: format!("{}:{}", host, port),
            host,
            dbuser,
            password,
            clientlocale,
            server_certs,
            dbname,
            network_group,
            #[cfg(feature = "alpha_nonblocking")]
            use_nonblocking,
        }
    }

    /// Returns a new builder for `ConnectParams`.
    pub fn builder() -> ConnectParamsBuilder {
        ConnectParamsBuilder::new()
    }

    pub(crate) fn redirect(&self, host: &str, port: u16) -> ConnectParams {
        let mut new_params = self.clone();
        new_params.dbname = None;
        new_params.host = host.to_string();
        new_params.addr = format!("{}:{}", host, port);
        new_params
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

    /// The database user.
    pub fn dbuser(&self) -> &str {
        self.dbuser.as_str()
    }

    /// The password.
    pub fn password(&self) -> &SecUtf8 {
        &self.password
    }

    /// The client locale.
    pub fn clientlocale(&self) -> Option<&str> {
        self.clientlocale.as_deref()
    }

    /// The name of the (MDC) database.
    pub fn dbname(&self) -> Option<String> {
        self.dbname.as_ref().map(ToString::to_string)
    }

    /// The name of a network group.
    pub fn network_group(&self) -> Option<String> {
        self.network_group.as_ref().map(ToString::to_string)
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
                        #[allow(clippy::map_err_ignore)]
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
}

impl std::fmt::Display for ConnectParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        format_as_url(
            self.use_tls(),
            &self.addr,
            &self.dbuser,
            &self.dbname,
            &self.network_group,
            &self.server_certs,
            &self.clientlocale,
            f,
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
    /// Defines that the server roots from <https://mkcert.org/> should be added to the
    /// trust store for TLS.
    RootCertificates,
    /// Defines that the server's identity is not validated. Don't use this
    /// option in productive setups!
    None,
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

    // fn verify_tls12_signature(
    //     &self,
    //     _message: &[u8],
    //     _cert: &rustls::Certificate,
    //     _dss: &rustls::internal::msgs::handshake::DigitallySignedStruct,
    // ) -> Result<rustls::HandshakeSignatureValid, rustls::TLSError> {
    //     Ok(rustls::HandshakeSignatureValid::assertion())
    // }

    // fn verify_tls13_signature(
    //     &self,
    //     _message: &[u8],
    //     _cert: &rustls::Certificate,
    //     _dss: &rustls::internal::msgs::handshake::DigitallySignedStruct,
    // ) -> Result<rustls::HandshakeSignatureValid, rustls::TLSError> {
    //     Ok(rustls::HandshakeSignatureValid::assertion())
    // }
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
            assert_eq!("schLau", params.password().unsecure());
            assert_eq!("abcd123:2222", params.addr());
            assert_eq!(None, params.clientlocale);
            assert!(params.server_certs().is_empty());
        }
        {
            let params = "hdbsql://meier:schLau@abcd123:2222?db=JOE"
                .into_connect_params()
                .unwrap();

            assert_eq!("meier", params.dbuser());
            assert_eq!("schLau", params.password().unsecure());
            assert_eq!("abcd123:2222", params.addr());
            assert_eq!(None, params.clientlocale);
            assert!(params.server_certs().is_empty());
            assert_eq!(Some("JOE".to_string()), params.dbname());

            let redirect_params = params.redirect("xyz9999", 11);
            assert_eq!("meier", redirect_params.dbuser());
            assert_eq!("schLau", redirect_params.password().unsecure());
            assert_eq!("xyz9999:11", redirect_params.addr());
            assert_eq!(None, redirect_params.clientlocale);
            assert!(redirect_params.server_certs().is_empty());
            assert_eq!(None, redirect_params.dbname());
        }
        {
            let params = "hdbsqls://meier:schLau@abcd123:2222\
                          ?client_locale=CL1\
                          &tls_certificate_dir=TCD\
                          &use_mozillas_root_certificates"
                .into_connect_params()
                .unwrap();

            assert_eq!("meier", params.dbuser());
            assert_eq!("schLau", params.password().unsecure());
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
            );
        }
        {
            let params = "hdbsqls://meier:schLau@abcd123:2222\
                          ?insecure_omit_server_certificate_check"
                .into_connect_params()
                .unwrap();

            assert_eq!("meier", params.dbuser());
            assert_eq!("schLau", params.password().unsecure());
            assert_eq!(ServerCerts::None, *params.server_certs().get(0).unwrap());
            assert_eq!(
                params.to_string(),
                "hdbsqls://meier@abcd123:2222?insecure_omit_server_certificate_check".to_owned() // no password
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
