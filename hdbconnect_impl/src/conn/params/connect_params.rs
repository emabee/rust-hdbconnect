//! Connection parameters
use super::cp_url::format_as_url;
use crate::{protocol::util, ConnectParamsBuilder, HdbError, HdbResult, IntoConnectParams};
use rustls::{
    client::{ServerCertVerified, ServerCertVerifier, ServerName},
    Certificate,
};
use secstr::SecUtf8;
use serde::de::Deserialize;
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_rustls::rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectParams {
    host: String,
    addr: String,
    dbuser: String,
    dbname: Option<String>,
    network_group: Option<String>,
    password: SecUtf8,
    clientlocale: Option<String>,
    tls: Tls,
    #[cfg(feature = "alpha_nonblocking")]
    use_nonblocking: bool,
}

/// Describes whether and how TLS is to be used.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub enum Tls {
    /// Plain TCP connection
    #[default]
    Off,
    /// TLS without server validation - dangerous!
    Insecure,
    /// TLS with server validation
    Secure(Vec<ServerCerts>),
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
        tls: Tls,
        #[cfg(feature = "alpha_nonblocking")] use_nonblocking: bool,
    ) -> Self {
        Self {
            addr: format!("{host}:{port}"),
            host,
            dbuser,
            password,
            clientlocale,
            tls,
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
        new_params.addr = format!("{host}:{port}");
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
    pub fn server_certs(&self) -> Option<&Vec<ServerCerts>> {
        match self.tls {
            Tls::Secure(ref certs) => Some(certs),
            Tls::Insecure | Tls::Off => None,
        }
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
    pub fn is_tls(&self) -> bool {
        !matches!(self.tls, Tls::Off)
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
    pub fn dbname(&self) -> Option<&str> {
        self.dbname.as_deref()
    }

    /// The name of a network group.
    pub fn network_group(&self) -> Option<&str> {
        self.network_group.as_deref()
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn rustls_clientconfig(&self) -> std::io::Result<ClientConfig> {
        match self.tls {
            Tls::Off => Err(util::io_error(
                "rustls_clientconfig called with Tls::Off - \
                    this should have been prevented earlier",
            )),
            Tls::Secure(ref server_certs) => {
                let mut root_store = RootCertStore::empty();
                for server_cert in server_certs {
                    match server_cert {
                        ServerCerts::RootCertificates => {
                            root_store.add_server_trust_anchors(
                                webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
                                    OwnedTrustAnchor::from_subject_spki_name_constraints(
                                        ta.subject,
                                        ta.spki,
                                        ta.name_constraints,
                                    )
                                }),
                            );
                        }
                        ServerCerts::Direct(ref pem) => {
                            let (n_ok, n_err) =
                                root_store.add_parsable_certificates(&[pem.clone().into_bytes()]);
                            if n_ok == 0 {
                                info!("None of the directly provided server certificates was accepted");
                            } else if n_err > 0 {
                                info!(
                                    "Not all directly provided server certificates were accepted"
                                );
                            }
                        }
                        ServerCerts::Environment(env_var) => {
                            match std::env::var(env_var) {
                                Ok(value) => {
                                    let (n_ok, n_err) =
                                        root_store.add_parsable_certificates(&[value.into_bytes()]);
                                    if n_ok == 0 {
                                        info!("None of the env-provided server certificates was accepted");
                                    } else if n_err > 0 {
                                        info!("Not all env-provided server certificates were accepted");
                                    }
                                }
                                Err(e) => {
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::InvalidInput,
                                        format!(
                                            "Environment variable {env_var} not found, reason: {e}"
                                        ),
                                    ));
                                }
                            }
                        }
                        ServerCerts::Directory(trust_anchor_dir) => {
                            let trust_anchor_files: Vec<PathBuf> =
                                std::fs::read_dir(trust_anchor_dir)?
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
                                let mut buf = Vec::<u8>::new();
                                std::fs::File::open(trust_anchor_file)?.read_to_end(&mut buf)?;
                                #[allow(clippy::map_err_ignore)]
                                let (n_ok, n_err) = root_store.add_parsable_certificates(&[buf]);
                                t_ok += n_ok;
                                t_err += n_err;
                            }
                            if t_ok == 0 {
                                warn!(
                                    "None of the server certificates in the directory was accepted"
                                );
                            } else if t_err > 0 {
                                warn!("Not all server certificates in the directory were accepted");
                            }
                        }
                    }
                }
                let config = ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();
                Ok(config)
            }
            Tls::Insecure => {
                let config = rustls::client::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_custom_certificate_verifier(Arc::new(NoCertificateVerification {}))
                    .with_no_client_auth();
                Ok(config)
            }
        }
    }
}

impl std::fmt::Display for ConnectParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        format_as_url(
            &self.addr,
            &self.dbuser,
            &self.dbname,
            &self.network_group,
            &self.tls,
            &self.clientlocale,
            f,
        )
    }
}

/// Expresses where Certificates for TLS are read from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
}

struct NoCertificateVerification {}
impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
}

#[allow(clippy::missing_errors_doc)]
impl<'de> Deserialize<'de> for ConnectParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct DeserializationHelper {
            host: String,
            port: u16,
            dbuser: String,
            dbname: Option<String>,
            network_group: Option<String>,
            password: String,
            clientlocale: Option<String>,
            tls: Tls,
            #[cfg(feature = "alpha_nonblocking")]
            use_nonblocking: bool,
        }
        let helper: DeserializationHelper = DeserializationHelper::deserialize(deserializer)?;
        Ok(ConnectParams::new(
            helper.host,
            helper.port,
            helper.dbuser,
            SecUtf8::from(helper.password),
            helper.dbname,
            helper.network_group,
            helper.clientlocale,
            helper.tls,
        ))
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Default implementation just delegates to `deserialize` impl.
        *place = Deserialize::deserialize(deserializer)?;
        Ok(())
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
            assert_eq!("schLau", params.password().unsecure());
            assert_eq!("abcd123:2222", params.addr());
            assert_eq!(None, params.clientlocale);
            assert!(params.server_certs().is_none());
            assert!(!params.is_tls());
        }
        {
            let params = "hdbsql://meier:schLau@abcd123:2222?db=JOE"
                .into_connect_params()
                .unwrap();

            assert_eq!("meier", params.dbuser());
            assert_eq!("schLau", params.password().unsecure());
            assert_eq!("abcd123:2222", params.addr());
            assert_eq!(None, params.clientlocale);
            assert!(params.server_certs().is_none());
            assert!(!params.is_tls());
            assert_eq!(Some("JOE"), params.dbname());

            let redirect_params = params.redirect("xyz9999", 11);
            assert_eq!("meier", redirect_params.dbuser());
            assert_eq!("schLau", redirect_params.password().unsecure());
            assert_eq!("xyz9999:11", redirect_params.addr());
            assert_eq!(None, redirect_params.clientlocale);
            assert!(redirect_params.server_certs().is_none());
            assert!(!redirect_params.is_tls());
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
                *params.server_certs().unwrap().get(0).unwrap()
            );
            assert_eq!(
                ServerCerts::RootCertificates,
                *params.server_certs().unwrap().get(1).unwrap()
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
            assert!(params.server_certs().is_none());
            assert!(params.is_tls());
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
