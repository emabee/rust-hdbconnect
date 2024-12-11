//! Connection parameters
use super::{cp_url::format_as_url, tls::Tls, Compression};
use crate::{ConnectParamsBuilder, HdbError, HdbResult, IntoConnectParams};
use rustls::{ClientConfig, RootCertStore};
use secstr::SecUtf8;
use serde::de::Deserialize;
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

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
    compression: Compression,
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
        compression: Compression,
        tls: Tls,
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
            compression,
        }
    }

    /// Returns a new builder for `ConnectParams`.
    #[must_use]
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
    #[must_use]
    pub fn server_certs(&self) -> Option<&Vec<ServerCerts>> {
        match self.tls {
            Tls::Secure(ref certs) => Some(certs),
            Tls::Insecure | Tls::Off => None,
        }
    }

    /// The host.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The socket address.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Whether TLS or a plain TCP connection is to be used.
    #[must_use]
    pub fn is_tls(&self) -> bool {
        !matches!(self.tls, Tls::Off)
    }

    /// The database user.
    #[must_use]
    pub fn dbuser(&self) -> &str {
        self.dbuser.as_str()
    }

    /// The password.
    #[must_use]
    pub fn password(&self) -> &SecUtf8 {
        &self.password
    }

    /// The client locale.
    #[must_use]
    pub fn clientlocale(&self) -> Option<&str> {
        self.clientlocale.as_deref()
    }

    #[must_use]
    pub(crate) fn compression(&self) -> Compression {
        self.compression
    }

    /// The name of the (MDC) database.
    #[must_use]
    pub fn dbname(&self) -> Option<&str> {
        self.dbname.as_deref()
    }

    /// The name of a network group.
    #[must_use]
    pub fn network_group(&self) -> Option<&str> {
        self.network_group.as_deref()
    }

    /// Provide detailed insight into acceptance of configured certificates
    ///
    /// # Errors
    ///
    /// Various errors with certificate handling can occur.
    pub fn precheck_certificates(&self) -> HdbResult<Vec<String>> {
        if matches!(self.tls, Tls::Off | Tls::Insecure) {
            Ok(Vec::new())
        } else {
            self.rustls_clientconfig().map(|tuple| tuple.1)
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn rustls_clientconfig(&self) -> HdbResult<(ClientConfig, Vec<String>)> {
        match self.tls {
            Tls::Off => Err(HdbError::Impl(
                "rustls_clientconfig called with Tls::Off - \
                    this should have been prevented earlier",
            )),
            Tls::Insecure => {
                let config = rustls::client::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(
                        insecure::NoCertificateVerification::new(),
                    ))
                    .with_no_client_auth();
                Ok((config, Vec::new()))
            }
            Tls::Secure(ref server_certs) => {
                let mut root_store = RootCertStore::empty();
                let cert_errors = std::cell::RefCell::new(Vec::<String>::new());

                for server_cert in server_certs {
                    match server_cert {
                        ServerCerts::RootCertificates => {
                            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                        }
                        ServerCerts::Direct(ref cert_string) => {
                            let (n_ok, n_err) = root_store.add_parsable_certificates([cert_string
                                .clone()
                                .into_bytes()
                                .into()]);
                            if n_ok == 0 {
                                cert_errors.borrow_mut().push(
                                    "None of the directly provided certificates was accepted"
                                        .to_string(),
                                );
                            } else if n_err > 0 {
                                cert_errors.borrow_mut().push(
                                    "Not all directly provided certificates were accepted"
                                        .to_string(),
                                );
                            }
                        }
                        ServerCerts::Environment(env_var) => match std::env::var(env_var) {
                            Ok(value) => {
                                let (n_ok, n_err) = root_store
                                    .add_parsable_certificates([value.into_bytes().into()]);
                                if n_ok == 0 {
                                    cert_errors.borrow_mut().push(
                                        "None of the env-provided certificates was accepted"
                                            .to_string(),
                                    );
                                } else if n_err > 0 {
                                    cert_errors.borrow_mut().push(
                                        "Not all env-provided certificates were accepted"
                                            .to_string(),
                                    );
                                }
                            }
                            Err(e) => {
                                return Err(HdbError::ImplDetailed(format!(
                                    "Environment variable {env_var} not found, reason: {e}"
                                )));
                            }
                        },
                        ServerCerts::Directory(trust_anchor_dir) => {
                            evaluate_certificate_directory(
                                trust_anchor_dir,
                                &mut root_store,
                                &cert_errors,
                            )?;
                        }
                    }
                }
                if root_store.is_empty() {
                    Err(HdbError::ImplDetailed(
                        cert_errors
                            .into_inner()
                            .iter()
                            .fold(String::new(), |mut acc, x| {
                                acc.push_str(x);
                                acc.push('\n');
                                acc
                            }),
                    ))
                } else {
                    let config = ClientConfig::builder()
                    .with_root_certificates(root_store)
                    // .with_safe_default_protocol_versions()
                        .with_no_client_auth();
                    Ok((config, cert_errors.into_inner()))
                }
            }
        }
    }
}

fn evaluate_certificate_directory(
    trust_anchor_dir: &String,
    root_store: &mut RootCertStore,
    cert_errors: &std::cell::RefCell<Vec<String>>,
) -> Result<(), HdbError> {
    let trust_anchor_files: Vec<PathBuf> = std::fs::read_dir(trust_anchor_dir)?
        .map(|r| {
            r.map_err(|e| {
                cert_errors
                    .borrow_mut()
                    .push(format!("Error in parsing the directory: {e}\n"));
                e
            })
        })
        .filter_map(Result::ok)
        .filter(|dir_entry| match dir_entry.file_type() {
            Err(e) => {
                cert_errors
                    .borrow_mut()
                    .push(format!("Error with dir entry: {e}\n"));
                false
            }
            Ok(file_type) => file_type.is_file(),
        })
        .filter(|dir_entry| {
            let path = dir_entry.path();
            let o_ext = path.extension().and_then(|ext| ext.to_str());
            let accept = o_ext.is_some() && ["cer", "crt", "pem"].binary_search(&o_ext.unwrap()).is_ok();
            if !accept {
                cert_errors
                    .borrow_mut()
                    .push(format!("{path:?} has wrong file suffix; only files with suffix 'cer', 'crt', or 'pem' are considered\n"));
            }
            accept
        })
        .map(|dir_entry| dir_entry.path())
        .collect();

    for trust_anchor_file in trust_anchor_files {
        let mut buf = Vec::<u8>::new();
        std::fs::File::open(trust_anchor_file.clone())?.read_to_end(&mut buf)?;
        let (n_ok, n_err) = root_store.add_parsable_certificates([(*buf).into()]);
        if n_err > 0 {
            if n_ok > 0 {
                cert_errors
                .borrow_mut()
                .push(format!("{trust_anchor_file:?} is not completely accepted: ({n_ok} parts good, {n_err} parts bad)\n"));
            } else {
                cert_errors
                    .borrow_mut()
                    .push(format!("{trust_anchor_file:?} is not accepted\n"));
            }
        }
    }
    Ok(())
}

impl std::fmt::Display for ConnectParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            format_as_url(
                &self.addr,
                &self.dbuser,
                self.dbname.as_deref(),
                self.network_group.as_deref(),
                &self.tls,
                self.clientlocale.as_deref(),
                self.compression,
            )
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
            compression: Compression,
            tls: Tls,
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
            helper.compression,
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

mod insecure {
    use rustls::{
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
        pki_types::{CertificateDer, ServerName, UnixTime},
        DigitallySignedStruct,
    };

    #[derive(Debug)]
    pub struct NoCertificateVerification(CryptoProvider);

    impl NoCertificateVerification {
        pub fn new() -> Self {
            Self(rustls::crypto::aws_lc_rs::default_provider())
        }
    }

    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
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
                *params.server_certs().unwrap().first().unwrap()
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
