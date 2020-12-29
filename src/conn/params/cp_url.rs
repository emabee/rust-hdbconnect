use crate::ServerCerts;

/// Constants for use in connection URLs.
///
/// Database connections are configured with an instance of `ConnectParams`.
/// Instances of `ConnectParams` can be created using a `ConnectParamsBuilder`.
///
/// Both `ConnectParams` and `ConnectParamsBuilder` can be created from a URL.
///  
/// Such a URL is supposed to have the form
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
/// > `<options>` = `?<key>[=<value>][{&<key>[=<value>]}]`  
///
/// Supported options are:
/// > `db=<databasename>` specifies the (MDC) database to which you want to connect  
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
/// This module contains String constants for the `hdbconnect`-specific URL constituents.
///
/// ### Example
///
/// ```rust
/// use hdbconnect::IntoConnectParams;
/// let conn_params = "hdbsql://my_user:my_passwd@the_host:2222"
///     .into_connect_params()
///     .unwrap();
/// ```
pub mod url {
    /// Denotes a folder in which server certificates can be found.
    pub const TLS_CERTIFICATE_DIR: &str = "tls_certificate_dir";
    /// Denotes an environment variable in which a server certificate can be found
    pub const TLS_CERTIFICATE_ENV: &str = "tls_certificate_env";
    /// Defines that the server roots from <https://mkcert.org/> should be added to the
    /// trust store for TLS.
    pub const USE_MOZILLAS_ROOT_CERTIFICATES: &str = "use_mozillas_root_certificates";
    /// Defines that the server's identity is not validated. Don't use this
    /// option in productive setups!
    pub const INSECURE_OMIT_SERVER_CERTIFICATE_CHECK: &str =
        "insecure_omit_server_certificate_check";
    /// Denotes the client locale.
    pub const CLIENT_LOCALE: &str = "client_locale";
    /// Denotes an environment variable which contains the client locale.
    pub const CLIENT_LOCALE_FROM_ENV: &str = "client_locale_from_env";
    /// Denotes the (MDC) database to which you want to connect.
    pub const DATABASE: &str = "db";
    /// Denotes a network group.
    pub const NETWORK_GROUP: &str = "network_group";
    /// Use nonblocking.
    #[cfg(feature = "alpha_nonblocking")]
    pub const NONBLOCKING: &str = "nonblocking";
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn format_as_url(
    use_tls: bool,
    addr: &str,
    dbuser: &str,
    database: &Option<String>,
    network_group: &Option<String>,
    server_certs: &[ServerCerts],
    client_locale: &Option<String>,
    f: &mut std::fmt::Formatter,
) -> std::fmt::Result {
    let it = database.iter().map(|db| format!("db={}", db));
    let it = it.chain(
        network_group
            .iter()
            .map(|network_group| format!("network_group={}", network_group)),
    );
    let it = it.chain(server_certs.iter().map(format_server_certs));
    let it = it.chain(
        client_locale
            .iter()
            .map(|cl| format!("{}={}", UrlOpt::ClientLocale.name(), cl)),
    );
    #[cfg(feature = "alpha_nonblocking")]
    let it = it.chain(
        {
            if self.use_nonblocking {
                Some(UrlOpt::NonBlocking.name().to_string())
            } else {
                None
            }
        }
        .into_iter(),
    );

    let mut option_string = String::with_capacity(200);
    for (i, assignment) in it.enumerate() {
        if i > 0 {
            option_string.push('&');
        }
        option_string.push_str(&assignment);
    }

    write!(
        f,
        "hdbsql{}://{}@{}{}{}",
        if use_tls { "s" } else { "" },
        dbuser,
        addr,
        if option_string.is_empty() { "" } else { "?" },
        option_string
    )
}

fn format_server_certs(sc: &ServerCerts) -> String {
    match sc {
        ServerCerts::Directory(s) => format!("{}={}", UrlOpt::TlsCertificateDir.name(), s),
        ServerCerts::Environment(s) => format!("{}={}", UrlOpt::TlsCertificateEnv.name(), s),
        ServerCerts::RootCertificates => UrlOpt::TlsCertificateMozilla.name().to_string(),
        ServerCerts::None => UrlOpt::InsecureOmitServerCheck.name().to_string(),
        ServerCerts::Direct(_s) => "NOT SUPPORTED IN URLs".to_string(),
    }
}

pub(crate) enum UrlOpt {
    TlsCertificateDir,
    TlsCertificateEnv,
    // TlsCertificateDirect,
    TlsCertificateMozilla,
    InsecureOmitServerCheck,
    ClientLocale,
    ClientLocaleFromEnv,
    #[cfg(feature = "alpha_nonblocking")]
    NonBlocking,
    Database,
    NetworkGroup,
}
impl UrlOpt {
    pub fn from(s: &str) -> Option<Self> {
        match s {
            url::TLS_CERTIFICATE_DIR => Some(UrlOpt::TlsCertificateDir),
            url::TLS_CERTIFICATE_ENV => Some(UrlOpt::TlsCertificateEnv),
            url::USE_MOZILLAS_ROOT_CERTIFICATES => Some(UrlOpt::TlsCertificateMozilla),
            url::INSECURE_OMIT_SERVER_CERTIFICATE_CHECK => Some(UrlOpt::InsecureOmitServerCheck),
            url::CLIENT_LOCALE => Some(UrlOpt::ClientLocale),
            url::CLIENT_LOCALE_FROM_ENV => Some(UrlOpt::ClientLocaleFromEnv),
            url::DATABASE => Some(UrlOpt::Database),
            url::NETWORK_GROUP => Some(UrlOpt::NetworkGroup),
            #[cfg(feature = "alpha_nonblocking")]
            url::NONBLOCKING => Some(UrlOpt::NonBlocking),
            _ => None,
        }
    }
    pub fn name(&self) -> &'static str {
        match self {
            UrlOpt::TlsCertificateDir => url::TLS_CERTIFICATE_DIR,
            UrlOpt::TlsCertificateEnv => url::TLS_CERTIFICATE_ENV,
            UrlOpt::TlsCertificateMozilla => url::USE_MOZILLAS_ROOT_CERTIFICATES,
            UrlOpt::InsecureOmitServerCheck => url::INSECURE_OMIT_SERVER_CERTIFICATE_CHECK,
            UrlOpt::ClientLocale => url::CLIENT_LOCALE,
            UrlOpt::ClientLocaleFromEnv => url::CLIENT_LOCALE_FROM_ENV,
            UrlOpt::Database => url::DATABASE,
            UrlOpt::NetworkGroup => url::NETWORK_GROUP,
            #[cfg(feature = "alpha_nonblocking")]
            UrlOpt::NonBlocking => url::NONBLOCKING,
        }
    }
}
