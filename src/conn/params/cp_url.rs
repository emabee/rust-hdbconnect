use crate::{ServerCerts, Tls};

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
    addr: &str,
    dbuser: &str,
    database: &Option<String>,
    network_group: &Option<String>,
    tls: &Tls,
    client_locale: &Option<String>,
    f: &mut std::fmt::Formatter,
) -> std::fmt::Result {
    write!(
        f,
        "hdbsql{}://{}@{}",
        match tls {
            Tls::Off => "",
            Tls::Insecure | Tls::Secure(_) => "s",
        },
        dbuser,
        addr,
    )?;

    if database.is_none()
        && network_group.is_none()
        && matches!(tls, Tls::Off)
        && client_locale.is_none()
    {
        return Ok(());
    }

    // write URL options
    let mut sep = std::iter::repeat(())
        .enumerate()
        .map(|(i, _)| if i == 0 { "?" } else { "&" });

    if let Some(db) = database {
        write!(f, "{}db={db}", sep.next().unwrap())?;
    }

    if let Some(ng) = network_group {
        write!(f, "{}network_group={ng}", sep.next().unwrap())?;
    }

    match tls {
        Tls::Off => {}
        Tls::Insecure => {
            write!(
                f,
                "{}{}",
                sep.next().unwrap(),
                UrlOpt::InsecureOmitServerCheck.name()
            )?;
        }
        Tls::Secure(server_certs) => {
            for sc in server_certs {
                match sc {
                    ServerCerts::Directory(s) => {
                        write!(
                            f,
                            "{}{}={s}",
                            sep.next().unwrap(),
                            UrlOpt::TlsCertificateDir.name()
                        )?;
                    }
                    ServerCerts::Environment(s) => {
                        write!(
                            f,
                            "{}{}={s}",
                            sep.next().unwrap(),
                            UrlOpt::TlsCertificateEnv.name()
                        )?;
                    }
                    ServerCerts::RootCertificates => {
                        write!(
                            f,
                            "{}{}",
                            sep.next().unwrap(),
                            UrlOpt::TlsCertificateMozilla.name()
                        )?;
                    }
                    ServerCerts::Direct(_s) => {
                        panic!("NOT SUPPORTED IN URLs");
                    }
                }
            }
        }
    }

    if let Some(cl) = client_locale {
        write!(
            f,
            "{}{}={cl}",
            sep.next().unwrap(),
            UrlOpt::ClientLocale.name()
        )?;
    }

    #[cfg(feature = "alpha_nonblocking")]
    if self.use_nonblocking {
        write!(f, "{}{}", sep.next().unwrap(), UrlOpt::NonBlocking.name())?;
    }

    Ok(())
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
