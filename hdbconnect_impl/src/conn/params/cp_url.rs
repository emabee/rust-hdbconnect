use super::Compression;
use crate::{
    url::{self, HDBSQL, HDBSQLS},
    ServerCerts, Tls,
};

#[allow(clippy::too_many_arguments)]
pub(crate) fn format_as_url(
    addr: &str,
    dbuser: &str,
    database: &Option<String>,
    network_group: &Option<String>,
    tls: &Tls,
    clientlocale: &Option<String>,
    compression: Compression,
    f: &mut std::fmt::Formatter,
) -> std::fmt::Result {
    write!(
        f,
        "{}://{}@{}",
        match tls {
            Tls::Off => HDBSQL,
            Tls::Insecure | Tls::Secure(_) => HDBSQLS,
        },
        dbuser,
        addr,
    )?;

    if database.is_none()
        && network_group.is_none()
        && matches!(tls, Tls::Off)
        && clientlocale.is_none()
    {
        return Ok(());
    }

    // write URL options
    let mut sep = std::iter::repeat(())
        .enumerate()
        .map(|(i, ())| if i == 0 { "?" } else { "&" });

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
                UrlOpt::InsecureOmitServerCheck
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
                            UrlOpt::TlsCertificateDir
                        )?;
                    }
                    ServerCerts::Environment(s) => {
                        write!(
                            f,
                            "{}{}={s}",
                            sep.next().unwrap(),
                            UrlOpt::TlsCertificateEnv
                        )?;
                    }
                    ServerCerts::RootCertificates => {
                        write!(
                            f,
                            "{}{}",
                            sep.next().unwrap(),
                            UrlOpt::TlsCertificateMozilla
                        )?;
                    }
                    ServerCerts::Direct(_s) => {
                        panic!("NOT SUPPORTED IN URLs");
                    }
                }
            }
        }
    }

    if let Some(cl) = clientlocale {
        write!(f, "{}{}={cl}", sep.next().unwrap(), UrlOpt::ClientLocale)?;
    }

    match compression {
        Compression::Always => {}
        // Compression::Remote => {}
        Compression::Off => {
            write!(f, "{}{}", sep.next().unwrap(), UrlOpt::NoCompression)?;
        }
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
    Database,
    NetworkGroup,
    NoCompression,
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
            url::NO_COMPRESSION => Some(UrlOpt::NoCompression),
            _ => None,
        }
    }
}
impl std::fmt::Display for UrlOpt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                UrlOpt::TlsCertificateDir => url::TLS_CERTIFICATE_DIR,
                UrlOpt::TlsCertificateEnv => url::TLS_CERTIFICATE_ENV,
                UrlOpt::TlsCertificateMozilla => url::USE_MOZILLAS_ROOT_CERTIFICATES,
                UrlOpt::InsecureOmitServerCheck => url::INSECURE_OMIT_SERVER_CERTIFICATE_CHECK,
                UrlOpt::ClientLocale => url::CLIENT_LOCALE,
                UrlOpt::ClientLocaleFromEnv => url::CLIENT_LOCALE_FROM_ENV,
                UrlOpt::Database => url::DATABASE,
                UrlOpt::NetworkGroup => url::NETWORK_GROUP,
                UrlOpt::NoCompression => url::NO_COMPRESSION,
            }
        )
    }
}
