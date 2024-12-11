use std::io::{Cursor, Write};

use super::{tls::Tls, Compression};
use crate::{
    url::{self, HDBSQL, HDBSQLS},
    ServerCerts,
};

#[allow(clippy::too_many_arguments)]
pub(crate) fn format_as_url(
    addr: &str,
    dbuser: &str,
    database: Option<&str>,
    network_group: Option<&str>,
    tls: &Tls,
    clientlocale: Option<&str>,
    compression: Compression,
) -> String {
    let mut buf = Cursor::new(Vec::<u8>::with_capacity(200));
    write!(
        &mut buf,
        "{}://{}@{}",
        match tls {
            Tls::Off => HDBSQL,
            Tls::Insecure | Tls::Secure(_) => HDBSQLS,
        },
        dbuser,
        addr,
    )
    .ok();

    if database.is_none()
        && network_group.is_none()
        && matches!(tls, Tls::Off)
        && clientlocale.is_none()
    {
    } else {
        // write URL options
        let mut sep = std::iter::repeat(())
            .enumerate()
            .map(|(i, ())| if i == 0 { "?" } else { "&" });

        if let Some(db) = database {
            write!(&mut buf, "{}db={db}", sep.next().unwrap()).ok();
        }

        if let Some(ng) = network_group {
            write!(&mut buf, "{}network_group={ng}", sep.next().unwrap()).ok();
        }

        match tls {
            Tls::Off => {}
            Tls::Insecure => {
                write!(
                    &mut buf,
                    "{}{}",
                    sep.next().unwrap(),
                    UrlOpt::InsecureOmitServerCheck
                )
                .ok();
            }
            Tls::Secure(server_certs) => {
                for sc in server_certs {
                    match sc {
                        ServerCerts::Directory(s) => {
                            write!(
                                &mut buf,
                                "{}{}={s}",
                                sep.next().unwrap(),
                                UrlOpt::TlsCertificateDir
                            )
                            .ok();
                        }
                        ServerCerts::Environment(s) => {
                            write!(
                                &mut buf,
                                "{}{}={s}",
                                sep.next().unwrap(),
                                UrlOpt::TlsCertificateEnv
                            )
                            .ok();
                        }
                        ServerCerts::RootCertificates => {
                            write!(
                                &mut buf,
                                "{}{}",
                                sep.next().unwrap(),
                                UrlOpt::TlsCertificateMozilla
                            )
                            .ok();
                        }
                        ServerCerts::Direct(_s) => {
                            panic!("NOT SUPPORTED IN URLs");
                        }
                    }
                }
            }
        }

        if let Some(cl) = clientlocale {
            write!(
                &mut buf,
                "{}{}={cl}",
                sep.next().unwrap(),
                UrlOpt::ClientLocale
            )
            .ok();
        }

        match compression {
            Compression::Always => {}
            // Compression::Remote => {}
            Compression::Off => {
                write!(&mut buf, "{}{}", sep.next().unwrap(), UrlOpt::NoCompression).ok();
            }
        }
    }

    String::from_utf8_lossy(&buf.into_inner()).to_string()
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
