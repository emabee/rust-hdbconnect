use crate::ServerCerts;

/// Describes whether and how TLS is to be used.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub(crate) enum Tls {
    /// Plain TCP connection
    #[default]
    Off,
    /// TLS without server validation - dangerous!
    Insecure,
    /// TLS with server validation
    Secure(Vec<ServerCerts>),
}
