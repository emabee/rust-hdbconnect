mod a_sync;
mod sync;

pub(crate) use sync::plain_connection::PlainConnection;
#[cfg(feature = "alpha_nonblocking")]
pub(crate) use sync::rustls_connection::TlsClient;
pub(crate) use sync::tls_connection::TlsConnection;
