#[cfg(feature = "alpha_nonblocking")]
use super::tcp::TlsClient;
use super::tcp::{PlainConnection, TlsConnection};
use super::ConnectParams;
use chrono::Local;

// A buffered tcp connection, with or without TLS.
#[allow(clippy::large_enum_variant)] // FIXME
#[derive(Debug)]
pub(crate) enum TcpConn {
    // A buffered tcp connection without TLS.
    SyncPlain(PlainConnection),
    // A buffered tcp connection with TLS.
    SyncSecure(TlsConnection),
    // A buffered tcp connection with TLS.
    #[cfg(feature = "alpha_nonblocking")]
    OtherSyncSecure(TlsClient),
}
impl TcpConn {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connect parameters.
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let start = Local::now();
        trace!("TcpConn: Connecting to {:?})", params.addr());

        let tcp_conn = if params.use_tls() {
            #[cfg(feature = "alpha_nonblocking")]
            {
                if params.use_nonblocking() {
                    Self::OtherSyncSecure(TlsClient::try_new(params)?)
                } else {
                    Self::SyncSecure(TlsConnection::try_new(params)?)
                }
            }
            #[cfg(not(feature = "alpha_nonblocking"))]
            Self::SyncSecure(TlsConnection::try_new(params)?)
        } else {
            Self::SyncPlain(PlainConnection::try_new(params)?)
        };

        trace!(
            "Connection of type {} is initialized ({} µs)",
            tcp_conn.s_type(),
            Local::now()
                .signed_duration_since(start)
                .num_microseconds()
                .unwrap_or(-1)
        );
        Ok(tcp_conn)
    }

    // Returns a descriptor of the chosen type
    pub fn s_type(&self) -> &'static str {
        match self {
            Self::SyncPlain(_) => "Plain TCP",
            Self::SyncSecure(_) => "TLS",
            #[cfg(feature = "alpha_nonblocking")]
            Self::OtherSyncSecure(_) => "Other TLS",
        }
    }
}
impl Drop for TcpConn {
    fn drop(&mut self) {
        trace!("Drop of TcpConn")
    }
}
