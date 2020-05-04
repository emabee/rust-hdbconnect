use super::{PlainTcpClient, TlsTcpClient};
use crate::ConnectParams;
use chrono::Local;

// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub(crate) enum TcpClient {
    // A buffered tcp connection without TLS.
    SyncPlain(PlainTcpClient),
    // A buffered blocking tcp connection with TLS.
    SyncTls(TlsTcpClient),
    // A buffered non-blocking tcp connection with TLS.
}
impl TcpClient {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connect parameters.
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let start = Local::now();
        trace!("TcpClient: Connecting to {:?})", params.addr());

        let tcp_conn = if params.use_tls() {
            Self::SyncTls(TlsTcpClient::try_new(params)?)
        } else {
            Self::SyncPlain(PlainTcpClient::try_new(params)?)
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
            Self::SyncTls(_) => "TLS",
        }
    }
}
impl Drop for TcpClient {
    fn drop(&mut self) {
        trace!("Drop of TcpClient")
    }
}
