mod sync_plain_tcp_client;
mod sync_tls_tcp_client;

use crate::ConnectParams;
use std::time::Instant;
use sync_plain_tcp_client::SyncPlainTcpClient;
use sync_tls_tcp_client::SyncTlsTcpClient;

// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub(crate) enum SyncTcpClient {
    // A buffered blocking tcp connection without TLS.
    Plain(SyncPlainTcpClient),
    // A buffered blocking tcp connection with TLS.
    Tls(SyncTlsTcpClient),
}
impl SyncTcpClient {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connect parameters.
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let start = Instant::now();
        trace!("TcpClient: Connecting to {:?})", params.addr());

        let tcp_conn = if params.is_tls() {
            Self::Tls(SyncTlsTcpClient::try_new(params)?)
        } else {
            Self::Plain(SyncPlainTcpClient::try_new(params)?)
        };

        trace!(
            "Connection of type {} is initialized ({} µs)",
            tcp_conn.s_type(),
            Instant::now().duration_since(start).as_micros(),
        );
        Ok(tcp_conn)
    }

    // Returns a descriptor of the chosen type
    pub fn s_type(&self) -> &'static str {
        match self {
            Self::Plain(_) => "Plain Sync TCP",
            Self::Tls(_) => "TLS Sync TCP",
        }
    }

    pub fn connect_params(&self) -> &ConnectParams {
        match self {
            Self::Plain(client) => client.connect_params(),
            Self::Tls(client) => client.connect_params(),
        }
    }
}
impl Drop for SyncTcpClient {
    fn drop(&mut self) {
        trace!("Drop of TcpClient");
    }
}
