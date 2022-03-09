#[cfg(feature = "async")]
mod plain_async_tcp_client;
mod plain_sync_tcp_client;
mod tls_sync_tcp_client;

use crate::ConnectParams;
#[cfg(feature = "async")]
use plain_async_tcp_client::PlainAsyncTcpClient;
use plain_sync_tcp_client::PlainSyncTcpClient;
use std::time::Instant;
use tls_sync_tcp_client::TlsSyncTcpClient;

// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub(crate) enum TcpClient {
    // A buffered blocking tcp connection without TLS.
    PlainSync(PlainSyncTcpClient),
    // A buffered blocking tcp connection with TLS.
    TlsSync(TlsSyncTcpClient),
    #[cfg(feature = "async")]
    // An async tcp connection without TLS.
    PlainAsync(PlainAsyncTcpClient),
}
impl TcpClient {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connect parameters.
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let start = Instant::now();
        trace!("TcpClient: Connecting to {:?})", params.addr());

        let tcp_conn = if params.is_tls() {
            Self::TlsSync(TlsSyncTcpClient::try_new(params)?)
        } else {
            Self::PlainSync(PlainSyncTcpClient::try_new(params)?)
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
            Self::PlainSync(_) => "Plain Sync TCP",
            Self::TlsSync(_) => "TLS Sync TCP",
            Self::PlainAsync(_) => "Plain Async TCP",
        }
    }

    pub fn connect_params(&self) -> &ConnectParams {
        match self {
            Self::PlainSync(client) => client.connect_params(),
            Self::TlsSync(client) => client.connect_params(),
            Self::PlainAsync(client) => client.connect_params(),
        }
    }
}
impl Drop for TcpClient {
    fn drop(&mut self) {
        trace!("Drop of TcpClient");
    }
}
