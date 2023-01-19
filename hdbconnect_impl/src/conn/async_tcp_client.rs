mod async_plain_tcp_client;
mod async_tls_tcp_client;

use crate::ConnectParams;
use async_plain_tcp_client::AsyncPlainTcpClient;
use async_tls_tcp_client::AsyncTlsTcpClient;
use std::time::Instant;

// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub(crate) enum AsyncTcpClient {
    Plain(AsyncPlainTcpClient),
    Tls(AsyncTlsTcpClient),
    // Needed for being able to send the Drop asynchronously
    Dead,
}
impl AsyncTcpClient {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connection parameters.
    pub async fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let start = Instant::now();
        trace!("TcpClient: Connecting to {:?})", params.addr());

        let tcp_conn = if params.is_tls() {
            Self::Tls(AsyncTlsTcpClient::try_new(params).await?)
        } else {
            Self::Plain(AsyncPlainTcpClient::try_new(params).await?)
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
            Self::Plain(_) => "Async Plain TCP",
            Self::Tls(_) => "Async TLS TCP",
            AsyncTcpClient::Dead => unreachable!(),
        }
    }

    pub fn connect_params(&self) -> &ConnectParams {
        match self {
            Self::Plain(client) => client.connect_params(),
            Self::Tls(client) => client.connect_params(),
            AsyncTcpClient::Dead => unreachable!(),
        }
    }
}
impl Drop for AsyncTcpClient {
    fn drop(&mut self) {
        trace!("Drop of AsyncTcpClient");
    }
}
