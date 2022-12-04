mod plain_async_tcp_client;

use crate::ConnectParams;
use plain_async_tcp_client::PlainAsyncTcpClient;
use std::time::Instant;

// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub(crate) enum AsyncTcpClient {
    // An async tcp connection without TLS.
    Plain(PlainAsyncTcpClient),
}
impl AsyncTcpClient {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connection parameters.
    pub async fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let start = Instant::now();
        trace!("TcpClient: Connecting to {:?})", params.addr());

        let tcp_conn = if params.is_tls() {
            unimplemented!("FIXME Async TLS is not yet implemented");
        } else {
            Self::Plain(PlainAsyncTcpClient::try_new(params).await?)
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
            Self::Plain(_) => "Plain Async TCP",
        }
    }

    pub fn connect_params(&self) -> &ConnectParams {
        match self {
            Self::Plain(client) => client.connect_params(),
        }
    }
}
impl Drop for AsyncTcpClient {
    fn drop(&mut self) {
        trace!("Drop of AsyncTcpClient");
    }
}
