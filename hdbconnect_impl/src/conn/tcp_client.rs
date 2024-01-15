#[cfg(feature = "sync")]
mod sync_plain_tcp_client;
#[cfg(feature = "sync")]
mod sync_tls_tcp_client;

#[cfg(feature = "sync")]
use sync_plain_tcp_client::SyncPlainTcpClient;
#[cfg(feature = "sync")]
use sync_tls_tcp_client::SyncTlsTcpClient;

#[cfg(feature = "async")]
mod async_plain_tcp_client;
#[cfg(feature = "async")]
mod async_tls_tcp_client;

#[cfg(feature = "async")]
use async_plain_tcp_client::AsyncPlainTcpClient;
#[cfg(feature = "async")]
use async_tls_tcp_client::AsyncTlsTcpClient;

use crate::{ConnectParams, HdbResult};
use std::time::Instant;

// A buffered tcp connection, synchronous or asynchronoues, with or without TLS.
#[derive(Debug)]
pub(crate) enum TcpClient {
    // A buffered blocking tcp connection without TLS.
    #[cfg(feature = "sync")]
    SyncPlain(SyncPlainTcpClient),

    // A buffered blocking tcp connection with TLS.
    #[cfg(feature = "sync")]
    SyncTls(SyncTlsTcpClient),

    // A buffered async tcp connection without TLS.
    #[cfg(feature = "async")]
    AsyncPlain(AsyncPlainTcpClient),

    // A buffered async tcp connection with TLS.
    #[cfg(feature = "async")]
    AsyncTls(AsyncTlsTcpClient),

    // Needed for being able to send the Drop asynchronously
    #[cfg(feature = "async")]
    Dead,
}
impl TcpClient {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connect parameters.
    #[cfg(feature = "sync")]
    pub fn try_new_sync(params: ConnectParams) -> HdbResult<Self> {
        let start = Instant::now();
        trace!("TcpClient: Connecting to {:?})", params.addr());

        let tcp_conn = if params.is_tls() {
            Self::SyncTls(SyncTlsTcpClient::try_new(params)?)
        } else {
            Self::SyncPlain(SyncPlainTcpClient::try_new(params)?)
        };

        trace!(
            "Connection of type {} is initialized ({} µs)",
            tcp_conn.s_type(),
            Instant::now().duration_since(start).as_micros(),
        );
        Ok(tcp_conn)
    }

    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connection parameters.
    #[cfg(feature = "async")]
    pub async fn try_new_async(params: ConnectParams) -> HdbResult<Self> {
        let start = Instant::now();
        trace!("TcpClient: Connecting to {:?})", params.addr());

        let tcp_conn = if params.is_tls() {
            Self::AsyncTls(AsyncTlsTcpClient::try_new(params).await?)
        } else {
            Self::AsyncPlain(AsyncPlainTcpClient::try_new(params).await?)
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
            #[cfg(feature = "sync")]
            Self::SyncPlain(_) => "Sync Plain TCP",
            #[cfg(feature = "sync")]
            Self::SyncTls(_) => "Sync TLS TCP",
            #[cfg(feature = "async")]
            Self::AsyncPlain(_) => "Async Plain TCP",
            #[cfg(feature = "async")]
            Self::AsyncTls(_) => "Async TLS TCP",
            #[cfg(feature = "async")]
            Self::Dead => unreachable!(),
            #[cfg(not(any(feature = "sync", feature = "async")))]
            _ => todo!(),
        }
    }

    pub fn connect_params(&self) -> &ConnectParams {
        match self {
            #[cfg(feature = "sync")]
            Self::SyncPlain(cl) => cl.connect_params(),
            #[cfg(feature = "sync")]
            Self::SyncTls(cl) => cl.connect_params(),
            #[cfg(feature = "async")]
            Self::AsyncPlain(cl) => cl.connect_params(),
            #[cfg(feature = "async")]
            Self::AsyncTls(cl) => cl.connect_params(),
            #[cfg(feature = "async")]
            Self::Dead => unreachable!(),
            #[cfg(not(any(feature = "sync", feature = "async")))]
            _ => todo!(),
        }
    }
}

impl Drop for TcpClient {
    fn drop(&mut self) {
        trace!("Drop of TcpClient");
    }
}
