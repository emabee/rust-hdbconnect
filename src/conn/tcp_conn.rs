use super::tcp::{PlainConnection, TlsConnection};
use super::ConnectParams;
use chrono::Local;

// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub(crate) enum TcpConn {
    // A buffered tcp connection without TLS.
    SyncPlain(PlainConnection),
    // A buffered tcp connection with TLS.
    SyncSecure(TlsConnection),
}
impl TcpConn {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connect parameters.
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let start = Local::now();
        trace!("TcpConn: Connecting to {:?})", params.addr());

        let buffalo = if params.use_tls() {
            Self::SyncSecure(TlsConnection::try_new(params)?)
        } else {
            Self::SyncPlain(PlainConnection::try_new(params)?)
        };

        trace!(
            "Connection of type {} is initialized ({} µs)",
            buffalo.s_type(),
            Local::now()
                .signed_duration_since(start)
                .num_microseconds()
                .unwrap_or(-1)
        );
        Ok(buffalo)
    }

    // Returns a descriptor of the chosen type
    pub fn s_type(&self) -> &'static str {
        match self {
            Self::SyncPlain(_) => "Plain TCP",
            Self::SyncSecure(_) => "TLS",
        }
    }
}
impl Drop for TcpConn {
    fn drop(&mut self) {
        trace!("Drop of Buffalo")
    }
}
