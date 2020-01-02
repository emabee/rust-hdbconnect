mod plain_connection;
mod tls_connection;
mod tls_stream;

use crate::conn_core::buffalo::plain_connection::PlainConnection;
use crate::conn_core::buffalo::tls_connection::TlsConnection;
use crate::conn_core::connect_params::ConnectParams;
use chrono::Local;

// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub(crate) enum Buffalo {
    // A buffered tcp connection without TLS.
    Plain(PlainConnection),
    // A buffered tcp connection with TLS.
    Secure(TlsConnection),
}
impl Buffalo {
    // Constructs a buffered tcp connection, with or without TLS,
    // depending on the given connect parameters.
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let start = Local::now();
        trace!("Buffalo: Connecting to {:?})", params.addr());

        let buffalo = if params.use_tls() {
            Self::Secure(TlsConnection::try_new(params)?)
        } else {
            Self::Plain(PlainConnection::try_new(params)?)
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
            Self::Plain(_) => "Plain TCP",
            Self::Secure(_) => "TLS",
        }
    }
}
impl Drop for Buffalo {
    fn drop(&mut self) {
        trace!("Drop of Buffalo")
    }
}
