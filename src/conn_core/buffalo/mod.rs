mod plain_connection;
#[cfg(feature = "tls")]
mod tls_connection;
#[cfg(feature = "tls")]
mod tls_stream;

use crate::conn_core::buffalo::plain_connection::PlainConnection;
#[cfg(feature = "tls")]
use crate::conn_core::buffalo::tls_connection::TlsConnection;
use crate::conn_core::connect_params::ConnectParams;
use chrono::Local;

/// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub enum Buffalo {
    /// A buffered tcp connection without TLS.
    Plain(PlainConnection),
    /// A buffered tcp connection with TLS.
    #[cfg(feature = "tls")]
    Secure(TlsConnection),
}
impl Buffalo {
    /// Constructs a buffered tcp connection, with or without TLS,
    /// depending on the given connect parameters.
    pub fn try_new(params: ConnectParams) -> std::io::Result<Buffalo> {
        let start = Local::now();
        trace!("Connecting to {:?})", params.addr());

        #[cfg(feature = "tls")]
        let buffalo = if params.use_tls() {
            Buffalo::Secure(TlsConnection::try_new(params)?)
        } else {
            Buffalo::Plain(PlainConnection::try_new(params)?)
        };

        #[cfg(not(feature = "tls"))]
        let buffalo = if params.use_tls() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "In order to use TLS connections, please compile hdbconnect with feature TLS",
            ));
        } else {
            Buffalo::Plain(PlainConnection::try_new(params)?)
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

    /// Returns a descriptor of the chosen type
    pub fn s_type(&self) -> &'static str {
        match self {
            Buffalo::Plain(_) => "Plain TCP",
            #[cfg(feature = "tls")]
            Buffalo::Secure(_) => "TLS",
        }
    }
}
