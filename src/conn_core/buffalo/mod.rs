mod plain_connection;
#[cfg(feature = "tls")]
mod tls_connection;
#[cfg(feature = "tls")]
mod tls_stream;

use chrono::Local;
use conn_core::buffalo::plain_connection::PlainConnection;
#[cfg(feature = "tls")]
use conn_core::buffalo::tls_connection::TlsConnection;
use conn_core::connect_params::ConnectParams;
use std::cell::RefCell;
use std::io;

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
    pub fn new(params: ConnectParams) -> io::Result<Buffalo> {
        let start = Local::now();
        trace!("Connecting to {:?})", params.addr());

        #[cfg(feature = "tls")]
        let buffalo = if params.use_tls() {
            Buffalo::Secure(TlsConnection::new(params)?)
        } else {
            Buffalo::Plain(PlainConnection::new(params)?)
        };

        #[cfg(not(feature = "tls"))]
        let buffalo = if params.use_tls() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "In order to use TLS connections, please compile hdbconnect with feature TLS",
            ));
        } else {
            Buffalo::Plain(PlainConnection::new(params)?)
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

    /// Provides access to the writer half.
    pub fn writer(&self) -> &RefCell<io::Write> {
        match self {
            Buffalo::Plain(pc) => pc.writer(),
            #[cfg(feature = "tls")]
            Buffalo::Secure(sc) => sc.writer(),
        }
    }

    /// Provides access to the reader half.
    pub fn reader(&self) -> &RefCell<io::BufRead> {
        match self {
            Buffalo::Plain(pc) => pc.reader(),
            #[cfg(feature = "tls")]
            Buffalo::Secure(sc) => sc.reader(),
        }
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
