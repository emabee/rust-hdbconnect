use chrono::Local;
use std::cell::RefCell;
use std::io;
use stream::buffalo::plain_connection::PlainConnection;
use stream::buffalo::tls_connection::TlsConnection;
use stream::connect_params::ConnectParams;

/// A buffered tcp connection, with or without TLS.
#[derive(Debug)]
pub enum Buffalo {
    /// A buffered tcp connection without TLS.
    Plain(PlainConnection),
    /// A buffered tcp connection with TLS.
    Secure(TlsConnection),
}
impl Buffalo {
    /// Constructs a buffered tcp connection, with or without TLS,
    /// depending on the given connect parameters.
    pub fn new(params: ConnectParams) -> io::Result<Buffalo> {
        let start = Local::now();
        trace!("Connecting to {:?})", params.addr());

        let buffalo = if params.options().is_empty() {
            Buffalo::Plain(PlainConnection::new(params)?)
        } else {
            Buffalo::Secure(TlsConnection::new(params)?)
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
            Buffalo::Secure(sc) => sc.writer(),
        }
    }

    /// Provides access to the reader half.
    pub fn reader(&self) -> &RefCell<io::BufRead> {
        match self {
            Buffalo::Plain(pc) => pc.reader(),
            Buffalo::Secure(sc) => sc.reader(),
        }
    }

    /// Returns a descriptor of the chosen type
    pub fn s_type(&self) -> &'static str {
        match self {
            Buffalo::Plain(_) => "plain tcp",
            Buffalo::Secure(_) => "tls",
        }
    }
}
