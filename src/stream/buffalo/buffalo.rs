use std::cell::RefCell;
use std::fmt::Debug;
use std::io;
use std::net::ToSocketAddrs;
use stream::buffalo::plain_connection::PlainConnection;
use stream::buffalo::tls_connection::TlsConnection;
use webpki::TLSServerTrustAnchors;

/// A buffered tcp connection, with or without TLS.
///
pub enum Buffalo<'a, A: ToSocketAddrs + Debug> {
    /// A buffered tcp connection without TLS.
    Plain(PlainConnection<A>),
    /// A buffered tcp connection with TLS.
    Secure(TlsConnection<'a, A>),
}
impl<'a, A: ToSocketAddrs + Debug> Buffalo<'a, A> {
    /// Constructs a buffered tcp connection, with or without TLS,
    /// depending on whether trust_anchors is Some(..). or None.
    pub fn new(
        addr: A,
        s_host: &'a str,
        trust_anchors: Option<&'a TLSServerTrustAnchors>,
    ) -> io::Result<Buffalo<'a, A>> {
        match trust_anchors {
            Some(ta) => {
                let conn = TlsConnection::new(addr, s_host, ta)?;
                Ok(Buffalo::Secure(conn))
            }
            None => {
                let conn = PlainConnection::new(addr)?;
                Ok(Buffalo::Plain(conn))
            }
        }
    }

    /// Provides access to the writer half.
    pub fn writer(&self, reconnect: bool) -> io::Result<&RefCell<io::Write>> {
        match self {
            Buffalo::Plain(pc) => pc.writer(reconnect),
            Buffalo::Secure(sc) => sc.writer(reconnect),
        }
    }

    /// Provides access to the reader half.
    pub fn reader(&self) -> &RefCell<io::BufRead> {
        match self {
            Buffalo::Plain(pc) => pc.reader(),
            Buffalo::Secure(sc) => sc.reader(),
        }
    }
}
