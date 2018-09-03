use std::cell::RefCell;
use std::fmt::Debug;
use std::io;
use std::net::ToSocketAddrs;
use stream::buffalo::configuration::Configuration;
use stream::buffalo::tls_stream::TlsStream;
use webpki::TLSServerTrustAnchors;

pub struct TlsConnection<'a, A: ToSocketAddrs + Debug> {
    config: Configuration<'a, A>,
    reader: RefCell<io::BufReader<TlsStream>>,
    writer: RefCell<io::BufWriter<TlsStream>>,
}
impl<'a, A: ToSocketAddrs + Debug> TlsConnection<'a, A> {
    pub fn new(
        addr: A,
        s_host: &'a str,
        trust_anchors: &'a TLSServerTrustAnchors,
    ) -> io::Result<TlsConnection<'a, A>>
    where
        A: ToSocketAddrs + Debug,
    {
        let config = Configuration::new(addr, s_host, trust_anchors)?;
        let tlsstream = TlsStream::new(&config)?;
        Ok(TlsConnection::<'a> {
            config,
            reader: RefCell::new(io::BufReader::new(tlsstream.try_clone()?)),
            writer: RefCell::new(io::BufWriter::new(tlsstream)),
        })
    }

    pub fn reconnect(&self) -> io::Result<()> {
        let tlsstream = TlsStream::new(&self.config)?;
        self.reader
            .replace(io::BufReader::new(tlsstream.try_clone()?));
        self.writer.replace(io::BufWriter::new(tlsstream));
        Ok(())
    }

    pub fn writer(&self, reconnect: bool) -> io::Result<&RefCell<io::Write>> {
        if reconnect {
            self.reconnect().unwrap();
        }
        Ok(&self.writer)
    }

    pub fn reader(&self) -> &RefCell<io::BufRead> {
        &self.reader
    }
}
