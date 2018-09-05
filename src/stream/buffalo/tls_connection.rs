use std::cell::RefCell;
use std::io;
use stream::buffalo::tls_stream::TlsStream;
use stream::connect_params::ConnectParams;

#[derive(Debug)]
pub struct TlsConnection {
    params: ConnectParams,
    reader: RefCell<io::BufReader<TlsStream>>,
    writer: RefCell<io::BufWriter<TlsStream>>,
}
impl TlsConnection {
    pub fn new(params: ConnectParams) -> io::Result<(TlsConnection)> {
        let tlsstream = TlsStream::new(&params)?;
        Ok(TlsConnection {
            params,
            reader: RefCell::new(io::BufReader::new(tlsstream.try_clone()?)),
            writer: RefCell::new(io::BufWriter::new(tlsstream)),
        })
    }

    #[allow(dead_code)]
    pub fn reconnect(&self) -> io::Result<()> {
        let tlsstream = TlsStream::new(&self.params)?;
        self.reader
            .replace(io::BufReader::new(tlsstream.try_clone()?));
        self.writer.replace(io::BufWriter::new(tlsstream));
        Ok(())
    }

    pub fn writer(&self) -> &RefCell<io::Write> {
        &self.writer
    }

    pub fn reader(&self) -> &RefCell<io::BufRead> {
        &self.reader
    }
}
