use crate::conn_core::buffalo::tls_stream::TlsStream;
use crate::conn_core::connect_params::ConnectParams;
use std::cell::RefCell;
use std::fmt;

pub struct TlsConnection {
    params: ConnectParams,
    reader: RefCell<std::io::BufReader<TlsStream>>,
    writer: RefCell<std::io::BufWriter<TlsStream>>,
}
impl fmt::Debug for TlsConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TlsConnection {{params: {:?}, ... }}", &self.params)
    }
}
impl TlsConnection {
    pub fn try_new(params: ConnectParams) -> std::io::Result<TlsConnection> {
        let tlsstream = TlsStream::try_new(&params)?;
        Ok(TlsConnection {
            params,
            reader: RefCell::new(std::io::BufReader::new(tlsstream.try_clone()?)),
            writer: RefCell::new(std::io::BufWriter::new(tlsstream)),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    #[allow(dead_code)]
    pub fn reconnect(&self) -> std::io::Result<()> {
        let tlsstream = TlsStream::try_new(&self.params)?;
        self.reader
            .replace(std::io::BufReader::new(tlsstream.try_clone()?));
        self.writer.replace(std::io::BufWriter::new(tlsstream));
        Ok(())
    }

    pub fn writer(&self) -> &RefCell<std::io::BufWriter<TlsStream>> {
        &self.writer
    }

    pub fn reader(&self) -> &RefCell<std::io::BufReader<TlsStream>> {
        &self.reader
    }
}
