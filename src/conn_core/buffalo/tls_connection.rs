use crate::conn_core::buffalo::tls_stream::TlsStream;
use crate::conn_core::connect_params::ConnectParams;
use std::cell::RefCell;
use std::fmt;
use std::io;

pub struct TlsConnection {
    params: ConnectParams,
    reader: RefCell<io::BufReader<TlsStream>>,
    writer: RefCell<io::BufWriter<TlsStream>>,
}
impl fmt::Debug for TlsConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TlsConnection {{params: {:?}, ... }}", &self.params)
    }
}
impl TlsConnection {
    pub fn try_new(params: ConnectParams) -> io::Result<(TlsConnection)> {
        let tlsstream = TlsStream::try_new(&params)?;
        Ok(TlsConnection {
            params,
            reader: RefCell::new(io::BufReader::new(tlsstream.try_clone()?)),
            writer: RefCell::new(io::BufWriter::new(tlsstream)),
        })
    }

    #[allow(dead_code)]
    pub fn reconnect(&self) -> io::Result<()> {
        let tlsstream = TlsStream::try_new(&self.params)?;
        self.reader
            .replace(io::BufReader::new(tlsstream.try_clone()?));
        self.writer.replace(io::BufWriter::new(tlsstream));
        Ok(())
    }

    pub fn writer(&self) -> &RefCell<io::BufWriter<TlsStream>> {
        &self.writer
    }

    pub fn reader(&self) -> &RefCell<io::BufReader<TlsStream>> {
        &self.reader
    }
}
