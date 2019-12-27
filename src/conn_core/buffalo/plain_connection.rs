use crate::conn_core::connect_params::ConnectParams;
use std::cell::RefCell;
use std::net::TcpStream;

#[derive(Debug)]
pub struct PlainConnection {
    params: ConnectParams,
    reader: RefCell<std::io::BufReader<TcpStream>>,
    writer: RefCell<std::io::BufWriter<TcpStream>>,
}

impl PlainConnection {
    /// Returns an initialized plain tcp connection
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let tcpstream = TcpStream::connect(params.addr())?;
        Ok(Self {
            params,
            writer: RefCell::new(std::io::BufWriter::new(tcpstream.try_clone()?)),
            reader: RefCell::new(std::io::BufReader::new(tcpstream)),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&self) -> &RefCell<std::io::BufWriter<TcpStream>> {
        &self.writer
    }

    pub fn reader(&self) -> &RefCell<std::io::BufReader<TcpStream>> {
        &self.reader
    }

    #[allow(dead_code)]
    pub fn reconnect(&self) -> std::io::Result<()> {
        let tcpstream = TcpStream::connect(self.params.addr())?;
        self.writer
            .replace(std::io::BufWriter::new(tcpstream.try_clone()?));
        self.reader.replace(std::io::BufReader::new(tcpstream));
        Ok(())
    }
}
