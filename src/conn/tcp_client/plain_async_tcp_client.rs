use crate::ConnectParams;
use std::cell::RefCell;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct PlainAsyncTcpClient {
    params: ConnectParams,
    reader: RefCell<tokio::io::BufReader<TcpStream>>,
    writer: RefCell<tokio::io::BufWriter<TcpStream>>,
}

impl PlainAsyncTcpClient {
    // Returns an initialized plain async tcp connection
    pub async fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let std_tcpstream = std::net::TcpStream::connect(params.addr())?;

        let tcpstream_w = TcpStream::from_std(std_tcpstream.try_clone()?)?;
        let tcpstream_r = TcpStream::from_std(std_tcpstream)?;
        Ok(Self {
            params,
            writer: RefCell::new(tokio::io::BufWriter::new(tcpstream_w)),
            reader: RefCell::new(tokio::io::BufReader::new(tcpstream_r)),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&self) -> &RefCell<tokio::io::BufWriter<TcpStream>> {
        &self.writer
    }

    pub fn reader(&self) -> &RefCell<tokio::io::BufReader<TcpStream>> {
        &self.reader
    }
}
