use crate::ConnectParams;
use std::net::TcpStream;

#[derive(Debug)]
pub struct SyncPlainTcpClient {
    params: ConnectParams,
    reader: TcpStream,
    writer: TcpStream,
}

impl SyncPlainTcpClient {
    // Returns an initialized plain tcp connection
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let tcpstream = TcpStream::connect(params.addr())?;
        Ok(Self {
            params,
            writer: tcpstream.try_clone()?,
            reader: tcpstream,
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&mut self) -> &mut TcpStream {
        &mut self.writer
    }

    pub fn reader(&mut self) -> &mut TcpStream {
        &mut self.reader
    }
}
