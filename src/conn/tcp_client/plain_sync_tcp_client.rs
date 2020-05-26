use crate::ConnectParams;
use std::net::TcpStream;

#[derive(Debug)]
pub struct PlainSyncTcpClient {
    params: ConnectParams,
    reader: std::io::BufReader<TcpStream>,
    writer: std::io::BufWriter<TcpStream>,
}

impl PlainSyncTcpClient {
    /// Returns an initialized plain tcp connection
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let tcpstream = TcpStream::connect(params.addr())?;
        Ok(Self {
            params,
            writer: std::io::BufWriter::new(tcpstream.try_clone()?),
            reader: std::io::BufReader::new(tcpstream),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&mut self) -> &mut std::io::BufWriter<TcpStream> {
        &mut self.writer
    }

    pub fn reader(&mut self) -> &mut std::io::BufReader<TcpStream> {
        &mut self.reader
    }
}
