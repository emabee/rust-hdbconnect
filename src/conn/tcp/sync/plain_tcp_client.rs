use crate::ConnectParams;
use std::net::TcpStream;

#[derive(Debug)]
pub struct PlainTcpClient {
    params: ConnectParams,
    reader: std::io::BufReader<TcpStream>,
    writer: std::io::BufWriter<TcpStream>,
}

impl PlainTcpClient {
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

    #[allow(dead_code)]
    pub fn reconnect(&mut self) -> std::io::Result<()> {
        let tcpstream = TcpStream::connect(self.params.addr())?;
        self.writer = std::io::BufWriter::new(tcpstream.try_clone()?);
        self.reader = std::io::BufReader::new(tcpstream);
        Ok(())
    }
}
