use crate::ConnectParams;
use std::sync::Arc;
use tokio::{
    io::{BufReader, BufWriter},
    net::TcpStream,
    sync::Mutex,
};

#[derive(Debug)]
pub struct PlainAsyncTcpClient {
    params: ConnectParams,
    reader: Arc<Mutex<tokio::io::BufReader<TcpStream>>>,
    writer: Arc<Mutex<tokio::io::BufWriter<TcpStream>>>,
}

impl PlainAsyncTcpClient {
    // Returns an initialized plain async tcp connection
    // FIXME remove async if it is not needed
    pub async fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let std_tcpstream = std::net::TcpStream::connect(params.addr())?;

        // FIXME Is this doing the right thing????
        // FIXME Docu says: the conversion assumes nothing about the underlying stream;
        // FIXME it is left up to the user to set it in non-blocking mode
        let tcpstream_w = TcpStream::from_std(std_tcpstream.try_clone()?)?;
        let tcpstream_r = TcpStream::from_std(std_tcpstream)?;
        Ok(Self {
            params,
            writer: Arc::new(Mutex::new(BufWriter::new(tcpstream_w))),
            reader: Arc::new(Mutex::new(BufReader::new(tcpstream_r))),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&self) -> Arc<Mutex<BufWriter<TcpStream>>> {
        Arc::clone(&self.writer)
    }

    pub fn reader(&self) -> Arc<Mutex<BufReader<TcpStream>>> {
        Arc::clone(&self.reader)
    }
}
