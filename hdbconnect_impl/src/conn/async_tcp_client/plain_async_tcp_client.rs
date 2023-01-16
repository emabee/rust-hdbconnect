use crate::ConnectParams;
use std::sync::Arc;
use tokio::{
    io::{BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::Mutex,
};

// An plain async tcp connection
#[derive(Debug)]
pub struct PlainAsyncTcpClient {
    params: ConnectParams,
    reader: Arc<Mutex<tokio::io::BufReader<OwnedReadHalf>>>,
    writer: Arc<Mutex<tokio::io::BufWriter<OwnedWriteHalf>>>,
}

impl PlainAsyncTcpClient {
    pub async fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let (reader, writer) = TcpStream::connect(params.addr()).await?.into_split();
        Ok(Self {
            params,
            writer: Arc::new(Mutex::new(BufWriter::new(writer))),
            reader: Arc::new(Mutex::new(BufReader::new(reader))),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&self) -> Arc<Mutex<BufWriter<OwnedWriteHalf>>> {
        Arc::clone(&self.writer)
    }

    pub fn reader(&self) -> Arc<Mutex<BufReader<OwnedReadHalf>>> {
        Arc::clone(&self.reader)
    }
}
