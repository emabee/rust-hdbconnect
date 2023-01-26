use crate::ConnectParams;
use tokio::{
    io::{BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};

// An plain async tcp connection
#[derive(Debug)]
pub struct AsyncPlainTcpClient {
    params: ConnectParams,
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl AsyncPlainTcpClient {
    pub async fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let (reader, writer) = TcpStream::connect(params.addr()).await?.into_split();
        Ok(Self {
            params,
            writer: BufWriter::new(writer),
            reader: BufReader::new(reader),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&mut self) -> &mut BufWriter<OwnedWriteHalf> {
        &mut self.writer
    }

    pub fn reader(&mut self) -> &mut BufReader<OwnedReadHalf> {
        &mut self.reader
    }
}
