use crate::{ConnectParams, HdbResult};
use tokio::net::TcpStream;

// A plain async tcp connection
#[derive(Debug)]
pub(crate) struct AsyncPlainTcpClient {
    params: ConnectParams,
    tcp_stream: TcpStream,
}

impl AsyncPlainTcpClient {
    pub async fn try_new(params: ConnectParams) -> HdbResult<Self> {
        let tcp_stream = TcpStream::connect(params.addr()).await?;
        Ok(Self { params, tcp_stream })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&mut self) -> &mut TcpStream {
        &mut self.tcp_stream
    }

    pub fn reader(&mut self) -> &mut TcpStream {
        &mut self.tcp_stream
    }
}
