use crate::{ConnectParams, HdbResult};
use std::net::TcpStream;

#[derive(Debug)]
pub(crate) struct SyncPlainTcpClient {
    params: ConnectParams,
    tcp_stream: TcpStream,
}

impl SyncPlainTcpClient {
    // Returns an initialized plain tcp connection
    pub fn try_new(params: ConnectParams) -> HdbResult<Self> {
        Ok(Self {
            tcp_stream: TcpStream::connect(params.addr())?,
            params,
        })
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
