use crate::{ConnectParams, HdbResult};
use std::{net::TcpStream, time::Duration};

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

    pub(crate) fn set_read_timeout(&mut self, o_duration: Option<Duration>) -> std::io::Result<()> {
        self.tcp_stream.set_read_timeout(o_duration)
    }

    pub fn writer(&mut self) -> &mut TcpStream {
        &mut self.tcp_stream
    }

    pub fn reader(&mut self) -> &mut TcpStream {
        &mut self.tcp_stream
    }
}
