use crate::{ConnectParams, HdbResult};
use rustls::{pki_types::ServerName, ClientConnection, StreamOwned};
use std::{sync::Arc, time::Duration};

pub(crate) struct SyncTlsTcpClient {
    params: ConnectParams,
    tls_stream: StreamOwned<ClientConnection, std::net::TcpStream>,
}
impl std::fmt::Debug for SyncTlsTcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "TlsTcpClient {{params: {:?}, ... }}", &self.params)
    }
}
impl SyncTlsTcpClient {
    pub fn try_new(params: ConnectParams) -> HdbResult<Self> {
        Ok(Self {
            tls_stream: try_new_tls_stream(&params)?,
            params,
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub(crate) fn set_read_timeout(&mut self, o_duration: Option<Duration>) -> std::io::Result<()> {
        self.tls_stream.sock.set_read_timeout(o_duration)
    }

    pub(crate) fn writer(&mut self) -> &mut dyn std::io::Write {
        &mut self.tls_stream
    }

    pub(crate) fn reader(&mut self) -> &mut dyn std::io::Read {
        &mut self.tls_stream
    }
}

fn try_new_tls_stream(
    params: &ConnectParams,
) -> HdbResult<StreamOwned<ClientConnection, std::net::TcpStream>> {
    let a_client_config = Arc::new(params.rustls_clientconfig()?.0);
    let server_name = ServerName::try_from(params.host().to_owned())?;
    let client_connection = ClientConnection::new(a_client_config, server_name)?;
    debug!("ClientConnection: {client_connection:?}");

    debug!("Connecting to {:?}", params.addr());
    let tcpstream = std::net::TcpStream::connect(params.addr())?;
    trace!("tcpstream working");

    Ok(StreamOwned::new(client_connection, tcpstream))
}
