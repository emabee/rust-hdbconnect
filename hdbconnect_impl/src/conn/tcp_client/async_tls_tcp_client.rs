use std::sync::Arc;

use rustls::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};

use crate::{protocol::util, ConnectParams, HdbError};

#[derive(Debug)]
pub struct AsyncTlsTcpClient {
    params: ConnectParams,
    tls_stream: TlsStream<TcpStream>,
}

impl AsyncTlsTcpClient {
    pub async fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let a_client_config = Arc::new(params.rustls_clientconfig()?);
        let server_name = ServerName::try_from(params.host())
            .map_err(|_| HdbError::TlsServerName)
            .map_err(util::io_error)?;

        let config = TlsConnector::from(a_client_config);

        let tcp_stream = TcpStream::connect(params.addr()).await?;
        let tls_stream = config.connect(server_name, tcp_stream).await?;
        Ok(AsyncTlsTcpClient { params, tls_stream })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub fn writer(&mut self) -> &mut TlsStream<TcpStream> {
        &mut self.tls_stream
    }

    pub fn reader(&mut self) -> &mut TlsStream<TcpStream> {
        &mut self.tls_stream
    }
}
