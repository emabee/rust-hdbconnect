use crate::{ConnectParams, HdbError, HdbResult};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_rustls::{
    rustls::ServerName,
    {client::TlsStream, TlsConnector},
};

#[derive(Debug)]
pub(crate) struct AsyncTlsTcpClient {
    params: ConnectParams,
    tls_stream: TlsStream<TcpStream>,
}

impl AsyncTlsTcpClient {
    pub async fn try_new(params: ConnectParams) -> HdbResult<Self> {
        let a_client_config = Arc::new(params.rustls_clientconfig()?);
        let server_name = ServerName::try_from(params.host())?;

        let config = TlsConnector::from(a_client_config);

        let tcp_stream = TcpStream::connect(params.addr()).await?;
        let tls_stream = config
            .connect(server_name, tcp_stream)
            .await
            .map_err(|e| HdbError::TlsInit { source: e })?;
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
