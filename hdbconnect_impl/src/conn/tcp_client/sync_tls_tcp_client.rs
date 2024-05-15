use crate::{protocol::util, ConnectParams, HdbResult};
use rustls::pki_types::ServerName;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_rustls::rustls::ClientConnection;

pub(crate) struct SyncTlsTcpClient {
    params: ConnectParams,
    tls_stream: TlsStream,
}
impl std::fmt::Debug for SyncTlsTcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "TlsTcpClient {{params: {:?}, ... }}", &self.params)
    }
}
impl SyncTlsTcpClient {
    pub fn try_new(params: ConnectParams) -> HdbResult<Self> {
        Ok(Self {
            tls_stream: TlsStream::try_new(&params)?,
            params,
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub(crate) fn set_read_timeout(&mut self, o_duration: Option<Duration>) -> std::io::Result<()> {
        self.tls_stream.set_read_timeout(o_duration)
    }

    pub(crate) fn writer(&mut self) -> &mut dyn std::io::Write {
        &mut self.tls_stream
    }

    pub(crate) fn reader(&mut self) -> &mut dyn std::io::Read {
        &mut self.tls_stream
    }
}

struct TlsStream {
    is_handshaking: bool,
    tcpstream: std::net::TcpStream,
    am_client_session: Arc<Mutex<ClientConnection>>,
}
impl TlsStream {
    fn try_new(params: &ConnectParams) -> HdbResult<Self> {
        debug!("Connecting to {:?}", params.addr());
        let tcpstream = std::net::TcpStream::connect(params.addr())?;
        trace!("tcpstream working");

        let a_client_config = Arc::new(params.rustls_clientconfig()?.0);
        let server_name = ServerName::try_from(params.host().to_owned())?;
        let am_client_session = Arc::new(Mutex::new(ClientConnection::new(
            a_client_config,
            server_name,
        )?));

        Ok(Self {
            is_handshaking: true,
            tcpstream,
            am_client_session,
        })
    }
    fn set_read_timeout(&mut self, o_duration: Option<Duration>) -> std::io::Result<()> {
        self.tcpstream.set_read_timeout(o_duration)
    }
}

impl std::io::Write for TlsStream {
    fn write(&mut self, request: &[u8]) -> std::io::Result<usize> {
        trace!(
            "std::io::Write::write() with request size {}",
            request.len()
        );
        let mut client_connection = self.am_client_session.lock().unwrap();

        let result = client_connection.writer().write(request)?;

        if self.is_handshaking {
            while client_connection.wants_write() {
                let count = client_connection.write_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::write(): wrote tls ({})", count);
            }
        }
        Ok(result)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        trace!("std::io::Write::flush()");
        let mut client_connection = self.am_client_session.lock().unwrap();

        loop {
            while client_connection.wants_write() {
                let count = client_connection.write_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::flush(): wrote tls ({})", count);
            }

            if self.is_handshaking && !client_connection.is_handshaking() {
                self.is_handshaking = false;

                if let Some(protocol) = client_connection.protocol_version() {
                    debug!("Protocol {:?} negotiated", protocol);
                } else {
                    debug!("No TLS Protocol negotiated");
                }
            }

            if client_connection.wants_read() {
                let count = client_connection.read_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::flush(): read_tls() -> {}", count);
                if count == 0 {
                    break;
                }

                if let Err(err) = client_connection.process_new_packets() {
                    return Err(util::io_error(err));
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl std::io::Read for TlsStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        trace!("std::io::Read::read() with buf size {}", buffer.len());
        let mut client_session = self.am_client_session.lock().unwrap();

        loop {
            if client_session.wants_read() {
                let count = client_session.read_tls(&mut self.tcpstream)?;
                trace!("transfer_read(): read_tls() -> {}", count);
                if count == 0 {
                    break;
                }

                if let Err(err) = client_session.process_new_packets() {
                    return Err(util::io_error(err));
                }
            } else {
                break;
            }
        }

        client_session.reader().read(&mut *buffer)
    }
}
