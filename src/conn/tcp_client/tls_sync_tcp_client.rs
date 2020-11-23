use crate::protocol::util;
use crate::ConnectParams;
use rustls::{ClientSession, Session};
use std::sync::{Arc, Mutex};
use webpki::DNSNameRef;

pub(crate) struct TlsSyncTcpClient {
    params: ConnectParams,
    reader: Stream,
    writer: std::io::BufWriter<Stream>,
}
impl std::fmt::Debug for TlsSyncTcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "TlsTcpClient {{params: {:?}, ... }}", &self.params)
    }
}
impl TlsSyncTcpClient {
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let stream = Stream::try_new(&params)?;
        Ok(Self {
            params,
            reader: stream.try_clone()?,
            writer: std::io::BufWriter::new(stream),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    pub(crate) fn writer(&mut self) -> &mut dyn std::io::Write {
        &mut self.writer
    }

    pub(crate) fn reader(&mut self) -> &mut dyn std::io::Read {
        &mut self.reader
    }
}

struct Stream {
    is_handshaking: bool,
    tcpstream: std::net::TcpStream,
    am_client_session: Arc<Mutex<ClientSession>>,
}
impl Stream {
    pub fn try_new(params: &ConnectParams) -> std::io::Result<Self> {
        debug!("Connecting to {:?}", params.addr());
        let tcpstream = std::net::TcpStream::connect(params.addr())?;
        trace!("tcpstream working");

        let am_client_session = Arc::new(Mutex::new(ClientSession::new(
            &Arc::new(params.rustls_clientconfig()?),
            DNSNameRef::try_from_ascii_str(params.host()).map_err(util::io_error)?,
        )));

        Ok(Self {
            is_handshaking: true,
            tcpstream,
            am_client_session,
        })
    }

    pub fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            is_handshaking: false,
            tcpstream: self.tcpstream.try_clone()?,
            am_client_session: Arc::clone(&self.am_client_session),
        })
    }
}

impl std::io::Write for Stream {
    fn write(&mut self, request: &[u8]) -> std::io::Result<usize> {
        trace!(
            "std::io::Write::write() with request size {}",
            request.len()
        );
        let mut client_session = self.am_client_session.lock().unwrap();

        let result = client_session.write(request)?;

        if self.is_handshaking {
            while client_session.wants_write() {
                let count = client_session.write_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::write(): wrote tls ({})", count);
            }
        }
        Ok(result)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        trace!("std::io::Write::flush()");
        let mut client_session = self.am_client_session.lock().unwrap();

        loop {
            while client_session.wants_write() {
                let count = client_session.write_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::flush(): wrote tls ({})", count);
            }

            if self.is_handshaking && !client_session.is_handshaking() {
                self.is_handshaking = false;

                if let Some(protocol) = client_session.get_protocol_version() {
                    debug!("Protocol {:?} negotiated", protocol)
                } else {
                    debug!("No TLS Protocol negotiated")
                }
            }

            if client_session.wants_read() {
                let count = client_session.read_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::flush(): read_tls() -> {}", count);
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
        Ok(())
    }
}

impl std::io::Read for Stream {
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

        client_session.read(&mut buffer[..])
    }
}
