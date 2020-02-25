use crate::protocol::util;
use crate::ConnectParams;
use rustls::{ClientSession, Session};
use std::sync::Arc;
use webpki::DNSNameRef;

pub(crate) struct TlsTcpClient {
    params: ConnectParams,
    stream: Stream,
}
impl std::fmt::Debug for TlsTcpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "TlsTcpClient {{params: {:?}, ... }}", &self.params)
    }
}
impl TlsTcpClient {
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let stream = Stream::try_new(&params)?;
        Ok(Self { params, stream })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    #[allow(dead_code)]
    pub fn reconnect(&mut self) -> std::io::Result<()> {
        self.stream = Stream::try_new(&self.params)?;
        Ok(())
    }

    pub(crate) fn writer(&mut self) -> &mut dyn std::io::Write {
        &mut self.stream
    }

    pub(crate) fn reader(&mut self) -> &mut dyn std::io::Read {
        &mut self.stream
    }
}

struct Stream {
    is_handshaking: bool,
    tcpstream: std::net::TcpStream,
    client_session: ClientSession,
}
impl Stream {
    pub fn try_new(params: &ConnectParams) -> std::io::Result<Self> {
        debug!("Stream: Connecting to {:?}", params.addr());
        let tcpstream = std::net::TcpStream::connect(params.addr())?;
        trace!("tcpstream working");

        let client_session = ClientSession::new(
            &Arc::new(params.rustls_clientconfig()?),
            DNSNameRef::try_from_ascii_str(params.host()).map_err(|_| {
                util::io_error(format!("Cannot use {} for DNSNameRef", params.host()))
            })?,
        );
        Ok(Self {
            is_handshaking: true,
            tcpstream,
            client_session,
        })
    }
}
impl std::io::Write for Stream {
    fn write(&mut self, request: &[u8]) -> std::io::Result<usize> {
        trace!(
            "std::io::Write::write() with request size {}",
            request.len()
        );

        let result = std::io::Write::write(&mut self.client_session, request)?;

        while self.client_session.wants_write() {
            let count = self.client_session.write_tls(&mut self.tcpstream)?;
            trace!("std::io::Write::write(): wrote tls ({})", count);
        }

        if self.is_handshaking {
            loop {
                while self.client_session.wants_write() {
                    let count = self.client_session.write_tls(&mut self.tcpstream)?;
                    trace!("std::io::Write::write(): wrote tls ({})", count);
                }
                if self.is_handshaking && !self.client_session.is_handshaking() {
                    self.is_handshaking = false;
                    if let Some(protocol) = self.client_session.get_protocol_version() {
                        debug!("write: Protocol {:?} negotiated", protocol)
                    } else {
                        debug!("write: No TLS Protocol negotiated")
                    }
                }
                if self.client_session.wants_read() {
                    let count = self.client_session.read_tls(&mut self.tcpstream)?;
                    trace!("std::io::Write::write(): read_tls() -> {}", count);
                    if count == 0 {
                        break;
                    }
                    if let Err(err) = self.client_session.process_new_packets() {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, err));
                    }
                } else {
                    break;
                }
            }
        }

        trace!("std::io::Write::write() done");
        Ok(result)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        trace!("std::io::Write::flush()");
        loop {
            while self.client_session.wants_write() {
                let count = self.client_session.write_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::flush(): wrote tls ({})", count);
            }

            if self.is_handshaking && !self.client_session.is_handshaking() {
                self.is_handshaking = false;

                if let Some(protocol) = self.client_session.get_protocol_version() {
                    debug!("flush: Protocol {:?} negotiated", protocol)
                } else {
                    debug!("flush: No TLS Protocol negotiated")
                }
            }

            if self.client_session.wants_read() {
                let count = self.client_session.read_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::flush(): read_tls() -> {}", count);
                if count == 0 {
                    break;
                }

                if let Err(err) = self.client_session.process_new_packets() {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, err));
                }
            } else {
                break;
            }
        }

        trace!("std::io::Write::flush() done");
        Ok(())
    }
}

impl std::io::Read for Stream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        trace!("std::io::Read::read() with buf size {}", buffer.len());
        loop {
            if self.client_session.wants_read() {
                let count = self.client_session.read_tls(&mut self.tcpstream)?;
                trace!("transfer_read(): read_tls() -> {}", count);
                if count == 0 {
                    break;
                }

                if let Err(err) = self.client_session.process_new_packets() {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, err));
                }
            } else {
                break;
            }
        }

        let read_bytes = self.client_session.read(&mut buffer[..])?;
        trace!("std::io::Read::read() done");
        Ok(read_bytes)
    }
}
