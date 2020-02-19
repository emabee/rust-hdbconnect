use crate::protocol::util;
use crate::ConnectParams;
use rustls::{ClientSession, Session};
use std::cell::RefCell;
use std::fmt;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use webpki::DNSNameRef;

pub(crate) struct TlsTcpClient {
    params: ConnectParams,
    reader: RefCell<std::io::BufReader<TlsTcpStream>>,
    writer: RefCell<std::io::BufWriter<TlsTcpStream>>,
}
impl fmt::Debug for TlsTcpClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TlsTcpClient {{params: {:?}, ... }}", &self.params)
    }
}
impl TlsTcpClient {
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let tlsstream = TlsTcpStream::try_new(&params)?;
        Ok(Self {
            params,
            reader: RefCell::new(std::io::BufReader::new(tlsstream.try_clone()?)),
            writer: RefCell::new(std::io::BufWriter::new(tlsstream)),
        })
    }

    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    #[allow(dead_code)]
    pub fn reconnect(&self) -> std::io::Result<()> {
        let tlsstream = TlsTcpStream::try_new(&self.params)?;
        self.reader
            .replace(std::io::BufReader::new(tlsstream.try_clone()?));
        self.writer.replace(std::io::BufWriter::new(tlsstream));
        Ok(())
    }

    pub(crate) fn writer(&self) -> &RefCell<dyn std::io::Write> {
        &self.writer
    }

    pub(crate) fn reader(&self) -> &RefCell<dyn std::io::Read> {
        &self.reader
    }
}

struct TlsTcpStream {
    is_handshaking: bool,
    tcpstream: TcpStream,
    tlssession: Arc<Mutex<ClientSession>>,
}
impl TlsTcpStream {
    pub fn try_new(params: &ConnectParams) -> std::io::Result<Self> {
        let (tcpstream, tlssession) = connect_tcp(params)?;
        Ok(Self {
            is_handshaking: true,
            tcpstream,
            tlssession: Arc::new(Mutex::new(tlssession)),
        })
    }
    pub fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            is_handshaking: false,
            tcpstream: self.tcpstream.try_clone()?,
            tlssession: Arc::clone(&self.tlssession),
        })
    }
}

fn connect_tcp(params: &ConnectParams) -> std::io::Result<(TcpStream, ClientSession)> {
    debug!("connect_tcp(): Connecting to {:?}", params.addr());
    let tcpstream = TcpStream::connect(params.addr())?;
    trace!("tcpstream working");

    let tlssession = ClientSession::new(
        &Arc::new(params.rustls_clientconfig()?),
        DNSNameRef::try_from_ascii_str(params.host())
            .map_err(|_| util::io_error(format!("Cannot use {} for DNSNameRef", params.host())))?,
    );

    Ok((tcpstream, tlssession))
}

impl std::io::Write for TlsTcpStream {
    fn write(&mut self, request: &[u8]) -> std::io::Result<usize> {
        trace!(
            "std::io::Write::write() with request size {}",
            request.len()
        );
        let mut tlssession = self.tlssession.lock().unwrap();

        let result = std::io::Write::write(&mut *tlssession, request)?;

        while tlssession.wants_write() {
            let count = tlssession.write_tls(&mut self.tcpstream)?;
            trace!("std::io::Write::write(): wrote tls ({})", count);
        }

        trace!("std::io::Write::write() done");
        Ok(result)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        trace!("std::io::Write::flush()");
        let mut tlssession = self.tlssession.lock().unwrap();

        loop {
            while tlssession.wants_write() {
                let count = tlssession.write_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::flush(): wrote tls ({})", count);
            }

            if self.is_handshaking && !tlssession.is_handshaking() {
                self.is_handshaking = false;

                if let Some(protocol) = tlssession.get_protocol_version() {
                    debug!("Protocol {:?} negotiated", protocol)
                } else {
                    debug!("No TLS Protocol negotiated")
                }
            }

            if tlssession.wants_read() {
                let count = tlssession.read_tls(&mut self.tcpstream)?;
                trace!("std::io::Write::flush(): read_tls() -> {}", count);
                if count == 0 {
                    break;
                }

                if let Err(err) = tlssession.process_new_packets() {
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

impl std::io::Read for TlsTcpStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        trace!("std::io::Read::read() with buf size {}", buffer.len());
        let mut tlssession = self.tlssession.lock().unwrap();

        loop {
            if tlssession.wants_read() {
                let count = tlssession.read_tls(&mut self.tcpstream)?;
                trace!("transfer_read(): read_tls() -> {}", count);
                if count == 0 {
                    break;
                }

                if let Err(err) = tlssession.process_new_packets() {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, err));
                }
            } else {
                break;
            }
        }

        let read_bytes = tlssession.read(&mut buffer[..])?;
        trace!("std::io::Read::read() done");
        Ok(read_bytes)
    }
}
