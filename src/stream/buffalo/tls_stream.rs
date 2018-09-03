use rustls::{ClientSession, Session};
use std::fmt::Debug;
use std::io::{self, Read};
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};
use stream::buffalo::configuration::Configuration;

pub struct TlsStream {
    tcpstream: TcpStream,
    tlsclient: Arc<Mutex<ClientSession>>,
}
impl TlsStream {
    pub fn new<A: ToSocketAddrs + Debug>(config: &Configuration<A>) -> io::Result<TlsStream> {
        let (tcpstream, tlsclient) = connect_tcp(config)?;
        Ok(TlsStream {
            tcpstream,
            tlsclient: Arc::new(Mutex::new(tlsclient)),
        })
    }
    pub fn try_clone(&self) -> io::Result<TlsStream> {
        Ok(TlsStream {
            tcpstream: self.tcpstream.try_clone()?,
            tlsclient: Arc::clone(&self.tlsclient),
        })
    }

    fn transfer(
        &mut self,
        mut is_handshaking: bool,
        req: Option<&[u8]>,
        buf: Option<&mut [u8]>,
    ) -> io::Result<usize> {
        debug!("transfer(): enter");
        let mut tlsclient = self.tlsclient.lock().unwrap();
        loop {
            trace!("transfer(): loop");
            while tlsclient.wants_write() {
                let count = tlsclient.write_tls(&mut self.tcpstream)?;
                trace!("transfer(): wrote tls ({})", count);
            }

            if is_handshaking && !tlsclient.is_handshaking() {
                trace!("Handshake complete");
                is_handshaking = false;

                match tlsclient.get_protocol_version() {
                    Some(protocol) => debug!("Protocol {:?} negotiated", protocol),
                    None => debug!("No TLS Protocol negotiated"),
                }
                if let Some(request) = req {
                    trace!("transfer(): writing");
                    io::Write::write_all(&mut *tlsclient, request)?;
                    let count = tlsclient.write_tls(&mut self.tcpstream)?;
                    trace!("transfer(): wrote tls ({})", count);
                }
            }

            if tlsclient.wants_read() {
                let count = tlsclient.read_tls(&mut self.tcpstream)?;
                trace!("transfer(): read_tls() -> {}", count);
                if count == 0 {
                    break;
                }

                if let Err(err) = tlsclient.process_new_packets() {
                    return Err(io::Error::new(io::ErrorKind::Other, err));
                }
            } else {
                break;
            }
        }
        if let Some(buffer) = buf {
            let read_bytes = tlsclient.read(&mut buffer[..])?;
            trace!("transfer(): read_bytes = {}", read_bytes);
            Ok(read_bytes)
        } else {
            Ok(0)
        }
    }
}

fn connect_tcp<A: ToSocketAddrs + Debug>(
    config: &Configuration<A>,
) -> io::Result<(TcpStream, ClientSession)> {
    debug!("connect_tcp(): Connecting to \"{:?}\"", config.addr());

    let tcpstream = TcpStream::connect(config.addr())?;
    let tlsclient = ClientSession::new(&config.tls_config(), *config.host());

    Ok((tcpstream, tlsclient))
}

impl io::Write for TlsStream {
    fn write(&mut self, raw_request: &[u8]) -> io::Result<usize> {
        let result = self.transfer(true, Some(raw_request), None)?;
        debug!("write() received this: {:?}", result);
        Ok(raw_request.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let result = self.transfer(false, None, None)?;
        debug!("flush() received this: {:?}", result);
        Ok(())
    }
}

impl io::Read for TlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let result = self.transfer(false, None, Some(buf))?;
        debug!("read() received this: {:?}", result);
        Ok(result)
    }
}
