use conn_core::connect_params::ConnectParams;
use rustls::ClientConfig;
use rustls::{ClientSession, Session};
use std::fs::{read_dir, File};
use std::io::{self, Read};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use webpki::DNSNameRef;

pub struct TlsStream {
    tcpstream: TcpStream,
    tlsconfig: Arc<ClientConfig>,
    tlssession: Arc<Mutex<ClientSession>>,
}
impl TlsStream {
    pub fn new(params: &ConnectParams) -> io::Result<TlsStream> {
        let (tcpstream, tlsconfig, tlssession) = connect_tcp(params)?;
        Ok(TlsStream {
            tcpstream,
            tlsconfig,
            tlssession: Arc::new(Mutex::new(tlssession)),
        })
    }
    pub fn try_clone(&self) -> io::Result<TlsStream> {
        Ok(TlsStream {
            tcpstream: self.tcpstream.try_clone()?,
            tlsconfig: Arc::clone(&self.tlsconfig),
            tlssession: Arc::clone(&self.tlssession),
        })
    }

    fn transfer(
        &mut self,
        mut is_handshaking: bool,
        req: Option<&[u8]>,
        buf: Option<&mut [u8]>,
    ) -> io::Result<usize> {
        debug!("transfer(): enter");
        let mut tlssession = self.tlssession.lock().unwrap();
        loop {
            trace!("transfer(): loop");
            while tlssession.wants_write() {
                let count = tlssession.write_tls(&mut self.tcpstream)?;
                trace!("transfer(): wrote tls ({})", count);
            }

            if is_handshaking && !tlssession.is_handshaking() {
                trace!("Handshake complete");
                is_handshaking = false;

                match tlssession.get_protocol_version() {
                    Some(protocol) => debug!("Protocol {:?} negotiated", protocol),
                    None => debug!("No TLS Protocol negotiated"),
                }
                if let Some(request) = req {
                    trace!("transfer(): writing");
                    io::Write::write_all(&mut *tlssession, request)?;
                    let count = tlssession.write_tls(&mut self.tcpstream)?;
                    trace!("transfer(): wrote tls ({})", count);
                }
            }

            if tlssession.wants_read() {
                let count = tlssession.read_tls(&mut self.tcpstream)?;
                trace!("transfer(): read_tls() -> {}", count);
                if count == 0 {
                    break;
                }

                if let Err(err) = tlssession.process_new_packets() {
                    return Err(io::Error::new(io::ErrorKind::Other, err));
                }
            } else {
                break;
            }
        }
        if let Some(buffer) = buf {
            let read_bytes = tlssession.read(&mut buffer[..])?;
            trace!("transfer(): read_bytes = {}", read_bytes);
            Ok(read_bytes)
        } else {
            Ok(0)
        }
    }
}

fn connect_tcp(
    params: &ConnectParams,
) -> io::Result<(TcpStream, Arc<ClientConfig>, ClientSession)> {
    debug!("connect_tcp(): Connecting to \"{:?}\"", params.addr());

    let tcpstream = TcpStream::connect(params.addr())?;

    let mut config = ClientConfig::new();
    let trust_anchor_dir = params
        .trust_anchor_dir()
        .ok_or_else(|| (io::Error::new(io::ErrorKind::Other, "No trust anchors provided")))?;
    debug!("Trust anchor directory = {}", trust_anchor_dir);

    let trust_anchor_files: Vec<PathBuf> = read_dir(trust_anchor_dir)?
        .filter_map(|r_dir_entry| r_dir_entry.ok())
        .filter(|dir_entry| {
            dir_entry.file_type().is_ok() && dir_entry.file_type().unwrap().is_file()
        })
        .filter(|dir_entry| {
            let path = dir_entry.path();
            let ext = path.extension();
            ext.is_some() && ext.unwrap() == "pem"
        })
        .map(|dir_entry| dir_entry.path())
        .collect();

    let mut t_ok = 0;
    let mut t_err = 0;
    for trust_anchor_file in trust_anchor_files {
        trace!("Trying trust anchor file {:?}", trust_anchor_file);
        let mut rd = io::BufReader::new(File::open(trust_anchor_file)?);
        let (n_ok, n_err) = config.root_store.add_pem_file(&mut rd).unwrap();
        t_ok += n_ok;
        t_err += n_err;
    }
    if t_ok == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "None of the provided trust anchors was accepted",
        ));
    }
    if t_err > 0 {
        warn!("Not all provided trust anchors were accepted");
    }

    let tlsconfig = Arc::new(config);

    let tlssession = ClientSession::new(
        &tlsconfig,
        DNSNameRef::try_from_ascii_str(params.host()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Cannot use {} for DNSNameRef", params.host()),
            )
        })?,
    );

    Ok((tcpstream, tlsconfig, tlssession))
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
