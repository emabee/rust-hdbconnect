use crate::{ConnectParams, ServerCerts};
use mio::tcp::TcpStream;
use rustls;
use rustls::Session;
use std::collections;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::process;
use std::str;
use std::sync::{Arc, Mutex};
use webpki;
use webpki_roots;

const CLIENT: mio::Token = mio::Token(0);

/// This encapsulates the TCP-level connection, some connection
/// state, and the underlying TLS-level session.
#[derive(Debug)]
pub struct TlsClient {
    params: ConnectParams,
    socket: TcpStream,
    closing: bool,
    clean_closure: bool,
    tls_session: rustls::ClientSession,
}

impl TlsClient {
    pub fn connect_params(&self) -> &ConnectParams {
        &self.params
    }

    fn ready(&mut self, poll: &mut mio::Poll, ev: &mio::Event) {
        assert_eq!(ev.token(), CLIENT);

        if ev.readiness().is_readable() {
            self.do_read();
        }

        if ev.readiness().is_writable() {
            self.do_write();
        }

        if self.is_closed() {
            println!("Connection closed");
            process::exit(if self.clean_closure { 0 } else { 1 });
        }

        self.reregister(poll);
    }
}

/// We implement `io::Write` and pass through to the TLS session
impl std::io::Write for TlsClient {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.tls_session.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.tls_session.flush()
    }
}

impl std::io::Read for TlsClient {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.tls_session.read(bytes)
    }
}

impl TlsClient {
    pub fn try_new(params: ConnectParams) -> std::io::Result<TlsClient> {
        let (socket, _tlsconfig, tls_session) = connect_tcp(&params)?;

        Ok(TlsClient {
            socket,
            params,
            closing: false,
            clean_closure: false,
            tls_session,
        })
    }

    fn read_source_to_end(&mut self, rd: &mut dyn std::io::Read) -> std::io::Result<usize> {
        let mut buf = Vec::new();
        let len = rd.read_to_end(&mut buf)?;
        self.tls_session.write_all(&buf).unwrap();
        Ok(len)
    }

    /// We're ready to do a read.
    fn do_read(&mut self) {
        // Read TLS data.  This fails if the underlying TCP connection
        // is broken.
        let rc = self.tls_session.read_tls(&mut self.socket);
        if let Err(error) = rc {
            // let error = rc.unwrap_err();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return;
            }
            println!("TLS read error: {:?}", error);
            self.closing = true;
            return;
        }

        // If we're ready but there's no data: EOF.
        if rc.unwrap() == 0 {
            println!("EOF");
            self.closing = true;
            self.clean_closure = true;
            return;
        }

        // Reading some TLS data might have yielded new TLS
        // messages to process.  Errors from this indicate
        // TLS protocol problems and are fatal.
        let processed = self.tls_session.process_new_packets();
        if let Err(e) = processed {
            println!("TLS error: {:?}", e);
            self.closing = true;
            return;
        }

        // Having read some TLS data, and processed any new messages,
        // we might have new plaintext as a result.
        //
        // Read it and then write it to stdout.
        let mut plaintext = Vec::new();
        let rc = self.tls_session.read_to_end(&mut plaintext);
        if !plaintext.is_empty() {
            std::io::stdout().write_all(&plaintext).unwrap();
        }

        // If that fails, the peer might have started a clean TLS-level
        // session closure.
        if let Err(err) = rc {
            println!("Plaintext read error: {:?}", err);
            self.clean_closure = err.kind() == std::io::ErrorKind::ConnectionAborted;
            self.closing = true;
            return;
        }
    }

    #[cfg(target_os = "windows")]
    fn do_write(&mut self) {
        self.tls_session.write_tls(&mut self.socket).unwrap();
    }

    #[cfg(not(target_os = "windows"))]
    fn do_write(&mut self) {
        use crate::util::WriteVAdapter;
        self.tls_session
            .writev_tls(&mut WriteVAdapter::new(&mut self.socket))
            .unwrap();
    }

    fn register(&self, poll: &mut mio::Poll) {
        poll.register(
            &self.socket,
            CLIENT,
            self.ready_interest(),
            mio::PollOpt::level() | mio::PollOpt::oneshot(),
        )
        .unwrap();
    }

    fn reregister(&self, poll: &mut mio::Poll) {
        poll.reregister(
            &self.socket,
            CLIENT,
            self.ready_interest(),
            mio::PollOpt::level() | mio::PollOpt::oneshot(),
        )
        .unwrap();
    }

    // Use wants_read/wants_write to register for different mio-level
    // IO readiness events.
    fn ready_interest(&self) -> mio::Ready {
        let rd = self.tls_session.wants_read();
        let wr = self.tls_session.wants_write();

        if rd && wr {
            mio::Ready::readable() | mio::Ready::writable()
        } else if wr {
            mio::Ready::writable()
        } else {
            mio::Ready::readable()
        }
    }

    fn is_closed(&self) -> bool {
        self.closing
    }
}

fn connect_tcp(
    params: &ConnectParams,
) -> std::io::Result<(TcpStream, Arc<rustls::ClientConfig>, rustls::ClientSession)> {
    debug!("connect_tcp(): Connecting to {:?}", params.addr());

    let mut config = rustls::ClientConfig::new();
    for server_cert in params.server_certs() {
        match server_cert {
            ServerCerts::None => {
                config
                    .dangerous()
                    .set_certificate_verifier(Arc::new(NoCertificateVerification {}));
            }
            ServerCerts::RootCertificates => {
                config
                    .root_store
                    .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
            }
            ServerCerts::Direct(pem) => {
                let mut cursor = std::io::Cursor::new(pem);
                let (n_ok, n_err) = config
                    .root_store
                    .add_pem_file(&mut cursor)
                    .unwrap_or((0, 0));
                if n_ok == 0 {
                    info!("None of the directly provided server certificates was accepted");
                } else if n_err > 0 {
                    info!("Not all directly provided server certificates were accepted");
                }
            }
            ServerCerts::Environment(env_var) => match std::env::var(env_var) {
                Ok(value) => {
                    let mut cursor = std::io::Cursor::new(value);
                    let (n_ok, n_err) = config
                        .root_store
                        .add_pem_file(&mut cursor)
                        .unwrap_or((0, 0));
                    if n_ok == 0 {
                        info!("None of the env-provided server certificates was accepted");
                    } else if n_err > 0 {
                        info!("Not all env-provided server certificates were accepted");
                    }
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Environment variable {} not found, reason: {}", env_var, e),
                    ));
                }
            },
            ServerCerts::Directory(trust_anchor_dir) => {
                #[allow(clippy::filter_map)]
                let trust_anchor_files: Vec<std::path::PathBuf> =
                    std::fs::read_dir(trust_anchor_dir)?
                        .filter_map(Result::ok)
                        .filter(|dir_entry| {
                            dir_entry.file_type().is_ok()
                                && dir_entry.file_type().unwrap().is_file()
                        })
                        .filter(|dir_entry| {
                            let path = dir_entry.path();
                            let ext = path.extension();
                            Some(AsRef::<std::ffi::OsStr>::as_ref("pem")) == ext
                        })
                        .map(|dir_entry| dir_entry.path())
                        .collect();

                let mut t_ok = 0;
                let mut t_err = 0;
                for trust_anchor_file in trust_anchor_files {
                    trace!("Trying trust anchor file {:?}", trust_anchor_file);
                    let mut rd = std::io::BufReader::new(std::fs::File::open(trust_anchor_file)?);
                    let (n_ok, n_err) = config.root_store.add_pem_file(&mut rd).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "server certificates in directory could not be parsed",
                        )
                    })?;
                    t_ok += n_ok;
                    t_err += n_err;
                }
                if t_ok == 0 {
                    warn!("None of the server certificates in the directory was accepted");
                } else if t_err > 0 {
                    warn!("Not all server certificates in the directory were accepted");
                }
            }
        }
    }

    let tlsconfig = Arc::new(config);

    let tcpstream = TcpStream::connect(&params.addr().to_socket_addrs().unwrap().next().unwrap())?;
    trace!("tcpstream working");

    let tlssession = rustls::ClientSession::new(
        &tlsconfig,
        webpki::DNSNameRef::try_from_ascii_str(params.host()).map_err(|_| {
            crate::protocol::util::io_error(format!("Cannot use {} for DNSNameRef", params.host()))
        })?,
    );

    Ok((tcpstream, tlsconfig, tlssession))
}

struct NoCertificateVerification {}
impl rustls::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _roots: &rustls::RootCertStore,
        _presented_certs: &[rustls::Certificate],
        _dns_name: webpki::DNSNameRef<'_>,
        _ocsp: &[u8],
    ) -> Result<rustls::ServerCertVerified, rustls::TLSError> {
        Ok(rustls::ServerCertVerified::assertion())
    }
}

/// This is an example cache for client session data.
/// It optionally dumps cached data to a file, but otherwise
/// is just in-memory.
///
/// Note that the contents of such a file are extremely sensitive.
/// Don't write this stuff to disk in production code.
struct PersistCache {
    cache: Mutex<collections::HashMap<Vec<u8>, Vec<u8>>>,
    filename: Option<String>,
}

impl PersistCache {
    /// Make a new cache.  If filename is Some, load the cache
    /// from it and flush changes back to that file.
    fn new(filename: &Option<String>) -> PersistCache {
        let cache = PersistCache {
            cache: Mutex::new(collections::HashMap::new()),
            filename: filename.clone(),
        };
        if cache.filename.is_some() {
            cache.load();
        }
        cache
    }

    /// If we have a filename, save the cache contents to it.
    fn save(&self) {
        use rustls::internal::msgs::base::PayloadU16;
        use rustls::internal::msgs::codec::Codec;

        if self.filename.is_none() {
            return;
        }

        let mut file =
            fs::File::create(self.filename.as_ref().unwrap()).expect("cannot open cache file");

        for (key, val) in self.cache.lock().unwrap().iter() {
            let mut item = Vec::new();
            let key_pl = PayloadU16::new(key.clone());
            let val_pl = PayloadU16::new(val.clone());
            key_pl.encode(&mut item);
            val_pl.encode(&mut item);
            file.write_all(&item).unwrap();
        }
    }

    /// We have a filename, so replace the cache contents from it.
    fn load(&self) {
        use rustls::internal::msgs::base::PayloadU16;
        use rustls::internal::msgs::codec::{Codec, Reader};

        let mut file = match fs::File::open(self.filename.as_ref().unwrap()) {
            Ok(f) => f,
            Err(_) => return,
        };
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();

        let mut cache = self.cache.lock().unwrap();
        cache.clear();
        let mut rd = Reader::init(&data);

        while rd.any_left() {
            let key_pl = PayloadU16::read(&mut rd).unwrap();
            let val_pl = PayloadU16::read(&mut rd).unwrap();
            cache.insert(key_pl.0, val_pl.0);
        }
    }
}

impl rustls::StoresClientSessions for PersistCache {
    /// put: insert into in-memory cache, and perhaps persist to disk.
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        self.cache.lock().unwrap().insert(key, value);
        self.save();
        true
    }

    /// get: from in-memory cache
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.cache.lock().unwrap().get(key).cloned()
    }
}

fn lookup_ipv4(host: &str, port: u16) -> SocketAddr {
    let addrs = (host, port).to_socket_addrs().unwrap();
    for addr in addrs {
        if let SocketAddr::V4(_) = addr {
            return addr;
        }
    }

    unreachable!("Cannot lookup address");
}

/// Find a ciphersuite with the given name
fn find_suite(name: &str) -> Option<&'static rustls::SupportedCipherSuite> {
    for suite in &rustls::ALL_CIPHERSUITES {
        if format!("{:?}", suite.suite).to_lowercase() == name.to_string().to_lowercase() {
            return Some(suite);
        }
    }

    None
}

/// Make a vector of ciphersuites named in `suites`
fn lookup_suites(suites: &[String]) -> Vec<&'static rustls::SupportedCipherSuite> {
    let mut out = Vec::new();

    for csname in suites {
        let scs = find_suite(csname);
        match scs {
            Some(s) => out.push(s),
            None => panic!("cannot look up ciphersuite '{}'", csname),
        }
    }

    out
}

/// Make a vector of protocol versions named in `versions`
fn lookup_versions(versions: &[String]) -> Vec<rustls::ProtocolVersion> {
    let mut out = Vec::new();

    for vname in versions {
        let version = match vname.as_ref() {
            "1.2" => rustls::ProtocolVersion::TLSv1_2,
            "1.3" => rustls::ProtocolVersion::TLSv1_3,
            _ => panic!(
                "cannot look up version '{}', valid are '1.2' and '1.3'",
                vname
            ),
        };
        out.push(version);
    }

    out
}

fn load_certs(filename: &str) -> Vec<rustls::Certificate> {
    let certfile = fs::File::open(filename).expect("cannot open certificate file");
    let mut reader = BufReader::new(certfile);
    rustls::internal::pemfile::certs(&mut reader).unwrap()
}

fn load_private_key(filename: &str) -> rustls::PrivateKey {
    let keyfile = fs::File::open(filename).expect("cannot open private key file");
    let mut reader = BufReader::new(keyfile);
    let keys = rustls::internal::pemfile::rsa_private_keys(&mut reader).unwrap();
    assert!(keys.len() == 1);
    keys[0].clone()
}

fn load_key_and_cert(config: &mut rustls::ClientConfig, keyfile: &str, certsfile: &str) {
    let certs = load_certs(certsfile);
    let privkey = load_private_key(keyfile);

    config.set_single_client_cert(certs, privkey);
}

mod danger {
    use super::rustls;
    use webpki;

    pub struct NoCertificateVerification {}

    impl rustls::ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _roots: &rustls::RootCertStore,
            _presented_certs: &[rustls::Certificate],
            _dns_name: webpki::DNSNameRef<'_>,
            _ocsp: &[u8],
        ) -> Result<rustls::ServerCertVerified, rustls::TLSError> {
            Ok(rustls::ServerCertVerified::assertion())
        }
    }
}

mod util {
    use rustls;
    use vecio::Rawv;
    /// This glues our `rustls::WriteV` trait to `vecio::Rawv`.
    pub struct WriteVAdapter<'a> {
        rawv: &'a mut dyn Rawv,
    }
    impl<'a> WriteVAdapter<'a> {
        pub fn new(rawv: &'a mut dyn Rawv) -> WriteVAdapter<'a> {
            WriteVAdapter { rawv }
        }
    }
    impl<'a> rustls::WriteV for WriteVAdapter<'a> {
        fn writev(&mut self, bytes: &[&[u8]]) -> std::io::Result<usize> {
            self.rawv.writev(bytes)
        }
    }
}
