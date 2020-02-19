use crate::ConnectParams;
use mio::tcp::TcpStream;
use rustls;
use rustls::Session;
use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::process;
use std::sync::Arc;
use webpki;

const CLIENT: mio::Token = mio::Token(0);

/// This encapsulates the TCP-level connection, some connection
/// state, and the underlying TLS-level session.
#[derive(Debug)]
pub struct NonblockingTlsClient {
    params: ConnectParams,
    socket: TcpStream,
    closing: bool,
    clean_closure: bool,
    tls_session: rustls::ClientSession,
}

impl NonblockingTlsClient {
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
impl std::io::Write for NonblockingTlsClient {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.tls_session.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.tls_session.flush()
    }
}

impl std::io::Read for NonblockingTlsClient {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.tls_session.read(bytes)
    }
}

impl NonblockingTlsClient {
    pub fn try_new(params: ConnectParams) -> std::io::Result<Self> {
        let (socket, _tlsconfig, tls_session) = connect_tcp(&params)?;

        let mut poll = mio::Poll::new().unwrap();
        let mut events = mio::Events::with_capacity(32);

        let mut tlsclient = Self {
            socket,
            params,
            closing: false,
            clean_closure: false,
            tls_session,
        };

        tlsclient.register(&mut poll);
        // loop {
        println!("LOOP STEP 1");
        poll.poll(&mut events, None).unwrap();

        println!("LOOP STEP 2");
        for ev in events.iter() {
            println!("LOOP STEP 3, {:?}", &ev);
            tlsclient.ready(&mut poll, &ev);
        }
        // }

        Ok(tlsclient)
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

    let tcpstream = TcpStream::connect(&params.addr().to_socket_addrs().unwrap().next().unwrap())?;
    trace!("tcpstream working");

    let tlsconfig = Arc::new(params.rustls_clientconfig()?);
    let tlssession = rustls::ClientSession::new(
        &tlsconfig,
        webpki::DNSNameRef::try_from_ascii_str(params.host()).map_err(|_| {
            crate::protocol::util::io_error(format!("Cannot use {} for DNSNameRef", params.host()))
        })?,
    );

    Ok((tcpstream, tlsconfig, tlssession))
}

#[cfg(not(target_os = "windows"))]
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
