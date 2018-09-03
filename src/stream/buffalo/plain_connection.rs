use std::cell::RefCell;
use std::fmt::Debug;
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::ops::Deref;

pub struct PlainConnection<A: ToSocketAddrs + Debug> {
    addr: Box<A>,
    reader: RefCell<io::BufReader<TcpStream>>,
    writer: RefCell<io::BufWriter<TcpStream>>,
}

impl<A: ToSocketAddrs + Debug> PlainConnection<A> {
    /// Returns an initialized plain tcp connection
    pub fn new(addr: A) -> io::Result<(PlainConnection<A>)> {
        let tcpstream = TcpStream::connect(&addr)?;
        let conn = PlainConnection {
            addr: Box::new(addr),
            writer: RefCell::new(io::BufWriter::new(tcpstream.try_clone()?)),
            reader: RefCell::new(io::BufReader::new(tcpstream)),
        };

        Ok(conn)
    }

    pub fn writer(&self, reconnect: bool) -> io::Result<&RefCell<io::Write>> {
        if reconnect {
            self.reconnect().unwrap();
        }
        Ok(&self.writer)
    }

    pub fn reader(&self) -> &RefCell<io::BufRead> {
        &self.reader
    }

    pub fn reconnect(&self) -> io::Result<()> {
        let tcpstream = TcpStream::connect(self.addr.deref())?;
        self.writer
            .replace(io::BufWriter::new(tcpstream.try_clone()?));
        self.reader.replace(io::BufReader::new(tcpstream));
        Ok(())
    }
}
