use super::lowlevel::init;
use super::lowlevel::message::*;

use std::io::{Error,Result};
use std::net::TcpStream;
use std::ops::{Add};


pub fn db_connect(host: &str, port: &str) -> Result<DbStream>  {
    trace!("Entering db_connect()");
    let connstr: &str = &(String::with_capacity(200).add(&host).add(":").add(&port));

    match TcpStream::connect(connstr) {
        Err(e) => Err(Error::from(e)),
        Ok(tcpstream) => {
            trace!("tcpstream is open");
            let mut dbstream = DbStream { stream: tcpstream, major_product_version: 0, minor_product_version: 0};
            try!(dbstream.init());
            Ok(dbstream)
        },
    }
}

/// Convenience Wrapper for the TcpStream
#[derive(Debug)]
#[allow(unused_variables)]
pub struct DbStream {
    stream: TcpStream,
    major_product_version: i8,
    minor_product_version: i16,
}

impl DbStream {

    /// does the initial handshake and fills the major and minor product version
    fn init(&mut self) -> Result<()> {
        trace!("Entering DbStream::init()");
        let resp = try!(init::send_and_receive(&mut self.stream));
        self.major_product_version = resp.major;
        self.minor_product_version = resp.minor;
        Ok(())
    }

    /// for all other requests
    pub fn send_and_receive(&mut self, msg: &mut Message) -> Result<Message> {
        msg.send(&mut self.stream)
    }
}
