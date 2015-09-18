use super::protocol::authentication::*;

use super::protocol::lowlevel::init;
use super::protocol::lowlevel::message::*;

use std::io;
use std::net::TcpStream;
use std::ops::{Add};

/// static factory: does low-level connect and login
pub fn connect(host: &str, port: &str, username: &str, password: &str)
               -> io::Result<Connection> {
    trace!("Entering connect()");
    let connect_string: &str = &(String::with_capacity(200).add(&host).add(":").add(&port));
    let mut tcpstream = try!(TcpStream::connect(connect_string).map_err(|e|{io::Error::from(e)}));
    trace!("tcpstream is open");

    let (major,minor) = try!(init::send_and_receive(&mut tcpstream));
    let mut conn = Connection {
        host: String::new().add(host),
        port: String::new().add(port),
        stream: tcpstream,
        major_product_version: major,
        minor_product_version: minor,
    };
    trace!("successfully initialized connection {:?}",conn);
    try!(conn.login(username, password));
    debug!("successfully logged on with connection");
    Ok(conn)
}

/// Connection object
#[derive(Debug)]
pub struct Connection {
    pub host: String,
    pub port: String,
    stream: TcpStream,
    pub major_product_version: i8,
    pub minor_product_version: i16,
}

impl Connection {
    fn login(&mut self, username: &str, password: &str) -> io::Result<()>{
        trace!("Entering login()");
        scram_sha256(&mut self.stream, username, password)
    }
}
