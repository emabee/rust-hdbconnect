use super::protocol::authentication::*;

use super::protocol::lowlevel::init;
use super::protocol::lowlevel::connect_option::*;
use super::protocol::lowlevel::message::*;
use super::protocol::lowlevel::topology_attribute::*;

use std::io;
use std::net::TcpStream;
use std::ops::{Add};
use time;

/// static factory: does low-level connect and login
pub fn connect(host: &str, port: &str, username: &str, password: &str)
               -> io::Result<Connection> {
    trace!("Entering connect()");
    let start = time::now();
    let connect_string: &str = &(String::with_capacity(200).add(&host).add(":").add(&port));
    let mut tcpstream = try!(TcpStream::connect(connect_string).map_err(|e|{io::Error::from(e)}));
    trace!("tcpstream is open");

    let (major,minor) = try!(init::send_and_receive(&mut tcpstream));
    let mut conn = Connection::new(String::new().add(host),  String::new().add(port), tcpstream, major, minor);
    trace!("connection is initialized {:?}",conn);
    let (mut connect_options, mut topology_attributes, server_proof) =
        try!(authenticate_with_scram_sha256(&mut conn.stream, username, password));
    conn.add_connect_options(&mut connect_options);
    conn.add_topology_info(&mut topology_attributes);
    trace!("don't know what to do with the server proof: {:?}", server_proof);
    info!("successfully logged on as user \"{}\" at {}:{} in  {} µs",
            username, host, port, (time::now() - start).num_microseconds().unwrap());
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
    connect_options: Vec<ConnectOption>,        // FIXME should be a map
    topology_attributes: Vec<TopologyAttr>,     // FIXME should be a map
}
impl Connection {
    fn new(host: String, port: String, stream: TcpStream, major_product_version: i8, minor_product_version: i16)
        -> Connection {
        Connection {
            host: host,
            port: port,
            stream: stream,
            major_product_version: major_product_version,
            minor_product_version: minor_product_version,
            connect_options: Vec::<ConnectOption>::new(),
            topology_attributes: Vec::<TopologyAttr>::new(),
        }
    }

    fn add_connect_options(&mut self, cos: &Vec<ConnectOption>) {
        for co in cos {
            self.connect_options.push(co.clone());
        }
    }
    pub fn get_connect_options(&self) -> &Vec<ConnectOption> {
        &(self.connect_options)
    }
    fn add_topology_info(&mut self, tas: &mut Vec<TopologyAttr>) {
        for ta in tas {
            self.topology_attributes.push(ta.clone());
        }
    }
    pub fn get_topology_info(&self) -> &Vec<TopologyAttr> {
        &(self.topology_attributes)
    }
}
