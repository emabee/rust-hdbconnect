use super::protocol::authentication::authenticate_with_scram_sha256;
use super::protocol::plain_statement;

use super::protocol::lowlevel::init;
use super::protocol::lowlevel::connect_option::*;
use super::protocol::Message;
use super::protocol::ResultSet;
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
    let (mut connect_options, mut topology_attributes, session_id) =
        try!(authenticate_with_scram_sha256(&mut conn.stream, username, password));
    conn.session_id = session_id;
    conn.add_connect_options(&mut connect_options);
    conn.add_topology_info(&mut topology_attributes);
    debug!("successfully logged on as user \"{}\" at {}:{} in  {} µs",
            username, host, port, (time::now() - start).num_microseconds().unwrap());
    Ok(conn)
}

/// Connection object
#[derive(Debug)]
pub struct Connection {
    pub host: String,
    pub port: String,
    session_id: i64,
    seq_number: i32,
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
            session_id: 0,
            seq_number: 0,
            stream: stream,
            major_product_version: major_product_version,
            minor_product_version: minor_product_version,
            connect_options: Vec::<ConnectOption>::new(),
            topology_attributes: Vec::<TopologyAttr>::new(),
        }
    }

    fn add_connect_options(&mut self, cos: &Vec<ConnectOption>) {
        for co in cos {
            self.connect_options.push(co.clone());  // FIXME avoid cloning!
        }
    }
    pub fn get_connect_options(&self) -> &Vec<ConnectOption> {
        &(self.connect_options)
    }
    fn add_topology_info(&mut self, tas: &mut Vec<TopologyAttr>) {
        for ta in tas {
            self.topology_attributes.push(ta.clone());  // FIXME avoid cloning!
        }
    }
    pub fn get_topology_info(&self) -> &Vec<TopologyAttr> {
        &(self.topology_attributes)
    }

    pub fn execute_statement(&mut self, stmt: String, auto_commit: bool) -> io::Result<ResultSet> {
        let msg = self.new_message();
        plain_statement::execute(&mut self.stream, msg, stmt, auto_commit)
    }

    fn new_message(&mut self) -> Message {
        self.seq_number += 1;
        Message::new(self.session_id, self.seq_number)
    }
}
