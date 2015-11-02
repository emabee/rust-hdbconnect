use super::protocol::authentication::authenticate_with_scram_sha256;
use super::protocol::plain_statement;

use super::protocol::lowlevel::init;
use super::protocol::lowlevel::connect_option::*;
use super::protocol::ResultSet;
use super::protocol::lowlevel::topology_attribute::*;

use chrono::Local;
use std::io;
use std::net::TcpStream;
use std::ops::{Add};


/// Connection object
#[derive(Debug)]
pub struct Connection {
    pub host: String,
    pub port: String,
    pub major_product_version: i8,
    pub minor_product_version: i16,
    state: ConnectionState,             //see below
    connect_options: Vec<ConnectOption>,
    topology_attributes: Vec<TopologyAttr>,
}

impl Connection {
    /// static factory: does low-level connect and login
    pub fn init(host: &str, port: &str, username: &str, password: &str) -> io::Result<Connection> {
        trace!("Entering connect()");
        let start = Local::now();
        let connect_string: &str = &(String::with_capacity(200).add(&host).add(":").add(&port));
        let mut tcpstream = try!(TcpStream::connect(connect_string).map_err(|e|{io::Error::from(e)}));
        trace!("tcpstream is open");

        let (major,minor) = try!(init::send_and_receive(&mut tcpstream));
        trace!("connection is initialized");
        let (connect_options, topology_attributes, session_id) =
            try!(authenticate_with_scram_sha256(&mut tcpstream, username, password));
        debug!("successfully logged on as user \"{}\" at {}:{} in  {} µs",
                username, host, port, (Local::now() - start).num_microseconds().unwrap());
        Ok( Connection::new(host, port, major, minor, session_id, tcpstream, connect_options, topology_attributes) )
    }

    fn new( host: &str, port: &str, major: i8, minor: i16,
            session_id: i64, stream: TcpStream,
            connect_options: Vec<ConnectOption>, topology_attributes: Vec<TopologyAttr> )
        -> Connection {
        Connection {
            host: String::new().add(host),
            port: String::new().add(port),
            major_product_version: major,
            minor_product_version: minor,
            state: ConnectionState::new( session_id, stream),
            connect_options: connect_options,
            topology_attributes: topology_attributes
        }
    }

    pub fn get_connect_options(&self) -> &Vec<ConnectOption> {
        &(self.connect_options)
    }
    pub fn get_topology_info(&self) -> &Vec<TopologyAttr> {
        &(self.topology_attributes)
    }

    pub fn execute_statement(&mut self, stmt: String, auto_commit: bool) -> io::Result<ResultSet> {
        plain_statement::execute(&mut self.state, stmt, auto_commit)
    }
}

#[derive(Debug)]
pub struct ConnectionState {
    pub session_id: i64,
    seq_number: i32,
    pub stream: TcpStream,
}

impl ConnectionState {
    pub fn new(session_id: i64, stream: TcpStream) -> ConnectionState{
        ConnectionState{ session_id: session_id, seq_number: 0, stream: stream }
    }

    pub fn get_next_seq_number(&mut self) -> i32 {
        self.seq_number += 1;
        self.seq_number
    }
}
