use super::protocol::lowlevel::init;
use super::protocol::lowlevel::conn_core::{ConnectionCore, ConnRef};
use super::protocol::lowlevel::connect_option::ConnectOption;
use super::protocol::lowlevel::topology_attribute::TopologyAttr;

use super::protocol::authentication::authenticate;
use super::callable_statement::CallableStatement;
use DbcResult;

use chrono::Local;
use std::net::TcpStream;
use std::ops::{Add};

/// Connection object
#[derive(Debug)]
pub struct Connection {
    pub host: String,
    pub port: String,
    pub major_product_version: i8,
    pub minor_product_version: i16,
    core: ConnRef,
    connect_options: Vec<ConnectOption>,
    topology_attributes: Vec<TopologyAttr>,
}

impl Connection {
    /// static factory: does low-level connect and login
    pub fn new(host: &str, port: &str, username: &str, password: &str) -> DbcResult<Connection> {
        trace!("Entering connect()");
        let start = Local::now();

        let connect_string = String::with_capacity(200).add(host).add(":").add(port);
        let mut tcp_stream = try!(TcpStream::connect(&connect_string as &str));
        trace!("tcp_stream is open");

        let (major,minor) = try!(init::send_and_receive(&mut tcp_stream));
        trace!("connection is initialized");

        let conn_ref = ConnectionCore::new_conn_ref(tcp_stream);
        let (conn_opts, topology_attrs) = try!(authenticate(&conn_ref, username, password));
        let delta = match (Local::now() - start).num_microseconds() {Some(m) => m, None => -1};
        debug!("successfully logged on as user \"{}\" at {}:{} in  {} µs", username, host, port, delta);

        Ok(Connection {
            host: String::from(host),
            port: String::from(port),
            major_product_version: major,
            minor_product_version: minor,
            core: conn_ref,
            connect_options: conn_opts,
            topology_attributes: topology_attrs
        })
    }

    pub fn set_fetch_size(&mut self, fetch_size: u32) {
        self.core.borrow_mut().set_fetch_size(fetch_size);
    }
    pub fn get_call_count(&self) -> i32 {
        self.core.borrow().last_seq_number()
    }
    pub fn get_connect_options(&self) -> &Vec<ConnectOption> {
        &(self.connect_options)
    }
    pub fn get_topology_info(&self) -> &Vec<TopologyAttr> {
        &(self.topology_attributes)
    }

    pub fn prepare_call(&self, stmt: String) -> DbcResult<CallableStatement> {
        CallableStatement::new(self.core.clone(), stmt)
    }
}
