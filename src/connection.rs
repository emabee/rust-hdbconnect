use callable_statement::CallableStatement;
use prepared_statement::PreparedStatement;
use {DbcResult,DbResult};

use protocol::authenticate;
use protocol::lowlevel::conn_core::{ConnectionCore, ConnRef};
use protocol::lowlevel::init;
use protocol::lowlevel::parts::connect_option::ConnectOption;
use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::rows_affected::RowsAffected;
use protocol::lowlevel::parts::topology_attribute::TopologyAttr;

use chrono::Local;
use std::net::TcpStream;
use std::ops::{Add};

const HOLD_OVER_COMMIT: u8 = 8;

/// Connection object
#[derive(Debug)]
pub struct Connection {
    pub host: String,
    pub port: String,
    pub major_product_version: i8,
    pub minor_product_version: i16,
    core: ConnRef,
    props: ConnProps,
}
#[derive(Debug)]
pub struct ConnProps {
    pub auto_commit: bool,
    pub command_options: u8,
    pub connect_options: Vec<ConnectOption>,
    pub topology_attributes: Vec<TopologyAttr>,
}

impl Connection {
    /// static factory: does low-level connect and login
    pub fn new(host: &str, port: &str) -> DbcResult<Connection> {
        trace!("Entering connect()");
        let start = Local::now();

        let connect_string = String::with_capacity(200).add(host).add(":").add(port);
        let mut tcp_stream = try!(TcpStream::connect(&connect_string as &str));
        trace!("tcp_stream is open");

        let (major,minor) = try!(init::send_and_receive(&mut tcp_stream));
        trace!("connection is initialized");

        let conn_ref = ConnectionCore::new_conn_ref(tcp_stream);
        let delta = match (Local::now() - start).num_microseconds() {Some(m) => m, None => -1};
        debug!("connection to {}:{} is initialized in {} µs", host, port, delta);

        Ok(Connection {
            host: String::from(host),
            port: String::from(port),
            major_product_version: major,
            minor_product_version: minor,
            core: conn_ref,
            props: ConnProps{
                auto_commit: true,
                command_options: HOLD_OVER_COMMIT,
                connect_options: Vec::<ConnectOption>::new(),
                topology_attributes: Vec::<TopologyAttr>::new()
            }
        })
    }

    pub fn authenticate_user_password(&mut self, username: &str, password: &str) -> DbcResult<()> {
        let start = Local::now();
        try!(authenticate::user_pw(&(self.core), &mut (self.props), username, password));
        let delta = match (Local::now() - start).num_microseconds() {Some(m) => m, None => -1};
        debug!("successfully logged on as user \"{}\" in {} µs", username, delta);
        Ok(())
    }

    //fn disconnect(&mut self) {
        // FIXME implement disconnect
        // should be done on drop (only?)
        // #Logout-request
        // request = Request { session_id: ..., msg_type: Disconnect, auto_commit: false, command_options: 0, parts: [] }
        // #Logout-response
        // reply = Reply { session_id: ..., function_code: Disconnect, parts: [] }
    //}

    pub fn set_auto_commit(&mut self, ac: bool) {
        self.props.auto_commit = ac;
    }
    pub fn set_fetch_size(&mut self, fetch_size: u32) {
        self.core.borrow_mut().set_fetch_size(fetch_size);
    }
    pub fn get_call_count(&self) -> i32 {
        self.core.borrow().last_seq_number()
    }
    pub fn get_connect_options(&self) -> &Vec<ConnectOption> {
        &(self.props.connect_options)
    }
    pub fn get_topology_info(&self) -> &Vec<TopologyAttr> {
        &(self.props.topology_attributes)
    }

    /// Execute a statement and expect either a ResultSet or a RowsAffected
    pub fn call_direct(&self, stmt: String) -> DbcResult<DbResult> {
        CallableStatement::new(self.core.clone(), stmt, self.props.auto_commit)
        .execute()
    }
    /// Execute a statement and expect a ResultSet
    pub fn query_direct(&self, stmt: String) -> DbcResult<ResultSet> {
        try!(self.call_direct(stmt)).as_resultset()
    }
    /// Execute a statement and expect a RowsAffected
    pub fn execute_direct(&self, stmt: String) -> DbcResult<Vec<RowsAffected>> {
        try!(self.call_direct(stmt)).as_rows_affected()
    }

    /// Prepare a statement
    pub fn prepare(&self, stmt: String) -> DbcResult<PreparedStatement> {
        PreparedStatement::prepare(self.core.clone(), stmt, self.props.auto_commit, self.props.command_options)
    }

    // pub fn commit(&self, stmt: String) -> DbcResult<()> {
    //     panic!("FIXME");
    // }
    //
    // pub fn rollback(&self, stmt: String) -> DbcResult<()> {
    //     panic!("FIXME");
    // }
    //
    // pub fn transaction(&self) -> DbcResult<Transaction> {
    //     panic!("FIXME");
    // }
    // - set_commit()
    // - set_rollback()
}
