use adhoc_statement::AdhocStatement;
use prepared_statement::PreparedStatement;
use {DbcResult,DbResponse};

use protocol::authenticate;
use protocol::lowlevel::conn_core::{ConnectionCore, ConnRef};
use protocol::lowlevel::init;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::parts::connect_option::ConnectOption;
use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::rows_affected::RowsAffected;
use protocol::lowlevel::parts::topology_attribute::TopologyAttr;
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;

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
#[derive(Clone,Debug)]
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

    fn disconnect(&mut self) -> DbcResult<()> {
        trace!("Entering Connection.disconnect()");
        let mut request = try!(Request::new( &(self.core), RequestType::Disconnect, false, 0));
        try!(request.send_and_receive(&(self.core), Some(ReplyType::Disconnect)));
        self.core.borrow_mut().session_id = 0;
        Ok(())
    }

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
    pub fn execute_or_query(&self, stmt: &str) -> DbcResult<DbResponse> {
        AdhocStatement::new(self.core.clone(), String::from(stmt), self.props.auto_commit)
        .execute()
    }
    /// Execute a statement and expect a ResultSet
    pub fn query(&self, stmt: &str) -> DbcResult<ResultSet> {
        try!(self.execute_or_query(stmt)).as_resultset()
    }
    /// Execute a statement and expect a RowsAffected
    pub fn execute(&self, stmt: &str) -> DbcResult<Vec<RowsAffected>> {
        try!(self.execute_or_query(stmt)).as_rows_affected()
    }

    /// Prepare a statement
    pub fn prepare(&self, stmt: &str) -> DbcResult<PreparedStatement> {
        PreparedStatement::prepare(self.core.clone(), String::from(stmt))
    }

    // pub fn commit(&self, stmt: String) -> DbcResult<()> {
    //     panic!("FIXME");
    // }
    //
    // pub fn rollback(&self, stmt: String) -> DbcResult<()> {
    //     panic!("FIXME");
    // }
    //
    // pub fn transaction(&self) -> DbcResult<Transaction> ???

    pub fn spawn(&self) -> DbcResult<Connection> {
        self.core.borrow_mut().increment_ref_count();
        Ok(Connection {
            host: self.host.clone(),
            port: self.port.clone(),
            major_product_version: self.major_product_version.clone(),
            minor_product_version: self.minor_product_version.clone(),
            core: self.core.clone(),
            props: self.props.clone(),
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.core.borrow_mut().decrement_ref_count();
        trace!("Entering Connection.drop()");
        if self.core.borrow().is_last_ref() {
            self.disconnect().ok();
        }
    }
}
