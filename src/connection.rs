use adhoc_statement::AdhocStatement;
use prepared_statement::PreparedStatement;
use {DbcResult,DbResponse};

use protocol::authenticate;
use protocol::lowlevel::conn_core::{ConnectionCore, ConnRef};
use protocol::lowlevel::init;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::rows_affected::RowsAffected;
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;

use chrono::Local;
use std::net::TcpStream;
use std::ops::{Add};

const HOLD_OVER_COMMIT: u8 = 8;

/// Connection object
#[derive(Debug)]
pub struct Connection {
    host: String,
    port: String,
    username: String,
    password: String,
    pub major_product_version: i8,
    pub minor_product_version: i16,
    auto_commit: bool,
    command_options: u8,
    core: ConnRef,
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
            username: String::new(),
            password: String::new(),
            major_product_version: major,
            minor_product_version: minor,
            auto_commit: true,
            command_options: HOLD_OVER_COMMIT,
            core: conn_ref,
        })
    }

    pub fn authenticate_user_password(&mut self, username: &str, password: &str) -> DbcResult<()> {
        let start = Local::now();
        try!(authenticate::user_pw(&(self.core), username, password));
        self.username = String::from(username);
        self.password = String::from(password);
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
        self.auto_commit = ac;
    }
    pub fn set_fetch_size(&mut self, fetch_size: u32) {
        self.core.borrow_mut().set_fetch_size(fetch_size);
    }
    pub fn set_lob_read_length(&mut self, lob_read_length: i32) {
        self.core.borrow_mut().set_lob_read_length(lob_read_length);
    }

    pub fn get_call_count(&self) -> i32 {
        self.core.borrow().last_seq_number()
    }


    /// Execute a statement and expect either a ResultSet or a RowsAffected
    pub fn execute_or_query(&self, stmt: &str) -> DbcResult<DbResponse> {
        AdhocStatement::new(self.core.clone(), String::from(stmt), self.auto_commit)
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
        let stmt = try!(PreparedStatement::prepare(self.core.clone(), String::from(stmt), self.auto_commit));
        debug!("PreparedStatement created with auto_commit = {}", stmt.auto_commit);
        Ok(stmt)
    }

    pub fn commit(&self) -> DbcResult<Vec<RowsAffected>> {
        self.execute("commit")
    }

    pub fn rollback(&self) -> DbcResult<Vec<RowsAffected>> {
        self.execute("rollback")
    }

    // pub fn transaction(&self) -> DbcResult<Transaction> ???

    pub fn spawn(&self) -> DbcResult<Connection> {
        let mut other_conn = try!(Connection::new(&(self.host),&(self.port)));
        try!(other_conn.authenticate_user_password(&(self.username),&(self.password)));
        other_conn.auto_commit = self.auto_commit;
        other_conn.command_options = self.command_options;
        other_conn.set_fetch_size(self.core.borrow().get_fetch_size());
        other_conn.set_lob_read_length(self.core.borrow().get_lob_read_length());
        Ok(other_conn)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        trace!("Entering Connection.drop()");
        self.disconnect().ok();
    }
}
