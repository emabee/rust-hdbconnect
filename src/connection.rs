use adhoc_statement::AdhocStatement;
use prepared_statement::PreparedStatement;
use {DbcError,DbcResult,DbResponses};

use protocol::authenticate;
use protocol::lowlevel::conn_core::{ConnectionCore, ConnRef};
use protocol::lowlevel::init;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::parts::resultset::ResultSet;
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
    credentials: Option<Credentials>,
    pub major_product_version: i8,
    pub minor_product_version: i16,
    auto_commit: bool,
    command_options: u8,
    core: ConnRef,
}
impl Connection {
    /// Creates a new connection object that is already TCP/IP-connected to the specified host/port
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
            credentials: None,
            major_product_version: major,
            minor_product_version: minor,
            auto_commit: true,
            command_options: HOLD_OVER_COMMIT,
            core: conn_ref,
        })
    }

    /// Authenticates with username and password
    pub fn authenticate_user_password(&mut self, username: &str, password: &str) -> DbcResult<()> {
        if self.is_authenticated() {
            return Err(DbcError::UsageError("Re-authentication not possible, create a new Connection instead"));
        }
        let start = Local::now();
        try!(authenticate::user_pw(&(self.core), username, password));
        self.credentials = Some(Credentials {
            username: String::from(username),
            password: String::from(password),
        });
        let delta = match (Local::now() - start).num_microseconds() {Some(m) => m, None => -1};
        debug!("successfully logged on as user \"{}\" in {} µs", username, delta);
        Ok(())
    }

    /// Configures the connection's auto-commit behavior for future calls
    pub fn set_auto_commit(&mut self, ac: bool) {
        self.auto_commit = ac;
    }
    /// Configures the connection's fetch size for future calls
    pub fn set_fetch_size(&mut self, fetch_size: u32) {
        self.core.borrow_mut().set_fetch_size(fetch_size);
    }
    /// Configures the connection's lob read length for future calls
    pub fn set_lob_read_length(&mut self, lob_read_length: i32) {
        self.core.borrow_mut().set_lob_read_length(lob_read_length);
    }

    /// Returns the number of roundtrips to DB that have been done through this connection
    pub fn get_call_count(&self) -> i32 {
        self.core.borrow().last_seq_number()
    }


    /// Executes a statement on the database
    ///
    /// This generic method can handle all kinds of calls, and thus has the most complex return type.
    /// In many cases it will be more appropriate to use one of query(), dml(),
    /// execute(), which have the simple result types you usually expect.
    pub fn what_ever(&self, stmt: &str) -> DbcResult<DbResponses> {
        AdhocStatement::new(self.core.clone(), String::from(stmt), self.auto_commit)
        .execute()
    }
    /// Executes a statement and expects a single ResultSet
    pub fn query(&self, stmt: &str) -> DbcResult<ResultSet> {
        try!(self.what_ever(stmt)).as_resultset()
    }
    /// Executes a statement and expects a single number of affected rows
    pub fn dml(&self, stmt: &str) -> DbcResult<usize> {
        try!(self.what_ever(stmt)).as_row_count()
    }
    /// Executes a statement and expects a plain success
    pub fn execute(&self, stmt: &str) -> DbcResult<()> {
        try!(self.what_ever(stmt)).as_success()
    }

    /// Prepares a statement
    pub fn prepare(&self, stmt: &str) -> DbcResult<PreparedStatement> {
        let stmt = try!(PreparedStatement::prepare(self.core.clone(), String::from(stmt), self.auto_commit));
        debug!("PreparedStatement created with auto_commit = {}", stmt.auto_commit);
        Ok(stmt)
    }

    // Commits the current transaction
    pub fn commit(&self) -> DbcResult<()> {
        try!(self.what_ever("commit")).as_success()
    }

    // Rolls back the current transaction
    pub fn rollback(&self) -> DbcResult<()> {
        try!(self.what_ever("rollback")).as_success()
    }

    /// Creates a new connection object with the same settings and authentication
    pub fn spawn(&self) -> DbcResult<Connection> {
        let mut other_conn = try!(Connection::new(&(self.host),&(self.port)));
        other_conn.auto_commit = self.auto_commit;
        other_conn.command_options = self.command_options;
        other_conn.set_fetch_size(self.core.borrow().get_fetch_size());
        other_conn.set_lob_read_length(self.core.borrow().get_lob_read_length());
        if let Some(ref creds) = self.credentials {
            try!(other_conn.authenticate_user_password(&(creds.username),&(creds.password)));
        }
        Ok(other_conn)
    }

    fn is_authenticated(&self) -> bool {
        match self.credentials {
            None => false,
            Some(_) => true,
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        trace!("Entering Connection.drop()");
        match Request::new( &(self.core), RequestType::Disconnect, false, 0) {
            Ok(mut request) => {request.send_and_receive(&(self.core), Some(ReplyType::Disconnect)).ok();},
            Err(_) => {},
        };
        self.core.borrow_mut().session_id = 0;
    }
}

#[derive(Debug)]  // FIXME we should implement this explicitly and avoid printing out sensitive data
struct Credentials {
    username: String,
    password: String,
}
