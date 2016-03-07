use adhoc_statement::AdhocStatement;
use prepared_statement::PreparedStatement;
use prepared_statement::factory as PreparedStatementFactory;
use {DbcError,DbcResult,DbResponse};

use protocol::authenticate;
use protocol::lowlevel::conn_core::{ConnectionCore, ConnRef};
use protocol::lowlevel::init;
use protocol::lowlevel::parts::resultset::ResultSet;

use chrono::Local;
use std::net::TcpStream;
use std::ops::{Add};
use std::fmt;

const HOLD_OVER_COMMIT: u8 = 8;

/// Connection object
///
/// This is the central starting point of hdbconnect.
/// The typical pattern is:
///
/// ```ignore
/// use hdbconnect::Connection;
/// let mut connection = try!(Connection::new("mymachine", "30415"));
/// try!(connection.authenticate_user_password("Annidda", "BF1äÖkn&nG"));

/// ```
#[derive(Debug)]
pub struct Connection {
    host: String,
    port: String,
    credentials: Option<Credentials>,
    pub major_product_version: i8,
    pub minor_product_version: i16,
    auto_commit: bool,
    command_options: u8,
    acc_server_proc_time: i32,
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
            acc_server_proc_time: 0,
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
    /// In many cases it will be more appropriate to use one of query_statement(), dml_statement(),
    /// exec_statement(), which have the simple result types you usually expect.
    pub fn any_statement(&mut self, stmt: &str) -> DbcResult<DbResponse> {
        AdhocStatement::new(self.core.clone(), String::from(stmt), self.auto_commit)
        .exec_statement(&mut self.acc_server_proc_time)
    }

    /// Executes a statement and expects a single ResultSet
    pub fn query_statement(&mut self, stmt: &str) -> DbcResult<ResultSet> {
        try!(self.any_statement(stmt)).as_resultset()
    }
    /// Executes a statement and expects a single number of affected rows
    pub fn dml_statement(&mut self, stmt: &str) -> DbcResult<usize> {
        let vec = &(try!(try!(self.any_statement(stmt)).as_affected_rows()));
        match vec.len() {
            1 => Ok(vec.get(0).unwrap().clone()),
            _ => Err(DbcError::UsageError("number of affected-rows-counts <> 1")),
        }
    }
    /// Executes a statement and expects a plain success
    pub fn exec_statement(&mut self, stmt: &str) -> DbcResult<()> {
        try!(self.any_statement(stmt)).as_success()
    }

    /// Prepares a statement
    pub fn prepare(&self, stmt: &str) -> DbcResult<PreparedStatement> {
        let stmt = try!(PreparedStatementFactory::prepare(self.core.clone(), String::from(stmt), self.auto_commit));
        debug!("PreparedStatement created with auto_commit = {}", stmt.auto_commit);
        Ok(stmt)
    }

    // Commits the current transaction
    pub fn commit(&mut self) -> DbcResult<()> {
        try!(self.any_statement("commit")).as_success()
    }

    // Rolls back the current transaction
    pub fn rollback(&mut self) -> DbcResult<()> {
        try!(self.any_statement("rollback")).as_success()
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

struct Credentials {
    username: String,
    password: String,
}
impl fmt::Debug for Credentials {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "username: {}, password: <not printed>",self.username).unwrap();
        Ok(())
    }
}
