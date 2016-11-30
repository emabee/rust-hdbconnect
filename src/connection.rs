use prepared_statement::PreparedStatement;
use prepared_statement::factory as PreparedStatementFactory;
use {HdbError, HdbResult, HdbResponse};

use protocol::authenticate;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::conn_core::{ConnectionCore, ConnRef};
use protocol::lowlevel::init;
use protocol::lowlevel::parts::resultset::ResultSet;

use chrono::Local;
use std::net::TcpStream;
use std::ops::Add;
use std::fmt;

const HOLD_OVER_COMMIT: u8 = 8;

/// Connection object.
///
/// The connection to the database. You get started with something like this:
///
/// ```ignore
/// use hdbconnect::Connection;
/// let mut connection = try!(Connection::new("mymachine", "30415"));
/// try!(connection.authenticate_user_password("Annidda", "BF1äÖkn&nG"));
/// ```
/// The most important attributes of a connection, including the TcpStream,
/// are "outsourced" into a ConnectionCore object, which is kept alive
/// through ref-counted references, from this connection object and / or
/// from other objects that are created by this
/// connection object and have their independent lifetime.
#[derive(Debug)]
pub struct Connection {
    host: String,
    port: String,
    credentials: Option<Credentials>,
    major_product_version: i8,
    minor_product_version: i16,
    auto_commit: bool,
    command_options: u8,
    acc_server_proc_time: i32,
    core: ConnRef,
}
impl Connection {
    /// Creates a new connection object that is already connected to the specified host/port
    pub fn new(host: &str, port: &str) -> HdbResult<Connection> {
        trace!("Entering connect()");
        let start = Local::now();

        let connect_string = String::with_capacity(200).add(host).add(":").add(port);
        let mut tcp_stream = try!(TcpStream::connect(&connect_string as &str));
        trace!("tcp_stream is open");

        let (major, minor) = try!(init::send_and_receive(&mut tcp_stream));
        trace!("connection is initialized");

        let conn_ref = ConnectionCore::new_conn_ref(tcp_stream);
        let delta = match (Local::now() - start).num_microseconds() {
            Some(m) => m,
            None => -1,
        };
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
    pub fn authenticate_user_password(&mut self, username: &str, password: &str) -> HdbResult<()> {
        if self.is_authenticated() {
            return Err(HdbError::UsageError("Re-authentication not possible, create a new \
                                             Connection instead"));
        }
        let start = Local::now();
        try!(authenticate::user_pw(&(self.core), username, password));
        self.credentials = Some(Credentials {
            username: String::from(username),
            password: String::from(password),
        });
        let delta = match (Local::now() - start).num_microseconds() {
            Some(m) => m,
            None => -1,
        };
        debug!("successfully logged on as user \"{}\" in {} µs", username, delta);
        Ok(())
    }

    /// Returns the HANA's product version info.
    pub fn get_major_and_minor_product_version(&self) -> (i8, i16) {
        (self.major_product_version, self.minor_product_version)
    }

    /// Sets the connection's auto-commit behavior for future calls.
    pub fn set_auto_commit(&mut self, ac: bool) {
        self.auto_commit = ac;
    }
    /// Configures the connection's fetch size for future calls.
    pub fn set_fetch_size(&mut self, fetch_size: u32) {
        self.core.borrow_mut().set_fetch_size(fetch_size);
    }
    /// Configures the connection's lob read length for future calls.
    pub fn set_lob_read_length(&mut self, lob_read_length: i32) {
        self.core.borrow_mut().set_lob_read_length(lob_read_length);
    }

    /// Returns the number of roundtrips to the database that
    /// have been done through this connection.
    pub fn get_call_count(&self) -> i32 {
        self.core.borrow().last_seq_number()
    }


    /// Executes a statement on the database.
    ///
    /// This generic method can handle all kinds of calls,
    /// and thus has the most complex return type.
    /// In many cases it will be more appropriate to use
    /// one of the methods query_statement(),
    /// dml_statement(), exec_statement(), which have the
    /// adequate simple result type you usually want.
    pub fn any_statement(&mut self, stmt: &str) -> HdbResult<HdbResponse> {
        exec_statement(self.core.clone(),
                       String::from(stmt),
                       self.auto_commit,
                       &mut self.acc_server_proc_time)
    }

    /// Executes a statement and expects a single ResultSet.
    pub fn query_statement(&mut self, stmt: &str) -> HdbResult<ResultSet> {
        try!(self.any_statement(stmt)).as_resultset()
    }
    /// Executes a statement and expects a single number of affected rows.
    pub fn dml_statement(&mut self, stmt: &str) -> HdbResult<usize> {
        let vec = &(try!(try!(self.any_statement(stmt)).as_affected_rows()));
        match vec.len() {
            1 => Ok(vec.get(0).unwrap().clone()),
            _ => Err(HdbError::UsageError("number of affected-rows-counts <> 1")),
        }
    }
    /// Executes a statement and expects a plain success.
    pub fn exec_statement(&mut self, stmt: &str) -> HdbResult<()> {
        try!(self.any_statement(stmt)).as_success()
    }

    /// Prepares a statement and returns a handle to it.
    /// Note that the handle keeps using the same connection.
    pub fn prepare(&self, stmt: &str) -> HdbResult<PreparedStatement> {
        let stmt = try!(PreparedStatementFactory::prepare(self.core.clone(),
                                                          String::from(stmt),
                                                          self.auto_commit));
        Ok(stmt)
    }

    /// Commits the current transaction.
    pub fn commit(&mut self) -> HdbResult<()> {
        try!(self.any_statement("commit")).as_success()
    }

    /// Rolls back the current transaction.
    pub fn rollback(&mut self) -> HdbResult<()> {
        try!(self.any_statement("rollback")).as_success()
    }

    /// Creates a new connection object with the same settings and authentication.
    pub fn spawn(&self) -> HdbResult<Connection> {
        let mut other_conn = try!(Connection::new(&(self.host), &(self.port)));
        other_conn.auto_commit = self.auto_commit;
        other_conn.command_options = self.command_options;
        other_conn.set_fetch_size(self.core.borrow().get_fetch_size());
        other_conn.set_lob_read_length(self.core.borrow().get_lob_read_length());
        if let Some(ref creds) = self.credentials {
            try!(other_conn.authenticate_user_password(&(creds.username), &(creds.password)));
        }
        Ok(other_conn)
    }

    /// Utility method to fire a couple of statements, ignoring errors and return values
    pub fn multiple_statements_ignore_err(&mut self, stmts: Vec<&str>) {
        for s in stmts {
            match self.any_statement(s) {
                Ok(_) => {}
                Err(_) => {}
            }
        }
    }

    /// Utility method to fire a couple of statements, ignoring their return values;
    /// the method returns with the first error, or with  ()
    pub fn multiple_statements(&mut self, prep: Vec<&str>) -> HdbResult<()> {
        for s in prep {
            try!(self.any_statement(s));
        }
        Ok(())
    }

    fn is_authenticated(&self) -> bool {
        match self.credentials {
            None => false,
            Some(_) => true,
        }
    }
}

pub fn exec_statement(conn_ref: ConnRef, stmt: String, auto_commit: bool,
                      acc_server_proc_time: &mut i32)
                      -> HdbResult<HdbResponse> {
    debug!("connection::exec_statement({})", stmt);
    let command_options = 0b_1000;
    let fetch_size = {
        conn_ref.borrow().get_fetch_size()
    };
    let mut request =
        try!(Request::new(&(conn_ref), RequestType::ExecuteDirect, auto_commit, command_options));
    request.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));
    request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

    request.send_and_get_response(None, None, &(conn_ref), None, acc_server_proc_time)
}

struct Credentials {
    username: String,
    password: String,
}
impl fmt::Debug for Credentials {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "username: {}, password: <not printed>", self.username).unwrap();
        Ok(())
    }
}
