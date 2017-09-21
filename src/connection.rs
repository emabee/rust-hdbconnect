use connect_params::ConnectParams;

use prepared_statement::PreparedStatement;
use prepared_statement::factory as PreparedStatementFactory;
use {HdbError, HdbResult, HdbResponse};

use protocol::authenticate;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::conn_core::{ConnectionCore, ConnCoreRef};
use protocol::lowlevel::init;
use protocol::lowlevel::parts::resultset::ResultSet;

use chrono::Local;
use std::net::TcpStream;
use std::fmt::Write;

const HOLD_OVER_COMMIT: u8 = 8;

/// Connection object.
///
/// The connection to the database.
///
/// # Example
///
/// ```ignore
/// use hdbconnect::{Connection,IntoConnectParams};
/// let params = "hdbsql://my_user:my_passwd@the_host:2222".into_connect_params().unwrap();
/// let mut connection = Connection::new(params).unwrap();
/// ```
#[derive(Debug)]
pub struct Connection {
    params: ConnectParams,
    major_product_version: i8,
    minor_product_version: i16,
    auto_commit: bool,
    command_options: u8,
    acc_server_proc_time: i32,
    core: ConnCoreRef,
}
impl Connection {
    /// Factory method for authenticated connections.
    pub fn new(params: ConnectParams) -> HdbResult<Connection> {
        trace!("Entering connect()");
        let start = Local::now();

        let mut connect_string = String::with_capacity(200);
        write!(connect_string, "{}:{}", params.hostname(), params.port())?;

        trace!("Connecting to \"{}\"", connect_string);
        let mut tcp_stream = TcpStream::connect(&connect_string as &str)?;
        trace!("tcp_stream is open");

        let (major, minor) = init::send_and_receive(&mut tcp_stream)?;

        let conn_ref = ConnectionCore::new_ref(tcp_stream);
        let delta = match (Local::now().signed_duration_since(start)).num_microseconds() {
            Some(m) => m,
            None => -1,
        };
        debug!("connection to {} is initialized ({} µs)", connect_string, delta);

        let conn = Connection {
            params: params,
            major_product_version: major,
            minor_product_version: minor,
            auto_commit: true,
            command_options: HOLD_OVER_COMMIT,
            core: conn_ref,
            acc_server_proc_time: 0,
        };

        authenticate::user_pw(&(conn.core), conn.params.dbuser(), conn.params.password())?;
        let delta = match (Local::now().signed_duration_since(start)).num_microseconds() {
            Some(m) => m,
            None => -1,
        };
        debug!("user \"{}\" successfully logged on ({} µs)", conn.params.dbuser(), delta);
        Ok(conn)
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
    pub fn set_fetch_size(&mut self, fetch_size: u32) -> HdbResult<()> {
        let mut guard = self.core.lock()?;
        (*guard).set_fetch_size(fetch_size);
        Ok(())
    }
    /// Configures the connection's lob read length for future calls.
    pub fn set_lob_read_length(&mut self, lob_read_length: i32) -> HdbResult<()> {
        let mut guard = self.core.lock()?;
        (*guard).set_lob_read_length(lob_read_length);
        Ok(())
    }

    /// Returns the number of roundtrips to the database that
    /// have been done through this connection.
    pub fn get_call_count(&self) -> HdbResult<i32> {
        let guard = self.core.lock()?;
        Ok((*guard).last_seq_number())
    }

    /// Executes a statement on the database.
    ///
    /// This generic method can handle all kinds of calls,
    /// and thus has the most complex return type.
    /// In many cases it will be more appropriate to use
    /// one of the methods query(), dml(), exec(), which have the
    /// adequate simple result type you usually want.
    pub fn statement(&mut self, stmt: &str) -> HdbResult<HdbResponse> {
        execute(self.core.clone(),
                String::from(stmt),
                self.auto_commit,
                &mut self.acc_server_proc_time)
    }

    /// Executes a statement and expects a single ResultSet.
    pub fn query(&mut self, stmt: &str) -> HdbResult<ResultSet> {
        self.statement(stmt)?.as_resultset()
    }
    /// Executes a statement and expects a single number of affected rows.
    pub fn dml(&mut self, stmt: &str) -> HdbResult<usize> {
        let vec = &(self.statement(stmt)?.as_affected_rows()?);
        match vec.len() {
            1 => Ok(vec.get(0).unwrap().clone()),
            _ => Err(HdbError::UsageError("number of affected-rows-counts <> 1".to_owned())),
        }
    }
    /// Executes a statement and expects a plain success.
    pub fn exec(&mut self, stmt: &str) -> HdbResult<()> {
        self.statement(stmt)?.as_success()
    }

    /// Prepares a statement and returns a handle to it.
    /// Note that the handle keeps using the same connection.
    pub fn prepare(&self, stmt: &str) -> HdbResult<PreparedStatement> {
        let stmt = PreparedStatementFactory::prepare(self.core.clone(),
                                                     String::from(stmt),
                                                     self.auto_commit)?;
        Ok(stmt)
    }

    /// Commits the current transaction.
    pub fn commit(&mut self) -> HdbResult<()> {
        self.statement("commit")?.as_success()
    }

    /// Rolls back the current transaction.
    pub fn rollback(&mut self) -> HdbResult<()> {
        self.statement("rollback")?.as_success()
    }

    /// Creates a new connection object with the same settings and authentication.
    pub fn spawn(&self) -> HdbResult<Connection> {
        let mut other_conn = Connection::new(self.params.clone())?;
        other_conn.auto_commit = self.auto_commit;
        other_conn.command_options = self.command_options;
        {
            let guard = self.core.lock()?;
            let core = &*guard;
            other_conn.set_fetch_size(core.get_fetch_size())?;
            other_conn.set_lob_read_length(core.get_lob_read_length())?;
        }
        Ok(other_conn)
    }

    /// Utility method to fire a couple of statements, ignoring errors and return values
    pub fn multiple_statements_ignore_err(&mut self, stmts: Vec<&str>) {
        for s in stmts {
            match self.statement(s) {
                Ok(_) => {}
                Err(_) => {}
            }
        }
    }

    /// Utility method to fire a couple of statements, ignoring their return values;
    /// the method returns with the first error, or with  ()
    pub fn multiple_statements(&mut self, stmts: Vec<&str>) -> HdbResult<()> {
        for s in stmts {
            self.statement(s)?;
        }
        Ok(())
    }
}

fn execute(conn_ref: ConnCoreRef, stmt: String, auto_commit: bool, acc_server_proc_time: &mut i32)
           -> HdbResult<HdbResponse> {
    debug!("connection::execute({})", stmt);
    let command_options = 0b_1000;
    let fetch_size: u32 = {
        let guard = conn_ref.lock()?;
        (*guard).get_fetch_size()
    };
    let mut request =
        Request::new(&(conn_ref), RequestType::ExecuteDirect, auto_commit, command_options)?;
    request.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));
    request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

    request.send_and_get_response(None, None, &(conn_ref), None, acc_server_proc_time)
}
