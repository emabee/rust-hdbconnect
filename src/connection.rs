use crate::authentication;
use crate::conn_core::AmConnCore;
use crate::prepared_statement::PreparedStatement;
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::command_info::CommandInfo;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::parts::server_error::ServerError;
use crate::protocol::request::{Request, HOLD_CURSORS_OVER_COMMIT};
use crate::protocol::request_type::RequestType;
use crate::protocol::server_usage::ServerUsage;
use crate::xa_impl::new_resource_manager;
use crate::{HdbErrorKind, HdbResponse, HdbResult, IntoConnectParams};
use chrono::Local;
use dist_tx::rm::ResourceManager;

/// A connection to the database.
#[derive(Clone, Debug)]
pub struct Connection {
    am_conn_core: AmConnCore,
}

impl Connection {
    /// Factory method for authenticated connections.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use hdbconnect::Connection;
    /// let mut conn = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").unwrap();
    /// ```
    pub fn new<P: IntoConnectParams>(p: P) -> HdbResult<Self> {
        trace!("connect()");
        let start = Local::now();
        let mut am_conn_core = AmConnCore::try_new(p.into_connect_params()?)?;
        authentication::authenticate(&mut am_conn_core)?;
        {
            let conn_core = am_conn_core.lock()?;
            debug!(
                "user \"{}\" successfully logged on ({} µs) to {:?} of {:?} (HANA version: {:?})",
                conn_core.connect_params().dbuser(),
                Local::now()
                    .signed_duration_since(start)
                    .num_microseconds()
                    .unwrap_or(-1),
                conn_core.connect_options().get_database_name(),
                conn_core.connect_options().get_system_id(),
                conn_core.connect_options().get_full_version_string()
            );
        }
        Ok(Self { am_conn_core })
    }

    /// Executes a statement on the database.
    ///
    /// This generic method can handle all kinds of calls,
    /// and thus has the most generic return type.
    /// In many cases it will be more convenient to use
    /// one of the dedicated methods `query()`, `dml()`, `exec()` below, which
    /// internally convert the `HdbResponse` to the
    /// respective adequate simple result type.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection, HdbResponse, HdbResult, IntoConnectParams};
    /// # fn main() -> HdbResult<()> {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).unwrap();
    /// # let statement_string = "";
    /// let mut response = connection.statement(&statement_string)?; // HdbResponse
    /// # Ok(())
    /// # }
    /// ```
    pub fn statement<S: AsRef<str>>(&mut self, stmt: S) -> HdbResult<HdbResponse> {
        execute(&mut self.am_conn_core, stmt.as_ref(), None)
    }

    /// Executes a statement and expects a single `ResultSet`.
    ///
    /// Should be used for query statements (like "SELECT ...") which return a single resultset.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, ResultSet};
    /// # fn main() -> HdbResult<()> {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).unwrap();
    /// # let statement_string = "";
    /// let mut rs = connection.query(&statement_string)?; // ResultSet
    /// # Ok(())
    /// # }
    /// ```
    pub fn query<S: AsRef<str>>(&mut self, stmt: S) -> HdbResult<ResultSet> {
        self.statement(stmt)?.into_resultset()
    }

    /// Executes a statement and expects a single number of affected rows.
    ///
    /// Should be used for DML statements only, i.e., INSERT, UPDATE, DELETE, UPSERT.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, ResultSet};
    /// # fn main() -> HdbResult<()> {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).unwrap();
    /// # let statement_string = "";
    /// let count = connection.dml(&statement_string)?; //usize
    /// # Ok(())
    /// # }
    /// ```
    pub fn dml<S: AsRef<str>>(&mut self, stmt: S) -> HdbResult<usize> {
        let vec = &(self.statement(stmt)?.into_affected_rows()?);
        match vec.len() {
            1 => Ok(vec[0]),
            _ => Err(HdbErrorKind::Usage("number of affected-rows-counts <> 1").into()),
        }
    }

    /// Executes a statement and expects a plain success.
    ///
    /// Should be used for SQL commands like "ALTER SYSTEM ...".
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, ResultSet};
    /// # fn main() -> HdbResult<()> {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).unwrap();
    /// # let statement_string = "";
    /// connection.exec(&statement_string)?;
    /// # Ok(())
    /// # }
    pub fn exec<S: AsRef<str>>(&mut self, stmt: S) -> HdbResult<()> {
        self.statement(stmt)?.into_success()
    }

    /// Prepares a statement and returns a handle (a `PreparedStatement`) to it.
    ///
    /// Note that the `PreparedStatement` keeps using the same database connection as
    /// this `Connection`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
    /// # fn main() -> HdbResult<()> {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).unwrap();
    /// let query_string = "select * from phrases where ID = ? and text = ?";
    /// let mut statement = connection.prepare(query_string)?; //PreparedStatement
    /// # Ok(())
    /// # }
    /// ```
    pub fn prepare<S: AsRef<str>>(&self, stmt: S) -> HdbResult<PreparedStatement> {
        Ok(PreparedStatement::try_new(
            self.am_conn_core.clone(),
            stmt.as_ref(),
        )?)
    }

    /// Prepares a statement and executes it a single time.
    pub fn prepare_and_execute<S, T>(&self, stmt: S, input: &T) -> HdbResult<HdbResponse>
    where
        S: AsRef<str>,
        T: serde::ser::Serialize,
    {
        let mut stmt = PreparedStatement::try_new(self.am_conn_core.clone(), stmt.as_ref())?;
        stmt.execute(input)
    }

    /// Commits the current transaction.
    pub fn commit(&mut self) -> HdbResult<()> {
        self.statement("commit")?.into_success()
    }

    /// Rolls back the current transaction.
    pub fn rollback(&mut self) -> HdbResult<()> {
        self.statement("rollback")?.into_success()
    }

    /// Creates a new connection object with the same settings and
    /// authentication.
    pub fn spawn(&self) -> HdbResult<Self> {
        let am_conn_core = self.am_conn_core.lock()?;
        let mut other = Self::new(am_conn_core.connect_params())?;
        other.set_auto_commit(am_conn_core.is_auto_commit())?;
        other.set_fetch_size(am_conn_core.get_fetch_size())?;
        other.set_lob_read_length(am_conn_core.get_lob_read_length())?;
        Ok(other)
    }

    /// Utility method to fire a couple of statements, ignoring errors and
    /// return values
    pub fn multiple_statements_ignore_err<S: AsRef<str>>(&mut self, stmts: Vec<S>) {
        for s in stmts {
            trace!("multiple_statements_ignore_err: firing \"{}\"", s.as_ref());
            let result = self.statement(s);
            match result {
                Ok(_) => {}
                Err(e) => debug!("Error intentionally ignored: {:?}", e),
            }
        }
    }

    /// Utility method to fire a couple of statements, ignoring their return
    /// values; the method returns with the first error, or with  ()
    pub fn multiple_statements<S: AsRef<str>>(&mut self, stmts: Vec<S>) -> HdbResult<()> {
        for s in stmts {
            self.statement(s)?;
        }
        Ok(())
    }

    /// Returns warnings that were returned from the server since the last call
    /// to this method.
    pub fn pop_warnings(&self) -> HdbResult<Option<Vec<ServerError>>> {
        self.am_conn_core.lock()?.pop_warnings()
    }

    /// Sets the connection's auto-commit behavior for future calls.
    pub fn set_auto_commit(&mut self, ac: bool) -> HdbResult<()> {
        self.am_conn_core.lock()?.set_auto_commit(ac);
        Ok(())
    }

    /// Returns the connection's auto-commit behavior.
    pub fn is_auto_commit(&self) -> HdbResult<bool> {
        Ok(self.am_conn_core.lock()?.is_auto_commit())
    }

    /// Configures the connection's fetch size for future calls.
    pub fn set_fetch_size(&mut self, fetch_size: u32) -> HdbResult<()> {
        self.am_conn_core.lock()?.set_fetch_size(fetch_size);
        Ok(())
    }
    /// Configures the connection's lob read length for future calls.
    pub fn get_lob_read_length(&self) -> HdbResult<u32> {
        Ok(self.am_conn_core.lock()?.get_lob_read_length())
    }
    /// Configures the connection's lob read length for future calls.
    pub fn set_lob_read_length(&mut self, l: u32) -> HdbResult<()> {
        self.am_conn_core.lock()?.set_lob_read_length(l);
        Ok(())
    }

    /// Configures the connection's lob write length for future calls.
    ///
    /// The intention of the parameter is to allow reducing the number of roundtrips
    /// to the database.
    /// Values smaller than rust's buffer size (8k) will have little effect, since
    /// each read() call to the Read impl in a `HdbValue::LOBSTREAM` will cause at most one
    /// write roundtrip to the database.
    pub fn get_lob_write_length(&self) -> HdbResult<usize> {
        Ok(self.am_conn_core.lock()?.get_lob_write_length())
    }
    /// Configures the connection's lob write length for future calls.
    pub fn set_lob_write_length(&mut self, l: usize) -> HdbResult<()> {
        self.am_conn_core.lock()?.set_lob_write_length(l);
        Ok(())
    }

    /// Returns the ID of the connection.
    ///
    /// The ID is set by the server. Can be handy for logging.
    pub fn id(&self) -> HdbResult<i32> {
        Ok(self
            .am_conn_core
            .lock()?
            .connect_options()
            .get_connection_id())
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this Connection object.
    pub fn server_usage(&self) -> HdbResult<ServerUsage> {
        Ok(self.am_conn_core.lock()?.server_usage())
    }

    #[doc(hidden)]
    pub fn data_format_version_2(&self) -> HdbResult<Option<i32>> {
        Ok(self
            .am_conn_core
            .lock()?
            .connect_options()
            .get_dataformat_version2())
    }

    #[doc(hidden)]
    pub fn dump_connect_options(&self) -> HdbResult<String> {
        Ok(self.am_conn_core.lock()?.dump_connect_options())
    }

    /// Returns the number of roundtrips to the database that
    /// have been done through this connection.
    pub fn get_call_count(&self) -> HdbResult<i32> {
        Ok(self.am_conn_core.lock()?.last_seq_number())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```ignore
    /// connection.set_application("MyApp, built in rust")?;
    /// ```
    pub fn set_application<S: AsRef<str>>(&self, application: S) -> HdbResult<()> {
        self.am_conn_core.lock()?.set_application(application)
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```ignore
    /// connection.set_application_user("K2209657")?;
    /// ```
    pub fn set_application_user<S: AsRef<str>>(&self, appl_user: S) -> HdbResult<()> {
        self.am_conn_core
            .lock()?
            .set_application_user(appl_user.as_ref())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```ignore
    /// connection.set_application_version("5.3.23")?;
    /// ```
    pub fn set_application_version<S: AsRef<str>>(&mut self, version: S) -> HdbResult<()> {
        self.am_conn_core
            .lock()?
            .set_application_version(version.as_ref())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```ignore
    /// connection.set_application_source("5.3.23","update_customer.rs")?;
    /// ```
    pub fn set_application_source<S: AsRef<str>>(&mut self, source: S) -> HdbResult<()> {
        self.am_conn_core
            .lock()?
            .set_application_source(source.as_ref())
    }

    /// Returns an implementation of `dist_tx::rm::ResourceManager` that is
    /// based on this connection.
    pub fn get_resource_manager(&self) -> Box<dyn ResourceManager> {
        Box::new(new_resource_manager(self.am_conn_core.clone()))
    }

    /// Tools like debuggers can provide additional information while stepping through a source
    pub fn execute_with_debuginfo<S: AsRef<str>>(
        &mut self,
        stmt: S,
        module: S,
        line: i32,
    ) -> HdbResult<HdbResponse> {
        execute(
            &mut self.am_conn_core,
            stmt,
            Some(CommandInfo::new(line, module.as_ref())),
        )
    }

    /// (MDC) Database name.
    pub fn get_database_name(&self) -> HdbResult<Option<String>> {
        Ok(self
            .am_conn_core
            .lock()?
            .connect_options()
            .get_database_name()
            .cloned())
    }

    /// The system id is set by the server with the SAPSYSTEMNAME of the
    /// connected instance (for tracing and supportability purposes).
    pub fn get_system_id(&self) -> HdbResult<Option<String>> {
        Ok(self
            .am_conn_core
            .lock()?
            .connect_options()
            .get_system_id()
            .cloned())
    }

    /// HANA Full version string.
    pub fn get_full_version_string(&self) -> HdbResult<Option<String>> {
        Ok(self
            .am_conn_core
            .lock()?
            .connect_options()
            .get_full_version_string()
            .cloned())
    }
}

fn execute<S>(
    am_conn_core: &mut AmConnCore,
    stmt: S,
    o_command_info: Option<CommandInfo>,
) -> HdbResult<HdbResponse>
where
    S: AsRef<str>,
{
    debug!(
        "connection[{:?}]::execute()",
        am_conn_core.lock()?.connect_options().get_connection_id()
    );
    let mut request = Request::new(RequestType::ExecuteDirect, HOLD_CURSORS_OVER_COMMIT);
    {
        let conn_core = am_conn_core.lock()?;
        let fetch_size = conn_core.get_fetch_size();
        request.push(Part::new(
            PartKind::FetchSize,
            Argument::FetchSize(fetch_size),
        ));
        if let Some(command_info) = o_command_info {
            request.push(Part::new(
                PartKind::CommandInfo,
                Argument::CommandInfo(command_info),
            ));
        }
        request.push(Part::new(
            PartKind::Command,
            Argument::Command(stmt.as_ref()),
        ));
    }

    let (internal_return_values, replytype) = am_conn_core
        .send(request)?
        .into_internal_return_values(am_conn_core, None)?;

    HdbResponse::try_new(internal_return_values, replytype)
}
