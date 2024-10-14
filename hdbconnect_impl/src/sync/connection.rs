use crate::{
    conn::{AmConnCore, ConnectionConfiguration, ConnectionStatistics, CursorHoldability},
    protocol::{
        parts::{ClientContext, ClientContextId, CommandInfo, ConnOptId, OptionValue, ServerError},
        MessageType, Part, Request, ServerUsage,
    },
    sync::{HdbResponse, PreparedStatement, ResultSet},
    {HdbError, HdbResult, IntoConnectParams},
};
use std::time::Duration;

#[cfg(feature = "dist_tx")]
use crate::xa_impl::new_resource_manager_sync;
#[cfg(feature = "dist_tx")]
use dist_tx::sync::rm::ResourceManager;

/// A synchronous connection to the database.
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
    /// let conn = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn new<P: IntoConnectParams>(params: P) -> HdbResult<Self> {
        Self::with_configuration(params, &ConnectionConfiguration::default())
    }

    /// Factory method for authenticated connections with given configuration.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn with_configuration<P: IntoConnectParams>(
        params: P,
        config: &ConnectionConfiguration,
    ) -> HdbResult<Self> {
        Ok(Self {
            am_conn_core: AmConnCore::try_new_sync(params.into_connect_params()?, config)?,
        })
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
    /// # let connection = Connection::new(params).unwrap();
    /// # let statement_string = "";
    /// let mut response = connection.statement(&statement_string)?; // HdbResponse
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn statement<S: AsRef<str>>(&self, stmt: S) -> HdbResult<HdbResponse> {
        self.execute(stmt.as_ref(), None)
    }

    /// Executes a statement and expects a single `ResultSet`.
    ///
    /// Should be used for query statements (like "SELECT ...") which return a single result set.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, ResultSet};
    /// # fn main() -> HdbResult<()> {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let connection = Connection::new(params).unwrap();
    /// # let statement_string = "";
    /// let mut rs = connection.query(&statement_string)?; // ResultSet
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn query<S: AsRef<str>>(&self, stmt: S) -> HdbResult<ResultSet> {
        self.statement(stmt)?.into_result_set()
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
    /// # let connection = Connection::new(params).unwrap();
    /// # let statement_string = "";
    /// let count = connection.dml(&statement_string)?; //usize
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn dml<S: AsRef<str>>(&self, stmt: S) -> HdbResult<usize> {
        let vec = &(self.statement(stmt)?.into_affected_rows()?);
        match vec.len() {
            1 => Ok(vec[0]),
            _ => Err(HdbError::Usage("number of affected-rows-counts <> 1")),
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
    /// # let connection = Connection::new(params).unwrap();
    /// # let statement_string = "";
    /// connection.exec(&statement_string)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn exec<S: AsRef<str>>(&self, stmt: S) -> HdbResult<()> {
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
    /// # let connection = Connection::new(params).unwrap();
    /// let query_string = "select * from phrases where ID = ? and text = ?";
    /// let statement = connection.prepare(query_string)?; //PreparedStatement
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn prepare<S: AsRef<str>>(&self, stmt: S) -> HdbResult<PreparedStatement> {
        PreparedStatement::try_new(self.am_conn_core.clone(), stmt.as_ref())
    }

    /// Prepares a statement and executes it a single time.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn prepare_and_execute<S, T>(&self, stmt: S, input: &T) -> HdbResult<HdbResponse>
    where
        S: AsRef<str>,
        T: serde::ser::Serialize,
    {
        let mut stmt = PreparedStatement::try_new(self.am_conn_core.clone(), stmt.as_ref())?;
        stmt.execute(input)
    }

    /// Commits the current transaction.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn commit(&self) -> HdbResult<()> {
        self.statement("commit")?.into_success()
    }

    /// Rolls back the current transaction.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn rollback(&self) -> HdbResult<()> {
        self.statement("rollback")?.into_success()
    }

    /// Creates a new connection object with the same settings and
    /// authentication.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn spawn(&self) -> HdbResult<Self> {
        let am_conn_core = self.am_conn_core.lock_sync()?;
        Ok(Self {
            am_conn_core: AmConnCore::try_new_sync(
                am_conn_core.connect_params().clone(),
                am_conn_core.configuration(),
            )?,
        })
    }

    /// Utility method to fire a couple of statements, ignoring errors and
    /// return values.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn multiple_statements_ignore_err<S: AsRef<str>>(&self, stmts: Vec<S>) {
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
    /// values; the method returns with the first error, or with `()`.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn multiple_statements<S: AsRef<str>>(&self, stmts: Vec<S>) -> HdbResult<()> {
        for s in stmts {
            self.statement(s)?;
        }
        Ok(())
    }

    /// Returns warnings that were returned from the server since the last call
    /// to this method.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn pop_warnings(&self) -> HdbResult<Option<Vec<ServerError>>> {
        Ok(self.am_conn_core.lock_sync()?.pop_warnings())
    }

    /// Sets the connection's auto-commit behavior.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_auto_commit(&self, ac: bool) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .configuration_mut()
            .set_auto_commit(ac);
        Ok(())
    }
    /// Returns the connection's auto-commit behavior.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn is_auto_commit(&self) -> HdbResult<bool> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .configuration()
            .is_auto_commit())
    }

    /// Sets the connection's cursor holdability.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_cursor_holdability(&self, holdability: CursorHoldability) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .configuration_mut()
            .set_cursor_holdability(holdability);
        Ok(())
    }
    /// Returns the connection's cursor holdability.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn cursor_holdability(&self) -> HdbResult<CursorHoldability> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .configuration()
            .cursor_holdability())
    }

    /// Returns the connection's fetch size.
    ///
    /// The default value is [`ConnectionConfiguration::DEFAULT_FETCH_SIZE`].
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn fetch_size(&self) -> HdbResult<u32> {
        Ok(self.am_conn_core.lock_sync()?.configuration().fetch_size())
    }
    /// Sets the connection's fetch size.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_fetch_size(&self, fetch_size: u32) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .configuration_mut()
            .set_fetch_size(fetch_size);
        Ok(())
    }

    /// Returns the connection's read timeout.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn read_timeout(&self) -> HdbResult<Option<Duration>> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .configuration()
            .read_timeout())
    }
    /// Sets the connection's read timeout.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_read_timeout(&self, read_timeout: Option<Duration>) -> HdbResult<()> {
        let mut conn_core = self.am_conn_core.lock_sync()?;
        conn_core.configuration_mut().set_read_timeout(read_timeout);
        conn_core.set_read_timeout_sync(read_timeout)?;
        Ok(())
    }

    /// Returns the connection's lob read length.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn lob_read_length(&self) -> HdbResult<u32> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .configuration()
            .lob_read_length())
    }
    /// Sets the connection's lob read length.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_lob_read_length(&self, l: u32) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .configuration_mut()
            .set_lob_read_length(l);
        Ok(())
    }

    /// Returns the connection's lob write length.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn lob_write_length(&self) -> HdbResult<u32> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .configuration()
            .lob_write_length())
    }
    /// Sets the connection's lob write length.
    ///
    /// The intention of the parameter is to allow reducing the number of roundtrips
    /// to the database.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_lob_write_length(&self, l: u32) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .configuration_mut()
            .set_lob_write_length(l);
        Ok(())
    }

    /// Sets the connection's maximum buffer size.
    ///
    /// See also [`ConnectionConfiguration::set_max_buffer_size`].
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_max_buffer_size(&mut self, max_buffer_size: usize) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .configuration_mut()
            .set_max_buffer_size(max_buffer_size);
        Ok(())
    }

    /// Returns the ID of the connection.
    ///
    /// The ID is set by the server. Can be handy for logging.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn id(&self) -> HdbResult<u32> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .connect_options()
            .get_connection_id())
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this Connection object.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn server_usage(&self) -> HdbResult<ServerUsage> {
        Ok(self.am_conn_core.lock_sync()?.server_usage())
    }

    #[doc(hidden)]
    pub fn data_format_version_2(&self) -> HdbResult<u8> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .connect_options()
            .get_dataformat_version2())
    }

    #[doc(hidden)]
    pub fn dump_connect_options(&self) -> HdbResult<String> {
        Ok(self.am_conn_core.lock_sync()?.dump_connect_options())
    }
    #[doc(hidden)]
    pub fn dump_client_info(&self) -> HdbResult<String> {
        Ok(self.am_conn_core.lock_sync()?.dump_client_info())
    }

    /// Returns some statistics snapshot about what was done with this connection so far.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn statistics(&self) -> HdbResult<ConnectionStatistics> {
        Ok(self.am_conn_core.lock_sync()?.statistics().clone())
    }

    /// Reset the counters in the Connection's statistic object.
    ///
    /// # Errors
    ///
    /// Only lock poisoning can occur.
    pub fn reset_statistics(&self) -> HdbResult<()> {
        self.am_conn_core.lock_sync()?.reset_statistics();
        Ok(())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection,HdbResult};
    /// # fn foo() -> HdbResult<()> {
    /// # let connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222")?;
    /// connection.set_application("MyApp, built in rust")?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_application<S: AsRef<str>>(&self, application: S) -> HdbResult<()> {
        self.am_conn_core.lock_sync()?.set_application(application);
        Ok(())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection,HdbResult};
    /// # fn foo() -> HdbResult<()> {
    /// # let connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222")?;
    /// connection.set_application_user("K2209657")?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_application_user<S: AsRef<str>>(&self, appl_user: S) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .set_application_user(appl_user.as_ref());
        Ok(())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection,HdbResult};
    /// # fn foo() -> HdbResult<()> {
    /// # let  connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222")?;
    /// connection.set_application_version("5.3.23")?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_application_version<S: AsRef<str>>(&self, version: S) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .set_application_version(version.as_ref());
        Ok(())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # use hdbconnect::{Connection,HdbResult};
    /// # fn foo() -> HdbResult<()> {
    /// # let connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222")?;
    /// connection.set_application_source("update_customer.rs")?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn set_application_source<S: AsRef<str>>(&self, source: S) -> HdbResult<()> {
        self.am_conn_core
            .lock_sync()?
            .set_application_source(source.as_ref());
        Ok(())
    }

    /// Returns an implementation of `dist_tx::rm::ResourceManager` that is
    /// based on this connection.
    #[cfg(feature = "dist_tx")]
    #[must_use]
    pub fn get_resource_manager(&self) -> Box<dyn ResourceManager> {
        Box::new(new_resource_manager_sync(self.am_conn_core.clone()))
    }

    /// Tools like debuggers can provide additional information while stepping through a source.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn execute_with_debuginfo<S: AsRef<str>>(
        &self,
        stmt: S,
        module: S,
        line: u32,
    ) -> HdbResult<HdbResponse> {
        self.execute(stmt, Some(CommandInfo::new(line, module.as_ref())))
    }

    /// (MDC) Database name.
    ///
    /// # Errors
    ///
    /// Errors are unlikely to occur.
    ///
    /// - `HdbError::ImplDetailed` if the database name was not provided by the database server.
    /// - `HdbError::Poison` if the shared mutex of the inner connection object is poisened.
    pub fn get_database_name(&self) -> HdbResult<String> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .connect_options()
            .get_database_name())
    }

    /// The system id is set by the server with the SAPSYSTEMNAME of the
    /// connected instance (for tracing and supportability purposes).
    ///
    /// # Errors
    ///
    /// Errors are unlikely to occur.
    ///
    /// - `HdbError::ImplDetailed` if the system id was not provided by the database server.
    /// - `HdbError::Poison` if the shared mutex of the inner connection object is poisened.
    pub fn get_system_id(&self) -> HdbResult<String> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .connect_options()
            .get_system_id())
    }

    /// Returns the information that is given to the server as client context.
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn client_info(&self) -> HdbResult<Vec<(String, String)>> {
        let mut result = Vec::<(String, String)>::with_capacity(7);
        let mut cc = ClientContext::new();
        for k in &[
            ClientContextId::ClientType,
            ClientContextId::ClientVersion,
            ClientContextId::ClientApplicationProgramm,
        ] {
            if let Some((k, OptionValue::STRING(s))) = cc.remove_entry(k) {
                result.push((k.to_string(), s));
            }
        }

        let conn_core = self.am_conn_core.lock_sync()?;
        let conn_opts = conn_core.connect_options();
        result.push((format!("{:?}", ConnOptId::OSUser), conn_opts.get_os_user()));
        result.push((
            format!("{:?}", ConnOptId::ConnectionID),
            conn_opts.get_connection_id().to_string(),
        ));
        Ok(result)
    }

    /// Returns a connect url (excluding the password) that reflects the options that were
    /// used to establish this connection.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub fn connect_string(&self) -> HdbResult<String> {
        Ok(self.am_conn_core.lock_sync()?.connect_string())
    }

    /// HANA Full version string.
    ///
    /// # Errors
    ///
    /// Errors are unlikely to occur.
    ///
    /// - `HdbError::ImplDetailed` if the version string was not provided by the database server.
    /// - `HdbError::Poison` if the shared mutex of the inner connection object is poisened.
    pub fn get_full_version_string(&self) -> HdbResult<String> {
        Ok(self
            .am_conn_core
            .lock_sync()?
            .connect_options()
            .get_full_version_string())
    }

    fn execute<S>(&self, stmt: S, o_command_info: Option<CommandInfo>) -> HdbResult<HdbResponse>
    where
        S: AsRef<str>,
    {
        debug!(
            "connection[{:?}]::execute()",
            self.am_conn_core
                .lock_sync()?
                .connect_options()
                .get_connection_id()
        );
        let request = {
            let conn_core = self.am_conn_core.lock_sync()?;
            let command_options = conn_core.configuration().command_options();
            let mut request = Request::new(MessageType::ExecuteDirect, command_options);
            let fetch_size = conn_core.configuration().fetch_size();
            request.push(Part::FetchSize(fetch_size));
            if let Some(command_info) = o_command_info {
                request.push(Part::CommandInfo(command_info));
            }
            request.push(Part::Command(stmt.as_ref()));
            request
        };
        let (internal_return_values, replytype) = self
            .am_conn_core
            .send_sync(request)?
            .into_internal_return_values_sync(&self.am_conn_core, None)?;
        HdbResponse::try_new(internal_return_values, replytype)
    }

    /// Returns true if the connection object lost its TCP connection.
    ///
    /// # Errors
    ///
    /// Only lock poisoning can occur.
    pub fn is_broken(&self) -> HdbResult<bool> {
        Ok(self.am_conn_core.lock_sync()?.is_broken())
    }
}
