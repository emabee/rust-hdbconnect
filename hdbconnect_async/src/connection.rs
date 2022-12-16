use crate::prepared_statement::PreparedStatement;
use dist_tx_async::rm::ResourceManager;
use hdbconnect_impl::{
    conn::AsyncAmConnCore,
    protocol::parts::{
        ClientContext, ClientContextId, CommandInfo, ConnOptId, OptionValue, ServerError,
    },
    protocol::{Part, Request, RequestType, ServerUsage, HOLD_CURSORS_OVER_COMMIT},
    xa_impl::async_new_resource_manager,
    {HdbError, HdbResponse, HdbResult, IntoConnectParams, ResultSet},
};

/// A synchronous connection to the database.
#[derive(Clone, Debug)]
pub struct Connection {
    am_conn_core: AsyncAmConnCore,
}

impl Connection {
    /// Factory method for authenticated connections.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// use hdbconnect_async::Connection;
    /// let mut conn = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").await.unwrap();
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn new<P: IntoConnectParams>(p: P) -> HdbResult<Self> {
        Ok(Self {
            am_conn_core: AsyncAmConnCore::try_new(p.into_connect_params()?).await?,
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
    /// # use hdbconnect_async::{Connection, HdbResponse, HdbResult, IntoConnectParams};
    /// # tokio_test::block_on(async {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).await.unwrap();
    /// # let statement_string = "";
    /// let mut response = connection.statement(&statement_string).await.unwrap(); // HdbResponse
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn statement<S: AsRef<str>>(&mut self, stmt: S) -> HdbResult<HdbResponse> {
        self.execute(stmt.as_ref(), None).await
    }

    /// Executes a statement and expects a single `ResultSet`.
    ///
    /// Should be used for query statements (like "SELECT ...") which return a single resultset.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection, HdbResult, IntoConnectParams, ResultSet};
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).await.unwrap();
    /// # let statement_string = "";
    /// let mut rs = connection.query(&statement_string).await.unwrap(); // ResultSet
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn query<S: AsRef<str>>(&mut self, stmt: S) -> HdbResult<ResultSet> {
        self.statement(stmt).await?.into_resultset()
    }

    /// Executes a statement and expects a single number of affected rows.
    ///
    /// Should be used for DML statements only, i.e., INSERT, UPDATE, DELETE, UPSERT.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use hdbconnect_async::{Connection, HdbResult, IntoConnectParams, ResultSet};
    /// # tokio_test::block_on(async {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).await.unwrap();
    /// # let statement_string = "";
    /// let count = connection.dml(&statement_string).await.unwrap(); //usize
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn dml<S: AsRef<str>>(&mut self, stmt: S) -> HdbResult<usize> {
        let vec = &(self.statement(stmt).await?.into_affected_rows()?);
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
    /// # use hdbconnect_async::{Connection, HdbResult, IntoConnectParams, ResultSet};
    /// # tokio_test::block_on(async {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).await.unwrap();
    /// # let statement_string = "";
    /// connection.exec(&statement_string).await.unwrap();
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn exec<S: AsRef<str>>(&mut self, stmt: S) -> HdbResult<()> {
        self.statement(stmt).await?.into_success()
    }

    /// Prepares a statement and returns a handle (a `PreparedStatement`) to it.
    ///
    /// Note that the `PreparedStatement` keeps using the same database connection as
    /// this `Connection`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use hdbconnect_async::{Connection, HdbResult, IntoConnectParams};
    /// # tokio_test::block_on(async {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).await.unwrap();
    /// let query_string = "select * from phrases where ID = ? and text = ?";
    /// let mut statement = connection.prepare(query_string).await.unwrap(); //PreparedStatement
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn prepare<S: AsRef<str>>(&self, stmt: S) -> HdbResult<PreparedStatement> {
        PreparedStatement::try_new(self.am_conn_core.clone(), stmt.as_ref()).await
    }

    /// Prepares a statement and executes it a single time.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn prepare_and_execute<S, T>(&self, stmt: S, input: &T) -> HdbResult<HdbResponse>
    where
        S: AsRef<str>,
        T: serde::ser::Serialize,
    {
        let mut stmt = PreparedStatement::try_new(self.am_conn_core.clone(), stmt.as_ref()).await?;
        stmt.execute(input).await
    }

    /// Commits the current transaction.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn commit(&mut self) -> HdbResult<()> {
        self.statement("commit").await?.into_success()
    }

    /// Rolls back the current transaction.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn rollback(&mut self) -> HdbResult<()> {
        self.statement("rollback").await?.into_success()
    }

    /// Creates a new connection object with the same settings and
    /// authentication.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn spawn(&self) -> HdbResult<Self> {
        let am_conn_core = self.am_conn_core.lock().await;
        let mut other = Self::new(am_conn_core.connect_params()).await?;
        other.set_auto_commit(am_conn_core.is_auto_commit()).await?;
        other.set_fetch_size(am_conn_core.get_fetch_size()).await?;
        other
            .set_lob_read_length(am_conn_core.get_lob_read_length())
            .await?;
        Ok(other)
    }

    /// Utility method to fire a couple of statements, ignoring errors and
    /// return values.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn multiple_statements_ignore_err<S: AsRef<str>>(&mut self, stmts: Vec<S>) {
        for s in stmts {
            trace!("multiple_statements_ignore_err: firing \"{}\"", s.as_ref());
            let result = self.statement(s).await;
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
    pub async fn multiple_statements<S: AsRef<str>>(&mut self, stmts: Vec<S>) -> HdbResult<()> {
        for s in stmts {
            self.statement(s).await?;
        }
        Ok(())
    }

    /// Returns warnings that were returned from the server since the last call
    /// to this method.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn pop_warnings(&self) -> HdbResult<Option<Vec<ServerError>>> {
        Ok(self.am_conn_core.lock().await.pop_warnings())
    }

    /// Sets the connection's auto-commit behavior for future calls.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_auto_commit(&mut self, ac: bool) -> HdbResult<()> {
        self.am_conn_core.lock().await.set_auto_commit(ac);
        Ok(())
    }

    /// Returns the connection's auto-commit behavior.
    ///
    /// # Errors
    ///
    /// Only `HdbError::POóison` can occur.
    pub async fn is_auto_commit(&self) -> HdbResult<bool> {
        Ok(self.am_conn_core.lock().await.is_auto_commit())
    }

    /// Configures the connection's fetch size for future calls.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_fetch_size(&mut self, fetch_size: u32) -> HdbResult<()> {
        self.am_conn_core.lock().await.set_fetch_size(fetch_size);
        Ok(())
    }
    /// Returns the connection's lob read length.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn get_lob_read_length(&self) -> HdbResult<u32> {
        Ok(self.am_conn_core.lock().await.get_lob_read_length())
    }
    /// Configures the connection's lob read length for future calls.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_lob_read_length(&mut self, l: u32) -> HdbResult<()> {
        self.am_conn_core.lock().await.set_lob_read_length(l);
        Ok(())
    }

    /// Configures the connection's lob write length for future calls.
    ///
    /// The intention of the parameter is to allow reducing the number of roundtrips
    /// to the database.
    /// Values smaller than rust's buffer size (8k) will have little effect, since
    /// each read() call to the Read impl in a `HdbValue::LOBSTREAM` will cause at most one
    /// write roundtrip to the database.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn get_lob_write_length(&self) -> HdbResult<usize> {
        Ok(self.am_conn_core.lock().await.get_lob_write_length())
    }
    /// Configures the connection's lob write length for future calls.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_lob_write_length(&mut self, l: usize) -> HdbResult<()> {
        self.am_conn_core.lock().await.set_lob_write_length(l);
        Ok(())
    }

    /// Returns the ID of the connection.
    ///
    /// The ID is set by the server. Can be handy for logging.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn id(&self) -> HdbResult<i32> {
        self.am_conn_core
            .lock()
            .await
            .connect_options()
            .get_connection_id()
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this Connection object.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn server_usage(&self) -> HdbResult<ServerUsage> {
        Ok(self.am_conn_core.lock().await.server_usage())
    }

    #[doc(hidden)]
    pub async fn data_format_version_2(&self) -> HdbResult<i32> {
        self.am_conn_core
            .lock()
            .await
            .connect_options()
            .get_dataformat_version2()
    }

    #[doc(hidden)]
    pub async fn dump_connect_options(&self) -> HdbResult<String> {
        Ok(self.am_conn_core.lock().await.dump_connect_options())
    }
    #[doc(hidden)]
    pub async fn dump_client_info(&self) -> HdbResult<String> {
        Ok(self.am_conn_core.lock().await.dump_client_info())
    }

    /// Returns the number of roundtrips to the database that
    /// have been done through this connection.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn get_call_count(&self) -> HdbResult<i32> {
        Ok(self.am_conn_core.lock().await.last_seq_number())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection,HdbResult};
    /// # let mut connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").await.unwrap();
    /// connection.set_application("MyApp, built in rust").await.unwrap();
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_application<S: AsRef<str>>(&self, application: S) -> HdbResult<()> {
        self.am_conn_core.lock().await.set_application(application);
        Ok(())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection,HdbResult};
    /// # let mut connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").await.unwrap();
    /// connection.set_application_user("K2209657").await.unwrap();
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_application_user<S: AsRef<str>>(&self, appl_user: S) -> HdbResult<()> {
        self.am_conn_core
            .lock()
            .await
            .set_application_user(appl_user.as_ref());
        Ok(())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection,HdbResult};
    /// # let mut connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").await.unwrap();
    /// connection.set_application_version("5.3.23").await.unwrap();
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_application_version<S: AsRef<str>>(&mut self, version: S) -> HdbResult<()> {
        self.am_conn_core
            .lock()
            .await
            .set_application_version(version.as_ref());
        Ok(())
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection,HdbResult};
    /// # let mut connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").await.unwrap();
    /// connection.set_application_source("update_customer.rs").await.unwrap();
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_application_source<S: AsRef<str>>(&mut self, source: S) -> HdbResult<()> {
        self.am_conn_core
            .lock()
            .await
            .set_application_source(source.as_ref());
        Ok(())
    }

    /// Returns an implementation of `dist_tx_async::rm::ResourceManager` that is
    /// based on this connection.
    pub async fn get_resource_manager(&self) -> Box<dyn ResourceManager> {
        Box::new(async_new_resource_manager(self.am_conn_core.clone()))
    }

    /// Tools like debuggers can provide additional information while stepping through a source.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn execute_with_debuginfo<S: AsRef<str>>(
        &mut self,
        stmt: S,
        module: S,
        line: i32,
    ) -> HdbResult<HdbResponse> {
        self.execute(stmt, Some(CommandInfo::new(line, module.as_ref())))
            .await
    }

    /// (MDC) Database name.
    ///
    /// # Errors
    ///
    /// Errors are unlikely to occur.
    ///
    /// - `HdbError::ImplDetailed` if the database name was not provided by the database server.
    /// - `HdbError::Poison` if the shared mutex of the inner connection object is poisened.
    pub async fn get_database_name(&self) -> HdbResult<String> {
        self.am_conn_core
            .lock()
            .await
            .connect_options()
            .get_database_name()
            .map(ToOwned::to_owned)
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
    pub async fn get_system_id(&self) -> HdbResult<String> {
        self.am_conn_core
            .lock()
            .await
            .connect_options()
            .get_system_id()
            .map(ToOwned::to_owned)
    }

    /// Returns the information that is given to the server as client context.
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn client_info(&self) -> HdbResult<Vec<(String, String)>> {
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

        let conn_core = self.am_conn_core.lock().await;
        let conn_opts = conn_core.connect_options();
        if let Ok(OptionValue::STRING(s)) = conn_opts.get(&ConnOptId::OSUser) {
            result.push((format!("{:?}", ConnOptId::OSUser), s.clone()));
        }
        if let Ok(OptionValue::INT(i)) = conn_opts.get(&ConnOptId::ConnectionID) {
            result.push((format!("{:?}", ConnOptId::ConnectionID), i.to_string()));
        }
        Ok(result)
    }

    /// Returns a connect url (excluding the password) that reflects the options that were
    /// used to establish this connection.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn connect_string(&self) -> HdbResult<String> {
        Ok(self.am_conn_core.lock().await.connect_string())
    }

    /// HANA Full version string.
    ///
    /// # Errors
    ///
    /// Errors are unlikely to occur.
    ///
    /// - `HdbError::ImplDetailed` if the version string was not provided by the database server.
    /// - `HdbError::Poison` if the shared mutex of the inner connection object is poisened.
    pub async fn get_full_version_string(&self) -> HdbResult<String> {
        self.am_conn_core
            .lock()
            .await
            .connect_options()
            .get_full_version_string()
            .map(ToOwned::to_owned)
    }

    async fn execute<S>(
        &mut self,
        stmt: S,
        o_command_info: Option<CommandInfo>,
    ) -> HdbResult<HdbResponse>
    where
        S: AsRef<str>,
    {
        debug!(
            "connection[{:?}]::execute()",
            self.am_conn_core
                .lock()
                .await
                .connect_options()
                .get_connection_id()
        );
        let mut request = Request::new(RequestType::ExecuteDirect, HOLD_CURSORS_OVER_COMMIT);
        {
            let conn_core = self.am_conn_core.lock().await;
            let fetch_size = conn_core.get_fetch_size();
            request.push(Part::FetchSize(fetch_size));
            if let Some(command_info) = o_command_info {
                request.push(Part::CommandInfo(command_info));
            }
            request.push(Part::Command(stmt.as_ref()));
        }
        let (internal_return_values, replytype) = self
            .am_conn_core
            .send(request)
            .await?
            .async_into_internal_return_values(&mut self.am_conn_core, None)
            .await?;
        HdbResponse::try_new(internal_return_values, replytype)
    }
}
