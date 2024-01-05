use super::{prepared_statement::PreparedStatement, resultset::ResultSet, HdbResponse};
#[cfg(feature = "dist_tx")]
use crate::xa_impl::new_resource_manager;
use crate::{
    conn::{AmConnCore, ConnectionConfiguration, ConnectionStatistics},
    protocol::parts::{
        ClientContext, ClientContextId, CommandInfo, ConnOptId, OptionValue, ServerError,
    },
    protocol::{MessageType, Part, Request, ServerUsage, HOLD_CURSORS_OVER_COMMIT},
    {HdbError, HdbResult, IntoConnectParams},
};
#[cfg(feature = "dist_tx")]
use dist_tx::a_sync::rm::ResourceManager;

/// An asynchronous connection to the database.
#[derive(Debug)]
pub struct Connection {
    am_conn_core: AmConnCore,
}

impl Connection {
    /// Factory method for authenticated connections.
    ///
    /// See [`ConnectParams`](struct.ConnectParams.html),
    /// [`ConnectParamsBuilder`](struct.ConnectParamsBuilder.html), and [`IntoConnectParams`]
    /// for the available options of providing the input to this function.
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
        Self::with_configuration(p, &ConnectionConfiguration::default()).await
    }

    /// Factory method for authenticated connections with given configuration.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn with_configuration<P: IntoConnectParams>(
        p: P,
        config: &ConnectionConfiguration,
    ) -> HdbResult<Self> {
        Ok(Self {
            am_conn_core: AmConnCore::try_new_async(p.into_connect_params()?, config).await?,
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
    pub async fn statement<S: AsRef<str>>(&self, stmt: S) -> HdbResult<HdbResponse> {
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
    pub async fn query<S: AsRef<str>>(&self, stmt: S) -> HdbResult<ResultSet> {
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
    pub async fn dml<S: AsRef<str>>(&self, stmt: S) -> HdbResult<usize> {
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
    pub async fn exec<S: AsRef<str>>(&self, stmt: S) -> HdbResult<()> {
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
    pub async fn commit(&self) -> HdbResult<()> {
        self.statement("commit").await?.into_success()
    }

    /// Rolls back the current transaction.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn rollback(&self) -> HdbResult<()> {
        self.statement("rollback").await?.into_success()
    }

    /// Creates a new connection object with the same settings and
    /// authentication.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn spawn(&self) -> HdbResult<Self> {
        let am_conn_core = self.am_conn_core.lock_async().await;
        Ok(Self {
            am_conn_core: AmConnCore::try_new_async(
                am_conn_core.connect_params().clone(),
                am_conn_core.configuration(),
            )
            .await?,
        })
    }

    /// Utility method to fire a couple of statements, ignoring errors and
    /// return values.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn multiple_statements_ignore_err<S: AsRef<str>>(&self, stmts: Vec<S>) {
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
    pub async fn multiple_statements<S: AsRef<str>>(&self, stmts: Vec<S>) -> HdbResult<()> {
        for s in stmts {
            self.statement(s).await?;
        }
        Ok(())
    }

    /// Returns warnings that were returned from the server since the last call
    /// to this method.
    pub async fn pop_warnings(&self) -> Option<Vec<ServerError>> {
        self.am_conn_core.lock_async().await.pop_warnings()
    }

    /// Sets the connection's auto-commit behavior.
    pub async fn set_auto_commit(&self, ac: bool) {
        self.am_conn_core
            .lock_async()
            .await
            .configuration_mut()
            .set_auto_commit(ac);
    }

    /// Returns the connection's auto-commit behavior.
    pub async fn is_auto_commit(&self) -> bool {
        self.am_conn_core
            .lock_async()
            .await
            .configuration()
            .is_auto_commit()
    }

    /// Returns the connection's fetch size.
    pub async fn fetch_size(&self) -> u32 {
        self.am_conn_core
            .lock_async()
            .await
            .configuration()
            .fetch_size()
    }
    /// Configures the connection's fetch size.
    pub async fn set_fetch_size(&self, fetch_size: u32) {
        self.am_conn_core
            .lock_async()
            .await
            .configuration_mut()
            .set_fetch_size(fetch_size);
    }

    /// Returns the connection's lob read length.
    pub async fn lob_read_length(&self) -> u32 {
        self.am_conn_core
            .lock_async()
            .await
            .configuration()
            .lob_read_length()
    }
    /// Configures the connection's lob read length.
    ///
    /// # Errors
    ///
    /// Only `HdbError::Poison` can occur.
    pub async fn set_lob_read_length(&self, l: u32) {
        self.am_conn_core
            .lock_async()
            .await
            .configuration_mut()
            .set_lob_read_length(l);
    }

    /// Returns the connection's lob write length.
    pub async fn lob_write_length(&self) -> u32 {
        self.am_conn_core
            .lock_async()
            .await
            .configuration()
            .lob_write_length()
    }
    /// Sets the connection's lob write length.
    ///
    /// The intention of the parameter is to allow reducing the number of roundtrips
    /// to the database.
    /// Values smaller than rust's buffer size (8k) will have little effect, since
    /// each read() call to the Read impl in a `HdbValue::LOBSTREAM` will cause at most one
    /// write roundtrip to the database.
    pub async fn set_lob_write_length(&self, l: u32) {
        self.am_conn_core
            .lock_async()
            .await
            .configuration_mut()
            .set_lob_write_length(l);
    }

    /// Sets the connection's maximum buffer size.
    ///
    /// See also [`ConnectionConfiguration::set_max_buffer_size`].
    pub async fn set_max_buffer_size(&mut self, max_buffer_size: usize) {
        self.am_conn_core
            .lock_async()
            .await
            .configuration_mut()
            .set_max_buffer_size(max_buffer_size);
    }

    /// Returns the ID of the connection.
    ///
    /// The ID is set by the server. Can be handy for logging.
    pub async fn id(&self) -> u32 {
        self.am_conn_core
            .lock_async()
            .await
            .connect_options()
            .get_connection_id()
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this Connection object.
    pub async fn server_usage(&self) -> ServerUsage {
        self.am_conn_core.lock_async().await.server_usage()
    }

    #[doc(hidden)]
    pub async fn data_format_version_2(&self) -> u8 {
        self.am_conn_core
            .lock_async()
            .await
            .connect_options()
            .get_dataformat_version2()
    }

    #[doc(hidden)]
    pub async fn dump_connect_options(&self) -> String {
        self.am_conn_core.lock_async().await.dump_connect_options()
    }
    #[doc(hidden)]
    pub async fn dump_client_info(&self) -> String {
        self.am_conn_core.lock_async().await.dump_client_info()
    }

    /// Returns some statistics snapshot about what was done with this connection so far.
    pub async fn statistics(&self) -> ConnectionStatistics {
        self.am_conn_core.lock_async().await.statistics().clone()
    }

    /// Reset the counters in the Connection's statistic object.
    pub async fn reset_statistics(&self) {
        self.am_conn_core.lock_async().await.reset_statistics();
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection,HdbResult};
    /// # let mut connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").await.unwrap();
    /// connection.set_application("MyApp, built in rust").await;
    /// # })
    /// ```
    pub async fn set_application<S: AsRef<str>>(&self, application: S) {
        self.am_conn_core
            .lock_async()
            .await
            .set_application(application);
    }

    /// Sets client information into a session variable on the server.
    ///
    /// Example:
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection,HdbResult};
    /// # let mut connection = Connection::new("hdbsql://my_user:my_passwd@the_host:2222").await.unwrap();
    /// connection.set_application_user("K2209657").await;
    /// # })
    /// ```
    pub async fn set_application_user<S: AsRef<str>>(&self, appl_user: S) {
        self.am_conn_core
            .lock_async()
            .await
            .set_application_user(appl_user.as_ref());
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
    pub async fn set_application_version<S: AsRef<str>>(&self, version: S) {
        self.am_conn_core
            .lock_async()
            .await
            .set_application_version(version.as_ref());
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
    pub async fn set_application_source<S: AsRef<str>>(&self, source: S) {
        self.am_conn_core
            .lock_async()
            .await
            .set_application_source(source.as_ref());
    }

    /// Returns an implementation of `dist_tx_async::rm::ResourceManager` that is
    /// based on this connection.
    #[must_use]
    #[cfg(feature = "dist_tx")]
    pub fn get_resource_manager(&self) -> Box<dyn ResourceManager> {
        Box::new(new_resource_manager(self.am_conn_core.clone()))
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
        line: u32,
    ) -> HdbResult<HdbResponse> {
        self.execute(stmt, Some(CommandInfo::new(line, module.as_ref())))
            .await
    }

    /// (MDC) Database name.
    pub async fn get_database_name(&self) -> String {
        self.am_conn_core
            .lock_async()
            .await
            .connect_options()
            .get_database_name()
    }

    /// The system id is set by the server with the SAPSYSTEMNAME of the
    /// connected instance (for tracing and supportability purposes).
    pub async fn get_system_id(&self) -> String {
        self.am_conn_core
            .lock_async()
            .await
            .connect_options()
            .get_system_id()
    }

    /// Returns the information that is given to the server as client context.
    pub async fn client_info(&self) -> Vec<(String, String)> {
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

        let conn_core = self.am_conn_core.lock_async().await;
        let conn_opts = conn_core.connect_options();
        result.push((format!("{:?}", ConnOptId::OSUser), conn_opts.get_os_user()));
        result.push((
            format!("{:?}", ConnOptId::ConnectionID),
            conn_opts.get_connection_id().to_string(),
        ));
        result
    }

    /// Returns a connect url (excluding the password) that reflects the options that were
    /// used to establish this connection.
    pub async fn connect_string(&self) -> String {
        self.am_conn_core.lock_async().await.connect_string()
    }

    /// HANA Full version string.
    ///
    /// # Errors
    ///
    /// Errors are unlikely to occur.
    ///
    /// - `HdbError::ImplDetailed` if the version string was not provided by the database server.
    /// - `HdbError::Poison` if the shared mutex of the inner connection object is poisened.
    pub async fn get_full_version_string(&self) -> String {
        self.am_conn_core
            .lock_async()
            .await
            .connect_options()
            .get_full_version_string()
    }

    async fn execute<S>(
        &self,
        stmt: S,
        o_command_info: Option<CommandInfo>,
    ) -> HdbResult<HdbResponse>
    where
        S: AsRef<str>,
    {
        debug!(
            "connection[{:?}]::execute()",
            self.am_conn_core
                .lock_async()
                .await
                .connect_options()
                .get_connection_id()
        );
        let mut request = Request::new(MessageType::ExecuteDirect, HOLD_CURSORS_OVER_COMMIT);
        {
            let conn_core = self.am_conn_core.lock_async().await;
            let fetch_size = conn_core.configuration().fetch_size();
            request.push(Part::FetchSize(fetch_size));
            if let Some(command_info) = o_command_info {
                request.push(Part::CommandInfo(command_info));
            }
            request.push(Part::Command(stmt.as_ref()));
        }
        let (internal_return_values, replytype) = self
            .am_conn_core
            .send_async(request)
            .await?
            .into_internal_return_values_async(&self.am_conn_core, None)
            .await?;
        HdbResponse::try_new(internal_return_values, replytype)
    }
}
