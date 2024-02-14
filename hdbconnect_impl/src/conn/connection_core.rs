use crate::{
    base::RsState,
    conn::{
        authentication, initial_request, AmConnCore, AuthenticationResult, CommandOptions,
        ConnectParams, ConnectionConfiguration, ConnectionStatistics, SessionState, TcpClient,
    },
    protocol::{
        parts::{
            ClientInfo, ConnectOptions, DbConnectInfo, ParameterDescriptors, ResultSetMetadata,
            ServerError, StatementContext, Topology, TransactionFlags,
        },
        MessageType, Part, Reply, ReplyType, Request, ServerUsage,
    },
    HdbError, HdbResult,
};
use debug_ignore::DebugIgnore;
#[cfg(feature = "sync")]
use std::time::Duration;
use std::{io::Cursor, io::ErrorKind, mem, sync::Arc};

#[doc(hidden)]
#[derive(Debug)]
pub(crate) struct ConnectionCore {
    authenticated: bool,
    session_id: i64,
    client_info: ClientInfo,
    client_info_touched: bool,
    statistics: ConnectionStatistics,
    server_usage: ServerUsage,
    config: ConnectionConfiguration,
    session_state: SessionState,
    statement_sequence: Option<i64>, // statement sequence within the transaction
    connect_options: ConnectOptions,
    topology: Option<Topology>,
    pub(crate) warnings: Vec<ServerError>,
    tcp_client: TcpClient,
    io_buffer: DebugIgnore<Cursor<Vec<u8>>>,
}

impl<'a> ConnectionCore {
    #[cfg(feature = "sync")]
    pub(crate) fn try_new_sync(
        params: ConnectParams,
        config: &ConnectionConfiguration,
    ) -> HdbResult<Self> {
        let o_dbname = params.dbname().map(ToString::to_string);
        let network_group = params.network_group().unwrap_or_default().to_string();
        let mut conn_core = ConnectionCore::try_new_initialized_sync(params, config)?;
        if let Some(dbname) = o_dbname {
            // since a dbname is specified, we ask explicitly for a redirect
            trace!("Redirect to {dbname} initiated by client");
            let mut request = Request::new(MessageType::DbConnectInfo, CommandOptions::EMPTY);
            request.push(Part::DbConnectInfo(DbConnectInfo::new(
                dbname,
                network_group,
            )));
            let reply = conn_core.roundtrip_sync(&request, None, None, None, &mut None)?;
            reply.assert_expected_reply_type(ReplyType::Nil)?;

            match reply.parts.into_iter().next() {
                Some(Part::DbConnectInfo(db_connect_info)) => {
                    trace!("Received DbConnectInfo");
                    if db_connect_info.on_correct_database()? {
                        trace!("Already connected to the right database");
                    } else {
                        let redirect_params = conn_core
                            .connect_params()
                            .redirect(db_connect_info.host()?, db_connect_info.port()?);
                        debug!("Redirected (1) to {}", redirect_params);
                        conn_core =
                            ConnectionCore::try_new_initialized_sync(redirect_params, config)?;
                    }
                }
                o_part => {
                    warn!("Did not find a DbConnectInfo; got {:?}", o_part);
                }
            }
        };

        // here we can encounter an additional implicit redirect, triggered by HANA itself
        loop {
            match authentication::authenticate_sync(&mut conn_core, false)? {
                AuthenticationResult::Ok => return Ok(conn_core),
                AuthenticationResult::Redirect(db_connect_info) => {
                    trace!("Redirect initiated by HANA");
                    let redirect_params = conn_core
                        .connect_params()
                        .redirect(db_connect_info.host()?, db_connect_info.port()?);
                    debug!("Redirected (2) to {}", redirect_params);
                    conn_core = ConnectionCore::try_new_initialized_sync(redirect_params, config)?;
                }
            }
        }
    }

    #[cfg(feature = "async")]
    pub(crate) async fn try_new_async(
        params: ConnectParams,
        config: &ConnectionConfiguration,
    ) -> HdbResult<Self> {
        let o_dbname = params.dbname().map(ToString::to_string);
        let network_group = params.network_group().unwrap_or_default().to_string();
        let mut conn_core = ConnectionCore::try_new_initialized_async(params, config).await?;
        if let Some(dbname) = o_dbname {
            // since a dbname is specified, we ask explicitly for a redirect
            trace!("Redirect to {dbname} initiated by client");
            let mut request = Request::new(MessageType::DbConnectInfo, CommandOptions::EMPTY);
            request.push(Part::DbConnectInfo(DbConnectInfo::new(
                dbname,
                network_group,
            )));
            let reply = conn_core
                .roundtrip_async(&request, None, None, None, &mut None)
                .await?;
            reply.assert_expected_reply_type(ReplyType::Nil)?;

            match reply.parts.into_iter().next() {
                Some(Part::DbConnectInfo(db_connect_info)) => {
                    trace!("Received DbConnectInfo");
                    if db_connect_info.on_correct_database()? {
                        trace!("Already connected to the right database");
                    } else {
                        let redirect_params = conn_core
                            .connect_params()
                            .redirect(db_connect_info.host()?, db_connect_info.port()?);
                        debug!("Redirected (1) to {}", redirect_params);
                        conn_core =
                            ConnectionCore::try_new_initialized_async(redirect_params, config)
                                .await?;
                    }
                }
                o_part => {
                    warn!("Did not find a DbConnectInfo; got {:?}", o_part);
                }
            }
        };

        // here we can encounter an additional implicit redirect, triggered by HANA itself
        loop {
            match authentication::authenticate_async(&mut conn_core, false).await? {
                AuthenticationResult::Ok => return Ok(conn_core),
                AuthenticationResult::Redirect(db_connect_info) => {
                    trace!("Redirect initiated by HANA");
                    let redirect_params = conn_core
                        .connect_params()
                        .redirect(db_connect_info.host()?, db_connect_info.port()?);
                    debug!("Redirected (2) to {}", redirect_params);
                    conn_core =
                        ConnectionCore::try_new_initialized_async(redirect_params, config).await?;
                }
            }
        }
    }

    #[cfg(feature = "sync")]
    fn try_new_initialized_sync(
        params: ConnectParams,
        config: &ConnectionConfiguration,
    ) -> HdbResult<Self> {
        let connect_options =
            ConnectOptions::new(params.clientlocale(), &get_os_user(), params.compression());
        let mut tcp_client = TcpClient::try_new_sync(params, config.read_timeout())?;
        initial_request::send_and_receive_sync(&mut tcp_client)?;
        Ok(Self {
            authenticated: false,
            session_id: 0,
            statistics: ConnectionStatistics::new(),
            server_usage: ServerUsage::default(),
            io_buffer: DebugIgnore::from(Cursor::new(Vec::<u8>::with_capacity(
                ConnectionConfiguration::MIN_BUFFER_SIZE,
            ))),
            config: config.clone(),
            client_info: ClientInfo::default(),
            client_info_touched: true,
            session_state: SessionState::default(),
            statement_sequence: None,
            connect_options,
            topology: None,
            warnings: Vec::<ServerError>::new(),
            tcp_client,
        })
    }

    #[cfg(feature = "async")]
    async fn try_new_initialized_async(
        params: ConnectParams,
        config: &ConnectionConfiguration,
    ) -> HdbResult<Self> {
        let connect_options =
            ConnectOptions::new(params.clientlocale(), &get_os_user(), params.compression());
        let mut tcp_client = TcpClient::try_new_async(params).await?;
        initial_request::send_and_receive_async(&mut tcp_client).await?;
        Ok(Self {
            authenticated: false,
            session_id: 0,
            statistics: ConnectionStatistics::new(),
            server_usage: ServerUsage::default(),
            io_buffer: DebugIgnore::from(Cursor::new(Vec::<u8>::with_capacity(
                ConnectionConfiguration::MIN_BUFFER_SIZE,
            ))),
            config: config.clone(),
            client_info: ClientInfo::default(),
            client_info_touched: true,
            session_state: SessionState::default(),
            statement_sequence: None,
            connect_options,
            topology: None,
            warnings: Vec::<ServerError>::new(),
            tcp_client,
        })
    }

    #[cfg(feature = "sync")]
    pub(crate) fn reconnect_sync(&mut self) -> HdbResult<()> {
        warn!("Trying to reconnect");
        let mut conn_params = self.tcp_client.connect_params().clone();
        loop {
            let mut tcp_conn =
                TcpClient::try_new_sync(conn_params.clone(), self.config.read_timeout())?;
            initial_request::send_and_receive_sync(&mut tcp_conn)?;
            self.tcp_client = tcp_conn;
            self.authenticated = false;
            self.session_id = 0;
            // fetch_size, lob_read_length, lob_write_length are considered automatically

            debug!("Reconnected, not yet authenticated");
            match authentication::authenticate_sync(self, true)? {
                AuthenticationResult::Ok => {
                    debug!("Re-authenticated");
                    return Ok(());
                }
                AuthenticationResult::Redirect(db_connect_info) => {
                    debug!("Redirected");
                    conn_params = self
                        .tcp_client
                        .connect_params()
                        .redirect(db_connect_info.host()?, db_connect_info.port()?);
                }
            }
        }
    }

    #[cfg(feature = "async")]
    pub(crate) async fn reconnect_async(&mut self) -> HdbResult<()> {
        debug!("Trying to reconnect");
        let mut conn_params = self.tcp_client.connect_params().clone();
        loop {
            let mut tcp_client = TcpClient::try_new_async(conn_params.clone()).await?;
            initial_request::send_and_receive_async(&mut tcp_client).await?;
            self.tcp_client = tcp_client;
            self.authenticated = false;
            self.session_id = 0;
            // fetch_size, lob_read_length, lob_write_length are considered automatically

            debug!("Reconnected, not yet authenticated");
            match authentication::authenticate_async(self, true).await? {
                AuthenticationResult::Ok => {
                    debug!("Re-authenticated");
                    return Ok(());
                }
                AuthenticationResult::Redirect(db_connect_info) => {
                    debug!("Redirected");
                    conn_params = self
                        .tcp_client
                        .connect_params()
                        .redirect(db_connect_info.host()?, db_connect_info.port()?);
                }
            }
        }
    }

    pub(crate) fn connect_params(&self) -> &ConnectParams {
        match self.tcp_client {
            #[cfg(feature = "sync")]
            TcpClient::SyncPlain(ref cl) => cl.connect_params(),
            #[cfg(feature = "sync")]
            TcpClient::SyncTls(ref cl) => cl.connect_params(),
            #[cfg(feature = "async")]
            TcpClient::AsyncPlain(ref cl) => cl.connect_params(),
            #[cfg(feature = "async")]
            TcpClient::AsyncTls(ref cl) => cl.connect_params(),
            TcpClient::Dead { ref params } => params,
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn set_read_timeout_sync(
        &mut self,
        client_timeout: Option<Duration>,
    ) -> HdbResult<()> {
        self.tcp_client.set_read_timeout_sync(client_timeout)
    }

    pub(crate) fn connect_string(&self) -> String {
        format!("{}", self.connect_params())
    }

    pub(crate) fn set_application<S: AsRef<str>>(&mut self, application: S) {
        self.client_info.set_application(application);
        self.client_info_touched = true;
    }

    pub(crate) fn set_application_version(&mut self, version: &str) {
        self.client_info.set_application_version(version);
        self.client_info_touched = true;
    }

    pub(crate) fn set_application_source(&mut self, source: &str) {
        self.client_info.set_application_source(source);
        self.client_info_touched = true;
    }

    pub(crate) fn set_application_user(&mut self, application_user: &str) {
        self.client_info.set_application_user(application_user);
        self.client_info_touched = true;
    }

    pub(crate) fn is_client_info_touched(&self) -> bool {
        self.client_info_touched
    }
    pub(crate) fn get_client_info_for_sending(&mut self) -> ClientInfo {
        debug!("cloning client info for sending");
        self.client_info_touched = false;
        self.client_info.clone()
    }

    pub(crate) fn evaluate_statement_context(&mut self, stmt_ctx: &StatementContext) {
        trace!(
            "Received StatementContext with sequence_info = {:?}",
            stmt_ctx.statement_sequence_info()
        );
        self.set_statement_sequence(stmt_ctx.statement_sequence_info());
        self.server_usage.update(
            stmt_ctx.server_processing_time(),
            stmt_ctx.server_cpu_time(),
            stmt_ctx.server_memory_usage(),
        );
        // todo do not ignore the other content of StatementContext
        // StatementContextId::SchemaName => 3,
        // StatementContextId::FlagSet => 4,
        // StatementContextId::QueryTimeout => 5,
        // StatementContextId::ClientReconnectionWaitTimeout => 6,
    }

    pub(crate) fn server_usage(&self) -> ServerUsage {
        self.server_usage
    }

    pub(crate) fn configuration(&self) -> &ConnectionConfiguration {
        &self.config
    }

    pub(crate) fn configuration_mut(&mut self) -> &mut ConnectionConfiguration {
        &mut self.config
    }

    pub(crate) fn set_session_id(&mut self, session_id: i64) {
        if session_id != self.session_id {
            debug!(
                "ConnectionCore: setting session_id from {} to {}",
                self.session_id, session_id
            );
            self.session_id = session_id;
        }
    }

    pub(crate) fn set_topology(&mut self, topology: Topology) {
        self.topology = Some(topology);
    }

    pub(crate) fn dump_client_info(&self) -> String {
        self.client_info.to_string()
    }

    pub(crate) fn dump_connect_options(&self) -> String {
        format!("{:?}", self.connect_options)
    }

    pub(crate) fn set_authenticated(&mut self) {
        self.authenticated = true;
    }

    pub(crate) fn statement_sequence(&self) -> &Option<i64> {
        &self.statement_sequence
    }

    fn set_statement_sequence(&mut self, statement_sequence: Option<i64>) {
        self.statement_sequence = statement_sequence;
    }

    pub(crate) fn session_id(&self) -> i64 {
        self.session_id
    }

    pub(crate) fn next_sequence_number(&mut self) -> u32 {
        self.statistics.next_sequence_number()
    }
    pub(crate) fn statistics(&self) -> &ConnectionStatistics {
        &self.statistics
    }
    pub(crate) fn reset_statistics(&mut self) {
        self.statistics.reset();
    }

    pub(crate) fn evaluate_ta_flags(&mut self, ta_flags: TransactionFlags) -> HdbResult<()> {
        self.session_state.update(ta_flags);
        if self.session_state.dead {
            Err(HdbError::SessionClosingTransactionError)
        } else {
            Ok(())
        }
    }

    pub(crate) fn pop_warnings(&mut self) -> Option<Vec<ServerError>> {
        if self.warnings.is_empty() {
            None
        } else {
            let mut v = Vec::<ServerError>::new();
            mem::swap(&mut v, &mut self.warnings);
            Some(v)
        }
    }

    pub(crate) fn connect_options(&self) -> &ConnectOptions {
        &self.connect_options
    }

    pub(crate) fn connect_options_mut(&mut self) -> &mut ConnectOptions {
        &mut self.connect_options
    }

    pub(crate) fn augment_request(&mut self, request: &mut Request<'a>) {
        if self.authenticated {
            if let Some(ssi_value) = self.statement_sequence() {
                request.add_statement_context(*ssi_value);
            }
            if self.is_client_info_touched() {
                request.push(Part::ClientInfo(self.get_client_info_for_sending()));
            }
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn roundtrip_sync(
        &mut self,
        request: &'a Request<'a>,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
    ) -> HdbResult<Reply> {
        let (session_id, nsn, default_error_handling) =
            if let MessageType::Authenticate = request.message_type() {
                (0, 1, false)
            } else {
                (self.session_id, self.next_sequence_number(), true)
            };
        let compress = self.connect_options().use_compression();

        let w: &mut dyn std::io::Write = match self.tcp_client {
            TcpClient::SyncPlain(ref mut cl) => cl.writer(),
            TcpClient::SyncTls(ref mut cl) => cl.writer(),
            TcpClient::Dead { .. } => return Err(HdbError::ConnectionBroken { source: None }),
            #[cfg(feature = "async")]
            _ => unreachable!("Async connections not supported here"),
        };

        let start = request
            .emit_sync(
                session_id,
                nsn,
                &self.config,
                compress,
                o_a_descriptors,
                &mut self.statistics,
                &mut self.io_buffer,
                w,
            )
            .map_err(|e| {
                info!(
                    "roundtrip_sync(): TCP connection discarded because write failed with \"{e}\""
                );
                self.tcp_client.die();
                HdbError::ConnectionBroken {
                    source: Some(Box::new(e)),
                }
            })?;

        let rdr: &mut dyn std::io::Read = match self.tcp_client {
            TcpClient::SyncPlain(ref mut cl) => cl.reader(),
            TcpClient::SyncTls(ref mut cl) => cl.reader(),
            TcpClient::Dead { .. } => return Err(HdbError::ConnectionBroken { source: None }),
            #[cfg(feature = "async")]
            _ => unreachable!("Async connections not supported here"),
        };
        let mut reply = match Reply::parse_sync(
            o_a_rsmd,
            o_a_descriptors,
            o_rs,
            o_am_conn_core,
            &mut self.statistics,
            start,
            &mut self.io_buffer,
            rdr,
        ) {
            Ok(reply) => reply,
            Err(e) => {
                info!("roundtrip_sync(): TCP connection discarded after \"{e}\"");
                self.tcp_client.die();
                return Err(connection_broken(e, &self.config.read_timeout()));
            }
        };

        if self.io_buffer.get_ref().capacity() > self.config.max_buffer_size() {
            *(self.io_buffer.get_mut()) = Vec::with_capacity(self.config.max_buffer_size());
            self.statistics.add_buffer_shrinking();
        }

        if default_error_handling {
            reply.handle_db_error(self)?;
        }
        Ok(reply)
    }

    #[cfg(feature = "async")]
    pub(crate) async fn roundtrip_async(
        &mut self,
        request: &'a Request<'a>,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
    ) -> HdbResult<Reply> {
        let (session_id, nsn, default_error_handling) =
            if let MessageType::Authenticate = request.message_type() {
                (0, 1, false)
            } else {
                (self.session_id(), self.next_sequence_number(), true)
            };
        let compress = self.connect_options().use_compression();

        let start = match self.tcp_client {
            TcpClient::AsyncPlain(ref mut cl) => {
                request
                    .emit_async(
                        session_id,
                        nsn,
                        &self.config,
                        compress,
                        o_a_descriptors,
                        &mut self.statistics,
                        &mut self.io_buffer,
                        cl.writer(),
                    )
                    .await
            }
            TcpClient::AsyncTls(ref mut cl) => {
                request
                    .emit_async(
                        session_id,
                        nsn,
                        &self.config,
                        compress,
                        o_a_descriptors,
                        &mut self.statistics,
                        &mut self.io_buffer,
                        cl.writer(),
                    )
                    .await
            }
            TcpClient::Dead { .. } => return Err(HdbError::ConnectionBroken { source: None }),
            #[cfg(feature = "sync")]
            _ => unreachable!("Sync connections not supported here"),
        }
        .map_err(|e| {
            info!("roundtrip_async(): TCP connection discarded because write failed with \"{e}\"");
            self.tcp_client.die();
            HdbError::ConnectionBroken {
                source: Some(Box::new(e)),
            }
        })?;

        let mut reply = if let Some(timeout) = self.config.read_timeout() {
            match tokio::time::timeout(
                timeout,
                Reply::parse_async(
                    o_a_rsmd,
                    o_a_descriptors,
                    o_rs,
                    o_am_conn_core,
                    start,
                    &mut self.statistics,
                    &mut self.io_buffer,
                    &mut self.tcp_client,
                ),
            )
            .await
            {
                Ok(res) => res,
                Err(_e) => Err(HdbError::Io {
                    source: std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("connection is broken (connection's read timeout was set to {timeout:?})"),
                    ),
                }),
            }
        } else {
            Reply::parse_async(
                o_a_rsmd,
                o_a_descriptors,
                o_rs,
                o_am_conn_core,
                start,
                &mut self.statistics,
                &mut self.io_buffer,
                &mut self.tcp_client,
            )
            .await
        }.map_err(|e|{
            info!("roundtrip_async(): TCP connection discarded after \"{e}\"");
            self.tcp_client.die();
            connection_broken(e, &self.config.read_timeout())
        })?;

        if self.io_buffer.get_ref().capacity() > self.config.max_buffer_size() {
            *(self.io_buffer.get_mut()) = Vec::with_capacity(self.config.max_buffer_size());
            self.statistics.add_buffer_shrinking();
        }

        if default_error_handling {
            reply.handle_db_error(self)?;
        }
        Ok(reply)
    }

    pub(crate) fn is_broken(&self) -> bool {
        self.tcp_client.has_died()
    }
}

impl Drop for ConnectionCore {
    // try to send a disconnect to the database, ignore all errors
    fn drop(&mut self) {
        debug!("Drop of ConnectionCore, session_id = {}", self.session_id);
        #[cfg(any(feature = "sync", feature = "async"))]
        if self.authenticated {
            let request = Request::new_for_disconnect();
            let session_id = self.session_id();
            let nsn = self.next_sequence_number();
            #[cfg(feature = "sync")]
            {
                let w: &mut dyn std::io::Write = match self.tcp_client {
                    TcpClient::SyncPlain(ref mut cl) => cl.writer() as &mut dyn std::io::Write,
                    TcpClient::SyncTls(ref mut cl) => cl.writer() as &mut dyn std::io::Write,
                    TcpClient::Dead { .. } => return,
                    #[cfg(feature = "async")]
                    _ => unreachable!("Async connections not supported here"),
                };
                request
                    .emit_sync(
                        session_id,
                        nsn,
                        &self.config,
                        false,
                        None,
                        &mut self.statistics,
                        &mut self.io_buffer,
                        w,
                    )
                    .map_err(|e| {
                        warn!("Disconnect request failed with {:?}", e);
                        e
                    })
                    .ok();
            }
            #[cfg(feature = "async")]
            {
                let mut tcp_client = TcpClient::Dead {
                    params: self.tcp_client.connect_params().clone(),
                };
                std::mem::swap(&mut tcp_client, &mut self.tcp_client);
                let mut io_buffer = Cursor::new(Vec::<u8>::with_capacity(200));
                let config = self.configuration().clone();
                // see https://www.reddit.com/r/rust/comments/vckd9h/async_drop/
                tokio::spawn(async move {
                    match tcp_client {
                        TcpClient::AsyncPlain(ref mut cl) => {
                            request
                                .emit_async(
                                    session_id,
                                    nsn,
                                    &config,
                                    false,
                                    None,
                                    &mut ConnectionStatistics::new(),
                                    &mut io_buffer,
                                    cl.writer(),
                                )
                                .await
                                .ok();
                        }
                        TcpClient::AsyncTls(ref mut cl) => {
                            request
                                .emit_async(
                                    session_id,
                                    nsn,
                                    &config,
                                    false,
                                    None,
                                    &mut ConnectionStatistics::new(),
                                    &mut io_buffer,
                                    cl.writer(),
                                )
                                .await
                                .ok();
                        }
                        TcpClient::Dead { .. } => {}
                        #[cfg(feature = "sync")]
                        _ => unreachable!("Sync connections not supported here"),
                    }
                    trace!("Disconnect: request successfully sent");
                });
            }
        }
    }
}

fn get_os_user() -> String {
    let os_user = username::get_user_name().unwrap_or_default();
    trace!("OS user: {}", os_user);
    os_user
}

fn connection_broken(mut e: HdbError, o_timeout: &Option<std::time::Duration>) -> HdbError {
    if let HdbError::Io {
        source: ref mut io_error,
    } = e
    {
        // timeout in linux: WouldBlock, timeout in windows: TimedOut
        if matches!(io_error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
            *io_error = std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                if let Some(timeout) = o_timeout {
                    format!(
                        "connection is broken (connection's read timeout had value {timeout:?})"
                    )
                } else {
                    "connection is broken (connection had no read timeout)".to_string()
                },
            );
        }
    }

    HdbError::ConnectionBroken {
        source: Some(Box::new(e)),
    }
}
