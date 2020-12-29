use crate::conn::{
    authentication, initial_request, AmConnCore, ConnectParams, SessionState, TcpClient,
};
use crate::protocol::parts::{
    ClientInfo, ConnectOptions, DbConnectInfo, ParameterDescriptors, ResultSetMetadata, RsState,
    ServerError, StatementContext, Topology, TransactionFlags,
};
use crate::protocol::{Part, Reply, ReplyType, Request, RequestType, ServerUsage};
use crate::{HdbError, HdbResult};
use std::mem;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ConnectionCore {
    authenticated: bool,
    session_id: i64,
    client_info: ClientInfo,
    client_info_touched: bool,
    seq_number: i32,
    auto_commit: bool,
    server_usage: ServerUsage,
    fetch_size: u32,
    lob_read_length: u32,
    lob_write_length: usize,
    session_state: SessionState,
    statement_sequence: Option<i64>, // statement sequence within the transaction
    connect_options: ConnectOptions,
    topology: Option<Topology>,
    pub warnings: Vec<ServerError>,
    tcp_conn: TcpClient,
}

impl<'a> ConnectionCore {
    pub(crate) fn try_new(params: ConnectParams) -> HdbResult<Self> {
        // If dbname is specified, run the redirect flow
        let mut conn_core = if let Some(dbname) = params.dbname() {
            let network_group = params.network_group().unwrap_or_default();
            let mut tentative_conn_core = ConnectionCore::try_new_initialized(params.clone())?;
            trace!("Trying redirect flow");
            // send a dbinfo-request with a single part of kind DbConnectInfo,
            // in which the dbname and the network group are specified. TODO WTF is a network group?
            let mut request1 = Request::new(RequestType::DbConnectInfo, 0);
            let db_connect_info: DbConnectInfo = DbConnectInfo::new(dbname, &network_group);
            request1.push(Part::DbConnectInfo(db_connect_info));
            let reply =
                tentative_conn_core.roundtrip_sync(&request1, None, None, None, &mut None)?;
            reply.assert_expected_reply_type(ReplyType::Nil)?;

            let o_part = reply.parts.into_iter().next();
            if let Some(Part::DbConnectInfo(db_connect_info)) = o_part {
                trace!("Received DbConnectInfo");
                if db_connect_info.on_correct_database()? {
                    trace!("We're on the right database already");
                    tentative_conn_core
                } else {
                    trace!("Retrieving correct host and port");
                    let redirect_params =
                        params.redirect(db_connect_info.host()?, db_connect_info.port()?);
                    debug!("Redirected to {}", redirect_params);
                    ConnectionCore::try_new_initialized(redirect_params)?
                }
            } else {
                warn!("Did not find a DbConnectInfo; got {:?}", o_part);
                tentative_conn_core
            }
        } else {
            ConnectionCore::try_new_initialized(params)?
        };

        authentication::authenticate(&mut conn_core, false)?;
        Ok(conn_core)
    }

    fn try_new_initialized(params: ConnectParams) -> HdbResult<Self> {
        let connect_options = ConnectOptions::for_server(params.clientlocale(), get_os_user());
        let mut tcp_conn = TcpClient::try_new(params)?;
        initial_request::send_and_receive(&mut tcp_conn)?;
        Ok(Self {
            authenticated: false,
            session_id: 0,
            seq_number: 0,
            auto_commit: true,
            server_usage: ServerUsage::default(),
            fetch_size: crate::DEFAULT_FETCH_SIZE,
            lob_read_length: crate::DEFAULT_LOB_READ_LENGTH,
            lob_write_length: crate::DEFAULT_LOB_WRITE_LENGTH,
            client_info: ClientInfo::default(),
            client_info_touched: true,
            session_state: SessionState::default(),
            statement_sequence: None,
            connect_options,
            topology: None,
            warnings: Vec::<ServerError>::new(),
            tcp_conn,
        })
    }

    pub(crate) fn reconnect(&mut self) -> HdbResult<()> {
        debug!("Trying to reconnect");
        let mut tcp_conn = TcpClient::try_new(self.tcp_conn.connect_params().clone())?;
        initial_request::send_and_receive(&mut tcp_conn)?;
        self.tcp_conn = tcp_conn;
        self.authenticated = false;
        self.session_id = 0;
        // fetch_size, lob_read_length, lob_write_length are considered automatically

        debug!("Successfully reconnected, not yet authenticated");
        authentication::authenticate(self, true)?;
        debug!("Successfully re-authenticated");
        Ok(())
    }

    pub(crate) fn connect_params(&self) -> &ConnectParams {
        match self.tcp_conn {
            TcpClient::SyncPlain(ref pc) => pc.connect_params(),
            TcpClient::SyncTls(ref sc) => sc.connect_params(),
        }
    }

    pub(crate) fn connect_string(&self) -> String {
        format!("{}", self.connect_params())
    }

    pub(crate) fn set_application<S: AsRef<str>>(&mut self, application: S) -> HdbResult<()> {
        self.client_info.set_application(application);
        self.client_info_touched = true;
        Ok(())
    }

    pub(crate) fn set_application_version(&mut self, version: &str) -> HdbResult<()> {
        self.client_info.set_application_version(version);
        self.client_info_touched = true;
        Ok(())
    }

    pub(crate) fn set_application_source(&mut self, source: &str) -> HdbResult<()> {
        self.client_info.set_application_source(source);
        self.client_info_touched = true;
        Ok(())
    }

    pub(crate) fn set_application_user(&mut self, application_user: &str) -> HdbResult<()> {
        self.client_info.set_application_user(application_user);
        self.client_info_touched = true;
        Ok(())
    }

    pub(crate) fn is_client_info_touched(&self) -> bool {
        self.client_info_touched
    }
    pub(crate) fn get_client_info_for_sending(&mut self) -> ClientInfo {
        debug!("cloning client info for sending");
        self.client_info_touched = false;
        self.client_info.clone()
    }

    pub(crate) fn evaluate_statement_context(
        &mut self,
        stmt_ctx: &StatementContext,
    ) -> HdbResult<()> {
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

        Ok(())
    }

    pub(crate) fn set_auto_commit(&mut self, ac: bool) {
        self.auto_commit = ac;
    }

    pub(crate) fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub(crate) fn server_usage(&self) -> ServerUsage {
        self.server_usage
    }

    pub(crate) fn get_fetch_size(&self) -> u32 {
        self.fetch_size
    }

    pub(crate) fn set_fetch_size(&mut self, fetch_size: u32) {
        self.fetch_size = fetch_size;
    }

    pub(crate) fn get_lob_read_length(&self) -> u32 {
        self.lob_read_length
    }

    pub(crate) fn set_lob_read_length(&mut self, lob_read_length: u32) {
        self.lob_read_length = lob_read_length;
    }

    pub(crate) fn get_lob_write_length(&self) -> usize {
        self.lob_write_length
    }

    pub(crate) fn set_lob_write_length(&mut self, lob_write_length: usize) {
        self.lob_write_length = lob_write_length;
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
        self.connect_options.to_string()
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

    pub(crate) fn next_seq_number(&mut self) -> i32 {
        self.seq_number += 1;
        self.seq_number
    }
    pub(crate) fn last_seq_number(&self) -> i32 {
        self.seq_number
    }

    pub(crate) fn evaluate_ta_flags(&mut self, ta_flags: TransactionFlags) -> HdbResult<()> {
        self.session_state.update(ta_flags);
        if self.session_state.dead {
            Err(HdbError::SessionClosingTransactionError)
        } else {
            Ok(())
        }
    }

    pub(crate) fn pop_warnings(&mut self) -> HdbResult<Option<Vec<ServerError>>> {
        if self.warnings.is_empty() {
            Ok(None)
        } else {
            let mut v = Vec::<ServerError>::new();
            mem::swap(&mut v, &mut self.warnings);
            Ok(Some(v))
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

    pub(crate) fn roundtrip_sync(
        &mut self,
        request: &'a Request<'a>,
        o_am_conn_core: Option<&AmConnCore>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
    ) -> HdbResult<Reply> {
        let (session_id, nsn) = if let RequestType::Authenticate = request.request_type {
            (0, 1)
        } else {
            (self.session_id(), self.next_seq_number())
        };
        let auto_commit = self.is_auto_commit();

        match self.tcp_conn {
            TcpClient::SyncPlain(ref mut pc) => {
                request.emit(session_id, nsn, auto_commit, o_a_descriptors, pc.writer())?;
            }
            TcpClient::SyncTls(ref mut tc) => {
                request.emit(session_id, nsn, auto_commit, o_a_descriptors, tc.writer())?;
            }
        }

        let mut reply = match self.tcp_conn {
            TcpClient::SyncPlain(ref mut pc) => {
                let reader = pc.reader();
                Reply::parse(o_a_rsmd, o_a_descriptors, o_rs, o_am_conn_core, reader)
            }
            TcpClient::SyncTls(ref mut tc) => {
                let reader = tc.reader();
                Reply::parse(o_a_rsmd, o_a_descriptors, o_rs, o_am_conn_core, reader)
            }
        }?;

        reply.handle_db_error(self)?;
        Ok(reply)
    }

    fn drop_impl(&mut self) -> std::io::Result<()> {
        debug!("Drop of ConnectionCore, session_id = {}", self.session_id);
        if self.authenticated {
            let request = Request::new_for_disconnect();
            let session_id = self.session_id();
            let nsn = self.next_seq_number();
            match self.tcp_conn {
                TcpClient::SyncPlain(ref mut pc) => {
                    request.emit(session_id, nsn, false, None, pc.writer())?;
                }
                TcpClient::SyncTls(ref mut tc) => {
                    request.emit(session_id, nsn, false, None, tc.writer())?;
                }
            }
            trace!("Disconnect: request successfully sent");
        }
        Ok(())
    }
}

impl Drop for ConnectionCore {
    // try to send a disconnect to the database, ignore all errors
    fn drop(&mut self) {
        if let Err(e) = self.drop_impl() {
            warn!("Disconnect request failed with {:?}", e);
        }
    }
}

fn get_os_user() -> String {
    let os_user = username::get_user_name().unwrap_or_default();
    trace!("OS user: {}", os_user);
    os_user
}
