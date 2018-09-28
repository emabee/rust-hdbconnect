use conn_core::buffalo::Buffalo;
use conn_core::connect_params::ConnectParams;
use conn_core::initial_request;
use conn_core::session_state::SessionState;
use protocol::part::Part;
use protocol::parts::client_info::ClientInfo;
use protocol::parts::connect_options::ConnectOptions;
use protocol::parts::parameter_descriptor::ParameterDescriptor;
use protocol::parts::resultset::ResultSet;
use protocol::parts::resultset_metadata::ResultSetMetadata;
use protocol::parts::server_error::ServerError;
use protocol::parts::statement_context::StatementContext;
use protocol::parts::topology::Topology;
use protocol::parts::transactionflags::TransactionFlags;
use protocol::reply::parse_message_and_sequence_header;
use protocol::reply::Reply;
use protocol::reply::SkipLastSpace;
use protocol::reply_type::ReplyType;
use protocol::request::Request;
use protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use protocol::util;
use std::cell::RefCell;
use {HdbError, HdbResult};

use std::io;
use std::mem;
use std::sync::{Arc, Mutex};

pub type AmConnCore = Arc<Mutex<ConnectionCore>>;

pub const DEFAULT_FETCH_SIZE: u32 = 32;
pub const DEFAULT_LOB_READ_LENGTH: i32 = 1_000_000;
const HOLD_OVER_COMMIT: u8 = 8;

#[derive(Debug)]
pub struct ConnectionCore {
    authenticated: bool,
    session_id: i64,
    client_info: ClientInfo,
    client_info_touched: bool,
    command_options: u8,
    seq_number: i32,
    auto_commit: bool,
    server_resource_consumption_info: ServerResourceConsumptionInfo,
    fetch_size: u32,
    lob_read_length: i32,
    session_state: SessionState,
    statement_sequence: Option<i64>, // statement sequence within the transaction
    connect_options: ConnectOptions,
    topology: Option<Topology>,
    pub warnings: Vec<ServerError>,
    buffalo: Buffalo,
}

impl ConnectionCore {
    pub fn initialize(params: ConnectParams) -> HdbResult<AmConnCore> {
        let mut buffalo = Buffalo::new(params)?;

        initial_request::send_and_receive(&mut buffalo)?;

        Ok(Arc::new(Mutex::new(ConnectionCore {
            authenticated: false,
            session_id: 0,
            seq_number: 0,
            command_options: HOLD_OVER_COMMIT,
            auto_commit: true,
            server_resource_consumption_info: Default::default(),
            fetch_size: DEFAULT_FETCH_SIZE,
            lob_read_length: DEFAULT_LOB_READ_LENGTH,
            client_info: Default::default(),
            client_info_touched: false,
            session_state: Default::default(),
            statement_sequence: None,
            connect_options: Default::default(),
            topology: None,
            warnings: Vec::<ServerError>::new(),
            buffalo,
        })))
    }

    pub fn set_application_info(&mut self, version: &str, source: &str) -> HdbResult<()> {
        self.client_info.set_application_version(version);
        self.client_info.set_application_source(source);
        self.client_info_touched = true;
        Ok(())
    }

    pub fn set_application_user(&mut self, application_user: &str) -> HdbResult<()> {
        self.client_info.set_application_user(application_user);
        self.client_info_touched = true;
        Ok(())
    }

    pub fn is_client_info_touched(&self) -> bool {
        self.client_info_touched
    }
    pub fn get_client_info_for_sending(&mut self) -> ClientInfo {
        debug!("cloning client info for sending");
        self.client_info_touched = false;
        self.client_info.clone()
    }

    pub fn evaluate_statement_context(&mut self, stmt_ctx: &StatementContext) -> HdbResult<()> {
        trace!(
            "Received StatementContext with sequence_info = {:?}",
            stmt_ctx.get_statement_sequence_info()
        );
        self.set_statement_sequence(stmt_ctx.get_statement_sequence_info());
        self.server_resource_consumption_info.update(
            stmt_ctx.get_server_processing_time(),
            stmt_ctx.get_server_cpu_time(),
            stmt_ctx.get_server_memory_usage(),
        );
        // FIXME do not ignore the other content of StatementContext
        // StatementContextId::SchemaName => 3,
        // StatementContextId::FlagSet => 4,
        // StatementContextId::QueryTimeout => 5,
        // StatementContextId::ClientReconnectionWaitTimeout => 6,

        Ok(())
    }

    pub fn set_auto_commit(&mut self, ac: bool) {
        self.auto_commit = ac;
    }

    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit
    }

    pub fn server_resource_consumption_info(&self) -> &ServerResourceConsumptionInfo {
        &self.server_resource_consumption_info
    }

    #[deprecated]
    pub fn get_server_proc_time(&self) -> i32 {
        self.server_resource_consumption_info.acc_server_proc_time
    }

    pub fn get_fetch_size(&self) -> u32 {
        self.fetch_size
    }

    pub fn set_fetch_size(&mut self, fetch_size: u32) {
        self.fetch_size = fetch_size;
    }

    pub fn get_lob_read_length(&self) -> i32 {
        self.lob_read_length
    }

    pub fn set_lob_read_length(&mut self, lob_read_length: i32) {
        self.lob_read_length = lob_read_length;
    }

    pub fn set_session_id(&mut self, session_id: i64) {
        self.session_id = session_id;
    }

    pub fn set_topology(&mut self, topology: Topology) {
        self.topology = Some(topology);
    }

    pub fn transfer_server_connect_options(&mut self, conn_opts: ConnectOptions) -> HdbResult<()> {
        self.connect_options
            .transfer_server_connect_options(conn_opts)
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn set_authenticated(&mut self, authenticated: bool) {
        self.authenticated = authenticated;
    }

    pub fn statement_sequence(&self) -> &Option<i64> {
        &self.statement_sequence
    }

    fn set_statement_sequence(&mut self, statement_sequence: Option<i64>) {
        self.statement_sequence = statement_sequence;
    }

    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    fn reader(&self) -> &RefCell<io::BufRead> {
        self.buffalo.reader()
    }

    fn writer(&self) -> &RefCell<io::Write> {
        self.buffalo.writer()
    }

    pub fn next_seq_number(&mut self) -> i32 {
        self.seq_number += 1;
        self.seq_number
    }
    pub fn last_seq_number(&self) -> i32 {
        self.seq_number
    }

    pub fn evaluate_ta_flags(&mut self, ta_flags: TransactionFlags) -> HdbResult<()> {
        self.session_state.update(ta_flags);
        if self.session_state.dead {
            Err(HdbError::DbIssue(
                "SessionclosingTaError received".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn get_database_name(&self) -> &str {
        self.connect_options
            .get_database_name()
            .map(|s| s.as_ref())
            .unwrap_or("")
    }
    pub fn get_system_id(&self) -> &str {
        self.connect_options
            .get_system_id()
            .map(|s| s.as_ref())
            .unwrap_or("")
    }
    pub fn get_full_version_string(&self) -> &str {
        self.connect_options
            .get_full_version_string()
            .map(|s| s.as_ref())
            .unwrap_or("")
    }

    pub fn pop_warnings(&mut self) -> HdbResult<Option<Vec<ServerError>>> {
        if self.warnings.is_empty() {
            Ok(None)
        } else {
            let mut v = Vec::<ServerError>::new();
            mem::swap(&mut v, &mut self.warnings);
            Ok(Some(v))
        }
    }

    #[allow(unknown_lints)]
    #[allow(too_many_arguments)]
    pub fn roundtrip(
        &mut self,
        request: Request,
        am_conn_core: &AmConnCore,
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
    ) -> HdbResult<Reply> {
        let request_type = request.request_type.clone();
        let auto_commit_flag: i8 = if self.is_auto_commit() { 1 } else { 0 };
        let nsn = self.next_seq_number();
        {
            let writer = &mut *(self.writer().borrow_mut());
            request.serialize(self.session_id(), nsn, auto_commit_flag, writer)?;
        }
        {
            let rdr = &mut *(self.reader().borrow_mut());

            let reply = Reply::parse(
                o_rs_md,
                o_par_md,
                o_rs,
                am_conn_core,
                expected_reply_type,
                skip,
                rdr,
            )?;
            trace!(
                "ConnectionCore::roundtrip(): request type {:?}, reply type {:?}",
                request_type,
                reply.replytype
            );

            Ok(reply)
        }
    }

    fn drop_impl(&mut self) -> HdbResult<()> {
        trace!("Drop of ConnectionCore, session_id = {}", self.session_id);
        if self.authenticated {
            let request = Request::new_for_disconnect();
            {
                let nsn = self.next_seq_number();
                let mut writer = self.buffalo.writer().borrow_mut();
                request.serialize(self.session_id, nsn, 0, &mut *writer)?;
                writer.flush()?;
                trace!("Disconnect: request successfully sent");
            }
            {
                let mut reader = self.buffalo.reader().borrow_mut();
                match parse_message_and_sequence_header(&mut *reader) {
                    Ok((no_of_parts, mut reply)) => {
                        trace!(
                            "Disconnect: response header parsed, now parsing {} parts",
                            no_of_parts
                        );
                        for _ in 0..no_of_parts {
                            let (part, padsize) = Part::parse(
                                &mut (reply.parts),
                                None,
                                None,
                                None,
                                &mut None,
                                &mut *reader,
                            )?;
                            util::skip_bytes(padsize, &mut *reader)?;
                            debug!("Drop of connection: got Part {:?}", part);
                        }
                        trace!("Disconnect: response successfully parsed");
                    }
                    Err(e) => {
                        trace!("Disconnect: could not parse response due to {:?}", e);
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for ConnectionCore {
    // try to send a disconnect to the database, ignore all errors
    fn drop(&mut self) {
        if let Err(e) = self.drop_impl() {
            error!("Disconnect request failed with {:?}", e);
        }
    }
}
