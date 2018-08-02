use protocol::lowlevel::initial_request;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::parts::client_info::ClientInfo;
use protocol::lowlevel::parts::connect_options::ConnectOptions;
use protocol::lowlevel::parts::server_error::ServerError;
use protocol::lowlevel::parts::statement_context::StatementContext;
use protocol::lowlevel::parts::topology::Topology;
use protocol::lowlevel::parts::transactionflags::SessionState;
use protocol::lowlevel::parts::transactionflags::TransactionFlags;
use protocol::lowlevel::reply::parse_message_and_sequence_header;
use protocol::lowlevel::request::Request;
use protocol::lowlevel::server_resource_consumption_info::ServerResourceConsumptionInfo;
use protocol::lowlevel::util;
use {HdbError, HdbResult};

use std::io;
use std::mem;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub type AmConnCore = Arc<Mutex<ConnectionCore>>;

pub const DEFAULT_FETCH_SIZE: u32 = 32;
pub const DEFAULT_LOB_READ_LENGTH: i32 = 1_000_000;

#[derive(Debug)]
pub struct ConnectionCore {
    authenticated: bool,
    session_id: i64,
    major_product_version: i8,
    minor_product_version: i16,
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
    writer: io::BufWriter<TcpStream>,
    reader: io::BufReader<TcpStream>,
}

impl ConnectionCore {
    pub fn initialize(mut tcp_stream: TcpStream) -> HdbResult<AmConnCore> {
        let (major_product_version, minor_product_version) =
            initial_request::send_and_receive(&mut tcp_stream)?;
        const HOLD_OVER_COMMIT: u8 = 8;

        Ok(Arc::new(Mutex::new(ConnectionCore {
            authenticated: false,
            session_id: 0,
            seq_number: 0,
            command_options: HOLD_OVER_COMMIT,
            auto_commit: true,
            server_resource_consumption_info: Default::default(),
            fetch_size: DEFAULT_FETCH_SIZE,
            lob_read_length: DEFAULT_LOB_READ_LENGTH,
            major_product_version,
            minor_product_version,
            client_info: Default::default(),
            client_info_touched: false,
            session_state: Default::default(),
            statement_sequence: None,
            connect_options: Default::default(),
            topology: None,
            warnings: Vec::<ServerError>::new(),
            writer: io::BufWriter::with_capacity(20_480_usize, tcp_stream.try_clone()?),
            reader: io::BufReader::new(tcp_stream),
        })))
    }

    pub fn set_client_info(
        &mut self,
        application: &str,
        application_version: &str,
        application_source: &str,
        application_user: &str,
    ) -> HdbResult<()> {
        self.client_info.set_application(application);
        self.client_info
            .set_application_version(application_version);
        self.client_info.set_application_source(application_source);
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

    pub fn get_major_and_minor_product_version(&self) -> (i8, i16) {
        (self.major_product_version, self.minor_product_version)
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

    pub fn reader(&mut self) -> &mut io::BufReader<TcpStream> {
        &mut self.reader
    }

    pub fn writer(&mut self) -> &mut io::BufWriter<TcpStream> {
        &mut self.writer
    }

    pub fn next_seq_number(&mut self) -> i32 {
        self.seq_number += 1;
        self.seq_number
    }
    pub fn last_seq_number(&self) -> i32 {
        self.seq_number
    }

    pub fn evaluate_ta_flags(&mut self, ta_flags: &TransactionFlags) -> HdbResult<()> {
        ta_flags.update_session_state(&mut self.session_state);
        if self.session_state.dead {
            Err(HdbError::Impl("SessionclosingTaError received".to_owned()))
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

    fn drop_impl(&mut self) -> HdbResult<()> {
        trace!("Drop of ConnectionCore, session_id = {}", self.session_id);
        if self.authenticated {
            let request = Request::new_for_disconnect();
            // request.push()
            request.serialize_impl(self.session_id, self.next_seq_number(), 0, &mut self.writer)?;
            trace!("Disconnect: request successfully sent");
            if let Ok((no_of_parts, mut reply)) =
                parse_message_and_sequence_header(&mut self.reader)
            {
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
                        &mut self.reader,
                    )?;
                    util::skip_bytes(padsize, &mut self.reader)?;
                    debug!("Drop of connection: got Part {:?}", part);
                }
            }
            trace!("Disconnect: response successfully parsed");
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
