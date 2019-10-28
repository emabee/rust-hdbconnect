use crate::conn_core::am_conn_core::AmConnCore;
use crate::conn_core::buffalo::Buffalo;
use crate::conn_core::connect_params::ConnectParams;
use crate::conn_core::initial_request;
use crate::conn_core::session_state::SessionState;
use crate::protocol::argument::Argument;
use crate::protocol::part::Parts;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::client_info::ClientInfo;
use crate::protocol::parts::connect_options::ConnectOptions;
use crate::protocol::parts::execution_result::ExecutionResult;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::parts::server_error::{ServerError, Severity};
use crate::protocol::parts::statement_context::StatementContext;
use crate::protocol::parts::topology::Topology;
use crate::protocol::parts::transactionflags::TransactionFlags;
use crate::protocol::reply::Reply;
use crate::protocol::request::Request;
use crate::protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use crate::{HdbError, HdbResult};
use std::io::Write;
use std::mem;

#[derive(Debug)]
pub(crate) struct ConnectionCore {
    authenticated: bool,
    session_id: i64,
    client_info: ClientInfo,
    client_info_touched: bool,
    seq_number: i32,
    auto_commit: bool,
    server_resource_consumption_info: ServerResourceConsumptionInfo,
    fetch_size: u32,
    lob_read_length: u32,
    lob_write_length: usize,
    session_state: SessionState,
    statement_sequence: Option<i64>, // statement sequence within the transaction
    connect_options: ConnectOptions,
    topology: Option<Topology>,
    pub warnings: Vec<ServerError>,
    buffalo: Buffalo,
}

impl<'a> ConnectionCore {
    pub(crate) fn try_new(params: ConnectParams) -> HdbResult<ConnectionCore> {
        let connect_options = ConnectOptions::for_server(params.clientlocale(), get_os_user());
        let mut buffalo = Buffalo::try_new(params)?;
        initial_request::send_and_receive(&mut buffalo)?;

        Ok(ConnectionCore {
            authenticated: false,
            session_id: 0,
            seq_number: 0,
            auto_commit: true,
            server_resource_consumption_info: Default::default(),
            fetch_size: crate::DEFAULT_FETCH_SIZE,
            lob_read_length: crate::DEFAULT_LOB_READ_LENGTH,
            lob_write_length: crate::DEFAULT_LOB_WRITE_LENGTH,
            client_info: Default::default(),
            client_info_touched: false,
            session_state: Default::default(),
            statement_sequence: None,
            connect_options,
            topology: None,
            warnings: Vec::<ServerError>::new(),
            buffalo,
        })
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
            stmt_ctx.get_statement_sequence_info()
        );
        self.set_statement_sequence(stmt_ctx.get_statement_sequence_info());
        self.server_resource_consumption_info.update(
            stmt_ctx.get_server_processing_time(),
            stmt_ctx.get_server_cpu_time(),
            stmt_ctx.get_server_memory_usage(),
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

    pub(crate) fn server_resource_consumption_info(&self) -> &ServerResourceConsumptionInfo {
        &self.server_resource_consumption_info
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
        self.session_id = session_id;
    }

    pub(crate) fn set_topology(&mut self, topology: Topology) {
        self.topology = Some(topology);
    }

    pub(crate) fn dump_connect_options(&self) -> String {
        self.connect_options().to_string()
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
            Err(HdbError::DbIssue(
                "SessionclosingTaError received".to_owned(),
            ))
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

    pub(crate) fn roundtrip(
        &mut self,
        request: Request<'a>,
        am_conn_core: &AmConnCore,
        o_rs_md: Option<&ResultSetMetadata>,
        o_descriptors: Option<&ParameterDescriptors>,
        o_rs: &mut Option<&mut ResultSet>,
    ) -> HdbResult<Reply> {
        let auto_commit_flag: i8 = if self.is_auto_commit() { 1 } else { 0 };
        let nsn = self.next_seq_number();

        match self.buffalo {
            Buffalo::Plain(ref pc) => {
                let writer = &mut *(pc.writer()).borrow_mut();
                request.emit(
                    self.session_id(),
                    nsn,
                    auto_commit_flag,
                    o_descriptors,
                    writer,
                )?;
            }
            #[cfg(feature = "tls")]
            Buffalo::Secure(ref sc) => {
                let writer = &mut *(sc.writer()).borrow_mut();
                request.emit(
                    self.session_id(),
                    nsn,
                    auto_commit_flag,
                    o_descriptors,
                    writer,
                )?;
            }
        }

        let mut reply = match self.buffalo {
            Buffalo::Plain(ref pc) => {
                let reader = &mut *(pc.reader()).borrow_mut();
                Reply::parse(o_rs_md, o_descriptors, o_rs, Some(am_conn_core), reader)?
            }
            #[cfg(feature = "tls")]
            Buffalo::Secure(ref sc) => {
                let reader = &mut *(sc.reader()).borrow_mut();
                Reply::parse(o_rs_md, o_descriptors, o_rs, Some(am_conn_core), reader)?
            }
        };

        self.handle_db_error(&mut reply.parts)?;
        Ok(reply)
    }

    fn handle_db_error(&mut self, parts: &mut Parts) -> HdbResult<()> {
        self.warnings.clear();

        // Retrieve errors from returned parts
        let mut errors = {
            let opt_error_part = parts.extract_first_part_of_type(PartKind::Error);
            match opt_error_part {
                None => {
                    // No error part found, reply evaluation happens elsewhere
                    return Ok(());
                }
                Some(error_part) => {
                    let (_, argument) = error_part.into_elements();
                    if let Argument::Error(server_errors) = argument {
                        // filter out warnings and add them to conn_core
                        #[allow(clippy::unnecessary_filter_map)]
                        let errors: Vec<ServerError> = server_errors
                            .into_iter()
                            .filter_map(|se| match se.severity() {
                                Severity::Warning => {
                                    self.warnings.push(se);
                                    None
                                }
                                _ => Some(se),
                            })
                            .collect();
                        if errors.is_empty() {
                            // Only warnings, so return Ok(())
                            return Ok(());
                        } else {
                            errors
                        }
                    } else {
                        unreachable!("129837938423")
                    }
                }
            }
        };

        // Evaluate the other parts
        let mut opt_rows_affected = None;
        parts.reverse(); // digest with pop
        while let Some(part) = parts.pop() {
            let (kind, arg) = part.into_elements();
            match arg {
                Argument::StatementContext(ref stmt_ctx) => {
                    self.evaluate_statement_context(stmt_ctx)?;
                }
                Argument::TransactionFlags(ta_flags) => {
                    self.evaluate_ta_flags(ta_flags)?;
                }
                Argument::ExecutionResult(vec) => {
                    opt_rows_affected = Some(vec);
                }
                arg => warn!(
                    "Reply::handle_db_error(): ignoring unexpected part of kind {:?}, arg = {:?}",
                    kind, arg
                ),
            }
        }

        match opt_rows_affected {
            Some(rows_affected) => {
                // mix errors into rows_affected
                let mut err_iter = errors.into_iter();
                let mut rows_affected = rows_affected
                    .into_iter()
                    .map(|ra| match ra {
                        ExecutionResult::Failure(_) => ExecutionResult::Failure(err_iter.next()),
                        _ => ra,
                    })
                    .collect::<Vec<ExecutionResult>>();
                for e in err_iter {
                    warn!(
                        "Reply::handle_db_error(): \
                         found more errors than instances of ExecutionResult::Failure"
                    );
                    rows_affected.push(ExecutionResult::Failure(Some(e)));
                }
                Err(HdbError::MixedResults(rows_affected))
            }
            None => {
                if errors.len() == 1 {
                    Err(HdbError::DbError(errors.remove(0)))
                } else {
                    unreachable!("hopefully...")
                }
            }
        }
    }

    fn drop_impl(&mut self) -> HdbResult<()> {
        trace!("Drop of ConnectionCore, session_id = {}", self.session_id);
        if self.authenticated {
            let request = Request::new_for_disconnect();

            let nsn = self.next_seq_number();
            match self.buffalo {
                Buffalo::Plain(ref pc) => {
                    let writer = &mut *(pc.writer()).borrow_mut();
                    request.emit(self.session_id(), nsn, 0, None, writer)?;
                    writer.flush()?;
                }
                #[cfg(feature = "tls")]
                Buffalo::Secure(ref sc) => {
                    let writer = &mut *(sc.writer()).borrow_mut();
                    request.emit(self.session_id(), nsn, 0, None, writer)?;
                    writer.flush()?;
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
