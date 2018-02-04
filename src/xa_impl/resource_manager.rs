use hdb_error::HdbResult;
use protocol::lowlevel::message::Reply;
use HdbError;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::ConnCoreRef;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::xat_options::XatOptions;
use dist_tx::rm::{CResourceManager, CRmWrapper, ErrorCode, Flags, RmError, RmRc, RmResult};
use dist_tx::tm::XaTransactionId;
use protocol::protocol_error::PrtError;


/// Handle for dealing with distributed transactions that is to be used by a transaction manager.
///
/// Is based on the connection from which it is obtained
/// (see [`Connection::get_xa_resource_manager`](
/// ../struct.Connection.html#method.get_xa_resource_manager)).
///
#[derive(Debug)]
pub struct HdbResourceManager {
    core: ConnCoreRef,
}

pub fn new_resource_manager(core: ConnCoreRef) -> CRmWrapper<HdbResourceManager> {
    CRmWrapper(HdbResourceManager { core: core })
}

impl CResourceManager for HdbResourceManager {
    fn start(&mut self, id: &XaTransactionId, flags: Flags) -> RmResult<RmRc> {
        debug!("CResourceManager::start()");
        if !flags.contains_only(Flags::JOIN | Flags::RESUME) {
            return Err(usage_error("start", flags));
        }

        // These two seem redundant: the server has to know this anyway
        // error if self.isDistributedTransaction()
        // error if self.is_xat_in_progress()

        // FIXME later: xa seems only to work on primary!!
        // ClientConnectionID ccid = getPrimaryConnection();

        self.xa_send_receive(RequestType::XAStart, id, flags)
    }

    fn end(&mut self, id: &XaTransactionId, flags: Flags) -> RmResult<RmRc> {
        debug!("CResourceManager::end()");
        if !flags.contains_only(Flags::SUCCESS | Flags::FAIL | Flags::SUSPEND) {
            return Err(usage_error("end", flags));
        }

        self.xa_send_receive(RequestType::XAEnd, id, flags)
    }

    fn prepare(&mut self, id: &XaTransactionId) -> RmResult<RmRc> {
        debug!("CResourceManager::prepare()");
        self.xa_send_receive(RequestType::XAPrepare, id, Flags::empty())
    }

    fn commit(&mut self, id: &XaTransactionId, flags: Flags) -> RmResult<RmRc> {
        debug!("CResourceManager::commit()");
        if !flags.contains_only(Flags::ONE_PHASE) {
            return Err(usage_error("commit", flags));
        }
        self.xa_send_receive(RequestType::XACommit, id, flags)
    }

    fn rollback(&mut self, id: &XaTransactionId) -> RmResult<RmRc> {
        debug!("CResourceManager::rollback()");
        self.xa_send_receive(RequestType::XARollback, id, Flags::empty())
    }

    fn forget(&mut self, id: &XaTransactionId) -> RmResult<RmRc> {
        debug!("CResourceManager::forget()");
        self.xa_send_receive(RequestType::XAForget, id, Flags::empty())
    }

    fn recover(&mut self, flags: Flags) -> RmResult<Vec<XaTransactionId>> {
        debug!("HdbResourceManager::recover()");
        if !flags.contains_only(Flags::START_RECOVERY_SCAN | Flags::END_RECOVERY_SCAN) {
            return Err(usage_error("recover", flags));
        }

        let command_options = 0b_1000;
        let mut request = Request::new(RequestType::XARecover, command_options)?;

        let mut xat_options = XatOptions::default();
        xat_options.set_flags(flags);
        request.push(Part::new(
            PartKind::XatOptions,
            Argument::XatOptions(xat_options),
        ));

        let mut reply: Reply = request.send_and_receive(&mut (self.core), None)?;
        while !reply.parts.is_empty() {
            reply.parts.drop_args_of_kind(PartKind::StatementContext);
            match reply.parts.pop_arg() {
                Some(Argument::XatOptions(xat_options)) => {
                    return Ok(xat_options.get_transactions()?);
                }
                Some(part) => warn!("recover: found unexpected part {:?}", part),
                None => panic!("recover: found None part"),
            }
        }

        Err(RmError::new(
            ErrorCode::ProtocolError,
            "recover did not get a list of xids, not even an empty one".to_owned(),
        ))
    }
}


fn usage_error(method: &'static str, flags: Flags) -> RmError {
    RmError::new(
        ErrorCode::ProtocolError,
        format!(
            "CResourceManager::{}(): Invalid transaction flags {:?}",
            method,
            flags
        ),
    )
}

// only few seem to be used by HANA
fn error_code_from_hana_code(code: i32) -> ErrorCode {
    match code {
        210 => ErrorCode::DuplicateTransactionId,
        211 => ErrorCode::InvalidArguments,
        212 => ErrorCode::InvalidTransactionId,
        214 => ErrorCode::ProtocolError,
        215 => ErrorCode::RmError,
        216 => ErrorCode::RmFailure,
        i => ErrorCode::UnknownErrorCode(i),
    }
}

impl HdbResourceManager {
    fn xa_send_receive(
        &mut self,
        request_type: RequestType,
        id: &XaTransactionId,
        flag: Flags,
    ) -> RmResult<RmRc> {
        self.xa_send_receive_impl(request_type, id, flag)
            .map(|opt| opt.unwrap_or(RmRc::Ok))
            .map_err(|hdb_error| {
                if let HdbError::ProtocolError(PrtError::DbMessage(ref v)) = hdb_error {
                    if v.len() == 1 {
                        return RmError::new(
                            error_code_from_hana_code(v[0].code),
                            v[0].text.clone(),
                        );
                    }
                };
                From::<HdbError>::from(hdb_error)
            })
    }

    fn xa_send_receive_impl(
        &mut self,
        request_type: RequestType,
        id: &XaTransactionId,
        flags: Flags,
    ) -> HdbResult<Option<RmRc>> {
        if self.core.lock()?.is_auto_commit() {
            return Err(HdbError::UsageError(
                "xa_*() not possible, connection is set to auto_commit".to_string(),
            ));
        }

        let mut xat_options = XatOptions::default();
        xat_options.set_xatid(id);
        if !flags.is_empty() {
            xat_options.set_flags(flags);
        }

        let command_options = 0b_1000;
        let mut request = Request::new(request_type, command_options)?;
        request.push(Part::new(
            PartKind::XatOptions,
            Argument::XatOptions(xat_options),
        ));

        let mut reply = request.send_and_receive(&mut (self.core), None)?;

        reply.parts.drop_args_of_kind(PartKind::StatementContext);
        match reply.parts.pop_arg_if_kind(PartKind::XatOptions) {
            Some(Argument::XatOptions(xat_options)) => {
                debug!("received xat_options: {:?}", xat_options);
                return Ok(xat_options.get_returncode());
            }
            _ => {}
        }
        Ok(None)
    }
}
