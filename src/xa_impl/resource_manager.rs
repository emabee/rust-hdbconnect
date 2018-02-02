use hdb_return_value::HdbReturnValue;
use hdb_response::HdbResponse;
use hdb_error::HdbResult;
use HdbError;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::ConnCoreRef;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::xat_options::XatOptions;
use dist_tx::rm::{CResourceManager, CRmWrapper, Flags, Kind, RmError, RmResult};
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
    fn start(&mut self, id: &XaTransactionId, flags: Flags) -> RmResult<()> {
        debug!("CResourceManager::start()");
        if !flags.contains_only(Flags::JOIN | Flags::RESUME) {
            return Err(usage_error("start", flags));
        }

        // These two seem redundant: the server has to know this anyway
        // error if self.isDistributedTransaction()
        // error if self.is_xat_in_progress()

        // FIXME later: xa seems only to work on primary!!
        // ClientConnectionID ccid = getPrimaryConnection();

        self.xa_send_receive(RequestType::XAStart, id, flags)?;
        Ok(())
    }

    fn end(&mut self, id: &XaTransactionId, flags: Flags) -> RmResult<()> {
        debug!("CResourceManager::end()");
        if !flags.contains_only(Flags::SUCCESS | Flags::FAIL | Flags::SUSPEND) {
            return Err(usage_error("end", flags));
        }

        self.xa_send_receive(RequestType::XAEnd, id, flags)?;
        Ok(())
    }

    fn prepare(&mut self, id: &XaTransactionId) -> RmResult<()> {
        debug!("CResourceManager::prepare()");
        self.xa_send_receive(RequestType::XAPrepare, id, Flags::empty())?;
        Ok(())
    }

    fn commit(&mut self, id: &XaTransactionId, flags: Flags) -> RmResult<()> {
        debug!("CResourceManager::commit()");
        if !flags.contains_only(Flags::ONE_PHASE) {
            return Err(usage_error("commit", flags));
        }

        self.xa_send_receive(RequestType::XACommit, id, flags)?;
        Ok(())
    }

    fn rollback(&mut self, id: &XaTransactionId) -> RmResult<()> {
        debug!("CResourceManager::rollback()");
        self.xa_send_receive(RequestType::XARollback, id, Flags::empty())?;
        Ok(())
    }

    fn forget(&mut self, id: &XaTransactionId) -> RmResult<()> {
        debug!("CResourceManager::forget()");
        self.xa_send_receive(RequestType::XAForget, id, Flags::empty())?;
        Ok(())
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

        let result: HdbResponse =
            request.send_and_get_response(None, None, &mut (self.core), None)?;
        let retval: HdbReturnValue = result.into_single_retval()?;
        if let HdbReturnValue::XaTransactionIds(vec_xatid) = retval {
            return Ok(vec_xatid);
        }

        Err(RmError::new(
            Kind::ProtocolError,
            "recover did not get a list of xids, not even an empty one".to_owned(),
        ))
    }
}


fn usage_error(method: &'static str, flags: Flags) -> RmError {
    RmError::new(
        Kind::ProtocolError,
        format!(
            "CResourceManager::{}(): Invalid transaction flags {:?}",
            method,
            flags
        ),
    )
}

// only few seem to be used by HANA
fn kind_from_code(code: i32) -> Kind {
    match code {
        210 => Kind::DuplicateTransactionId,
        211 => Kind::InvalidArguments,
        212 => Kind::InvalidTransactionId,
        214 => Kind::ProtocolError,
        215 => Kind::RmError,
        216 => Kind::RmFailure,
        i => Kind::UnknownErrorCode(i),
    }
}

impl HdbResourceManager {
    fn xa_send_receive(
        &mut self,
        request_type: RequestType,
        id: &XaTransactionId,
        flag: Flags,
    ) -> RmResult<()> {
        match self.xa_send_receive_impl(request_type, id, flag) {
            Ok(_) => Ok(()),
            Err(hdb_error) => {
                if let HdbError::ProtocolError(ref prt_error) = hdb_error {
                    if let PrtError::DbMessage(ref v) = *prt_error {
                        if v.len() == 1 {
                            return Err(RmError::new(kind_from_code(v[0].code), v[0].text.clone()));
                        }
                    }
                };
                Err(From::<HdbError>::from(hdb_error))
            }
        }
    }

    fn xa_send_receive_impl(
        &mut self,
        request_type: RequestType,
        id: &XaTransactionId,
        flags: Flags,
    ) -> HdbResult<HdbResponse> {
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

        let conn_ref = &mut (self.core);
        request.send_and_get_response(None, None, conn_ref, None)
    }
}
