use crate::{
    conn::AmConnCore,
    protocol::{parts::XatOptions, MessageType, Part, PartKind, Reply, Request},
    HdbError, HdbResult,
};
use async_trait::async_trait;
#[cfg(feature = "dist_tx")]
use dist_tx::{
    a_sync::rm::{CResourceManager, CRmWrapper},
    ErrorCode, Flags, ReturnCode, RmError, XaTransactionId,
};

/// Handle for dealing with distributed transactions that is to be used by a
/// transaction manager.
///
/// Is based on the connection from which it is obtained
/// (see [`Connection::get_resource_manager`](crate::Connection::get_resource_manager)).
///
#[derive(Debug)]
pub struct HdbCResourceManager {
    am_conn_core: AmConnCore,
}

pub fn new_resource_manager(am_conn_core: AmConnCore) -> CRmWrapper<HdbCResourceManager> {
    CRmWrapper(HdbCResourceManager { am_conn_core })
}

#[async_trait]
impl CResourceManager for HdbCResourceManager {
    async fn start(&mut self, id: XaTransactionId, flags: Flags) -> Result<ReturnCode, RmError> {
        debug!("CResourceManager::start()");
        if !flags.contains_only(Flags::JOIN | Flags::RESUME) {
            return Err(usage_error("start", flags));
        }

        // These two seem redundant: the server has to know this anyway
        // error if self.isDistributedTransaction()
        // error if self.is_xat_in_progress()

        // TODO: xa seems only to work on primary!!
        // ClientConnectionID ccid = getPrimaryConnection();

        self.xa_send_receive(MessageType::XAStart, id, flags).await
    }

    async fn end(&mut self, id: XaTransactionId, flags: Flags) -> Result<ReturnCode, RmError> {
        debug!("CResourceManager::end()");
        if !flags.contains_only(Flags::SUCCESS | Flags::FAIL | Flags::SUSPEND) {
            return Err(usage_error("end", flags));
        }

        self.xa_send_receive(MessageType::XAEnd, id, flags).await
    }

    async fn prepare(&mut self, id: XaTransactionId) -> Result<ReturnCode, RmError> {
        debug!("CResourceManager::prepare()");
        self.xa_send_receive(MessageType::XAPrepare, id, Flags::empty())
            .await
    }

    async fn commit(&mut self, id: XaTransactionId, flags: Flags) -> Result<ReturnCode, RmError> {
        debug!("CResourceManager::commit()");
        if !flags.contains_only(Flags::ONE_PHASE) {
            return Err(usage_error("commit", flags));
        }
        self.xa_send_receive(MessageType::XACommit, id, flags).await
    }

    async fn rollback(&mut self, id: XaTransactionId) -> Result<ReturnCode, RmError> {
        debug!("CResourceManager::rollback()");
        self.xa_send_receive(MessageType::XARollback, id, Flags::empty())
            .await
    }

    async fn forget(&mut self, id: XaTransactionId) -> Result<ReturnCode, RmError> {
        debug!("CResourceManager::forget()");
        self.xa_send_receive(MessageType::XAForget, id, Flags::empty())
            .await
    }

    async fn recover(&mut self, flags: Flags) -> Result<Vec<XaTransactionId>, RmError> {
        debug!("HdbCResourceManager::recover()");
        if !flags.contains_only(Flags::START_RECOVERY_SCAN | Flags::END_RECOVERY_SCAN) {
            return Err(usage_error("recover", flags));
        }

        let mut request = Request::new(MessageType::XARecover, 0);

        let mut xat_options = XatOptions::default();
        xat_options.set_flags(flags);
        request.push(Part::XatOptions(xat_options));

        let mut reply: Reply = self.am_conn_core.send_async(request).await?;
        while !reply.parts.is_empty() {
            reply.parts.drop_parts_of_kind(PartKind::StatementContext);
            match reply.parts.pop() {
                Some(Part::XatOptions(xat_options)) => {
                    return Ok(xat_options.get_transactions());
                }
                Some(part) => warn!("recover: found unexpected part {:?}", part),
                None => warn!("recover: did not find next part"),
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
        format!("CResourceManager::{method}(): Invalid transaction flags {flags:?}",),
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

impl HdbCResourceManager {
    async fn xa_send_receive(
        &mut self,
        request_type: MessageType,
        id: XaTransactionId,
        flag: Flags,
    ) -> Result<ReturnCode, RmError> {
        self.xa_send_receive_impl(request_type, id, flag)
            .await
            .map(|opt| opt.unwrap_or(ReturnCode::Ok))
            .map_err(|hdb_error| {
                if let Some(server_error) = hdb_error.server_error() {
                    return RmError::new(
                        error_code_from_hana_code(server_error.code()),
                        server_error.text().to_string(),
                    );
                } else if let HdbError::ExecutionResults(_) = hdb_error {
                    return RmError::new(
                        ErrorCode::RmError,
                        "HdbError::ExecutionResults".to_string(),
                    );
                };
                From::<HdbError>::from(hdb_error)
            })
    }

    async fn xa_send_receive_impl(
        &mut self,
        request_type: MessageType,
        id: XaTransactionId,
        flags: Flags,
    ) -> HdbResult<Option<ReturnCode>> {
        if self.am_conn_core.async_lock().await.is_auto_commit() {
            return Err(HdbError::Usage(
                "xa_*() not possible, connection is set to auto_commit",
            ));
        }

        let mut xat_options = XatOptions::default();
        xat_options.set_xatid(&id);
        if !flags.is_empty() {
            xat_options.set_flags(flags);
        }

        let mut request = Request::new(request_type, 0);
        request.push(Part::XatOptions(xat_options));

        let mut reply = self.am_conn_core.send_async(request).await?;

        reply.parts.drop_parts_of_kind(PartKind::StatementContext);
        if let Some(Part::XatOptions(xat_options)) = reply.parts.pop_if_kind(PartKind::XatOptions) {
            debug!("received xat_options: {:?}", xat_options);
            return Ok(xat_options.get_returncode());
        }
        Ok(None)
    }
}
