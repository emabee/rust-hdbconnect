use crate::{
    base::{OAM, PreparedStatementCore, XMutexed},
    conn::{AmConnCore, CommandOptions},
    protocol::{MessageType, Part, PartAttributes, PartKind, Request},
};
use std::sync::Arc;

// Keeps the connection core and eventually a prepared statement core alive.
// (Note: if either of these is dropped, then the respective server representation will be dropped,
// which would break the owning result set if it is not yet fully fetched)
#[derive(Debug)]
pub(crate) struct RsCore {
    am_conn_core: AmConnCore,
    o_am_pscore: OAM<PreparedStatementCore>,
    // todo: move attributes into RsState to reduce locking
    attributes: PartAttributes,
    result_set_id: u64,
}

impl RsCore {
    pub(super) fn new(
        am_conn_core: &AmConnCore,
        attributes: PartAttributes,
        result_set_id: u64,
    ) -> Self {
        Self {
            am_conn_core: am_conn_core.clone(),
            o_am_pscore: None,
            attributes,
            result_set_id,
        }
    }

    pub(super) fn am_conn_core(&self) -> &AmConnCore {
        &self.am_conn_core
    }
    pub(super) fn result_set_id(&self) -> u64 {
        self.result_set_id
    }
    pub(super) fn inject_ps_core(&mut self, am_ps_core: Arc<XMutexed<PreparedStatementCore>>) {
        self.o_am_pscore = Some(am_ps_core);
    }
    pub(super) fn set_attributes(&mut self, attributes: PartAttributes) {
        self.attributes = attributes;
    }
    pub(super) fn attributes(&self) -> &PartAttributes {
        &self.attributes
    }
}

impl Drop for RsCore {
    // inform the server in case the result set is not yet closed, ignore all errors
    fn drop(&mut self) {
        let rs_id = self.result_set_id;
        trace!("RsCore::drop(), result_set_id {rs_id}");
        if !self.attributes.result_set_is_closed() {
            #[cfg(feature = "sync")]
            {
                let mut request = Request::new(MessageType::CloseResultSet, CommandOptions::EMPTY);
                request.push(Part::ResultSetId(rs_id));
                if let Ok(mut reply) = self.am_conn_core.send_sync(request) {
                    reply.parts.pop_if_kind(PartKind::StatementContext);
                }
            }
            #[cfg(feature = "async")]
            {
                let mut request = Request::new(MessageType::CloseResultSet, CommandOptions::EMPTY);
                request.push(Part::ResultSetId(rs_id));
                let am_conn_core = self.am_conn_core.clone();
                tokio::task::spawn(async move {
                    if let Ok(mut reply) = am_conn_core.send_async(request).await {
                        reply.parts.pop_if_kind(PartKind::StatementContext);
                    }
                });
            }
        }
    }
}
