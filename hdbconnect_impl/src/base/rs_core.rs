use crate::{
    base::{PreparedStatementCore, XMutexed, OAM},
    conn::AmConnCore,
    protocol::{MessageType, Part, PartAttributes, PartKind, Request},
};
use std::sync::Arc;

// Keeps the connection core and eventually a prepared statement core alive.
// (Note: if either of these is dropped, then the respective server representation will be dropped,
// which would break the owning resultset if it is not yet fully fetched)
#[derive(Debug)]
pub(crate) struct RsCore {
    am_conn_core: AmConnCore,
    o_am_pscore: OAM<PreparedStatementCore>,
    attributes: PartAttributes,
    resultset_id: u64,
}

impl RsCore {
    pub(super) fn new(
        am_conn_core: &AmConnCore,
        attributes: PartAttributes,
        resultset_id: u64,
    ) -> Self {
        Self {
            am_conn_core: am_conn_core.clone(),
            o_am_pscore: None,
            attributes,
            resultset_id,
        }
    }

    pub(super) fn am_conn_core(&self) -> &AmConnCore {
        &self.am_conn_core
    }
    pub(super) fn resultset_id(&self) -> u64 {
        self.resultset_id
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
    // inform the server in case the resultset is not yet closed, ignore all errors
    fn drop(&mut self) {
        let rs_id = self.resultset_id;
        trace!("RsCore::drop(), resultset_id {}", rs_id);
        if !self.attributes.resultset_is_closed() {
            #[cfg(feature = "sync")]
            {
                let mut request = Request::new(MessageType::CloseResultSet, 0);
                request.push(Part::ResultSetId(rs_id));
                if let Ok(mut reply) = self.am_conn_core.send_sync(request) {
                    reply.parts.pop_if_kind(PartKind::StatementContext);
                }
            }
            #[cfg(feature = "async")]
            {
                let mut request = Request::new(MessageType::CloseResultSet, 0);
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
