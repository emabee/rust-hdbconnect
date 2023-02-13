use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    conn::AmConnCore,
    protocol::{Part, PartKind, Request, RequestType},
};

pub type AsyncAmPsCore = Arc<Mutex<PreparedStatementCore>>;

// Needs connection for its Drop implementation
#[derive(Debug)]
pub struct PreparedStatementCore {
    pub am_conn_core: AmConnCore,
    pub statement_id: u64,
}

impl Drop for PreparedStatementCore {
    /// Frees all server-side resources that belong to this prepared statement.
    fn drop(&mut self) {
        let mut request = Request::new(RequestType::DropStatementId, 0);
        request.push(Part::StatementId(self.statement_id));

        let am_conn_core = self.am_conn_core.clone();
        tokio::task::spawn(async move {
            if let Ok(mut reply) = am_conn_core.async_send(request).await {
                reply.parts.pop_if_kind(PartKind::StatementContext);
            }
        });
    }
}
