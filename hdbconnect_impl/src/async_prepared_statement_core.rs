use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    conn::AsyncAmConnCore,
    protocol::{Part, PartKind, Request, RequestType},
};

pub type AmPsCore = Arc<Mutex<AsyncPreparedStatementCore>>;

// Needs connection for its Drop implementation
#[derive(Debug)]
pub struct AsyncPreparedStatementCore {
    pub am_conn_core: AsyncAmConnCore,
    pub statement_id: u64,
}

impl Drop for AsyncPreparedStatementCore {
    /// Frees all server-side resources that belong to this prepared statement.
    fn drop(&mut self) {
        let mut request = Request::new(RequestType::DropStatementId, 0);
        request.push(Part::StatementId(self.statement_id));

        let mut am_conn_core = self.am_conn_core.clone();
        tokio::task::spawn(async move {
            if let Ok(mut reply) = am_conn_core.send(request).await {
                reply.parts.pop_if_kind(PartKind::StatementContext);
            }
        });
    }
}
