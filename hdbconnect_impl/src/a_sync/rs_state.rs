use crate::{
    a_sync::AsyncAmPsCore,
    conn::AmConnCore,
    protocol::{parts::AmRsCore, MessageType, Part, PartAttributes, PartKind, ReplyType, Request},
    HdbError, HdbResult, ResultSetMetadata, Row, Rows, ServerUsage,
};

use std::sync::Arc;

// the references to the connection (core) and the prepared statement (core)
// ensure that these are not dropped before all missing content is fetched
#[derive(Debug)]
pub(crate) struct AsyncRsState {
    pub o_am_rscore: Option<AmRsCore>,
    pub next_rows: Vec<Row>,
    pub row_iter: <Vec<Row> as IntoIterator>::IntoIter,
    pub server_usage: ServerUsage,
}
impl AsyncRsState {
    #[allow(clippy::wrong_self_convention)]
    pub(crate) async fn into_rows(&mut self, a_rsmd: Arc<ResultSetMetadata>) -> HdbResult<Rows> {
        let mut rows = Vec::<Row>::new();
        while let Some(row) = self.next_row(&a_rsmd).await? {
            rows.push(row);
        }
        Rows::async_new(a_rsmd, rows).await
    }

    pub async fn fetch_all(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        while !self.is_complete().await? {
            self.fetch_next(a_rsmd).await?;
        }
        Ok(())
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.next_rows.len() + self.row_iter.len()
    }

    pub async fn total_number_of_rows(
        &mut self,
        a_rsmd: &Arc<ResultSetMetadata>,
    ) -> HdbResult<usize> {
        self.fetch_all(a_rsmd).await?;
        Ok(self.len())
    }

    pub fn next_row_no_fetch(&mut self) -> Option<Row> {
        if let Some(r) = self.row_iter.next() {
            Some(r)
        } else {
            if self.next_rows.is_empty() {
                return None;
            }
            let mut tmp_vec = Vec::<Row>::new();
            std::mem::swap(&mut tmp_vec, &mut self.next_rows);
            self.row_iter = tmp_vec.into_iter();
            self.row_iter.next()
        }
    }

    pub async fn single_row(&mut self) -> HdbResult<Row> {
        if self.has_multiple_rows().await {
            Err(HdbError::Usage("Resultset has more than one row"))
        } else {
            Ok(self
                .next_row_no_fetch()
                .ok_or_else(|| HdbError::Usage("Resultset is empty"))?)
        }
    }

    pub async fn next_row(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<Option<Row>> {
        if let Some(r) = self.row_iter.next() {
            Ok(Some(r))
        } else {
            if self.next_rows.is_empty() {
                if self.is_complete().await? {
                    return Ok(None);
                }
                self.fetch_next(a_rsmd).await?;
            }
            let mut tmp_vec = Vec::<Row>::new();
            std::mem::swap(&mut tmp_vec, &mut self.next_rows);
            self.row_iter = tmp_vec.into_iter();
            Ok(self.row_iter.next())
        }
    }

    // Returns true if the resultset contains more than one row.
    pub async fn has_multiple_rows(&self) -> bool {
        let is_complete = self.is_complete().await.unwrap_or(false);
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }

    pub async fn fetch_next(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        trace!("ResultSet::fetch_next()");
        let (conn_core, resultset_id, fetch_size) = {
            // scope the borrow
            if let Some(ref am_rscore) = self.o_am_rscore {
                let rs_core = am_rscore.async_lock().await;
                let am_conn_core = rs_core.am_conn_core.clone();
                let fetch_size = { am_conn_core.async_lock().await.get_fetch_size() };
                (am_conn_core, rs_core.resultset_id, fetch_size)
            } else {
                return Err(HdbError::Impl("Fetch no more possible"));
            }
        };

        // build the request, provide resultset-id and fetch-size
        debug!("ResultSet::fetch_next() with fetch_size = {}", fetch_size);
        let mut request = Request::new(MessageType::FetchNext, 0);
        request.push(Part::ResultSetId(resultset_id));
        request.push(Part::FetchSize(fetch_size));

        let mut reply = conn_core
            .async_full_send(request, Some(a_rsmd), None, &mut Some(self))
            .await?;
        reply.assert_expected_reply_type(ReplyType::Fetch)?;
        reply.parts.pop_if_kind(PartKind::ResultSet);

        let mut drop_rs_core = false;
        if let Some(ref am_rscore) = self.o_am_rscore {
            drop_rs_core = am_rscore.async_lock().await.attributes.is_last_packet();
        };
        if drop_rs_core {
            self.o_am_rscore = None;
        }
        Ok(())
    }

    pub async fn is_complete(&self) -> HdbResult<bool> {
        if let Some(ref am_rscore) = self.o_am_rscore {
            let rs_core = am_rscore.async_lock().await;
            if (!rs_core.attributes.is_last_packet())
                && (rs_core.attributes.row_not_found() || rs_core.attributes.resultset_is_closed())
            {
                Err(HdbError::Impl(
                    "ResultSet attributes inconsistent: incomplete, but already closed on server",
                ))
            } else {
                Ok(rs_core.attributes.is_last_packet())
            }
        } else {
            Ok(true)
        }
    }

    pub async fn parse_rows<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        &mut self,
        no_of_rows: usize,
        metadata: &Arc<ResultSetMetadata>,
        rdr: &mut R,
    ) -> HdbResult<()> {
        self.next_rows.reserve(no_of_rows);
        let no_of_cols = metadata.len();
        debug!("parse_rows(): {} lines, {} columns", no_of_rows, no_of_cols);

        if let Some(ref mut am_rscore) = self.o_am_rscore {
            let rscore = am_rscore.async_lock().await;
            let am_conn_core: &AmConnCore = &rscore.am_conn_core;
            let o_am_rscore = Some(am_rscore.clone());
            for i in 0..no_of_rows {
                let row =
                    Row::parse_async(Arc::clone(metadata), &o_am_rscore, am_conn_core, rdr).await?;
                trace!("parse_rows(): Found row #{}: {}", i, row);
                self.next_rows.push(row);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct AsyncResultSetCore {
    am_conn_core: AmConnCore,
    o_am_pscore: Option<AsyncAmPsCore>,
    pub attributes: PartAttributes,
    resultset_id: u64,
}

#[cfg(feature = "async")]
impl AsyncResultSetCore {
    pub fn new(am_conn_core: &AmConnCore, attributes: PartAttributes, resultset_id: u64) -> Self {
        Self {
            am_conn_core: am_conn_core.clone(),
            o_am_pscore: None,
            attributes,
            resultset_id,
        }
    }

    pub fn inject_statement_id(&mut self, am_ps_core: AsyncAmPsCore) {
        self.o_am_pscore = Some(am_ps_core);
    }
}

#[cfg(feature = "async")]
impl Drop for AsyncResultSetCore {
    // inform the server in case the resultset is not yet closed, ignore all errors
    fn drop(&mut self) {
        let rs_id = self.resultset_id;
        trace!("ResultSetCore::drop(), resultset_id {}", rs_id);
        if !self.attributes.resultset_is_closed() {
            let mut request = Request::new(MessageType::CloseResultSet, 0);
            request.push(Part::ResultSetId(rs_id));

            let am_conn_core = self.am_conn_core.clone();
            tokio::task::spawn(async move {
                if let Ok(mut reply) = am_conn_core.async_send(request).await {
                    reply.parts.pop_if_kind(PartKind::StatementContext);
                }
            });
        }
    }
}
