use crate::{
    protocol::{Part, PartAttributes, PartKind, ReplyType, Request, RequestType},
    HdbError, HdbResult, ResultSetMetadata, Row, Rows, ServerUsage,
};
use std::sync::Arc;

#[cfg(feature = "sync")]
use crate::{protocol::util, sync::prepared_statement_core::SyncAmPsCore};

#[cfg(feature = "async")]
use crate::a_sync::prepared_statement_core::AsyncAmPsCore;
use crate::conn::AmConnCore;

pub(crate) type AmRsCore = Arc<MRsCore>;

#[derive(Debug)]
pub(crate) enum MRsCore {
    #[cfg(feature = "sync")]
    Sync(std::sync::Mutex<SyncResultSetCore>),
    #[cfg(feature = "async")]
    Async(tokio::sync::Mutex<AsyncResultSetCore>),
}
impl MRsCore {
    #[cfg(feature = "sync")]
    pub(crate) fn sync_lock(&self) -> HdbResult<std::sync::MutexGuard<SyncResultSetCore>> {
        match self {
            MRsCore::Sync(m_rscore) => Ok(m_rscore.lock()?),
            #[cfg(feature = "async")]
            _ => unimplemented!("async not supported here"),
        }
    }
    #[cfg(feature = "async")]
    pub(crate) async fn async_lock(&self) -> tokio::sync::MutexGuard<AsyncResultSetCore> {
        match self {
            MRsCore::Async(m_rscore) => m_rscore.lock().await,
            #[cfg(feature = "sync")]
            _ => unimplemented!("sync not supported here"),
        }
    }
}

// the references to the connection (core) and the prepared statement (core)
// ensure that these are not dropped before all missing content is fetched
#[derive(Debug)]
pub(crate) struct RsState {
    pub o_am_rscore: Option<AmRsCore>,
    pub next_rows: Vec<Row>,
    pub row_iter: <Vec<Row> as IntoIterator>::IntoIter,
    pub server_usage: ServerUsage,
}
impl RsState {
    #[cfg(feature = "sync")]
    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn sync_into_rows(&mut self, a_rsmd: Arc<ResultSetMetadata>) -> HdbResult<Rows> {
        let mut rows = Vec::<Row>::new();
        while let Some(row) = self.sync_next_row(&a_rsmd)? {
            rows.push(row);
        }
        Rows::sync_new(a_rsmd, rows)
    }
    #[allow(clippy::wrong_self_convention)]
    #[cfg(feature = "async")]
    pub(crate) async fn async_into_rows(
        &mut self,
        a_rsmd: Arc<ResultSetMetadata>,
    ) -> HdbResult<Rows> {
        let mut rows = Vec::<Row>::new();
        while let Some(row) = self.async_next_row(&a_rsmd).await? {
            rows.push(row);
        }
        Rows::async_new(a_rsmd, rows).await
    }

    #[cfg(feature = "sync")]
    pub fn sync_fetch_all(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        while !self.sync_is_complete()? {
            self.sync_fetch_next(a_rsmd)?;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn async_fetch_all(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        while !self.async_is_complete().await? {
            self.async_fetch_next(a_rsmd).await?;
        }
        Ok(())
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.next_rows.len() + self.row_iter.len()
    }

    #[cfg(feature = "sync")]
    pub fn sync_total_number_of_rows(
        &mut self,
        a_rsmd: &Arc<ResultSetMetadata>,
    ) -> HdbResult<usize> {
        self.sync_fetch_all(a_rsmd)?;
        Ok(self.len())
    }

    #[cfg(feature = "async")]
    pub async fn async_total_number_of_rows(
        &mut self,
        a_rsmd: &Arc<ResultSetMetadata>,
    ) -> HdbResult<usize> {
        self.async_fetch_all(a_rsmd).await?;
        Ok(self.len())
    }

    #[cfg(feature = "sync")]
    pub fn sync_next_row(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<Option<Row>> {
        if let Some(r) = self.row_iter.next() {
            Ok(Some(r))
        } else {
            if self.next_rows.is_empty() {
                if self.sync_is_complete()? {
                    return Ok(None);
                }
                self.sync_fetch_next(a_rsmd)?;
            }
            let mut tmp_vec = Vec::<Row>::new();
            std::mem::swap(&mut tmp_vec, &mut self.next_rows);
            self.row_iter = tmp_vec.into_iter();
            Ok(self.row_iter.next())
        }
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

    #[cfg(feature = "sync")]
    pub fn sync_single_row(&mut self) -> HdbResult<Row> {
        if self.sync_has_multiple_rows() {
            Err(HdbError::Usage("Resultset has more than one row"))
        } else {
            Ok(self
                .next_row_no_fetch()
                .ok_or_else(|| HdbError::Usage("Resultset is empty"))?)
        }
    }

    #[cfg(feature = "async")]
    pub async fn async_single_row(&mut self) -> HdbResult<Row> {
        if self.async_has_multiple_rows().await {
            Err(HdbError::Usage("Resultset has more than one row"))
        } else {
            Ok(self
                .next_row_no_fetch()
                .ok_or_else(|| HdbError::Usage("Resultset is empty"))?)
        }
    }

    #[cfg(feature = "async")]
    pub async fn async_next_row(
        &mut self,
        a_rsmd: &Arc<ResultSetMetadata>,
    ) -> HdbResult<Option<Row>> {
        if let Some(r) = self.row_iter.next() {
            Ok(Some(r))
        } else {
            if self.next_rows.is_empty() {
                if self.async_is_complete().await? {
                    return Ok(None);
                }
                self.async_fetch_next(a_rsmd).await?;
            }
            let mut tmp_vec = Vec::<Row>::new();
            std::mem::swap(&mut tmp_vec, &mut self.next_rows);
            self.row_iter = tmp_vec.into_iter();
            Ok(self.row_iter.next())
        }
    }

    // Returns true if the resultset contains more than one row.
    #[cfg(feature = "sync")]
    pub fn sync_has_multiple_rows(&self) -> bool {
        let is_complete = self.sync_is_complete().unwrap_or(false);
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }
    #[cfg(feature = "async")]
    pub async fn async_has_multiple_rows(&self) -> bool {
        let is_complete = self.async_is_complete().await.unwrap_or(false);
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }

    #[cfg(feature = "sync")]
    pub fn sync_fetch_next(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        trace!("ResultSet::fetch_next()");
        let (mut conn_core, resultset_id, fetch_size) = {
            // scope the borrow
            if let Some(ref am_rscore) = self.o_am_rscore {
                let rs_core = (**am_rscore).sync_lock()?;
                let am_conn_core = rs_core.am_conn_core.clone();
                let fetch_size = { am_conn_core.sync_lock()?.get_fetch_size() };
                (am_conn_core, rs_core.resultset_id, fetch_size)
            } else {
                return Err(HdbError::Impl("Fetch no more possible"));
            }
        };

        // build the request, provide resultset-id and fetch-size
        debug!("ResultSet::fetch_next() with fetch_size = {}", fetch_size);
        let mut request = Request::new(RequestType::FetchNext, 0);
        request.push(Part::ResultSetId(resultset_id));
        request.push(Part::FetchSize(fetch_size));

        let mut reply = conn_core.sync_full_send(request, Some(a_rsmd), None, &mut Some(self))?;
        reply.assert_expected_reply_type(ReplyType::Fetch)?;
        reply.parts.pop_if_kind(PartKind::ResultSet);

        let mut drop_rs_core = false;
        if let Some(ref am_rscore) = self.o_am_rscore {
            drop_rs_core = am_rscore.sync_lock()?.attributes.is_last_packet();
        };
        if drop_rs_core {
            self.o_am_rscore = None;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn async_fetch_next(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        trace!("ResultSet::fetch_next()");
        let (mut conn_core, resultset_id, fetch_size) = {
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
        let mut request = Request::new(RequestType::FetchNext, 0);
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

    #[cfg(feature = "sync")]
    pub fn sync_is_complete(&self) -> HdbResult<bool> {
        if let Some(ref am_rscore) = self.o_am_rscore {
            let rs_core = am_rscore.sync_lock()?;
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

    #[cfg(feature = "async")]
    pub async fn async_is_complete(&self) -> HdbResult<bool> {
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

    #[cfg(feature = "sync")]
    pub fn parse_rows_sync(
        &mut self,
        no_of_rows: usize,
        metadata: &Arc<ResultSetMetadata>,
        rdr: &mut dyn std::io::Read,
    ) -> std::io::Result<()> {
        self.next_rows.reserve(no_of_rows);
        let no_of_cols = metadata.len();
        debug!("parse_rows(): {} lines, {} columns", no_of_rows, no_of_cols);

        if let Some(ref mut am_rscore) = self.o_am_rscore {
            let rscore = am_rscore
                .sync_lock()
                .map_err(|e| util::io_error(e.to_string()))?;
            let am_conn_core: &AmConnCore = &rscore.am_conn_core;
            let o_am_rscore = Some(am_rscore.clone());
            for i in 0..no_of_rows {
                let row = Row::parse_sync(Arc::clone(metadata), &o_am_rscore, am_conn_core, rdr)?;
                trace!("parse_rows(): Found row #{}: {}", i, row);
                self.next_rows.push(row);
            }
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn parse_rows_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        &mut self,
        no_of_rows: usize,
        metadata: &Arc<ResultSetMetadata>,
        rdr: &mut R,
    ) -> std::io::Result<()> {
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

// // This is a poor replacement for an "impl AsyncIterator for ResultSet"
// // see https://rust-lang.github.io/rfcs/2996-async-iterator.html for reasoning
// #[cfg(feature = "async")]
// impl RsState {
//     pub async fn next(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> Option<HdbResult<Row>> {
//         match self.async_next_row(a_rsmd).await {
//             Ok(Some(row)) => Some(Ok(row)),
//             Ok(None) => None,
//             Err(e) => Some(Err(e)),
//         }
//     }
// }

#[derive(Debug)]
#[cfg(feature = "sync")]
pub(crate) struct SyncResultSetCore {
    am_conn_core: AmConnCore,

    o_am_pscore: Option<SyncAmPsCore>,
    pub attributes: PartAttributes,
    resultset_id: u64,
}

#[cfg(feature = "sync")]
impl SyncResultSetCore {
    pub fn new(am_conn_core: &AmConnCore, attributes: PartAttributes, resultset_id: u64) -> Self {
        Self {
            am_conn_core: am_conn_core.clone(),
            o_am_pscore: None,
            attributes,
            resultset_id,
        }
    }

    pub fn inject_statement_id(&mut self, am_ps_core: SyncAmPsCore) {
        self.o_am_pscore = Some(am_ps_core);
    }
}

#[cfg(feature = "sync")]
impl Drop for SyncResultSetCore {
    // inform the server in case the resultset is not yet closed, ignore all errors
    fn drop(&mut self) {
        let rs_id = self.resultset_id;
        trace!("ResultSetCore::drop(), resultset_id {}", rs_id);
        if !self.attributes.resultset_is_closed() {
            let mut request = Request::new(RequestType::CloseResultSet, 0);
            request.push(Part::ResultSetId(rs_id));

            if let Ok(mut reply) = self.am_conn_core.sync_send(request) {
                reply.parts.pop_if_kind(PartKind::StatementContext);
            }
        }
    }
}
#[cfg(feature = "async")]
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
            let mut request = Request::new(RequestType::CloseResultSet, 0);
            request.push(Part::ResultSetId(rs_id));

            let mut am_conn_core = self.am_conn_core.clone();
            tokio::task::spawn(async move {
                if let Ok(mut reply) = am_conn_core.async_send(request).await {
                    reply.parts.pop_if_kind(PartKind::StatementContext);
                }
            });
        }
    }
}
