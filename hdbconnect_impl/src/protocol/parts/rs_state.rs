use std::sync::Arc;

#[cfg(feature = "sync")]
use std::sync::Mutex;
#[cfg(feature = "async")]
use tokio::sync::Mutex;

#[cfg(feature = "async")]
use crate::conn::AsyncAmConnCore;
#[cfg(feature = "sync")]
use crate::conn::SyncAmConnCore;

#[cfg(feature = "async")]
use crate::async_prepared_statement_core::AmPsCore;
#[cfg(feature = "sync")]
use crate::sync_prepared_statement_core::AmPsCore;

#[cfg(feature = "sync")]
use crate::protocol::util;
use crate::{
    protocol::{Part, PartAttributes, PartKind, ReplyType, Request, RequestType},
    HdbError, HdbResult, ResultSetMetadata, Row, ServerUsage,
};

pub type AmRsCore = Arc<Mutex<ResultSetCore>>;

// the references to the connection (core) and the prepared statement (core)
// ensure that these are not dropped before all missing content is fetched
#[derive(Debug)]
pub struct RsState {
    pub o_am_rscore: Option<AmRsCore>,
    pub next_rows: Vec<Row>,
    pub row_iter: <Vec<Row> as IntoIterator>::IntoIter,
    pub server_usage: ServerUsage,
}
impl RsState {
    #[cfg(feature = "sync")]
    pub fn sync_fetch_all(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        while !self.is_complete()? {
            self.sync_fetch_next(a_rsmd)?;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn async_fetch_all(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        while !self.is_complete().await? {
            self.async_fetch_next(a_rsmd).await?;
        }
        Ok(())
    }

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
                if self.is_complete()? {
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
        self.row_iter.next()
    }

    #[cfg(feature = "sync")]
    pub fn single_row(&mut self) -> HdbResult<Row> {
        if self.has_multiple_rows() {
            Err(HdbError::Usage("Resultset has more than one row"))
        } else {
            Ok(self
                .next_row_no_fetch()
                .ok_or_else(|| HdbError::Usage("Resultset is empty"))?)
        }
    }

    #[cfg(feature = "async")]
    pub async fn single_row(&mut self) -> HdbResult<Row> {
        if self.has_multiple_rows().await {
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
                if self.is_complete().await? {
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
    pub fn has_multiple_rows(&self) -> bool {
        let is_complete = self.is_complete().unwrap_or(false);
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }
    #[cfg(feature = "async")]
    pub async fn has_multiple_rows(&self) -> bool {
        let is_complete = self.is_complete().await.unwrap_or(false);
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }

    #[cfg(feature = "sync")]
    pub fn sync_fetch_next(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        trace!("ResultSet::fetch_next()");
        let (mut conn_core, resultset_id, fetch_size) = {
            // scope the borrow
            if let Some(ref am_rscore) = self.o_am_rscore {
                let rs_core = am_rscore.lock()?;
                let am_conn_core = rs_core.am_conn_core.clone();
                let fetch_size = { am_conn_core.lock()?.get_fetch_size() };
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

        let mut reply = conn_core.full_send(request, Some(a_rsmd), None, &mut Some(self))?;
        reply.assert_expected_reply_type(ReplyType::Fetch)?;
        reply.parts.pop_if_kind(PartKind::ResultSet);

        let mut drop_rs_core = false;
        if let Some(ref am_rscore) = self.o_am_rscore {
            drop_rs_core = am_rscore.lock()?.attributes.is_last_packet();
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
                let rs_core = am_rscore.lock().await;
                let am_conn_core = rs_core.am_conn_core.clone();
                let fetch_size = { am_conn_core.lock().await.get_fetch_size() };
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
            .full_send(request, Some(a_rsmd), None, &mut Some(self))
            .await?;
        reply.assert_expected_reply_type(ReplyType::Fetch)?;
        reply.parts.pop_if_kind(PartKind::ResultSet);

        let mut drop_rs_core = false;
        if let Some(ref am_rscore) = self.o_am_rscore {
            drop_rs_core = am_rscore.lock().await.attributes.is_last_packet();
        };
        if drop_rs_core {
            self.o_am_rscore = None;
        }
        Ok(())
    }

    #[cfg(feature = "sync")]
    pub fn is_complete(&self) -> HdbResult<bool> {
        if let Some(ref am_rscore) = self.o_am_rscore {
            let rs_core = am_rscore.lock()?;
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
    pub async fn is_complete(&self) -> HdbResult<bool> {
        if let Some(ref am_rscore) = self.o_am_rscore {
            let rs_core = am_rscore.lock().await;
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
                .lock()
                .map_err(|e| util::io_error(e.to_string()))?;
            let am_conn_core: &SyncAmConnCore = &rscore.am_conn_core;
            let o_am_rscore = Some(am_rscore.clone());
            for i in 0..no_of_rows {
                let row = Row::parse_sync(Arc::clone(&metadata), &o_am_rscore, am_conn_core, rdr)?;
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
            let rscore = am_rscore.lock().await;
            let am_conn_core: &AsyncAmConnCore = &rscore.am_conn_core;
            let o_am_rscore = Some(am_rscore.clone());
            for i in 0..no_of_rows {
                let row = Row::parse_async(Arc::clone(&metadata), &o_am_rscore, am_conn_core, rdr)
                    .await?;
                trace!("parse_rows(): Found row #{}: {}", i, row);
                self.next_rows.push(row);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ResultSetCore {
    #[cfg(feature = "sync")]
    am_conn_core: SyncAmConnCore,
    #[cfg(feature = "async")]
    am_conn_core: AsyncAmConnCore,

    o_am_pscore: Option<AmPsCore>,
    pub attributes: PartAttributes,
    resultset_id: u64,
}

impl ResultSetCore {
    pub fn new_am_rscore(
        #[cfg(feature = "sync")] am_conn_core: &SyncAmConnCore,
        #[cfg(feature = "async")] am_conn_core: &AsyncAmConnCore,
        attributes: PartAttributes,
        resultset_id: u64,
    ) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            #[cfg(feature = "sync")]
            am_conn_core: am_conn_core.clone(),
            #[cfg(feature = "async")]
            am_conn_core: am_conn_core.clone(),
            o_am_pscore: None,
            attributes,
            resultset_id,
        }))
    }

    pub fn inject_statement_id(&mut self, am_ps_core: AmPsCore) {
        self.o_am_pscore = Some(am_ps_core);
    }
}

impl Drop for ResultSetCore {
    // inform the server in case the resultset is not yet closed, ignore all errors
    fn drop(&mut self) {
        let rs_id = self.resultset_id;
        trace!("ResultSetCore::drop(), resultset_id {}", rs_id);
        if !self.attributes.resultset_is_closed() {
            let mut request = Request::new(RequestType::CloseResultSet, 0);
            request.push(Part::ResultSetId(rs_id));
            #[cfg(feature = "sync")]
            if let Ok(mut reply) = self.am_conn_core.send(request) {
                reply.parts.pop_if_kind(PartKind::StatementContext);
            }
            // #[cfg(feature = "async")] FIXME
            // if let Ok(mut reply) = self.am_conn_core.send_sync(request) {
            //     reply.parts.pop_if_kind(PartKind::StatementContext);
            // }
        }
    }
}
