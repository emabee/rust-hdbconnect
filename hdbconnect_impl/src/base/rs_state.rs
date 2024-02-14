use crate::{
    base::{PreparedStatementCore, RsCore, XMutexed, OAM},
    conn::{AmConnCore, CommandOptions},
    protocol::{
        parts::{Parts, StatementContext},
        MessageType, Part, PartAttributes, PartKind, ReplyType, Request,
    },
    HdbError, HdbResult, ResultSetMetadata, Row, Rows, ServerUsage,
};
use std::sync::Arc;

#[cfg(feature = "async")]
use super::new_am_async;
#[cfg(feature = "sync")]
use super::new_am_sync;

// the references to the connection (core) and the prepared statement (core)
// ensure that these are not dropped before all missing content is fetched
#[derive(Debug)]
pub(crate) struct RsState {
    next_rows: Vec<Row>,
    row_iter: <Vec<Row> as IntoIterator>::IntoIter,
    server_usage: ServerUsage,
    o_am_rscore: OAM<RsCore>,
}

impl RsState {
    #[cfg(feature = "sync")]
    pub(crate) fn new_sync(
        o_stmt_ctx: Option<StatementContext>,
        am_conn_core: &AmConnCore,
        attrs: PartAttributes,
        rs_id: u64,
    ) -> Self {
        let mut new_instance: RsState = Self {
            next_rows: Vec::<Row>::new(),
            row_iter: Vec::<Row>::new().into_iter(),
            server_usage: ServerUsage::default(),
            o_am_rscore: Some(new_am_sync(RsCore::new(am_conn_core, attrs, rs_id))),
        };
        if let Some(stmt_ctx) = o_stmt_ctx {
            new_instance.server_usage.update(
                stmt_ctx.server_processing_time(),
                stmt_ctx.server_cpu_time(),
                stmt_ctx.server_memory_usage(),
            );
        }
        new_instance
    }

    #[cfg(feature = "async")]
    pub(crate) fn new_async(
        o_stmt_ctx: Option<StatementContext>,
        am_conn_core: &AmConnCore,
        attrs: PartAttributes,
        rs_id: u64,
    ) -> Self {
        let mut new_instance = Self {
            next_rows: Vec::<Row>::new(),
            row_iter: Vec::<Row>::new().into_iter(),
            server_usage: ServerUsage::default(),
            o_am_rscore: Some(new_am_async(RsCore::new(am_conn_core, attrs, rs_id))),
        };
        if let Some(stmt_ctx) = o_stmt_ctx {
            new_instance.server_usage.update(
                stmt_ctx.server_processing_time(),
                stmt_ctx.server_cpu_time(),
                stmt_ctx.server_memory_usage(),
            );
        }
        new_instance
    }

    #[cfg(feature = "sync")]
    fn rs_core_sync(&self) -> HdbResult<std::sync::MutexGuard<'_, RsCore>> {
        match self.o_am_rscore {
            Some(ref am_rs_core) => Ok(am_rs_core.lock_sync()?),
            None => Err(HdbError::Impl("RsCore is already dropped")),
        }
    }
    #[cfg(feature = "async")]
    async fn rs_core_async(&self) -> HdbResult<tokio::sync::MutexGuard<'_, RsCore>> {
        match self.o_am_rscore {
            Some(ref am_rs_core) => Ok(am_rs_core.lock_async().await),
            None => Err(HdbError::Impl("RsCore is already dropped")),
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn set_attributes_sync(&mut self, attributes: PartAttributes) -> HdbResult<()> {
        self.rs_core_sync()?.set_attributes(attributes);
        Ok(())
    }
    #[cfg(feature = "async")]
    pub(crate) async fn set_attributes_async(
        &mut self,
        attributes: PartAttributes,
    ) -> HdbResult<()> {
        self.rs_core_async().await?.set_attributes(attributes);
        Ok(())
    }

    pub(crate) fn update_server_usage(&mut self, stmt_ctx: &StatementContext) {
        self.server_usage.update(
            stmt_ctx.server_processing_time(),
            stmt_ctx.server_cpu_time(),
            stmt_ctx.server_memory_usage(),
        );
    }

    pub(crate) fn server_usage(&self) -> &ServerUsage {
        &self.server_usage
    }

    #[cfg(feature = "sync")]
    pub(crate) fn inject_ps_core_sync(
        &mut self,
        am_ps_core: Arc<XMutexed<PreparedStatementCore>>,
    ) -> HdbResult<()> {
        if let Some(ref am_rs_core) = self.o_am_rscore {
            am_rs_core.lock_sync()?.inject_ps_core(am_ps_core);
        }
        Ok(())
    }
    #[cfg(feature = "async")]
    pub(crate) async fn inject_ps_core_async(
        &mut self,
        am_ps_core: Arc<XMutexed<PreparedStatementCore>>,
    ) -> HdbResult<()> {
        if let Some(ref am_rs_core) = self.o_am_rscore {
            am_rs_core.lock_async().await.inject_ps_core(am_ps_core);
        }
        Ok(())
    }

    #[cfg(feature = "sync")]
    pub(crate) fn as_rows_sync(&mut self, a_rsmd: Arc<ResultSetMetadata>) -> HdbResult<Rows> {
        let mut rows = Vec::<Row>::new();
        while let Some(row) = self.next_row_sync(&a_rsmd)? {
            rows.push(row);
        }
        Rows::new_sync(a_rsmd, rows)
    }
    #[cfg(feature = "async")]
    pub(crate) async fn as_rows_async(
        &mut self,
        a_rsmd: Arc<ResultSetMetadata>,
    ) -> HdbResult<Rows> {
        let mut rows = Vec::<Row>::new();
        while let Some(row) = self.next_row_async(&a_rsmd).await? {
            rows.push(row);
        }
        Rows::new_async(a_rsmd, rows).await
    }

    #[cfg(feature = "sync")]
    pub(crate) fn fetch_all_sync(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        while !self.is_complete_sync()? {
            self.fetch_next_sync(a_rsmd)?;
        }
        Ok(())
    }
    #[cfg(feature = "async")]
    pub async fn fetch_all_async(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        while !self.is_complete_async().await? {
            self.fetch_next_async(a_rsmd).await?;
        }
        Ok(())
    }

    #[allow(clippy::len_without_is_empty)]
    pub(crate) fn len(&self) -> usize {
        self.next_rows.len() + self.row_iter.len()
    }

    #[cfg(feature = "sync")]
    pub(crate) fn total_number_of_rows_sync(
        &mut self,
        a_rsmd: &Arc<ResultSetMetadata>,
    ) -> HdbResult<usize> {
        self.fetch_all_sync(a_rsmd)?;
        Ok(self.len())
    }
    #[cfg(feature = "async")]
    pub async fn total_number_of_rows_async(
        &mut self,
        a_rsmd: &Arc<ResultSetMetadata>,
    ) -> HdbResult<usize> {
        self.fetch_all_async(a_rsmd).await?;
        Ok(self.len())
    }

    #[cfg(feature = "sync")]
    pub(crate) fn next_row_sync(
        &mut self,
        a_rsmd: &Arc<ResultSetMetadata>,
    ) -> HdbResult<Option<Row>> {
        if let Some(r) = self.row_iter.next() {
            Ok(Some(r))
        } else {
            if self.next_rows.is_empty() {
                if self.is_complete_sync()? {
                    return Ok(None);
                }
                self.fetch_next_sync(a_rsmd)?;
            }
            let mut tmp_vec = Vec::<Row>::new();
            std::mem::swap(&mut tmp_vec, &mut self.next_rows);
            self.row_iter = tmp_vec.into_iter();
            Ok(self.row_iter.next())
        }
    }
    #[cfg(feature = "async")]
    pub async fn next_row_async(
        &mut self,
        a_rsmd: &Arc<ResultSetMetadata>,
    ) -> HdbResult<Option<Row>> {
        if let Some(r) = self.row_iter.next() {
            Ok(Some(r))
        } else {
            if self.next_rows.is_empty() {
                if self.is_complete_async().await? {
                    return Ok(None);
                }
                self.fetch_next_async(a_rsmd).await?;
            }
            let mut tmp_vec = Vec::<Row>::new();
            std::mem::swap(&mut tmp_vec, &mut self.next_rows);
            self.row_iter = tmp_vec.into_iter();
            Ok(self.row_iter.next())
        }
    }

    pub(crate) fn next_row_no_fetch(&mut self) -> Option<Row> {
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
    pub(crate) fn single_row_sync(&mut self) -> HdbResult<Row> {
        if self.has_multiple_rows_sync() {
            Err(HdbError::Usage("Resultset has more than one row"))
        } else {
            Ok(self
                .next_row_no_fetch()
                .ok_or_else(|| HdbError::Usage("Resultset is empty"))?)
        }
    }
    #[cfg(feature = "async")]
    pub async fn single_row_async(&mut self) -> HdbResult<Row> {
        if self.has_multiple_rows_async().await {
            Err(HdbError::Usage("Resultset has more than one row"))
        } else {
            Ok(self
                .next_row_no_fetch()
                .ok_or_else(|| HdbError::Usage("Resultset is empty"))?)
        }
    }

    // Returns true if the resultset contains more than one row.
    #[cfg(feature = "sync")]
    pub(crate) fn has_multiple_rows_sync(&self) -> bool {
        let is_complete = self.is_complete_sync().unwrap_or(false);
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }
    #[cfg(feature = "async")]
    // Returns true if the resultset contains more than one row.
    pub async fn has_multiple_rows_async(&self) -> bool {
        let is_complete = self.is_complete_async().await.unwrap_or(false);
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }

    #[cfg(feature = "sync")]
    fn fetch_next_sync(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        trace!("ResultSet::fetch_next()");
        let (am_conn_core, resultset_id) = {
            let rs_core = self.rs_core_sync()?;
            let am_conn_core = rs_core.am_conn_core().clone();
            (am_conn_core, rs_core.resultset_id())
        };
        let fetch_size = { am_conn_core.lock_sync()?.configuration().fetch_size() };

        // build the request, provide resultset-id and fetch-size
        debug!("ResultSet::fetch_next() with fetch_size = {}", fetch_size);
        let mut request = Request::new(MessageType::FetchNext, CommandOptions::EMPTY);
        request.push(Part::ResultSetId(resultset_id));
        request.push(Part::FetchSize(fetch_size));
        let mut reply =
            am_conn_core.full_send_sync(request, Some(a_rsmd), None, &mut Some(self))?;
        reply.assert_expected_reply_type(ReplyType::Fetch)?;
        reply.parts.pop_if_kind(PartKind::ResultSet);

        let mut drop_rs_core = false;
        if let Some(ref am_rscore) = self.o_am_rscore {
            drop_rs_core = am_rscore.lock_sync()?.attributes().is_last_packet();
        };
        if drop_rs_core {
            self.o_am_rscore = None;
        }
        Ok(())
    }
    #[cfg(feature = "async")]
    pub async fn fetch_next_async(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        trace!("ResultSet::fetch_next()");
        let (conn_core, resultset_id, fetch_size) = {
            // scope the borrow
            if let Some(ref am_rscore) = self.o_am_rscore {
                let rs_core = am_rscore.lock_async().await;
                let am_conn_core = rs_core.am_conn_core().clone();
                let fetch_size = { am_conn_core.lock_async().await.configuration().fetch_size() };
                (am_conn_core, rs_core.resultset_id(), fetch_size)
            } else {
                return Err(HdbError::Impl("Fetch no more possible"));
            }
        };

        // build the request, provide resultset-id and fetch-size
        debug!("ResultSet::fetch_next() with fetch_size = {}", fetch_size);
        let mut request = Request::new(MessageType::FetchNext, CommandOptions::EMPTY);
        request.push(Part::ResultSetId(resultset_id));
        request.push(Part::FetchSize(fetch_size));

        let mut reply = conn_core
            .full_send_async(request, Some(a_rsmd), None, &mut Some(self))
            .await?;
        reply.assert_expected_reply_type(ReplyType::Fetch)?;
        reply.parts.pop_if_kind(PartKind::ResultSet);

        let mut drop_rs_core = false;
        if let Some(ref am_rscore) = self.o_am_rscore {
            drop_rs_core = am_rscore.lock_async().await.attributes().is_last_packet();
        };
        if drop_rs_core {
            self.o_am_rscore = None;
        }
        Ok(())
    }

    #[cfg(feature = "sync")]
    pub(crate) fn is_complete_sync(&self) -> HdbResult<bool> {
        if let Some(ref am_rscore) = self.o_am_rscore {
            let rs_core = am_rscore.lock_sync()?;
            let attributes = rs_core.attributes();
            if (!attributes.is_last_packet())
                && (attributes.row_not_found() || attributes.resultset_is_closed())
            {
                Err(HdbError::Impl(
                    "ResultSet attributes inconsistent: incomplete, but already closed on server",
                ))
            } else {
                Ok(attributes.is_last_packet())
            }
        } else {
            Ok(true)
        }
    }
    #[cfg(feature = "async")]
    pub async fn is_complete_async(&self) -> HdbResult<bool> {
        if let Some(ref am_rscore) = self.o_am_rscore {
            let rs_core = am_rscore.lock_async().await;
            if (!rs_core.attributes().is_last_packet())
                && (rs_core.attributes().row_not_found()
                    || rs_core.attributes().resultset_is_closed())
            {
                Err(HdbError::Impl(
                    "ResultSet attributes inconsistent: incomplete, but already closed on server",
                ))
            } else {
                Ok(rs_core.attributes().is_last_packet())
            }
        } else {
            Ok(true)
        }
    }

    // resultsets can be part of the response in three cases which differ
    // in regard to metadata handling:
    //
    // a) a response to a plain "execute" will contain the metadata in one of the
    //    other parts; the metadata parameter will thus have the variant None
    //
    // b) a response to an "execute prepared" will only contain data;
    //    the metadata had beeen returned already to the "prepare" call, and are
    //    provided with parameter metadata
    //
    // c) a response to a "fetch more lines" is triggered from an older resultset
    //    which already has its metadata
    //
    // For first resultset packets, we create and return a new ResultSet object.
    // We then expect the previous three parts to be
    // a matching ResultSetMetadata, a ResultSetId, and a StatementContext.
    #[cfg(feature = "sync")]
    pub(crate) fn parse_sync(
        no_of_rows: usize,
        attributes: PartAttributes,
        parts: &mut Parts,
        am_conn_core: &AmConnCore,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_rs: &mut Option<&mut RsState>,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Option<(Self, Arc<ResultSetMetadata>)>> {
        match o_rs {
            None => {
                // case a) or b)
                let o_stmt_ctx = match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(stmt_ctx)) => Some(stmt_ctx),
                    None => None,
                    Some(_) => return Err(HdbError::Impl("Inconsistent StatementContext")),
                };

                let Some(Part::ResultSetId(rs_id)) = parts.pop() else {
                    return Err(HdbError::Impl("ResultSetId missing"));
                };

                let a_rsmd = match parts.pop_if_kind(PartKind::ResultSetMetadata) {
                    Some(Part::ResultSetMetadata(rsmd)) => Arc::new(rsmd),
                    None => match o_a_rsmd {
                        Some(a_rsmd) => Arc::clone(a_rsmd),
                        None => return Err(HdbError::Impl("No metadata provided for ResultSet")),
                    },
                    Some(_) => {
                        return Err(HdbError::Impl(
                            "Inconsistent metadata part found for ResultSet",
                        ));
                    }
                };

                let mut rs_state = Self::new_sync(o_stmt_ctx, am_conn_core, attributes, rs_id);
                rs_state.parse_rows_sync(no_of_rows, &a_rsmd, rdr)?;
                Ok(Some((rs_state, a_rsmd)))
            }

            Some(fetching_state) => {
                match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(ref stmt_ctx)) => {
                        fetching_state.update_server_usage(stmt_ctx);
                    }
                    None => {}
                    Some(_) => {
                        return Err(HdbError::Impl(
                            "Inconsistent StatementContext part found for ResultSet",
                        ));
                    }
                };

                fetching_state.set_attributes_sync(attributes).ok();

                let a_rsmd = if let Some(a_rsmd) = o_a_rsmd {
                    Arc::clone(a_rsmd)
                } else {
                    return Err(HdbError::Impl("RsState provided without RsMetadata"));
                };
                fetching_state.parse_rows_sync(no_of_rows, &a_rsmd, rdr)?;
                Ok(None)
            }
        }
    }
    #[cfg(feature = "async")]
    pub(crate) async fn parse_async(
        no_of_rows: usize,
        attributes: PartAttributes,
        parts: &mut Parts<'_>,
        am_conn_core: &AmConnCore,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_rs: &mut Option<&mut RsState>,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<Option<(Self, Arc<ResultSetMetadata>)>> {
        match o_rs {
            None => {
                // case a) or b)
                let o_stmt_ctx = match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(stmt_ctx)) => Some(stmt_ctx),
                    None => None,
                    Some(_) => return Err(HdbError::Impl("Inconsistent StatementContext")),
                };

                let Some(Part::ResultSetId(rs_id)) = parts.pop() else {
                    return Err(HdbError::Impl("ResultSetId missing"));
                };

                let a_rsmd = match parts.pop_if_kind(PartKind::ResultSetMetadata) {
                    Some(Part::ResultSetMetadata(rsmd)) => Arc::new(rsmd),
                    None => match o_a_rsmd {
                        Some(a_rsmd) => Arc::clone(a_rsmd),
                        None => return Err(HdbError::Impl("No metadata provided for ResultSet")),
                    },
                    Some(_) => {
                        return Err(HdbError::Impl(
                            "Inconsistent metadata part found for ResultSet",
                        ));
                    }
                };

                let mut rs_state = Self::new_async(o_stmt_ctx, am_conn_core, attributes, rs_id);
                rs_state.parse_rows_async(no_of_rows, &a_rsmd, rdr).await?;
                Ok(Some((rs_state, a_rsmd)))
            }

            Some(fetching_state) => {
                match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(ref stmt_ctx)) => {
                        fetching_state.update_server_usage(stmt_ctx);
                    }
                    None => {}
                    Some(_) => {
                        return Err(HdbError::Impl(
                            "Inconsistent StatementContext part found for ResultSet",
                        ));
                    }
                };

                fetching_state.set_attributes_async(attributes).await.ok();

                let a_rsmd = if let Some(a_rsmd) = o_a_rsmd {
                    Arc::clone(a_rsmd)
                } else {
                    return Err(HdbError::Impl("RsState provided without RsMetadata"));
                };
                fetching_state
                    .parse_rows_async(no_of_rows, &a_rsmd, rdr)
                    .await?;
                Ok(None)
            }
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn parse_rows_sync(
        &mut self,
        no_of_rows: usize,
        metadata: &Arc<ResultSetMetadata>,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<()> {
        self.next_rows.reserve(no_of_rows);
        let no_of_cols = metadata.len();
        debug!("parse_rows(): {} lines, {} columns", no_of_rows, no_of_cols);

        if let Some(ref mut am_rscore) = self.o_am_rscore {
            let rs_core = am_rscore.lock_sync()?;
            let am_conn_core: &AmConnCore = rs_core.am_conn_core();
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
    pub(crate) async fn parse_rows_async(
        &mut self,
        no_of_rows: usize,
        metadata: &Arc<ResultSetMetadata>,
        rdr: &mut std::io::Cursor<Vec<u8>>,
    ) -> HdbResult<()> {
        self.next_rows.reserve(no_of_rows);
        let no_of_cols = metadata.len();
        debug!("parse_rows(): {} lines, {} columns", no_of_rows, no_of_cols);

        if let Some(ref mut am_rscore) = self.o_am_rscore {
            let rs_core = am_rscore.lock_async().await;
            let am_conn_core: &AmConnCore = rs_core.am_conn_core();
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

impl std::fmt::Display for RsState {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        for row in self.row_iter.as_slice() {
            writeln!(fmt, "{}\n", &row)?;
        }
        for row in &self.next_rows {
            writeln!(fmt, "{}\n", &row)?;
        }
        Ok(())
    }
}
