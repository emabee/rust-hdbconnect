use crate::conn::AmConnCore;
use crate::protocol::parts::{Parts, ResultSetMetadata, Row, StatementContext};
use crate::protocol::{
    util, Part, PartAttributes, PartKind, ReplyType, Request, RequestType, ServerUsage,
};
use crate::sync_prepared_statement::AmPsCore;
use crate::{HdbError, HdbResult};
use serde_db::de::DeserializableResultset;
use std::fmt;
use std::sync::{Arc, Mutex};

pub(crate) type AmRsCore = Arc<Mutex<ResultSetCore>>;

/// The result of a database query.
///
/// This is essentially a set of `Row`s, and each `Row` is a set of `HdbValue`s.
///
/// The method [`try_into`](#method.try_into) converts the data from this generic format
/// in a singe step into your application specific format.
///
/// `ResultSet` implements `std::iter::Iterator`, so you can
/// directly iterate over the rows of a resultset.
/// While iterating, the not yet transported rows are fetched "silently" on demand, which can fail.
/// The Iterator-Item is thus not `Row`, but `HdbResult<Row>`.
///
/// ```rust, no_run
/// # use hdbconnect::{Connection,ConnectParams,HdbResult};
/// # use serde::Deserialize;
/// # fn main() -> HdbResult<()> {
/// # #[derive(Debug, Deserialize)]
/// # struct Entity();
/// # let mut connection = Connection::new(ConnectParams::builder().build()?)?;
/// # let query_string = "";
/// for row in connection.query(query_string)? {
///     // handle fetch errors and convert each line individually:
///     let entity: Entity = row?.try_into()?;
///     println!("Got entity: {:?}", entity);
/// }
/// # Ok(())
/// # }
///
/// ```
///
#[derive(Debug)]
pub struct ResultSet {
    metadata: Arc<ResultSetMetadata>,
    state: Arc<Mutex<RsState>>,
}

// the references to the connection (core) and the prepared statement (core)
// ensure that they are not dropped before all missing content is fetched
#[derive(Debug)]
pub(crate) struct RsState {
    o_am_rscore: Option<AmRsCore>,
    next_rows: Vec<Row>,
    row_iter: <Vec<Row> as IntoIterator>::IntoIter,
    server_usage: ServerUsage,
}
impl RsState {
    fn fetch_all(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
        while !self.is_complete()? {
            self.fetch_next(a_rsmd)?;
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.next_rows.len() + self.row_iter.len()
    }

    fn total_number_of_rows(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<usize> {
        self.fetch_all(a_rsmd)?;
        Ok(self.len())
    }

    fn next_row(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<Option<Row>> {
        if let Some(r) = self.row_iter.next() {
            Ok(Some(r))
        } else {
            if self.next_rows.is_empty() {
                if self.is_complete()? {
                    return Ok(None);
                }
                self.fetch_next(a_rsmd)?;
            }
            let mut tmp_vec = Vec::<Row>::new();
            std::mem::swap(&mut tmp_vec, &mut self.next_rows);
            self.row_iter = tmp_vec.into_iter();
            Ok(self.row_iter.next())
        }
    }

    // Returns true if the resultset contains more than one row.
    pub(crate) fn has_multiple_rows(&self) -> bool {
        let is_complete = match self.is_complete() {
            Ok(b) => b,
            Err(_) => false,
        };
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }

    fn fetch_next(&mut self, a_rsmd: &Arc<ResultSetMetadata>) -> HdbResult<()> {
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

        let mut reply = conn_core.full_send_sync(request, Some(a_rsmd), None, &mut Some(self))?;
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

    fn is_complete(&self) -> HdbResult<bool> {
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

    fn parse_rows(
        &mut self,
        no_of_rows: usize,
        metadata: &Arc<ResultSetMetadata>,
        rdr: &mut dyn std::io::Read,
    ) -> std::io::Result<()> {
        self.next_rows.reserve(no_of_rows);
        let no_of_cols = metadata.number_of_fields();
        debug!("parse_rows(): {} lines, {} columns", no_of_rows, no_of_cols);

        if let Some(ref mut am_rscore) = self.o_am_rscore {
            let rscore = am_rscore
                .lock()
                .map_err(|e| util::io_error(e.to_string()))?;
            let am_conn_core: &AmConnCore = &rscore.am_conn_core;
            let o_am_rscore = Some(am_rscore.clone());
            for i in 0..no_of_rows {
                let row = Row::parse(Arc::clone(&metadata), &o_am_rscore, am_conn_core, rdr)?;
                trace!("parse_rows(): Found row #{}: {}", i, row);
                self.next_rows.push(row);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ResultSetCore {
    am_conn_core: AmConnCore,
    o_am_pscore: Option<AmPsCore>,
    attributes: PartAttributes,
    resultset_id: u64,
}

impl ResultSetCore {
    fn new_am_rscore(
        am_conn_core: &AmConnCore,
        attributes: PartAttributes,
        resultset_id: u64,
    ) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            am_conn_core: am_conn_core.clone(),
            o_am_pscore: None,
            attributes,
            resultset_id,
        }))
    }

    fn inject_statement_id(&mut self, am_ps_core: AmPsCore) {
        self.o_am_pscore = Some(am_ps_core);
    }
}

impl Drop for ResultSetCore {
    // inform the server in case the resultset is not yet closed, ignore all errors
    fn drop(&mut self) {
        let rs_id = self.resultset_id;
        trace!("ResultSetCore::drop(), resultset_id {}", rs_id);
        if !self.attributes.resultset_is_closed() {
            if let Ok(mut conn_guard) = self.am_conn_core.lock() {
                let mut request = Request::new(RequestType::CloseResultSet, 0);
                request.push(Part::ResultSetId(rs_id));

                if let Ok(mut reply) =
                    conn_guard.roundtrip_sync(request, &self.am_conn_core, None, None, &mut None)
                {
                    reply.parts.pop_if_kind(PartKind::StatementContext);
                    while let Some(part) = reply.parts.pop() {
                        warn!(
                            "CloseResultSet got a reply with a part of kind {:?}",
                            part.kind()
                        );
                    }
                }
            }
        }
    }
}

impl ResultSet {
    /// Conveniently translates the complete resultset into a rust type that implements
    /// `serde::Deserialize` and has an adequate structure.
    /// The implementation of this method uses
    /// [`serde_db::de`](https://docs.rs/serde_db/*/serde_db/de/index.html).
    ///
    /// A resultset is essentially a two-dimensional structure, given as a list
    /// of rows, where each row is a list of fields; the name of each field is
    /// given in the metadata of the resultset.
    ///
    /// The method supports a variety of target data structures, with the only
    /// strong limitation that no data loss is supported.
    ///
    /// It depends on the dimension of the resultset what target data
    /// structure   you can choose for deserialization:
    ///
    /// * You can always use a `Vec<line_struct>`, if the elements of
    /// `line_struct` match the field list of the resultset.
    ///
    /// * If the resultset contains only a single line (e.g. because you
    /// specified `TOP 1` in your select clause),
    /// then you can optionally choose to deserialize directly into a plain
    /// `line_struct`.
    ///
    /// * If the resultset contains only a single column, then you can
    /// optionally choose to deserialize directly into a
    /// `Vec<plain_field>`.
    ///
    /// * If the resultset contains only a single value (one row with one
    /// column), then you can optionally choose to deserialize into a
    /// plain `line_struct`, or a `Vec<plain_field>`, or a `plain_field`.
    ///
    /// Also the translation of the individual field values provides flexibility.
    ///
    /// * You can e.g. convert values from a nullable column
    /// into a plain field, provided that no NULL values are given in the
    /// resultset.
    ///
    /// * Vice versa, you can use an `Option<plain_field>`, even if the column is
    /// marked as NOT NULL.
    ///
    /// * Similarly, integer types can differ, as long as the concrete values
    /// can   be assigned without loss.
    ///
    /// As usual with serde deserialization, you need to specify the type of your target variable
    /// explicitly, so that `try_into()` can derive the type it needs to instantiate:
    ///
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct Entity {
    ///     ...
    /// }
    /// let typed_result: Vec<Entity> = resultset.try_into()?;
    /// ```
    ///
    /// # Errors
    ///
    /// `HdbError::Deserialization` if the deserialization into the target type is not possible.
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Resultset::try_into()");
        Ok(DeserializableResultset::into_typed(self)?)
    }

    /// Converts the resultset into a single row.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the resultset contains more than a single row, or is empty.
    pub fn into_single_row(self) -> HdbResult<Row> {
        let mut state = self.state.lock()?;
        if state.has_multiple_rows() {
            Err(HdbError::Usage("Resultset has more than one row"))
        } else {
            Ok(state
                .next_row(&self.metadata)?
                .ok_or_else(|| HdbError::Usage("Resultset is empty"))?)
        }
    }

    // Fast access to metadata, only for internal purposes.
    pub(crate) fn metadata_ref(&self) -> &ResultSetMetadata {
        &self.metadata
    }

    /// Access to metadata.
    pub fn metadata(&self) -> Arc<ResultSetMetadata> {
        Arc::clone(&self.metadata)
    }

    /// Returns the total number of rows in the resultset,
    /// including those that still need to be fetched from the database,
    /// but excluding those that have already been removed from the resultset.
    ///
    /// This method can be expensive, and it can fail, since it fetches all yet
    /// outstanding rows from the database.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub fn total_number_of_rows(&self) -> HdbResult<usize> {
        self.state.lock()?.total_number_of_rows(&self.metadata)
    }

    /// Removes the next row and returns it, or None if the `ResultSet` is empty.
    ///
    /// Consequently, the `ResultSet` has one row less after the call.
    /// May need to fetch further rows from the database, which can fail.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub fn next_row(&mut self) -> HdbResult<Option<Row>> {
        self.state.lock()?.next_row(&self.metadata)
    }

    /// Fetches all not yet transported result lines from the server.
    ///
    /// Bigger resultsets are typically not transported in one roundtrip from the database;
    /// the number of roundtrips depends on the total number of rows in the resultset
    /// and the configured fetch-size of the connection.
    ///
    /// Fetching can fail, e.g. if the network connection is broken.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub fn fetch_all(&self) -> HdbResult<()> {
        self.state.lock()?.fetch_all(&self.metadata)
    }

    pub(crate) fn has_multiple_rows_impl(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .has_multiple_rows()
    }

    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        attrs: PartAttributes,
        rs_id: u64,
        a_rsmd: Arc<ResultSetMetadata>,
        o_stmt_ctx: Option<StatementContext>,
    ) -> Self {
        let mut server_usage: ServerUsage = ServerUsage::default();

        if let Some(stmt_ctx) = o_stmt_ctx {
            server_usage.update(
                stmt_ctx.server_processing_time(),
                stmt_ctx.server_cpu_time(),
                stmt_ctx.server_memory_usage(),
            );
        }

        Self {
            metadata: a_rsmd,
            state: Arc::new(Mutex::new(RsState {
                o_am_rscore: Some(ResultSetCore::new_am_rscore(am_conn_core, attrs, rs_id)),
                next_rows: Vec::<Row>::new(),
                row_iter: Vec::<Row>::new().into_iter(),
                server_usage,
            })),
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
    pub(crate) fn parse(
        no_of_rows: usize,
        attributes: PartAttributes,
        parts: &mut Parts,
        am_conn_core: &AmConnCore,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_rs: &mut Option<&mut RsState>,
        rdr: &mut dyn std::io::Read,
    ) -> std::io::Result<Option<Self>> {
        match *o_rs {
            None => {
                // case a) or b)
                let o_stmt_ctx = match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(stmt_ctx)) => Some(stmt_ctx),
                    None => None,
                    _ => return Err(util::io_error("Inconsistent StatementContext")),
                };

                let rs_id = match parts.pop() {
                    Some(Part::ResultSetId(rs_id)) => rs_id,
                    _ => return Err(util::io_error("ResultSetId missing")),
                };

                let a_rsmd = match parts.pop_if_kind(PartKind::ResultSetMetadata) {
                    Some(Part::ResultSetMetadata(rsmd)) => Arc::new(rsmd),
                    None => match o_a_rsmd {
                        Some(a_rsmd) => Arc::clone(a_rsmd),
                        _ => return Err(util::io_error("No metadata provided for ResultSet")),
                    },
                    _ => {
                        return Err(util::io_error(
                            "Inconsistent metadata part found for ResultSet",
                        ));
                    }
                };

                let rs = Self::new(am_conn_core, attributes, rs_id, a_rsmd, o_stmt_ctx);
                rs.parse_rows(no_of_rows, rdr)?;
                Ok(Some(rs))
            }

            Some(ref mut fetching_state) => {
                match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(stmt_ctx)) => {
                        fetching_state.server_usage.update(
                            stmt_ctx.server_processing_time(),
                            stmt_ctx.server_cpu_time(),
                            stmt_ctx.server_memory_usage(),
                        );
                    }
                    None => {}
                    _ => {
                        return Err(util::io_error(
                            "Inconsistent StatementContext part found for ResultSet",
                        ));
                    }
                };

                if let Some(ref mut am_rscore) = fetching_state.o_am_rscore {
                    let mut rscore = am_rscore
                        .lock()
                        .map_err(|e| util::io_error(e.to_string()))?;
                    rscore.attributes = attributes;
                }
                let a_rsmd = if let Some(a_rsmd) = o_a_rsmd {
                    Arc::clone(&a_rsmd)
                } else {
                    return Err(util::io_error("RsState provided without RsMetadata"));
                };
                fetching_state.parse_rows(no_of_rows, &a_rsmd, rdr)?;
                Ok(None)
            }
        }
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `ResultSet` object.
    pub fn server_usage(&self) -> ServerUsage {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .server_usage
    }

    fn parse_rows(&self, no_of_rows: usize, rdr: &mut dyn std::io::Read) -> std::io::Result<()> {
        self.state
            .lock()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
            .parse_rows(no_of_rows, &self.metadata, rdr)
    }

    pub(crate) fn inject_statement_id(&mut self, am_ps_core: AmPsCore) -> HdbResult<()> {
        if let Some(rs_core) = &(self.state.lock()?).o_am_rscore {
            rs_core.lock()?.inject_statement_id(am_ps_core);
        }
        Ok(())
    }
}

impl fmt::Display for ResultSet {
    // Writes a header and then the data
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "{}\n", &self.metadata)?;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for row in state.row_iter.as_slice() {
            writeln!(fmt, "{}\n", &row)?;
        }
        for row in &state.next_rows {
            writeln!(fmt, "{}\n", &row)?;
        }
        Ok(())
    }
}

impl Iterator for ResultSet {
    type Item = HdbResult<Row>;
    fn next(&mut self) -> Option<HdbResult<Row>> {
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
