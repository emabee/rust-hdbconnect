use crate::conn_core::AmConnCore;
use crate::protocol::argument::Argument;
use crate::protocol::part::{Part, Parts};
use crate::protocol::part_attributes::PartAttributes;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::parts::row::Row;
use crate::protocol::parts::statement_context::StatementContext;
use crate::protocol::reply_type::ReplyType;
use crate::protocol::request::Request;
use crate::protocol::request_type::RequestType;
use crate::protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use crate::{HdbError, HdbResult};

use serde;
use serde_db::de::DeserializableResultset;
use std::fmt;
use std::sync::{Arc, Mutex};

/// The result of a database query.
///
/// This is essentially a set of `Row`s, which is a set of `HdbValue`s.
///
/// In most cases, you will want to use the powerful method
/// [`try_into`](#method.try_into) to convert the data from this generic format
/// into your application specific format.
#[derive(Debug)]
pub struct ResultSet {
    core_ref: Arc<Mutex<ResultSetCore>>,
    metadata: Arc<ResultSetMetadata>,
    next_rows: Vec<Row>,
    row_iter: <Vec<Row> as IntoIterator>::IntoIter,
    server_resource_consumption_info: ServerResourceConsumptionInfo,
}

#[derive(Debug)]
pub struct ResultSetCore {
    o_am_conn_core: Option<AmConnCore>,
    attributes: PartAttributes,
    resultset_id: u64,
}

impl ResultSetCore {
    fn new_am_rscore(
        am_conn_core: &AmConnCore,
        attributes: PartAttributes,
        resultset_id: u64,
    ) -> Arc<Mutex<ResultSetCore>> {
        Arc::new(Mutex::new(ResultSetCore {
            o_am_conn_core: Some(am_conn_core.clone()),
            attributes,
            resultset_id,
        }))
    }

    fn drop_impl(&mut self) -> HdbResult<()> {
        let rs_id = self.resultset_id;
        trace!("ResultSetCore::drop(), resultset_id {}", rs_id);
        if !self.attributes.resultset_is_closed() {
            if let Some(ref conn_core) = self.o_am_conn_core {
                if let Ok(mut conn_guard) = conn_core.lock() {
                    let mut request = Request::new(RequestType::CloseResultSet, 0);
                    request.push(Part::new(
                        PartKind::ResultSetId,
                        Argument::ResultSetId(rs_id),
                    ));

                    if let Ok(mut reply) =
                        conn_guard.roundtrip(request, conn_core, None, None, &mut None)
                    {
                        let _ = reply.parts.pop_arg_if_kind(PartKind::StatementContext);
                        for part in &reply.parts {
                            warn!(
                                "CloseResultSet got a reply with a part of kind {:?}",
                                part.kind()
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for ResultSetCore {
    // inform the server in case the resultset is not yet closed, ignore all errors
    fn drop(&mut self) {
        if let Err(e) = self.drop_impl() {
            warn!("CloseResultSet request failed with {:?}", e);
        }
    }
}

impl ResultSet {
    /// Translates a generic resultset into a given rust type that implements
    /// serde::Deserialize. The implementation of this function uses serde_db.
    /// See [there](https://docs.rs/serde_db/) for more details.
    ///
    /// A resultset is essentially a two-dimensional structure, given as a list
    /// of rows (a <code>Vec&lt;Row&gt;</code>),
    /// where each row is a list of fields (a
    /// <code>Vec&lt;HdbValue&gt;</code>); the name of each field is
    /// given in the metadata of the resultset.
    ///
    /// The method supports a variety of target data structures, with the only
    /// strong limitation that no data loss is supported.
    ///
    /// * It depends on the dimension of the resultset what target data
    /// structure   you can choose for deserialization:
    ///
    ///     * You can always use a <code>Vec&lt;line_struct&gt;</code>, where
    ///       <code>line_struct</code> matches the field list of the resultset.
    ///
    /// * If the resultset contains only a single line (e.g. because you
    /// specified       TOP 1 in your select),
    /// then you can optionally choose to deserialize into a plain
    /// <code>line_struct</code>.
    ///
    /// * If the resultset contains only a single column, then you can
    /// optionally choose to deserialize into a
    /// <code>Vec&lt;plain_field&gt;</code>.
    ///
    /// * If the resultset contains only a single value (one row with one
    /// column), then you can optionally choose to deserialize into a
    /// plain <code>line_struct</code>, or a
    /// <code>Vec&lt;plain_field&gt;</code>, or a plain variable.
    ///
    /// * Also the translation of the individual field values provides a lot of
    /// flexibility. You can e.g. convert values from a nullable column
    /// into a plain field, provided that no NULL values are given in the
    /// resultset.
    ///
    /// Vice versa, you always can use an
    /// Option<code>&lt;plain_field&gt;</code>, even if the column is
    /// marked as NOT NULL.
    ///
    /// * Similarly, integer types can differ, as long as the concrete values
    /// can   be assigned without loss.
    ///
    /// Note that you need to specify the type of your target variable
    /// explicitly, so that <code>try_into()</code> can derive the type it
    /// needs to serialize into:
    ///
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct MyStruct {
    ///     ...
    /// }
    /// let typed_result: Vec<MyStruct> = resultset.try_into()?;
    /// ```
    ///
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Resultset::try_into()");
        Ok(DeserializableResultset::into_typed(self)?)
    }

    /// Access to metadata.
    pub fn metadata(&self) -> &ResultSetMetadata {
        &self.metadata
    }

    /// Returns the total number of rows in the resultset,
    /// including those that still need to be fetched from the database,
    /// but excluding those that have already been removed from the resultset.
    ///
    /// This method can be expensive, and it can fail, since it fetches all yet
    /// outstanding rows from the database.
    pub fn total_number_of_rows(&mut self) -> HdbResult<usize> {
        self.fetch_all()?;
        Ok(self.row_iter.len())
    }

    /// Removes the next row and returns it, or None if the ResultSet is empty.
    ///
    /// Consequently, the ResultSet has one row less after the call.
    /// May need to fetch further rows from the database, which can fail, and thus returns
    /// an HdbResult.
    ///
    /// Using ResultSet::into_iter() is preferrable due to better performance.
    pub fn next_row(&mut self) -> HdbResult<Option<Row>> {
        match self.row_iter.next() {
            Some(r) => Ok(Some(r)),
            None => {
                if self.next_rows.is_empty() {
                    if self.is_complete()? {
                        return Ok(None);
                    }
                    self.fetch_next()?;
                }
                let mut tmp_vec = Vec::<Row>::new();
                std::mem::swap(&mut tmp_vec, &mut self.next_rows);
                self.row_iter = tmp_vec.into_iter();
                Ok(self.row_iter.next())
            }
        }
    }

    // Returns true if the resultset contains more than one row.
    pub(crate) fn has_multiple_rows(&mut self) -> bool {
        let is_complete = match self.is_complete() {
            Ok(b) => b,
            Err(_) => false,
        };
        !is_complete || (self.next_rows.len() + self.row_iter.len() > 1)
    }

    /// Fetches all not yet transported result lines from the server.
    ///
    /// Bigger resultsets are typically not transported in one roundtrip from the database;
    /// the number of roundtrips depends on the total number of rows in the resultset
    /// and the configured fetch-size of the connection.
    pub fn fetch_all(&mut self) -> HdbResult<()> {
        while !self.is_complete()? {
            self.fetch_next()?;
        }
        Ok(())
    }

    fn fetch_next(&mut self) -> HdbResult<()> {
        trace!("ResultSet::fetch_next()");
        let (mut conn_core, resultset_id, fetch_size) = {
            // scope the borrow
            let guard = self.core_ref.lock()?;
            let rs_core = &*guard;
            let conn_core = match rs_core.o_am_conn_core {
                Some(ref am_conn_core) => am_conn_core.clone(),
                None => {
                    return Err(HdbError::impl_("Fetch no more possible"));
                }
            };
            let fetch_size = {
                let guard = conn_core.lock()?;
                (*guard).get_fetch_size()
            };
            (conn_core, rs_core.resultset_id, fetch_size)
        };

        // build the request, provide resultset id, define FetchSize
        debug!("ResultSet::fetch_next() with fetch_size = {}", fetch_size);
        let mut request = Request::new(RequestType::FetchNext, 0);
        request.push(Part::new(
            PartKind::ResultSetId,
            Argument::ResultSetId(resultset_id),
        ));
        request.push(Part::new(
            PartKind::FetchSize,
            Argument::FetchSize(fetch_size),
        ));

        let mut reply = conn_core.full_send(request, None, None, &mut Some(self))?;
        reply.assert_expected_reply_type(&ReplyType::Fetch)?;
        reply.parts.pop_arg_if_kind(PartKind::ResultSet);

        let mut guard = self.core_ref.lock()?;
        let rs_core = &mut *guard;
        if rs_core.attributes.is_last_packet() {
            rs_core.o_am_conn_core = None;
        }
        Ok(())
    }

    fn is_complete(&self) -> HdbResult<bool> {
        let guard = self.core_ref.lock()?;
        let rs_core = &*guard;
        if (!rs_core.attributes.is_last_packet())
            && (rs_core.attributes.row_not_found() || rs_core.attributes.resultset_is_closed())
        {
            Err(HdbError::impl_(
                "ResultSet attributes inconsistent: incomplete, but already closed on server",
            ))
        } else {
            Ok(rs_core.attributes.is_last_packet())
        }
    }

    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        attrs: PartAttributes,
        rs_id: u64,
        rsm: ResultSetMetadata,
        o_stmt_ctx: Option<StatementContext>,
    ) -> ResultSet {
        let mut server_resource_consumption_info: ServerResourceConsumptionInfo =
            Default::default();

        if let Some(stmt_ctx) = o_stmt_ctx {
            server_resource_consumption_info.update(
                stmt_ctx.get_server_processing_time(),
                stmt_ctx.get_server_cpu_time(),
                stmt_ctx.get_server_memory_usage(),
            );
        }

        ResultSet {
            core_ref: ResultSetCore::new_am_rscore(am_conn_core, attrs, rs_id),
            metadata: Arc::new(rsm),
            next_rows: Vec::<Row>::new(),
            row_iter: Vec::<Row>::new().into_iter(),
            server_resource_consumption_info,
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
    pub(crate) fn parse<T: std::io::BufRead>(
        no_of_rows: usize,
        attributes: PartAttributes,
        parts: &mut Parts,
        am_conn_core: &AmConnCore,
        rs_md: Option<&ResultSetMetadata>,
        o_rs: &mut Option<&mut ResultSet>,
        rdr: &mut T,
    ) -> HdbResult<Option<ResultSet>> {
        match *o_rs {
            None => {
                // case a) or b)
                let o_stmt_ctx = match parts.pop_arg_if_kind(PartKind::StatementContext) {
                    Some(Argument::StatementContext(stmt_ctx)) => Some(stmt_ctx),
                    None => None,
                    _ => {
                        return Err(HdbError::impl_(
                            "Inconsistent StatementContext part found for ResultSet",
                        ))
                    }
                };

                let rs_id = match parts.pop_arg() {
                    Some(Argument::ResultSetId(rs_id)) => rs_id,
                    _ => return Err(HdbError::impl_("No ResultSetId part found for ResultSet")),
                };

                let rs_metadata = match parts.pop_arg_if_kind(PartKind::ResultSetMetadata) {
                    Some(Argument::ResultSetMetadata(rsmd)) => rsmd,
                    None => match rs_md {
                        Some(rs_md) => rs_md.clone(),
                        _ => return Err(HdbError::impl_("No metadata provided for ResultSet")),
                    },
                    _ => {
                        return Err(HdbError::impl_(
                            "Inconsistent metadata part found for ResultSet",
                        ))
                    }
                };

                let mut result =
                    ResultSet::new(am_conn_core, attributes, rs_id, rs_metadata, o_stmt_ctx);
                ResultSet::parse_rows(&mut result, no_of_rows, rdr)?;
                Ok(Some(result))
            }

            Some(ref mut fetching_resultset) => {
                match parts.pop_arg_if_kind(PartKind::StatementContext) {
                    Some(Argument::StatementContext(stmt_ctx)) => {
                        fetching_resultset.server_resource_consumption_info.update(
                            stmt_ctx.get_server_processing_time(),
                            stmt_ctx.get_server_cpu_time(),
                            stmt_ctx.get_server_memory_usage(),
                        );
                    }
                    None => {}
                    _ => {
                        return Err(HdbError::impl_(
                            "Inconsistent StatementContext part found for ResultSet",
                        ))
                    }
                };

                {
                    let mut guard = fetching_resultset.core_ref.lock()?;
                    let rs_core = &mut *guard;
                    rs_core.attributes = attributes;
                }
                ResultSet::parse_rows(fetching_resultset, no_of_rows, rdr)?;
                Ok(None)
            }
        }
    }

    fn parse_rows(
        resultset: &mut ResultSet,
        no_of_rows: usize,
        rdr: &mut std::io::BufRead,
    ) -> HdbResult<()> {
        let no_of_cols = resultset.metadata.number_of_fields();
        debug!(
            "resultset::parse_rows() reading {} lines with {} columns",
            no_of_rows, no_of_cols
        );
        resultset.next_rows.reserve(no_of_rows);

        let guard = resultset.core_ref.lock()?;
        let rs_core = &*guard;
        if let Some(ref am_conn_core) = rs_core.o_am_conn_core {
            for r in 0..no_of_rows {
                let mut values = Vec::<HdbValue>::new();
                trace!("Parsing row {}", r,);
                for c in 0..no_of_cols {
                    let type_id = resultset
                        .metadata
                        .type_id(c)
                        .map_err(|_| HdbError::impl_("Not enough metadata"))?;
                    trace!("Parsing row {}, column {}, type_id {}", r, c, type_id,);
                    let value = HdbValue::parse_from_reply(&type_id, am_conn_core, rdr)?;
                    debug!(
                        "Parsed row {}, column {}, value {}, type_id {}",
                        r, c, value, type_id,
                    );
                    values.push(value);
                }
                let row = Row::new(Arc::clone(&resultset.metadata), values);
                trace!("parse_rows(): Found row {}", row);
                resultset.next_rows.push(row);
            }
        }
        Ok(())
    }
}

impl fmt::Display for ResultSet {
    // Writes a header and then the data
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "{}\n", &self.metadata)?;

        for row in self.row_iter.as_slice() {
            writeln!(fmt, "{}\n", &row)?;
        }
        for row in &self.next_rows {
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
