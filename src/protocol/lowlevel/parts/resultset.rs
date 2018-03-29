use {HdbError, HdbResult};
use protocol::lowlevel::{prot_err, PrtError, PrtResult};
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::AmConnCore;
use protocol::lowlevel::message::{parse_message_and_sequence_header, Message, Request};
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::part_attributes::PartAttributes;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::row::Row;
use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
use serde_db::de::DeserializableResultset;

use serde;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

/// Contains the result of a database read command, including the describing metadata.
///
/// In most cases, you will want to use the powerful method [`try_into`](#method.try_into)
/// to convert the data from the generic format into your application specific format.
#[derive(Debug)]
pub struct ResultSet {
    core_ref: Arc<Mutex<ResultSetCore>>,
    metadata: Arc<ResultSetMetadata>,
    rows: Vec<Row>,
    acc_server_proc_time: i32,
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
            o_am_conn_core: Some(Arc::clone(am_conn_core)),
            attributes,
            resultset_id,
        }))
    }
}
impl Drop for ResultSetCore {
    // inform the server in case the resultset is not yet closed, ignore all errors
    fn drop(&mut self) {
        let rs_id = self.resultset_id;
        trace!("ResultSetCore::drop(), resultset_id {}", rs_id);
        if !self.attributes.is_resultset_closed() {
            if let Some(ref conn_core) = self.o_am_conn_core {
                if let Ok(mut conn_guard) = conn_core.lock() {
                    let session_id = (*conn_guard).session_id();
                    let next_seq_number = (*conn_guard).next_seq_number();
                    let stream = &mut (*conn_guard).stream();

                    let mut request = Request::new(RequestType::CloseResultSet, 0);
                    request.push(Part::new(
                        PartKind::ResultSetId,
                        Argument::ResultSetId(rs_id),
                    ));
                    match request.serialize_impl(session_id, next_seq_number, 0, stream) {
                        Ok(()) => {
                            let mut rdr = io::BufReader::new(stream);
                            if let Ok((no_of_parts, msg)) =
                                parse_message_and_sequence_header(&mut rdr)
                            {
                                if let Message::Reply(mut msg) = msg {
                                    for _ in 0..no_of_parts {
                                        Part::parse(
                                            &mut (msg.parts),
                                            None,
                                            None,
                                            None,
                                            &mut None,
                                            &mut rdr,
                                        ).ok();
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("CloseResultSet request failed with {:?}", e);
                        }
                    }
                }
            }
        }
    }
}

impl ResultSet {
    /// Returns the total number of rows in the resultset,
    /// including those that still need to be fetched from the database,
    /// but excluding those that have already been removed from the resultset.
    ///
    /// This method can be expensive, and it can fail, since it fetches all yet
    /// outstanding rows from the database.
    pub fn total_number_of_rows(&mut self) -> HdbResult<usize> {
        self.fetch_all()?;
        Ok(self.rows.len())
    }

    /// Removes the last row and returns it, or None if it is empty.
    pub fn pop_row(&mut self) -> Option<Row> {
        self.rows.pop()
    }

    /// Returns true if more than 1 row is contained
    pub fn has_multiple_rows(&mut self) -> bool {
        let is_complete = match self.is_complete() {
            Ok(b) => b,
            Err(_) => false,
        };
        !is_complete || (self.rows.len() > 1)
    }

    /// Reverses the order of the rows
    pub fn reverse_rows(&mut self) {
        trace!("ResultSet::reverse_rows()");
        self.rows.reverse()
    }

    /// Access to metadata.
    pub fn metadata(&self) -> &ResultSetMetadata {
        &self.metadata
    }

    /// Fetches all not yet transported result lines from the server.
    ///
    /// Bigger resultsets are typically not transported in one DB roundtrip;
    /// the number of roundtrips depends on the size of the resultset
    /// and the configured fetch_size of the connection.
    pub fn fetch_all(&mut self) -> HdbResult<()> {
        while !self.is_complete()? {
            self.fetch_next()?;
        }
        Ok(())
    }

    fn fetch_next(&mut self) -> PrtResult<()> {
        trace!("ResultSet::fetch_next()");
        let (mut conn_core, resultset_id, fetch_size) = {
            // scope the borrow
            let guard = self.core_ref.lock()?;
            let rs_core = &*guard;
            let conn_core = match rs_core.o_am_conn_core {
                Some(ref cr) => Arc::clone(cr),
                None => {
                    return Err(prot_err("Fetch no more possible"));
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
        let command_options = 0;
        let mut request = Request::new(RequestType::FetchNext, command_options);
        request.push(Part::new(
            PartKind::ResultSetId,
            Argument::ResultSetId(resultset_id),
        ));
        request.push(Part::new(
            PartKind::FetchSize,
            Argument::FetchSize(fetch_size),
        ));

        let mut reply = request.send_and_receive_detailed(
            None,
            None,
            &mut Some(self),
            &mut conn_core,
            Some(ReplyType::Fetch),
        )?;
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
            && (rs_core.attributes.row_not_found() || rs_core.attributes.is_resultset_closed())
        {
            Err(HdbError::ProtocolError(PrtError::ProtocolError(
                String::from("ResultSet incomplete, but already closed on server"),
            )))
        } else {
            Ok(rs_core.attributes.is_last_packet())
        }
    }

    /// Returns the accumulated server processing time
    /// of the calls that produced this resultset, i.e.
    /// the initial call and potentially a subsequent number of fetches.
    pub fn accumulated_server_processing_time(&self) -> i32 {
        self.acc_server_proc_time
    }

    /// Translates a generic resultset into a given rust type that implements
    /// serde::Deserialize. The implementation of this function uses serde_db.
    /// See [there](https://docs.rs/serde_db/) for more details.
    ///
    /// A resultset is essentially a two-dimensional structure, given as a list of rows
    /// (a <code>Vec&lt;Row&gt;</code>),
    /// where each row is a list of fields (a <code>Vec&lt;TypedValue&gt;</code>);
    /// the name of each field is given in the metadata of the resultset.
    ///
    /// The method supports a variety of target data structures, with the only strong limitation
    /// that no data loss is supported.
    ///
    /// * It depends on the dimension of the resultset what target data structure
    ///   you can choose for deserialization:
    ///
    ///     * You can always use a <code>Vec&lt;line_struct&gt;</code>, where
    ///       <code>line_struct</code> matches the field list of the resultset.
    ///
    ///     * If the resultset contains only a single line (e.g. because you specified
    ///       TOP 1 in your select),
    ///       then you can optionally choose to deserialize into a plain <code>line_struct</code>.
    ///
    ///     * If the resultset contains only a single column, then you can optionally choose to
    ///       deserialize into a <code>Vec&lt;plain_field&gt;</code>.
    ///
    ///     * If the resultset contains only a single value (one row with one column),
    ///       then you can optionally choose to deserialize into a plain <code>line_struct</code>,
    ///       or a <code>Vec&lt;plain_field&gt;</code>, or a plain variable.
    ///
    /// * Also the translation of the individual field values provides a lot of flexibility.
    ///   You can e.g. convert values from a nullable column into a plain field,
    ///   provided that no NULL values are given in the resultset.
    ///
    ///   Vice versa, you always can use an Option<code>&lt;plain_field&gt;</code>,
    ///   even if the column is marked as NOT NULL.
    ///
    /// * Similarly, integer types can differ, as long as the concrete values can
    ///   be assigned without loss.
    ///
    /// Note that you need to specify the type of your target variable explicitly, so that
    /// <code>try_into()</code> can derive the type it needs to serialize into:
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
}

impl fmt::Display for ResultSet {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.metadata, fmt).unwrap(); // write a header
        writeln!(fmt, "").unwrap();
        for row in &self.rows {
            fmt::Display::fmt(&row, fmt).unwrap(); // write the data
            writeln!(fmt, "").unwrap();
        }
        Ok(())
    }
}

impl IntoIterator for ResultSet {
    type Item = HdbResult<Row>;
    type IntoIter = RowIterator;

    fn into_iter(self) -> Self::IntoIter {
        RowIterator { rs: self }
    }
}

pub struct RowIterator {
    rs: ResultSet,
}
impl RowIterator {
    fn next_int(&mut self) -> HdbResult<Option<Row>> {
        if self.rs.rows.is_empty() {
            if self.rs.is_complete()? {
                return Ok(None);
            } else {
                self.rs.fetch_next()?;
            }
        }
        Ok(self.rs.rows.pop())
    }
}

impl Iterator for RowIterator {
    type Item = HdbResult<Row>;
    fn next(&mut self) -> Option<HdbResult<Row>> {
        match self.next_int() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub mod factory {
    use super::{ResultSet, ResultSetCore, Row};
    use protocol::protocol_error::{prot_err, PrtResult};
    use protocol::lowlevel::argument::Argument;
    use protocol::lowlevel::conn_core::AmConnCore;
    use protocol::lowlevel::part::Parts;
    use protocol::lowlevel::part_attributes::PartAttributes;
    use protocol::lowlevel::partkind::PartKind;
    use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
    use protocol::lowlevel::parts::statement_context::StatementContext;
    use protocol::lowlevel::parts::typed_value::TypedValue;
    use protocol::lowlevel::parts::typed_value::factory as TypedValueFactory;
    use std::io;
    use std::sync::Arc;

    pub fn resultset_new(
        am_conn_core: &AmConnCore,
        attrs: PartAttributes,
        rs_id: u64,
        rsm: ResultSetMetadata,
        o_stmt_ctx: Option<StatementContext>,
    ) -> ResultSet {
        let server_processing_time = match o_stmt_ctx {
            Some(stmt_ctx) => stmt_ctx.get_server_processing_time(),
            None => 0,
        };

        ResultSet {
            core_ref: ResultSetCore::new_am_rscore(am_conn_core, attrs, rs_id),
            metadata: Arc::new(rsm),
            rows: Vec::<Row>::new(),
            acc_server_proc_time: server_processing_time,
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
    pub fn parse(
        no_of_rows: i32,
        attributes: PartAttributes,
        parts: &mut Parts,
        am_conn_core: &AmConnCore,
        rs_md: Option<&ResultSetMetadata>,
        o_rs: &mut Option<&mut ResultSet>,
        rdr: &mut io::BufRead,
    ) -> PrtResult<Option<ResultSet>> {
        match *o_rs {
            None => {
                // case a) or b)
                let o_stmt_ctx = match parts.pop_arg_if_kind(PartKind::StatementContext) {
                    Some(Argument::StatementContext(stmt_ctx)) => Some(stmt_ctx),
                    None => None,
                    _ => {
                        return Err(prot_err(
                            "Inconsistent StatementContext part found for ResultSet",
                        ))
                    }
                };

                let rs_id = match parts.pop_arg() {
                    Some(Argument::ResultSetId(rs_id)) => rs_id,
                    _ => return Err(prot_err("No ResultSetId part found for ResultSet")),
                };

                let rs_metadata = match parts.pop_arg_if_kind(PartKind::ResultSetMetadata) {
                    Some(Argument::ResultSetMetadata(rsmd)) => rsmd,
                    None => match rs_md {
                        Some(rs_md) => rs_md.clone(),
                        _ => return Err(prot_err("No metadata provided for ResultSet")),
                    },
                    _ => return Err(prot_err("Inconsistent metadata part found for ResultSet")),
                };

                let mut result =
                    resultset_new(am_conn_core, attributes, rs_id, rs_metadata, o_stmt_ctx);
                parse_rows(&mut result, no_of_rows, rdr)?;
                Ok(Some(result))
            }

            Some(ref mut fetching_resultset) => {
                match parts.pop_arg_if_kind(PartKind::StatementContext) {
                    Some(Argument::StatementContext(stmt_ctx)) => {
                        fetching_resultset.acc_server_proc_time +=
                            stmt_ctx.get_server_processing_time();
                    }
                    None => {}
                    _ => {
                        return Err(prot_err(
                            "Inconsistent StatementContext part found for ResultSet",
                        ))
                    }
                };

                {
                    let mut guard = fetching_resultset.core_ref.lock()?;
                    let rs_core = &mut *guard;
                    rs_core.attributes = attributes;
                }
                parse_rows(fetching_resultset, no_of_rows, rdr)?;
                Ok(None)
            }
        }
    }

    fn parse_rows(
        resultset: &mut ResultSet,
        no_of_rows: i32,
        rdr: &mut io::BufRead,
    ) -> PrtResult<()> {
        let no_of_cols = resultset.metadata.number_of_fields();
        debug!(
            "resultset::parse_rows() reading {} lines with {} columns",
            no_of_rows, no_of_cols
        );

        let guard = resultset.core_ref.lock()?;
        let rs_core = &*guard;
        if let Some(ref am_conn_core) = rs_core.o_am_conn_core {
            for r in 0..no_of_rows {
                let mut values = Vec::<TypedValue>::new();
                for c in 0..no_of_cols {
                    let typecode = resultset
                        .metadata
                        .type_id(c)
                        .map_err(|_| prot_err("Not enough metadata"))?;
                    let nullable: bool = resultset
                        .metadata
                        .is_nullable(c)
                        .map_err(|_| prot_err("Not enough metadata"))?;
                    trace!(
                        "Parsing row {}, column {}, typecode {}, nullable {}",
                        r,
                        c,
                        typecode,
                        nullable
                    );
                    let value =
                        TypedValueFactory::parse_from_reply(typecode, nullable, am_conn_core, rdr)?;
                    trace!("Found value {:?}", value);
                    values.push(value);
                }
                resultset
                    .rows
                    .push(Row::new(Arc::clone(&resultset.metadata), values));
            }
        }
        Ok(())
    }
}
