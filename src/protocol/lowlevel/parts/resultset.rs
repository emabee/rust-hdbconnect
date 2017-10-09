use {HdbError, HdbResult};
use protocol::lowlevel::{PrtError, PrtResult, prot_err};
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::ConnCoreRef;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::part_attributes::PartAttributes;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::row::Row;
use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
use serde_db::de::{DeserializationError, DeserializationResult, DeserializableResultset};

use serde;
use std::fmt;
use std::sync::{Arc, Mutex};


// pub type Row = Row<ResultSetMetadata, TypedValue>;
// impl Row {
//     /// Returns a clone of the ith value.
//     pub fn get(&self, i: usize) -> HdbResult<TypedValue> {
//         Ok(DeserializableRow::get(self, i)?.clone())
//     }
//
//     // FIXME implement field_into, using a deserializer? Or again twenty variants?
//     // pub fn field_into<T>(&mut self, i: usize) -> HdbResult<T>
//     //     where T: serde::de::Deserialize<'x>
//     // {
//     //     ...
//     // }
// }


/// Contains the result of a database read command, including the describing metadata.
///
/// In most cases, you will want to use the powerful method [into_typed](#method.into_typed)
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
    o_conn_ref: Option<ConnCoreRef>,
    attributes: PartAttributes,
    resultset_id: u64,
}

impl ResultSetCore {
    fn new_rs_ref(conn_ref: Option<&ConnCoreRef>, attrs: PartAttributes, rs_id: u64)
                  -> Arc<Mutex<ResultSetCore>> {
        Arc::new(Mutex::new(ResultSetCore {
            o_conn_ref: match conn_ref {
                Some(conn_ref) => Some(conn_ref.clone()),
                None => None,
            },
            attributes: attrs,
            resultset_id: rs_id,
        }))
    }
    // FIXME implement DROP as send a request of type CLOSERESULTSET in case
    // the resultset is not yet closed (RESULTSETCLOSED)
}

impl DeserializableResultset for ResultSet {
    type ROW = Row;
    type E = HdbError;

    /// Returns true if more than 1 row is contained
    fn has_multiple_rows(&mut self) -> Result<bool, DeserializationError> {
        let is_complete = match self.is_complete() {
            Ok(b) => b,
            Err(_) => false,
        };
        Ok(!is_complete || (self.rows.len() > 1))
    }

    /// Reverses the order of the rows
    fn reverse_rows(&mut self) {
        trace!("ResultSet::reverse_rows()");
        self.rows.reverse()
    }

    /// Removes the last row and returns it, or None if it is empty.
    fn pop_row(&mut self) -> DeserializationResult<Option<Row>> {
        Ok(ResultSet::pop_row(self))
    }

    /// Returns the number of fields
    fn number_of_fields(&self) -> usize {
        self.metadata.len()
    }

    /// Returns the name of the column at the specified index
    fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        self.metadata.get_fieldname(field_idx)
    }

    /// Fetches all not yet transported result lines from the server.
    ///
    /// Bigger resultsets are typically not transported in one DB roundtrip;
    /// the number of roundtrips depends on the size of the resultset
    /// and the configured fetch_size of the connection.
    fn fetch_all(&mut self) -> HdbResult<()> {
        ResultSet::fetch_all(self)
    }
}


impl ResultSet {
    ///
    pub fn len(&mut self) -> HdbResult<usize> {
        self.fetch_all()?;
        Ok(self.rows.len())
    }

    /// Removes the last row and returns it, or None if it is empty.
    pub fn pop_row(&mut self) -> Option<Row> {
        self.rows.pop()
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
        let (conn_ref, resultset_id, fetch_size) = {
            // scope the borrow
            let guard = self.core_ref.lock()?;
            let rs_core = &*guard;
            let conn_ref = match rs_core.o_conn_ref {
                Some(ref cr) => cr.clone(),
                None => {
                    return Err(prot_err("Fetch no more possible"));
                }
            };
            let fetch_size = {
                let guard = conn_ref.lock()?;
                (*guard).get_fetch_size()
            };
            (conn_ref, rs_core.resultset_id, fetch_size)
        };

        // build the request, provide resultset id, define FetchSize
        debug!("ResultSet::fetch_next() with fetch_size = {}", fetch_size);
        let command_options = 0;
        let mut request = Request::new(&conn_ref, RequestType::FetchNext, true, command_options)?;
        request.push(Part::new(PartKind::ResultSetId, Argument::ResultSetId(resultset_id)));
        request.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));

        let mut reply = request.send_and_receive_detailed(None,
                                                          None,
                                                          &mut Some(self),
                                                          &conn_ref,
                                                          Some(ReplyType::Fetch))?;
        reply.parts.pop_arg_if_kind(PartKind::ResultSet);

        let mut guard = self.core_ref.lock()?;
        let mut rs_core = &mut *guard;
        if rs_core.attributes.is_last_packet() {
            rs_core.o_conn_ref = None;
        }
        Ok(())
    }

    fn is_complete(&self) -> HdbResult<bool> {
        let guard = self.core_ref.lock()?;
        let rs_core = &*guard;
        if (!rs_core.attributes.is_last_packet()) &&
           (rs_core.attributes.row_not_found() || rs_core.attributes.is_resultset_closed()) {
            Err(HdbError::ProtocolError(PrtError::ProtocolError(String::from("ResultSet \
                                                                              incomplete, but \
                                                                              already closed \
                                                                              on server"))))
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

    /// Translates a generic resultset into a given rust type (that implements Deserialize).
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
    /// <code>into_typed()</code> can derive the type it needs to serialize into:
    ///
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct MyStruct {
    ///     ...
    /// }
    /// let typed_result: Vec<MyStruct> = resultset.into_typed()?;
    /// ```

    // pub fn into_typed<'de, T>(mut self) -> HdbResult<T>
    //     where T: serde::de::Deserialize<'de>
    // {
    //     trace!("ResultSet::into_typed()");
    //     self.fetch_all()?;
    //     Ok(serde::de::Deserialize::deserialize(&mut RsDeserializer::new(self)?)?)
    // }
    // Expose the capability from serde_db
    pub fn into_typed<'de, T>(mut self) -> HdbResult<T>
        where T: serde::de::Deserialize<'de>
    {
        trace!("Resultset::into_typed()");
        self.fetch_all()?;
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
        if self.rs.rows.len() == 0 {
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
    use protocol::protocol_error::{PrtResult, prot_err};
    use protocol::lowlevel::argument::Argument;
    use protocol::lowlevel::conn_core::ConnCoreRef;
    use protocol::lowlevel::part::Parts;
    use protocol::lowlevel::part_attributes::PartAttributes;
    use protocol::lowlevel::partkind::PartKind;
    use protocol::lowlevel::parts::option_value::OptionValue;
    use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
    use protocol::lowlevel::parts::statement_context::StatementContext;
    use protocol::lowlevel::parts::typed_value::TypedValue;
    use protocol::lowlevel::parts::typed_value::factory as TypedValueFactory;
    use std::io;
    use std::sync::Arc;

    pub fn resultset_new(conn_ref: Option<&ConnCoreRef>, attrs: PartAttributes, rs_id: u64,
                         rsm: ResultSetMetadata, o_stmt_ctx: Option<StatementContext>)
                         -> ResultSet {
        let server_processing_time = match o_stmt_ctx {
            Some(stmt_ctx) => {
                if let Some(OptionValue::INT(i)) = stmt_ctx.server_processing_time {
                    i
                } else {
                    0
                }
            }
            None => 0,
        };

        ResultSet {
            core_ref: ResultSetCore::new_rs_ref(conn_ref, attrs, rs_id),
            metadata: Arc::new(rsm),
            rows: Vec::<Row>::new(),
            acc_server_proc_time: server_processing_time,
        }
    }


    /// Factory for ResultSets, only useful for tests.
    pub fn new_for_tests(rsm: ResultSetMetadata, rows: Vec<Row>) -> ResultSet {
        ResultSet {
            core_ref: ResultSetCore::new_rs_ref(None, PartAttributes::new(0b_0000_0001), 0_u64),
            metadata: Arc::new(rsm),
            rows: rows,
            acc_server_proc_time: 0,
        }
    }

    // resultsets can be part of the response in three cases which differ
    // in regard to metadata handling:
    //
    // a) a response to a plain "execute" will contain the metadata in one of the other parts;
    //    the metadata parameter will thus have the variant None
    //
    // b) a response to an "execute prepared" will only contain data;
    //    the metadata had beeen returned already to the "prepare" call, and are provided
    //    with parameter metadata
    //
    // c) a response to a "fetch more lines" is triggered from an older resultset
    //    which already has its metadata
    //
    // For first resultset packets, we create and return a new ResultSet object
    // we expect the previous three parts to be
    // a matching ResultSetMetadata, a ResultSetId, and a StatementContext
    pub fn parse(no_of_rows: i32, attributes: PartAttributes, parts: &mut Parts,
                 o_conn_ref: Option<&ConnCoreRef>, rs_md: Option<&ResultSetMetadata>,
                 o_rs: &mut Option<&mut ResultSet>, rdr: &mut io::BufRead)
                 -> PrtResult<Option<ResultSet>> {

        match *o_rs {
            None => {
                // case a) or b)
                let o_stmt_ctx = match parts.pop_arg_if_kind(PartKind::StatementContext) {
                    Some(Argument::StatementContext(stmt_ctx)) => Some(stmt_ctx),
                    None => None,
                    _ => {
                        return Err(prot_err("Inconsistent StatementContext part found for \
                                             ResultSet"))
                    }
                };

                let rs_id = match parts.pop_arg() {
                    Some(Argument::ResultSetId(rs_id)) => rs_id,
                    _ => return Err(prot_err("No ResultSetId part found for ResultSet")),
                };

                let rs_metadata = match parts.pop_arg_if_kind(PartKind::ResultSetMetadata) {
                    Some(Argument::ResultSetMetadata(rsmd)) => rsmd,
                    None => {
                        match rs_md {
                            Some(rs_md) => rs_md.clone(),
                            _ => return Err(prot_err("No metadata provided for ResultSet")),
                        }
                    }
                    _ => return Err(prot_err("Inconsistent metadata part found for ResultSet")),
                };

                let mut result =
                    resultset_new(o_conn_ref, attributes, rs_id, rs_metadata, o_stmt_ctx);
                parse_rows(&mut result, no_of_rows, rdr)?;
                Ok(Some(result))
            }

            Some(ref mut fetching_resultset) => {
                match parts.pop_arg_if_kind(PartKind::StatementContext) {
                    Some(Argument::StatementContext(stmt_ctx)) => {
                        if let Some(OptionValue::INT(i)) = stmt_ctx.server_processing_time {
                            fetching_resultset.acc_server_proc_time += i;
                        }
                    }
                    None => {}
                    _ => {
                        return Err(prot_err("Inconsistent StatementContext part found for \
                                             ResultSet"))
                    }
                };

                {
                    let mut guard = fetching_resultset.core_ref.lock()?;
                    let mut rs_core = &mut *guard;
                    rs_core.attributes = attributes;
                }
                parse_rows(fetching_resultset, no_of_rows, rdr)?;
                Ok(None)
            }
        }
    }

    fn parse_rows(resultset: &mut ResultSet, no_of_rows: i32, rdr: &mut io::BufRead)
                  -> PrtResult<()> {
        let no_of_cols = resultset.metadata.count();
        debug!("resultset::parse_rows() reading {} lines with {} columns", no_of_rows, no_of_cols);

        let guard = resultset.core_ref.lock()?;
        let rs_core = &*guard;
        match rs_core.o_conn_ref {
            None => {
                // cannot happen FIXME: make this more robust
            }
            Some(ref conn_ref) => {
                for r in 0..no_of_rows {
                    // let mut row = Row { values: Vec::<TypedValue>::new() };
                    let mut values = Vec::<TypedValue>::new();
                    for c in 0..no_of_cols {
                        let field_md = resultset.metadata.get_fieldmetadata(c as usize).unwrap();
                        let typecode = field_md.value_type;
                        let nullable = field_md.column_option.is_nullable();
                        trace!("Parsing row {}, column {}, typecode {}, nullable {}",
                               r,
                               c,
                               typecode,
                               nullable);
                        let value =
                            TypedValueFactory::parse_from_reply(typecode, nullable, conn_ref, rdr)?;
                        trace!("Found value {:?}", value);
                        values.push(value);
                    }
                    resultset.rows
                             .push(Row::new(resultset.metadata.clone(), values));
                }
            }
        }
        Ok(())
    }
}
