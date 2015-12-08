use DbcResult;
use super::{PrtError,PrtResult,prot_err};
use super::argument::Argument;
use super::conn_core::ConnRef;
use super::function_code::FunctionCode;
use super::message::RequestMessage;
use super::message_type::MessageType;
use super::option_value::OptionValue;
use super::part::Part;
use super::partkind::PartKind;
use super::part_attributes::PartAttributes;
use super::resultset_metadata::ResultSetMetadata;
use super::statement_context::StatementContext;
use super::typed_value::TypedValue;
use super::util;

use rs_serde::deserialize::RsDeserializer;

use serde;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

#[derive(Debug)]
pub struct ResultSet {
    pub core_ref: RsRef,
    pub metadata: ResultSetMetadata,
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub struct ResultSetCore {
    pub o_conn_ref: Option<ConnRef>,
    pub attributes: PartAttributes,
    pub resultset_id: u64,
    pub statement_contexts: Vec<StatementContext>,
}
pub type RsRef = Rc<RefCell<ResultSetCore>>;

impl ResultSetCore {
    pub fn new_rs_ref(conn_ref: Option<&ConnRef>, attrs: PartAttributes, rs_id: u64, stmt_ctx: StatementContext) -> RsRef{
        Rc::new(RefCell::new(ResultSetCore{
            o_conn_ref: match conn_ref {Some(conn_ref) => Some(conn_ref.clone()), None => None},
            attributes: attrs,
            resultset_id: rs_id,
            statement_contexts: vec![stmt_ctx],
        }))
    }
    pub fn latest_stmt_seq_info(&self) -> Option<OptionValue> {
        self.statement_contexts.last().as_ref().unwrap().statement_sequence_info.clone()
    }
}

impl ResultSet {
    pub fn new(conn_ref: Option<&ConnRef>, attrs: PartAttributes, rs_id: u64, stmt_ctx: StatementContext, rsm: ResultSetMetadata)
    -> ResultSet {
        ResultSet {
            core_ref: ResultSetCore::new_rs_ref(conn_ref, attrs, rs_id, stmt_ctx),
            metadata: rsm,
            rows: Vec::<Row>::new(),
        }
    }

    pub fn size(&self) -> PrtResult<usize> {
        let mut size = 0;
        for row in &self.rows {
            size += try!(row.size());
        }
        Ok(size)
    }
    pub fn parse( no_of_rows: i32, attributes: PartAttributes, parts: &mut Vec<Part>,
                  conn_ref: &ConnRef, o_rs: &mut Option<&mut ResultSet>, rdr: &mut io::BufRead )
    -> PrtResult<Option<ResultSet>> {
        match *o_rs {
            mut None => {
                // for first resultset packets, we create and return a new ResultSet object
                // we expect to already have received a matching metadata part, a ResultSetId, and a StatementContext
                let rs_metadata = {
                    let mdpart = match util::get_first_part_of_kind(PartKind::ResultSetMetadata, &parts) {
                        Some(idx) => parts.remove(idx),
                        None => return Err(prot_err("No metadata found for ResultSet")),
                    };
                    match mdpart.arg {
                        Argument::ResultSetMetadata(r) => r,
                        _ => return Err(prot_err("Inconsistent metadata part found for ResultSet")),
                    }
                };
                let rs_id = {
                    let rs_id_part = match util::get_first_part_of_kind(PartKind::ResultSetId, &parts) {
                        Some(idx) => parts.remove(idx),
                        None => return Err(prot_err("No ResultSetId found for ResultSet")),
                    };
                    match rs_id_part.arg {
                        Argument::ResultSetId(i) => i,
                        _ => return Err(prot_err("Inconstent ResultSetId part found for ResultSet")),
                    }
                };
                let stmt_context = {
                    let scpart = match util::get_first_part_of_kind(PartKind::StatementContext, &parts) {
                        Some(idx) => parts.remove(idx),
                        None => return Err(prot_err("No StatementContext found for ResultSet")),
                    };
                    match scpart.arg {
                        Argument::StatementContext(s) => s,
                        _ => return Err(prot_err("Inconstent StatementContext part found for ResultSet")),
                    }
                };

                let mut result = ResultSet::new(Some(conn_ref), attributes, rs_id, stmt_context, rs_metadata);
                try!(result.parse_rows(no_of_rows,rdr));
                Ok(Some(result))
            },

            Some(ref mut rs) => {
                // follow-up fetches append their data to the first resultset object
                let scpart = match util::get_first_part_of_kind(PartKind::StatementContext, &parts) {
                    Some(idx) => parts.remove(idx),
                    None => return Err(prot_err("No StatementContext found for ResultSet")),
                };
                let stmt_context = match scpart.arg {
                    Argument::StatementContext(s) => s,
                    _ => return Err(prot_err("Inconstent StatementContext part found for ResultSet")),
                };

                {
                    let mut rs_core = rs.core_ref.borrow_mut();
                    match (&rs_core.latest_stmt_seq_info(), &stmt_context.statement_sequence_info) {
                        (&Some(OptionValue::BSTRING(ref b1)), &Some(OptionValue::BSTRING(ref b2))) => {
                            if b1 != b2 {
                                return Err(prot_err("statement_sequence_info of fetch does not match"));
                            }
                        },
                        _ => return Err(prot_err("invalid value type for statement_sequence_info")),
                    }
                    rs_core.attributes = attributes;
                    rs_core.statement_contexts.push(stmt_context);
                }
                try!(rs.parse_rows(no_of_rows, rdr));
                Ok(None)
            },
        }
    }


    fn parse_rows(&mut self, no_of_rows: i32, rdr: &mut io::BufRead ) -> PrtResult<()> {
        let no_of_cols = self.metadata.count();
        debug!("resultset::parse_rows() reading {} lines with {} columns", no_of_rows, no_of_cols);

        for r in 0..no_of_rows {
            let mut row = Row{values: Vec::<TypedValue>::new()};
            for c in 0..no_of_cols {
                let field_md = self.metadata.fields.get(c as usize).unwrap();
                let typecode = field_md.value_type;
                let nullable = field_md.column_option.is_nullable();
                trace!("Parsing row {}, column {}, typecode {}, nullable {}", r, c, typecode, nullable);
                let value = try!(TypedValue::parse(typecode, nullable, &(self.core_ref), rdr));
                trace!("Found value {:?}", value);
                row.values.push(value);
            }
            self.rows.push(row);
        }
        Ok(())
    }

    pub fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        self.metadata.get_fieldname(field_idx)
    }

    pub fn get_value(&self, row: usize, column: usize) -> Option<&TypedValue> {
        match self.rows.get(row) {
            Some(row) => row.values.get(column),
            None => None,
        }
    }

    pub fn no_of_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn no_of_cols(&self) -> usize {
        self.metadata.fields.len()
    }

    fn is_complete(&self) -> PrtResult<bool> {
        let rs_core = self.core_ref.borrow();
        if (!rs_core.attributes.is_last_packet())
           && (rs_core.attributes.row_not_found() || rs_core.attributes.is_resultset_closed()) {
            Err(PrtError::ProtocolError(String::from("ResultSet incomplete, but already closed on server")))
        } else {
            Ok(rs_core.attributes.is_last_packet())
        }
    }

    pub fn fetch_all(&mut self) -> PrtResult<()> {
        while ! try!(self.is_complete()) {
            try!(self.fetch_next());
        }
        Ok(())
    }

    fn fetch_next(&mut self) -> PrtResult<()> {
        trace!("ResultSet::fetch_next()");
        let (conn_ref, resultset_id, statement_sequence_info, fetch_size) = { // scope the borrow
            let rs_core = self.core_ref.borrow();
            let conn_ref = match rs_core.o_conn_ref {
                Some(ref cr) => cr.clone(),
                None => {return Err(prot_err("Fetch no more possible"));},
            };
            let fetch_size = { conn_ref.borrow().get_fetch_size() };
            (conn_ref, rs_core.resultset_id, rs_core.latest_stmt_seq_info(), fetch_size)
        };

        // build the request, provide StatementContext and resultset id, define FetchSize
        let command_options = 0; // FIXME not sure if this is OK
        let mut message = RequestMessage::new(0, MessageType::FetchNext, true, command_options);
        let mut stmt_ctx = StatementContext::new();
        stmt_ctx.statement_sequence_info = statement_sequence_info;
        message.push(Part::new(PartKind::StatementContext, Argument::StatementContext(stmt_ctx)));
        message.push(Part::new(PartKind::ResultSetId, Argument::ResultSetId(resultset_id)));
        message.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));

        try!(message.send_and_receive(&mut Some(self), &conn_ref, Some(FunctionCode::Fetch)));
        Ok(())
    }


    // ///
    pub fn server_processing_times(&self) -> Vec<i64> {
        let rs_core = self.core_ref.borrow();
        rs_core.statement_contexts
            .iter()
            .filter(|s: &&StatementContext|{
                if let Some(_) = s.server_processing_time {
                    return true
                } else {
                    return false
                }
            })
            .map(|s: &StatementContext|{
                                 if let Some(OptionValue::BIGINT(ref i)) = (&s).server_processing_time.clone() {
                                     i.clone()
                                 } else {
                                     0_i64
                                 }
            })
            .collect()
    }


    /// Translates a generic result set into a given type
    pub fn into_typed<T>(self) -> DbcResult<T>
    where T: serde::de::Deserialize {
        trace!("ResultSet::into_typed()");
        let mut deserializer = RsDeserializer::new(self);
        Ok(try!(serde::de::Deserialize::deserialize(&mut deserializer)))
    }
}


#[derive(Debug,Clone)]
pub struct Row {
    pub values: Vec<TypedValue>,
}
impl Row{
    fn size(&self) -> PrtResult<usize> {
        let mut size = 0;
        for value in &self.values {
            size += try!(value.size());
        }
        Ok(size)
    }
}
