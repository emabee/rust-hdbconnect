mod deserialize;
mod rs_error;

use super::argument::Argument;
use super::message::Message;
use super::option_value::OptionValue;
use super::part::Part;
use super::partkind::PartKind;
use super::part_attributes::PartAttributes;
use super::resultset_metadata::ResultSetMetadata;
use super::segment;
use super::statement_context::StatementContext;
use super::typed_value::TypedValue;
use super::util;
use super::super::super::connection::ConnectionState;

use serde;
use std::io;


#[derive(Debug)]
pub struct ResultSet {
    pub attributes: PartAttributes,
    pub resultset_id: u64,
    statement_contexts: Vec<StatementContext>,
    pub metadata: ResultSetMetadata,
    pub rows: Vec<Row>,
}
impl ResultSet {
    pub fn new(attrs: PartAttributes, rs_id: u64, stmt_ctx: StatementContext, rsm: ResultSetMetadata) -> ResultSet {
        ResultSet {attributes: attrs, resultset_id: rs_id, statement_contexts: vec![stmt_ctx], metadata: rsm,
                    rows: Vec::<Row>::new()}
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for row in &self.rows {
            size += row.size();
        }
        size
    }
    pub fn parse( no_of_rows: i32, attributes: PartAttributes,
                  parts: &mut Vec<Part>, o_rs: &mut Option<&mut ResultSet>, rdr: &mut io::BufRead )
            -> io::Result<Option<ResultSet>> {

        match o_rs {
            &mut None => {
                // for first resultset packets, we create and return a new ResultSet object
                // we expect to already have received a matching metadata part, a ResultSetId, and a StatementContext
                let mdpart = match util::get_first_part_of_kind(PartKind::ResultSetMetadata, &parts) {
                    Some(idx) => parts.remove(idx),
                    None => return Err(util::io_error("No metadata found for ResultSet")),
                };
                let rs_metadata = match mdpart.arg {
                    Argument::ResultSetMetadata(r) => r,
                    _ => return Err(util::io_error("Inconstent metadata part found for ResultSet")),
                };

                let ripart = match util::get_first_part_of_kind(PartKind::ResultSetId, &parts) {
                    Some(idx) => parts.remove(idx),
                    None => return Err(util::io_error("No ResultSetId found for ResultSet")),
                };
                let rs_id = match ripart.arg {
                    Argument::ResultSetId(i) => i,
                    _ => return Err(util::io_error("Inconstent ResultSetId part found for ResultSet")),
                };

                let scpart = match util::get_first_part_of_kind(PartKind::StatementContext, &parts) {
                    Some(idx) => parts.remove(idx),
                    None => return Err(util::io_error("No StatementContext found for ResultSet")),
                };
                let stmt_context = match scpart.arg {
                    Argument::StatementContext(s) => s,
                    _ => return Err(util::io_error("Inconstent StatementContext part found for ResultSet")),
                };

                let mut result = ResultSet::new(attributes, rs_id, stmt_context, rs_metadata);
                try!(ResultSet::parse_rows(no_of_rows, &result.metadata, &mut result.rows, rdr));
                Ok(Some(result))
            },

            &mut Some(ref mut rs) => {
                // follow-up fetches append their data to the first resultset object
                let scpart = match util::get_first_part_of_kind(PartKind::StatementContext, &parts) {
                    Some(idx) => parts.remove(idx),
                    None => return Err(util::io_error("No StatementContext found for ResultSet")),
                };
                let stmt_context = match scpart.arg {
                    Argument::StatementContext(s) => s,
                    _ => return Err(util::io_error("Inconstent StatementContext part found for ResultSet")),
                };

                match (&rs.statement_contexts.last().unwrap().statement_sequence_info,
                       &stmt_context.statement_sequence_info) {
                    (&Some(OptionValue::BSTRING(ref b1)), &Some(OptionValue::BSTRING(ref b2))) => {
                        if b1 != b2 {
                            return Err(util::io_error("statement_sequence_info of fetch does not match"));
                        }
                    },
                    _ => return Err(util::io_error("invalid value type for statement_sequence_info")),
                }

                rs.statement_contexts.push(stmt_context);
                try!(ResultSet::parse_rows(no_of_rows, &rs.metadata, &mut rs.rows, rdr));
                rs.attributes = attributes;
                Ok(None)
            },
        }
    }


    fn parse_rows(no_of_rows: i32, rs_md: &ResultSetMetadata, rows: &mut Vec<Row>, rdr: &mut io::BufRead )
      -> io::Result<()>
    {
        let no_of_cols = rs_md.count();
        debug!("resultset::parse_rows() reading {} lines with {} columns", no_of_rows, no_of_cols);
        for r in 0..no_of_rows {
            let mut row = Row{values: Vec::<TypedValue>::new()};
            for c in 0..no_of_cols {
                let field_md = rs_md.fields.get(c as usize).unwrap();
                let typecode = field_md.value_type;
                let nullable = field_md.column_option.is_nullable();
                trace!("Parsing row {}, column {}, typecode {}, nullable {}", r, c, typecode, nullable);
                let value = try!(TypedValue::parse(typecode, nullable, rdr));
                trace!("Found value {:?}", value);
                row.values.push(value);
            }
            rows.push(row);
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

    fn is_complete(&self) -> io::Result<bool> {
        if (!self.attributes.is_last_packet())
           && (self.attributes.row_not_found() || self.attributes.is_resultset_closed()) {
            Err(util::io_error("ResultSet incomplete, but already closed on server"))
        } else {
            Ok(self.attributes.is_last_packet())
        }
    }

    pub fn fetch_all(&mut self, conn_state: &mut ConnectionState) -> io::Result<()> {
        while ! try!(self.is_complete()) {
            try!(self.fetch_next(conn_state));
        }
        Ok(())
    }

    fn fetch_next(&mut self, conn_state: &mut ConnectionState) -> io::Result<()> {
        trace!("plain_statement::fetch_next()");
        // build the request, provide StatementContext and resultset id, define FetchSize
        let mut segment = segment::new_request_seg(segment::MessageType::FetchNext, true);
        let mut stmt_ctx = StatementContext::new();
        stmt_ctx.statement_sequence_info = self.statement_contexts.last().unwrap().statement_sequence_info.clone();
        segment.push(Part::new(PartKind::StatementContext, Argument::StatementContext(stmt_ctx)));
        segment.push(Part::new(PartKind::ResultSetId, Argument::ResultSetId(self.resultset_id)));
        segment.push(Part::new(PartKind::FetchSize, Argument::FetchSize(1024)));
        let mut message = Message::new(conn_state.session_id, conn_state.get_next_seq_number());
        message.segments.push(segment);

        // send it
        try!(message.send_and_receive(&mut Some(self), &mut (conn_state.stream)));
        Ok(())
    }

    /// Translates a generic result set into a given type
    pub fn as_table<T>(self) -> io::Result<T>
      where T: serde::de::Deserialize
    {
        trace!("ResultSet::as_table()");
        let mut deserializer = self::deserialize::RsDeserializer::new(self);
        serde::de::Deserialize::deserialize(&mut deserializer).map_err(|e|{io::Error::from(e)})
    }
}


#[derive(Debug,Clone)]
pub struct Row {
    pub values: Vec<TypedValue>,
}
impl Row{
    pub fn size(&self) -> usize {
        let mut size = 0;
        for value in &self.values {
            size += value.size();
        }
        size
    }
}
