use DbcResult;
use protocol::lowlevel::{PrtError,PrtResult,prot_err};
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::ConnRef;
use protocol::lowlevel::message::{Metadata,Request};
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::part_attributes::PartAttributes;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::option_value::OptionValue;
use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
use protocol::lowlevel::parts::typed_value::TypedValue;
use rs_serde::deserializer::RsDeserializer;

use serde;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

#[derive(Debug)]
pub struct ResultSet {
    core_ref: RsRef,
    pub metadata: ResultSetMetadata,
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub struct ResultSetCore {
    pub o_conn_ref: Option<ConnRef>, // public because it is needed in lob
    pub attributes: PartAttributes,
    pub resultset_id: u64,
    pub execution_times: Vec<i64>,
}
pub type RsRef = Rc<RefCell<ResultSetCore>>;

impl ResultSetCore {
    pub fn new_rs_ref(conn_ref: Option<&ConnRef>, attrs: PartAttributes, rs_id: u64) -> RsRef{
        Rc::new(RefCell::new(ResultSetCore{
            o_conn_ref: match conn_ref {Some(conn_ref) => Some(conn_ref.clone()), None => None},
            attributes: attrs,
            resultset_id: rs_id,
            execution_times: Vec::<i64>::new(),
        }))
    }
}

impl ResultSet {
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
        let (conn_ref, resultset_id, fetch_size) = { // scope the borrow
            let rs_core = self.core_ref.borrow();
            let conn_ref = match rs_core.o_conn_ref {
                Some(ref cr) => cr.clone(),
                None => {return Err(prot_err("Fetch no more possible"));},
            };
            let fetch_size = conn_ref.borrow().get_fetch_size();
            (conn_ref, rs_core.resultset_id, fetch_size)
        };

        // build the request, provide resultset id, define FetchSize
        let command_options = 0;
        let mut request = try!(Request::new(&conn_ref, RequestType::FetchNext, true, command_options));
        request.push(Part::new(PartKind::ResultSetId, Argument::ResultSetId(resultset_id)));
        request.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));

        let mut reply = try!(request.send_and_receive_detailed(
                                Metadata::None, &mut Some(self), &conn_ref, Some(ReplyType::Fetch)
        ));

        if let Some(OptionValue::BIGINT(server_processing_time)) = reply.server_processing_time {
            self.core_ref.borrow_mut().execution_times.push(server_processing_time);
        }

        try!(reply.retrieve_first_part_of_kind(PartKind::ResultSet)); // don't leave parts to avoid a warning
        Ok(())
    }

    ///
    pub fn server_processing_times(&self) -> Vec<i64> {
        self.core_ref.borrow().execution_times.clone()
    }

    /// Translates a generic result set into a given type
    pub fn into_typed<T>(mut self) -> DbcResult<T>
    where T: serde::de::Deserialize {
        trace!("ResultSet::into_typed()");
        try!(self.fetch_all());
        let mut deserializer = RsDeserializer::new(self);
        Ok(try!(serde::de::Deserialize::deserialize(&mut deserializer)))
    }

    // FIXME implement DROP as send a request of type CLOSERESULTSET in case the resultset is not yet closed (RESULTSETCLOSED)
}

impl fmt::Display for ResultSet {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.metadata, fmt).unwrap();     // write a header
        writeln!(fmt, "").unwrap();
        for row in &self.rows {
            fmt::Display::fmt(&row, fmt).unwrap();           // write the data
            writeln!(fmt, "").unwrap();
        }
        Ok(())
    }
}

#[derive(Debug,Clone)]
pub struct Row {
    pub values: Vec<TypedValue>,
}

impl fmt::Display for Row {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for value in &self.values {
            fmt::Display::fmt(&value, fmt).unwrap();         // write the value
            write!(fmt, ", ").unwrap();
        }
        Ok(())
    }
}


pub mod factory {
    use super::{ResultSet,ResultSetCore,Row};
    use protocol::protocol_error::{PrtResult,prot_err};
    use protocol::lowlevel::argument::Argument;
    use protocol::lowlevel::conn_core::ConnRef;
    use protocol::lowlevel::message::{Metadata,retrieve_first_part_of_kind};
    use protocol::lowlevel::part::Part;
    use protocol::lowlevel::part_attributes::PartAttributes;
    use protocol::lowlevel::partkind::PartKind;
    use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
    use protocol::lowlevel::parts::typed_value::TypedValue;
    use std::io;

    pub fn new(conn_ref: Option<&ConnRef>, attrs: PartAttributes, rs_id: u64, rsm: ResultSetMetadata)
    -> ResultSet {
        ResultSet {
            core_ref: ResultSetCore::new_rs_ref(conn_ref, attrs, rs_id),
            metadata: rsm,
            rows: Vec::<Row>::new(),
        }
    }

    pub fn new_for_tests(rsm: ResultSetMetadata) -> ResultSet {
        ResultSet {
            core_ref: ResultSetCore::new_rs_ref(None, PartAttributes::new(0b_0000_0001), 0_u64),
            metadata: rsm,
            rows: Vec::<Row>::new(),
        }
    }

    /// resultsets can be part of the response in three cases which differ especially in regard to metadata handling:
    ///
    /// a) a response to a plain "execute" will contain the metadata in one of the other parts;
    ///    the metadata parameter will thus have the variant None
    ///
    /// b) a response to an "execute prepared" will only contain data;
    ///    the metadata had beeen returned already to the "prepare" call
    ///
    /// c) a response to a "fetch more lines" is triggered from an older resultset which already has its metadata
    pub fn parse( no_of_rows: i32,
                  attributes: PartAttributes,
                  parts: &mut Vec<Part>,
                  o_conn_ref: Option<&ConnRef>,
                  metadata: Metadata,
                  o_rs: &mut Option<&mut ResultSet>,
                  rdr: &mut io::BufRead )
    -> PrtResult<Option<ResultSet>> {
        match *o_rs {
            mut None => {
                // case a) or b)
                // for first resultset packets, we create and return a new ResultSet object
                // we expect to already have received a matching metadata part, a ResultSetId, and a StatementContext
                let rs_metadata = {
                    match retrieve_first_part_of_kind(PartKind::ResultSetMetadata, parts) {
                        Ok(mdpart) => {
                            match mdpart.arg {
                                Argument::ResultSetMetadata(r) => r,
                                _ => return Err(prot_err("Inconsistent metadata part found for ResultSet")),
                            }
                        },
                        Err(e) => {
                            if let Metadata::ResultSetMetadata(ref rsmd) = metadata {
                                (*rsmd).clone()
                            }
                            else { return Err(e); }
                        },
                    }
                };
                let rs_id = {
                    let rs_id_part = try!(retrieve_first_part_of_kind(PartKind::ResultSetId, parts));
                    match rs_id_part.arg {
                        Argument::ResultSetId(i) => i,
                        _ => return Err(prot_err("Inconsistent ResultSetId part found for ResultSet")),
                    }
                };
                let mut result = new(o_conn_ref, attributes, rs_id, rs_metadata);
                try!(parse_rows(&mut result,no_of_rows,rdr));
                Ok(Some(result))
            },

            Some(ref mut rs) => {
                rs.core_ref.borrow_mut().attributes = attributes;
                try!(parse_rows(rs,no_of_rows,rdr));
                Ok(None)
            },
        }
    }

    fn parse_rows(resultset: &mut ResultSet, no_of_rows: i32, rdr: &mut io::BufRead ) -> PrtResult<()> {
        let no_of_cols = resultset.metadata.count();
        debug!("resultset::parse_rows() reading {} lines with {} columns", no_of_rows, no_of_cols);

        for r in 0..no_of_rows {
            let mut row = Row{values: Vec::<TypedValue>::new()};
            for c in 0..no_of_cols {
                let field_md = resultset.metadata.fields.get(c as usize).unwrap();
                let typecode = field_md.value_type;
                let nullable = field_md.column_option.is_nullable();
                trace!("Parsing row {}, column {}, typecode {}, nullable {}", r, c, typecode, nullable);
                let value = try!(TypedValue::parse_from_reply(typecode, nullable, Some(&(resultset.core_ref)), rdr));
                trace!("Found value {:?}", value);
                row.values.push(value);
            }
            resultset.rows.push(row);
        }
        Ok(())
    }
}
