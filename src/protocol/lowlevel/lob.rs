use super::{PrtError,PrtResult,prot_err};
use super::argument::Argument;
use super::message::Message;
use super::part::Part;
use super::partkind::PartKind;
use super::resultset::RsRef;
use super::segment;
use super::statement_context::StatementContext;
use super::util;

use std::borrow::Cow;
use std::cmp;

#[derive(Clone,Debug)]
pub struct BLOB {
    rs_ref: Option<RsRef>,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
}
impl BLOB {
    pub fn new(rs_ref: &RsRef, is_data_complete: bool, length_c: u64, length_b: u64, locator_id: u64, data: Vec<u8>) -> BLOB {
        trace!(
            "Instantiate BLOB with length_c = {}, length_b = {}, is_data_complete = {}, data.length() = {}",
            length_c, length_b, is_data_complete, data.len()
        );
        BLOB {
            rs_ref: Some(rs_ref.clone()),
            length_c: length_c,
            length_b: length_b,
            is_data_complete: is_data_complete,
            locator_id: locator_id,
            data: data,
        }
    }

    // FIXME should be sth like as_byte_stream()
    // pub fn to_owned_data(self) -> Vec<u8> {
    //     self.data
    // }
}


#[derive(Clone,Debug)]
pub struct CLOB {
    rs_ref: Option<RsRef>,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    data: String,
}
impl CLOB {
    pub fn new(rs_ref: &RsRef, is_data_complete: bool, length_c: u64, length_b: u64, locator_id: u64, data: String) -> CLOB {
        trace!(
            "Instantiate CLOB with length_c = {}, length_b = {}, is_data_complete = {}, data.length() = {}",
            length_c, length_b, is_data_complete, data.len()
        );
        if (length_c < 1000) && (length_c != length_b) {
            trace!("=============================\n{}\n===========================",data);
        }
        CLOB {
            rs_ref: Some(rs_ref.clone()),
            length_c: length_c,
            length_b: length_b,
            is_data_complete: is_data_complete,
            locator_id: locator_id,
            data: data,
        }
    }

    fn fetch_next_chunk(&mut self) -> PrtResult<()> {
        trace!("CLOB::load_next_chunk() with is_data_complete = {}, locator_id = {}", self.is_data_complete, self.locator_id);
        if self.is_data_complete {
            return Ok(())         // FIXME Just do nothing? Or better throw an error?
        };


        // build the request, provide StatementContext and length_to_read
        let (conn_ref, statement_sequence_info, length_to_read) = { // scope the borrow
            match self.rs_ref {
                None =>  {
                    return Err(prot_err("CLOB is not complete, but fetching more chunks is no more possible (resultset is closed)"));
                },
                Some(ref rs) => {
                    let rs_core = rs.borrow();
                    let conn_ref = match rs_core.o_conn_ref {
                        Some(ref cr) => cr.clone(),
                        None => {return Err(prot_err("CLOB is not complete, but fetching more chunks is no more possible (connection is closed)"));},
                    };
                    let mut length_to_read = { conn_ref.borrow().get_lob_read_length() };
                    trace!("Before: length_to_read = {}", length_to_read);
                    length_to_read = cmp::min(length_to_read as u64, self.length_b - self.data.len() as u64) as i32;
                    trace!("After: length_to_read = {}, self.length_b = {}, self.data.len() = {}", length_to_read, self.length_b, self.data.len());
                    (
                        conn_ref,
                        rs_core.statement_contexts.last().as_ref().unwrap().statement_sequence_info.clone(),
                        length_to_read
                    )
                },
            }
        };
        let mut segment = segment::new_request_seg(segment::MessageType::ReadLob, true, 0);
        let mut stmt_ctx = StatementContext::new();
        stmt_ctx.statement_sequence_info = statement_sequence_info;
        segment.push(Part::new(PartKind::StatementContext, Argument::StatementContext(stmt_ctx)));

        let offset = self.data.len() as i64;  // FIXME it is not clear yet how offset is to be specified
        segment.push(Part::new(PartKind::ReadLobRequest,
                    Argument::ReadLobRequest(self.locator_id, offset, length_to_read)
        ));

        let mut message = Message::new();
        message.segments.push(segment);

        let mut response = try!(message.send_and_receive(&mut None, &conn_ref));

        if response.segments.len() != 1 {
            return Err(PrtError::ProtocolError("Wrong count of segments".to_string()));
        }

        let segment = response.segments.remove(0);
        match (&segment.kind, &segment.function_code) {
            (&segment::Kind::Reply, &Some(segment::FunctionCode::ReadLob)) => {},
            _ => {
                return Err(PrtError::ProtocolError(
                    format!("unexpected segment {:?} or function code {:?} at 1", &segment.kind, &segment.function_code)
                ));
            },
        }

        let part = match util::get_first_part_of_kind(PartKind::ReadLobReply, &segment.parts) {
            Some(idx) => segment.parts.get(idx).unwrap(),
            None => return Err(PrtError::ProtocolError("no part of kind ReadLobReply".to_string())),
        };

        if let Argument::ReadLobReply(ref locator, ref is_last_data, ref data) = part.arg {
            if *locator != self.locator_id {
                return Err(PrtError::ProtocolError("lob::load_next_chunk(): locator ids do not match".to_string()));
            }
            match try!(util::from_cesu8(&data)) {
                Cow::Owned(s) => self.data.push_str(&s),
                Cow::Borrowed(s) => self.data.push_str(s),
            }
            self.is_data_complete = *is_last_data;
            trace!(
                "After lob fetch: CLOB with length_c = {}, length_b = {}, is_data_complete = {}, data.length() = {}",
                self.length_c, self.length_b, self.is_data_complete, self.data.len()
            );
            Ok(())
        } else {
            Err(PrtError::ProtocolError("wrong Argument variant".to_string()))
        }
    }

    fn load_complete(&mut self) -> PrtResult<()> {
        trace!("CLOB::load_complete() while is_data_complete = {}", self.is_data_complete);
        while !self.is_data_complete {
            try!(self.fetch_next_chunk());
        }
        Ok(())
    }

    pub fn into_string(mut self) -> PrtResult<String> {
        trace!("CLOB::into_string()");
        try!(self.load_complete());
        Ok(self.data)
    }

    // FIXME implement sth like into_character_stream() with deferred chunk fetching
}
