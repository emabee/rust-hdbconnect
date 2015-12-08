use super::{PrtResult,prot_err};
use super::argument::Argument;
use super::function_code::FunctionCode;
use super::message::RequestMessage;
use super::message_type::MessageType;
use super::part::Part;
use super::partkind::PartKind;
use super::resultset::RsRef;
use super::statement_context::StatementContext;
use super::util;

use std::borrow::Cow;
use std::cmp;

#[derive(Clone,Debug)]
pub struct BLOB {
    rs_ref: Option<RsRef>,
    is_data_complete: bool,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
}
impl BLOB {
    pub fn new(rs_ref: &RsRef, is_data_complete: bool, length_b: u64, locator_id: u64, data: Vec<u8>) -> BLOB {
        trace!(
            "Instantiate BLOB with length_b = {}, is_data_complete = {}, data.length() = {}",
            length_b, is_data_complete, data.len()
        );
        BLOB {
            rs_ref: Some(rs_ref.clone()),
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

    fn fetch_next_chunk(&mut self) -> PrtResult<()> {
        let (mut reply_data, reply_is_last_data)
            = try!(fetch_a_lob_chunk(&self.rs_ref, self.locator_id, self.length_b, self.data.len() as u64));

        self.data.append(&mut reply_data);
        self.is_data_complete = reply_is_last_data;

        assert_eq!(self.is_data_complete, self.length_b == self.data.len() as u64);
        trace!("After BLOB fetch: is_data_complete = {}, data.length() = {}", self.is_data_complete, self.length_b);
        Ok(())
    }

    pub fn into_bytes(mut self) -> PrtResult<Vec<u8>> {
        trace!("BLOB::into_bytes()");
        while !self.is_data_complete {
            try!(self.fetch_next_chunk());
        }
        Ok(self.data)
    }
}


#[derive(Clone,Debug)]
pub struct CLOB {
    rs_ref: Option<RsRef>,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    char_count: u64,
    locator_id: u64,
    data: String,
}
impl CLOB {
    pub fn new(rs_ref: &RsRef, is_data_complete: bool, length_c: u64, length_b: u64, char_count: u64, locator_id: u64, data: String)
    -> CLOB {
        trace!(
            "CLOB::new() with length_c = {}, length_b = {}, is_data_complete = {}, data.len() = {}",
            length_c, length_b, is_data_complete, data.len()
        );
        if (length_c < 1000) && (length_c != length_b) {
            trace!("=============================\n{}\n===========================",data);
        }
        CLOB {
            rs_ref: Some(rs_ref.clone()),
            length_c: length_c,
            length_b: length_b,
            char_count: char_count,
            is_data_complete: is_data_complete,
            locator_id: locator_id,
            data: data,
        }
    }

    fn fetch_next_chunk(&mut self) -> PrtResult<()> {
        let (reply_data, reply_is_last_data)
            = try!(fetch_a_lob_chunk(&self.rs_ref, self.locator_id, self.length_b, self.char_count));

        let (s,char_count) = try!(util::from_cesu8_with_count(&reply_data));
        trace!("self.data: =============================\n{}\n===========================", self.data);
        trace!("new data: =============================\n{}\n===========================", s);
        match s {
            Cow::Owned(s) => self.data.push_str(&s),
            Cow::Borrowed(s) => self.data.push_str(s),
        }
        self.char_count += char_count;
        self.is_data_complete = reply_is_last_data;

        assert_eq!(self.is_data_complete, self.length_b == self.data.len() as u64);
        trace!("After CLOB fetch: is_data_complete = {}, data.length() = {}", self.is_data_complete, self.length_b);
        Ok(())
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


fn fetch_a_lob_chunk(rs_ref: &Option<RsRef>, locator_id: u64, length_b: u64, data_len: u64 )
-> PrtResult<(Vec<u8>,bool)> {
    // build the request, provide StatementContext and length_to_read
    let (conn_ref, statement_sequence_info, length_to_read) = { // scope the borrow
        match *rs_ref {
            None =>  {
                return Err(prot_err("LOB is not complete, but fetching more chunks is no more possible (resultset is closed)"));
            },
            Some(ref rs) => {
                let rs_core = rs.borrow();
                let conn_ref = match rs_core.o_conn_ref {
                    Some(ref cr) => cr.clone(),
                    None => {return Err(prot_err("LOB is not complete, but fetching more chunks is no more possible (connection of the resultset is closed)"));},
                };
                let length_to_read = cmp::min(
                        conn_ref.borrow().get_lob_read_length() as u64,
                        length_b - data_len
                );
                (conn_ref, rs_core.latest_stmt_seq_info(), length_to_read as i32)
            },
        }
    };
    let mut message = RequestMessage::new(0, MessageType::ReadLob, true, 0);

    let mut stmt_ctx = StatementContext::new();
    stmt_ctx.statement_sequence_info = statement_sequence_info;
    message.push(Part::new(PartKind::StatementContext, Argument::StatementContext(stmt_ctx)));

    let offset = data_len + 1;
    message.push(Part::new(PartKind::ReadLobRequest, Argument::ReadLobRequest(locator_id, offset, length_to_read)));

    let mut response = try!(message.send_and_receive(&mut None, &conn_ref, Some(FunctionCode::ReadLob)));

    let part = match util::get_first_part_of_kind(PartKind::ReadLobReply, &response.parts) {
        Some(idx) => response.parts.swap_remove(idx),
        None => return Err(prot_err("no part of kind ReadLobReply")),
    };

    if let Argument::ReadLobReply(reply_locator_id, reply_is_last_data, reply_data) = part.arg {
        if reply_locator_id != locator_id {
            return Err(prot_err("lob::fetch_a_lob_chunk(): locator ids do not match"));
        }
        Ok((reply_data, reply_is_last_data))
    } else { Err(prot_err("wrong Argument variant")) }
}
