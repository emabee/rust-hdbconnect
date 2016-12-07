use protocol::protocol_error::{PrtResult, prot_err};
use protocol::lowlevel::util;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::parts::option_value::OptionValue;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::conn_core::ConnRef;

use std::borrow::Cow;
use std::cmp;

/// BlobHandle is used for BLOBs that we receive from the database.
/// The data are often not transferred completely,
/// so we carry internally a database connection and the
/// necessary controls to support fetching remaining data on demand.
#[derive(Clone,Debug)]
pub struct BlobHandle {
    o_conn_ref: Option<ConnRef>,
    is_data_complete: bool,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
    acc_server_proc_time: i32,
}
impl BlobHandle {
    pub fn new(conn_ref: &ConnRef, is_data_complete: bool, length_b: u64, locator_id: u64,
               data: Vec<u8>)
               -> BlobHandle {
        trace!("BlobHandle::new() with length_b = {}, is_data_complete = {}, data.length() = {}",
               length_b,
               is_data_complete,
               data.len());
        BlobHandle {
            o_conn_ref: Some(conn_ref.clone()),
            length_b: length_b,
            is_data_complete: is_data_complete,
            locator_id: locator_id,
            data: data,
            acc_server_proc_time: 0,
        }
    }

    pub fn len(&mut self) -> PrtResult<usize> {
        try!(self.fetch_all());
        Ok(self.data.len())
    }

    fn fetch_next_chunk(&mut self) -> PrtResult<()> {
        let (mut reply_data, reply_is_last_data, server_processing_time) =
            try!(fetch_a_lob_chunk(&self.o_conn_ref,
                                   self.locator_id,
                                   self.length_b,
                                   self.data.len() as u64));

        self.data.append(&mut reply_data);
        self.is_data_complete = reply_is_last_data;
        self.acc_server_proc_time += server_processing_time;

        assert_eq!(self.is_data_complete, self.length_b == self.data.len() as u64);
        trace!("After BlobHandle fetch: is_data_complete = {}, data.length() = {}",
               self.is_data_complete,
               self.length_b);
        Ok(())
    }

    /// Converts a BLOB into a Vec<u8> containing its data.
    fn fetch_all(&mut self) -> PrtResult<()> {
        trace!("BlobHandle::fetch_all()");
        while !self.is_data_complete {
            try!(self.fetch_next_chunk());
        }
        Ok(())
    }
    /// Converts a BLOB into a Vec<u8> containing its data.
    pub fn into_bytes(mut self) -> PrtResult<Vec<u8>> {
        trace!("BlobHandle::into_bytes()");
        try!(self.fetch_all());
        Ok(self.data)
    }
    // FIXME we should also have sth like into_byte_stream()
}



/// ClobHandle is used for CLOBs and NCLOBs that we receive from the database.
/// The data are often not transferred completely, so we carry internally
/// a database connection and the
/// necessary controls to support fetching remaining data on demand.
#[derive(Clone,Debug)]
pub struct ClobHandle {
    o_conn_ref: Option<ConnRef>,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    char_count: u64,
    locator_id: u64,
    data: String,
    acc_server_proc_time: i32,
}
impl ClobHandle {
    pub fn new(conn_ref: &ConnRef, is_data_complete: bool, length_c: u64, length_b: u64,
               char_count: u64, locator_id: u64, data: String)
               -> ClobHandle {
        ClobHandle {
            o_conn_ref: Some(conn_ref.clone()),
            length_c: length_c,
            length_b: length_b,
            char_count: char_count,
            is_data_complete: is_data_complete,
            locator_id: locator_id,
            data: data,
            acc_server_proc_time: 0,
        }
    }

    pub fn len(&mut self) -> PrtResult<usize> {
        try!(self.load_complete());
        Ok(self.data.len())
    }

    fn fetch_next_chunk(&mut self) -> PrtResult<()> {
        let (reply_data, reply_is_last_data, server_processing_time) =
            try!(fetch_a_lob_chunk(&self.o_conn_ref,
                                   self.locator_id,
                                   self.length_b,
                                   self.char_count));

        let (s, char_count) = try!(util::from_cesu8_with_count(&reply_data));
        match s {
            Cow::Owned(s) => self.data.push_str(&s),
            Cow::Borrowed(s) => self.data.push_str(s),
        }
        self.char_count += char_count;
        self.is_data_complete = reply_is_last_data;
        self.acc_server_proc_time += server_processing_time;

        assert_eq!(self.is_data_complete, self.length_b == self.data.len() as u64);
        trace!("After ClobHandle fetch: is_data_complete = {}, data.length() = {}",
               self.is_data_complete,
               self.length_b);
        Ok(())
    }

    fn load_complete(&mut self) -> PrtResult<()> {
        trace!("ClobHandle::load_complete()");
        while !self.is_data_complete {
            try!(self.fetch_next_chunk());
        }
        Ok(())
    }

    /// Converts a ClobHandle into a String containing its data.
    pub fn into_string(mut self) -> PrtResult<String> {
        trace!("ClobHandle::into_string()");
        try!(self.load_complete());
        Ok(self.data)
    }
    // FIXME we should also have sth like into_character_stream() with deferred chunk fetching
}


//
fn fetch_a_lob_chunk(o_conn_ref: &Option<ConnRef>, locator_id: u64, length_b: u64, data_len: u64)
                     -> PrtResult<(Vec<u8>, bool, i32)> {
    // build the request, provide StatementContext and length_to_read
    let (conn_ref, length_to_read) = match *o_conn_ref {
        None => {
            return Err(prot_err("LOB is not complete, but fetching more chunks is no more \
                                 possible (connection not available)"));
        }
        Some(ref conn_ref) => {
            let length_to_read = cmp::min(conn_ref.borrow().get_lob_read_length() as u64,
                                          length_b - data_len);
            (conn_ref, length_to_read as i32)
        }
    };
    let mut request = try!(Request::new(&conn_ref, RequestType::ReadLob, true, 0));

    let offset = data_len + 1;
    request.push(Part::new(PartKind::ReadLobRequest,
                           Argument::ReadLobRequest(locator_id, offset, length_to_read)));

    let mut reply = try!(request.send_and_receive(&conn_ref, Some(ReplyType::ReadLob)));

    let (reply_data, reply_is_last_data) = match reply.parts
                                                      .pop_arg_if_kind(PartKind::ReadLobReply) {
        Some(Argument::ReadLobReply(reply_locator_id, reply_is_last_data, reply_data)) => {
            if reply_locator_id != locator_id {
                return Err(prot_err("lob::fetch_a_lob_chunk(): locator ids do not match"));
            }
            (reply_data, reply_is_last_data)
        }
        _ => return Err(prot_err("No ReadLobReply part found")),
    };

    let server_processing_time = match reply.parts.pop_arg_if_kind(PartKind::StatementContext) {
        Some(Argument::StatementContext(stmt_ctx)) => {
            if let Some(OptionValue::INT(i)) = stmt_ctx.server_processing_time {
                i
            } else {
                0
            }
        }
        None => 0,
        _ => return Err(prot_err("Inconsistent StatementContext part found for ResultSet")),
    };
    Ok((reply_data, reply_is_last_data, server_processing_time))
}
