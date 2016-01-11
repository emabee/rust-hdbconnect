use super::{PrtResult,prot_err,util};
use super::resultset::RsRef;
use super::super::argument::Argument;
use super::super::message::Request;
use super::super::reply_type::ReplyType;
use super::super::request_type::RequestType;
use super::super::part::Part;
use super::super::partkind::PartKind;

use byteorder::{LittleEndian,ReadBytesExt};
use std::borrow::Cow;
use std::cmp;
use std::io::{self};

#[derive(Clone,Debug)]
pub enum BLOB {
    FromDB(BlobFromDB),
    ToDB(BlobToDB),
}

/// Is used for BLOBS that we receive from the DB.
/// The data are often not transferred completely, so we support fetching remaining data on demand.
#[derive(Clone,Debug)]
pub struct BlobFromDB {
    rs_ref: Option<RsRef>,
    is_data_complete: bool,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
}
impl BlobFromDB {
    pub fn new(rs_ref: &RsRef, is_data_complete: bool, length_b: u64, locator_id: u64, data: Vec<u8>) -> BlobFromDB {
        trace!(
            "Instantiate BlobFromDB with length_b = {}, is_data_complete = {}, data.length() = {}",
            length_b, is_data_complete, data.len()
        );
        BlobFromDB {
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
        trace!("After BlobFromDB fetch: is_data_complete = {}, data.length() = {}", self.is_data_complete, self.length_b);
        Ok(())
    }

    pub fn into_bytes(mut self) -> PrtResult<Vec<u8>> {
        trace!("BlobFromDB::into_bytes()");
        while !self.is_data_complete {
            try!(self.fetch_next_chunk());
        }
        Ok(self.data)
    }
}

// Is used for writing BLOBS to requests
#[derive(Clone,Debug)]
pub struct BlobToDB {
    is_complete: bool,
    length: u32,
    position: u32,
    data: Vec<u8>,
}

// only for read-wire
// parse from request (for read-wire)
pub fn parse_blob_from_request(rdr: &mut io::BufRead) -> PrtResult<BLOB> {
    let options = try!(rdr.read_u8());                                      // I1
    let length = try!(rdr.read_u32::<LittleEndian>());                      // I4
    let position = try!(rdr.read_u32::<LittleEndian>());                    // I4
    Ok(BLOB::ToDB(BlobToDB {
        is_complete: (options & 0b_100_u8) != 0,
        length: length,
        position: position,
        data: Vec::<u8>::new()
    }))
}

#[derive(Clone,Debug)]
pub enum CLOB {
    FromDB(ClobFromDB),
    ToDB(ClobToDB),
}

#[derive(Clone,Debug)]
pub struct ClobFromDB {
    rs_ref: Option<RsRef>,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    char_count: u64,
    locator_id: u64,
    data: String,
}
impl ClobFromDB {
    pub fn new(rs_ref: &RsRef, is_data_complete: bool, length_c: u64, length_b: u64, char_count: u64, locator_id: u64, data: String)
    -> ClobFromDB {
        trace!(
            "ClobFromDB::new() with length_c = {}, length_b = {}, is_data_complete = {}, data.len() = {}",
            length_c, length_b, is_data_complete, data.len()
        );
        if (length_c < 1000) && (length_c != length_b) {
            trace!("=============================\n{}\n===========================",data);
        }
        ClobFromDB {
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
        trace!("After ClobFromDB fetch: is_data_complete = {}, data.length() = {}", self.is_data_complete, self.length_b);
        Ok(())
    }

    fn load_complete(&mut self) -> PrtResult<()> {
        trace!("ClobFromDB::load_complete() while is_data_complete = {}", self.is_data_complete);
        while !self.is_data_complete {
            try!(self.fetch_next_chunk());
        }
        Ok(())
    }

    // FIXME implement sth like into_character_stream() with deferred chunk fetching
    pub fn into_string(mut self) -> PrtResult<String> {
        trace!("ClobFromDB::into_string()");
        try!(self.load_complete());
        Ok(self.data)
    }
}

// Is used for writing BLOBS to requests
#[derive(Clone,Debug)]
pub struct ClobToDB {
    is_complete: bool,
    length: u32,
    position: u32,
    data: String,
}

// only for read-wire
// parse from request (for read-wire)
pub fn parse_clob_from_request(rdr: &mut io::BufRead) -> PrtResult<CLOB> {
    let options = try!(rdr.read_u8());                                      // I1
    let length = try!(rdr.read_u32::<LittleEndian>());                      // I4
    let position = try!(rdr.read_u32::<LittleEndian>());                    // I4
    Ok(CLOB::ToDB(ClobToDB {
        is_complete: (options & 0b_100_u8) != 0,
        length: length,
        position: position,
        data: String::new()
    }))
}


// ===
// regular parse
pub fn parse_blob_from_reply(rs_ref: &RsRef, rdr: &mut io::BufRead) -> PrtResult<BLOB> {
    match try!(parse_nullable_blob_from_reply(rs_ref, rdr)) {
        Some(blob) => Ok(blob),
        None => Err(prot_err("Null value found for non-null blob column"))
    }
}
pub fn parse_nullable_blob_from_reply(rs_ref: &RsRef, rdr: &mut io::BufRead) -> PrtResult<Option<BLOB>> {
    let (is_null, is_last_data) = try!(parse_lob_1(rdr));
    match is_null {
        true => { return Ok(None); },
        false => {
            let (_, length_b, locator_id, data) = try!(parse_lob_2(rdr));
            Ok(Some(BLOB::FromDB(BlobFromDB::new(rs_ref, is_last_data, length_b, locator_id, data))))
        }
    }
}

pub fn parse_clob_from_reply(rs_ref: &RsRef, rdr: &mut io::BufRead) -> PrtResult<CLOB> {
    match try!(parse_nullable_clob_from_reply(rs_ref, rdr)) {
        Some(clob) => Ok(clob),
        None => Err(prot_err("Null value found for non-null clob column"))
    }
}
pub fn parse_nullable_clob_from_reply(rs_ref: &RsRef, rdr: &mut io::BufRead) -> PrtResult<Option<CLOB>> {
    let (is_null, is_last_data) = try!(parse_lob_1(rdr));
    match is_null {
        true => { return Ok(None); },
        false => {
            let (length_c, length_b, locator_id, data) = try!(parse_lob_2(rdr));
            let (s,char_count) = try!(util::from_cesu8_with_count(&data));
            let s = match s {
                Cow::Owned(s) => s,
                Cow::Borrowed(s) => String::from(s)
            };
            assert_eq!(data.len(), s.len());
            trace!("parse_nullable_clob(): s: =============================\n{}\n===========================", s);
            Ok(Some(CLOB::FromDB(ClobFromDB::new(rs_ref, is_last_data, length_c, length_b, char_count, locator_id, s))))
        }
    }
}

fn parse_lob_1(rdr: &mut io::BufRead) -> PrtResult<(bool, bool)> {
    rdr.consume(1);    //let data_type = try!(rdr.read_u8());               // I1  "type of data": unclear
    let options = try!(rdr.read_u8());                                      // I1
    let is_null = (options & 0b_1_u8) != 0;
    // let is_data_included = (options & 0b_10_u8) != 0;
    let is_last_data = (options & 0b_100_u8) != 0;
    Ok((is_null, is_last_data))
}
fn parse_lob_2(rdr: &mut io::BufRead) -> PrtResult<(u64,u64,u64,Vec<u8>)> {
    rdr.consume(2);                                                         // U2 (filler)
    let length_c = try!(rdr.read_u64::<LittleEndian>());                    // I8
    let length_b = try!(rdr.read_u64::<LittleEndian>());                    // I8
    let locator_id = try!(rdr.read_u64::<LittleEndian>());                  // I8
    let chunk_length = try!(rdr.read_i32::<LittleEndian>());                // I4
    let data = try!(util::parse_bytes(chunk_length as usize,rdr));          // B[chunk_length]
    trace!("Got LOB locator {}", locator_id);
    Ok((length_c, length_b, locator_id, data))
}


// write to request


//
fn fetch_a_lob_chunk(rs_ref: &Option<RsRef>, locator_id: u64, length_b: u64, data_len: u64 )
-> PrtResult<(Vec<u8>,bool)> {
    // build the request, provide StatementContext and length_to_read
    let (conn_ref, length_to_read) = { // scope the borrow
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
                (conn_ref, length_to_read as i32)
            },
        }
    };
    let mut request = try!(Request::new(&conn_ref, RequestType::ReadLob, true, 0));

    let offset = data_len + 1;
    request.push(Part::new(PartKind::ReadLobRequest, Argument::ReadLobRequest(locator_id, offset, length_to_read)));

    let mut reply = try!(request.send_and_receive(&conn_ref, Some(ReplyType::ReadLob)));

    let part = try!(reply.retrieve_first_part_of_kind(PartKind::ReadLobReply));
    if let Argument::ReadLobReply(reply_locator_id, reply_is_last_data, reply_data) = part.arg {
        if reply_locator_id != locator_id {
            return Err(prot_err("lob::fetch_a_lob_chunk(): locator ids do not match"));
        }
        Ok((reply_data, reply_is_last_data))
    } else { Err(prot_err("wrong Argument variant")) }
}
