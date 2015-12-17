//! Since there is obviously no usecase for multiple segments in one request, we model message and segment together.
//! Instead we differentiate explicitly between request messages and reply messages.

use super::{PrtError,PrtResult,prot_err};
use super::conn_core::ConnRef;
use super::argument::Argument;
use super::function_code::FunctionCode;
use super::message_type::MessageType;
use super::part::Part;
use super::partkind::PartKind;
use super::parts::resultset::ResultSet;
use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use chrono::Local;
use std::io::{self,BufRead};

const BUFFER_SIZE: u32 = 130000;
const MESSAGE_HEADER_SIZE: u32 = 32;
const SEGMENT_HEADER_SIZE: usize = 24; // same for in and out

#[derive(Debug)]
pub enum Message {
    Request(Request),
    Reply(Reply),
}

// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Request {
    pub session_id: i64,
    pub msg_type: MessageType,
    pub auto_commit: bool,
    pub command_options: u8,
    pub parts: Vec<Part>,
}
impl Request {
    pub fn new(session_id: i64, msg_type: MessageType, auto_commit: bool, command_options: u8) -> Request  {
        Request {
            session_id: session_id,
            msg_type: msg_type,
            auto_commit: auto_commit,
            command_options: command_options,
            parts: Vec::<Part>::new(),
        }
    }

    pub fn send_and_receive(self,
                            o_rs: &mut Option<&mut ResultSet>,
                            conn_ref: &ConnRef,
                            expected_fc: Option<FunctionCode>)
    -> PrtResult<Reply> {
        trace!("Request::send_and_receive()");
        let start = Local::now();
        try!(self.serialize(conn_ref));
        let delta1 = (Local::now() - start).num_milliseconds();

        let mut response = try!(Reply::parse(o_rs, conn_ref));
        try!(response.assert_no_error());
        try!(response.assert_expected_fc(expected_fc));
        let delta2 = (Local::now() - start).num_milliseconds();
        debug!("Request::send_and_receive() took {total} ms (send: {send} ms)", send=delta1, total=delta2);

        Ok(response)
    }


    fn serialize(&self, conn_ref: &ConnRef) -> PrtResult<()> {
        trace!("Entering Message::serialize()");

        let varpart_size = try!(self.varpart_size());
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing Message with total size {}", total_size);
        let mut remaining_bufsize = BUFFER_SIZE - MESSAGE_HEADER_SIZE;

        let conn_ref_local = conn_ref.clone();
        let mut cs = conn_ref_local.borrow_mut();
        let session_id = cs.session_id;
        let seq_number = cs.next_seq_number();
        let w = &mut io::BufWriter::with_capacity(total_size as usize, &mut cs.stream);

        // MESSAGE HEADER
        try!(w.write_i64::<LittleEndian>(session_id));                  // I8
        try!(w.write_i32::<LittleEndian>(seq_number));                  // I4
        try!(w.write_u32::<LittleEndian>(varpart_size));                // UI4
        try!(w.write_u32::<LittleEndian>(remaining_bufsize));           // UI4
        try!(w.write_i16::<LittleEndian>(1));                           // I2    Number of segments
        for _ in 0..10 { try!(w.write_u8(0)); }                         // I1+ B[9]  unused

        // SEGMENT HEADER
        let size = try!(self.seg_size()) as i32;
        try!(w.write_i32::<LittleEndian>(size));                        // I4    Length including the header
        try!(w.write_i32::<LittleEndian>(0));                           // I4    Offset within the message buffer
        try!(w.write_i16::<LittleEndian>(self.parts.len() as i16));     // I2    Number of contained parts
        try!(w.write_i16::<LittleEndian>(1));                           // I2    Number of this segment, starting with 1
        try!(w.write_i8(1));                                            // I1    Segment kind: always Reply
        try!(w.write_i8(self.msg_type.to_i8()));                        // I1    Message type
        try!(w.write_i8(match self.auto_commit {true => 1, _ => 0}));   // I1    auto_commit on/off
        try!(w.write_u8(self.command_options));                         // I1    Bit set for options
        for _ in 0..8 { try!(w.write_u8(0)); }                          // [B;8] Reserved, do not use

        remaining_bufsize -= SEGMENT_HEADER_SIZE as u32;
        // PARTS
        for ref part in &self.parts {
            remaining_bufsize = try!(part.serialize(remaining_bufsize, w));
        }
        Ok(())
    }

    pub fn push(&mut self, part: Part){
        self.parts.push(part);
    }

    /// Length in bytes of the variable part of the message, i.e. total message without the header
    fn varpart_size(&self) -> PrtResult<u32> {
        let mut len = 0_u32;
        len += try!(self.seg_size()) as u32;
        trace!("varpart_size = {}",len);
        Ok(len)
    }

    fn seg_size(&self) -> PrtResult<usize> {
        let mut len = SEGMENT_HEADER_SIZE;
        for part in &self.parts {
            len += try!(part.size(true));
        }
        Ok(len)
    }
}

#[derive(Debug)]
pub struct Reply {
    pub session_id: i64,
    pub function_code: FunctionCode,
    pub parts: Vec<Part>,
}
impl Reply {
    fn new(session_id: i64, function_code: FunctionCode) -> Reply {
        Reply {
            session_id: session_id,
            function_code: function_code,
            parts: Vec::<Part>::new(),
        }
    }

    ///
    fn parse(o_rs: &mut Option<&mut ResultSet>, conn_ref: &ConnRef) -> PrtResult<Reply> {
        trace!("Reply::parse()");
        let stream = &mut (conn_ref.borrow_mut().stream);
        let mut rdr = io::BufReader::new(stream);

        let (no_of_parts, msg) = try!(parse_message_and_sequence_header(&mut rdr));
        match msg {
            Message::Request(_) => Err(prot_err("Reply::parse() found Request")),
            Message::Reply(mut msg) => {
                for _ in 0..no_of_parts {
                    let part = try!(Part::parse(&mut (msg.parts), Some(conn_ref), o_rs, &mut rdr));
                    msg.push(part);
                }
                Ok(msg)
            }
        }
    }

    fn assert_expected_fc(&self, expected_fc: Option<FunctionCode>) -> PrtResult<()> {
        match expected_fc {
            None => Ok(()),     // we had no clear expectation
            Some(fc) => {
                if self.function_code.to_i16() == fc.to_i16() {
                    Ok(())      // we got what we expected
                } else {
                    Err(PrtError::ProtocolError(format!("unexpected function code {:?}", self.function_code)))
                }
            },
        }
    }

    fn assert_no_error(&mut self) -> PrtResult<()> {
        if let Some(idx) = util::get_first_part_of_kind(PartKind::Error, &self.parts) {
            let errpart = self.parts.remove(idx);
            let vec = match errpart.arg {
                Argument::Error(v) => v,
                _ => return Err(prot_err("Inconsistent error part found")),
            };
            let err = PrtError::DbMessage(vec);
            warn!("{}",err);
            return Err(err);
        }
        Ok(())
    }

    pub fn push(&mut self, part: Part){
        self.parts.push(part);
    }
}

pub fn parse_message_and_sequence_header(rdr: &mut BufRead) -> PrtResult<(i16,Message)> {
    // MESSAGE HEADER: 32 bytes
    let session_id: i64 = try!(rdr.read_i64::<LittleEndian>());                             // I8
    let packet_seq_number: i32 = try!(rdr.read_i32::<LittleEndian>());                      // I4
    let varpart_size: u32 = try!(rdr.read_u32::<LittleEndian>());                           // UI4  not needed?
    let remaining_bufsize: u32 = try!(rdr.read_u32::<LittleEndian>());                      // UI4  not needed?
    let no_of_segs = try!(rdr.read_i16::<LittleEndian>());                                  // I2
    assert!(no_of_segs == 1);

    rdr.consume(10usize);                                                                   // (I1 + B[9])

    // SEGMENT HEADER: 24 bytes
    try!(rdr.read_i32::<LittleEndian>());                                                   // I4 seg_size
    try!(rdr.read_i32::<LittleEndian>());                                                   // I4 seg_offset
    let no_of_parts: i16 = try!(rdr.read_i16::<LittleEndian>());                                 // I2
    try!(rdr.read_i16::<LittleEndian>());                                                   // I2 seg_number
    let seg_kind = try!(Kind::from_i8(try!(rdr.read_i8())));                                // I1

    trace!("message and segment header: \
            {{ packet_seq_number = {}, varpart_size = {}, remaining_bufsize = {}, no_of_parts = {} }}",
            packet_seq_number, varpart_size, remaining_bufsize, no_of_parts);

    match seg_kind {
        Kind::Request => {
            let msg_type = try!(MessageType::from_i8(try!(rdr.read_i8())));                 // I1
            let commit = try!(rdr.read_i8()) != 0_i8;                                       // I1
            let command_options = try!(rdr.read_u8());                                      // I1 command_options
            rdr.consume(8_usize);                                                           // B[8] reserved1
            Ok((no_of_parts, Message::Request(Request::new(session_id, msg_type, commit, command_options))))
        },
        Kind::Reply | Kind::Error => {
            rdr.consume(1_usize);                                                           // I1 reserved2
            let fc = try!(FunctionCode::from_i16(try!(rdr.read_i16::<LittleEndian>())));    // I2
            rdr.consume(8_usize);                                                           // B[8] reserved3
            Ok((no_of_parts, Message::Reply(Reply::new(session_id, fc))))
        },
    }
}

/// Specifies the layout of the remaining segment header structure
#[derive(Debug)]
enum Kind {
    Request,
    Reply,
    Error,    // sp1sk_proccall, sp1sk_procreply ,sp1sk_last_segment_kind see api/Communication/Protocol/Layout.hpp
}
impl Kind {
    fn from_i8(val: i8) -> PrtResult<Kind> {match val {
        1 => Ok(Kind::Request),
        2 => Ok(Kind::Reply),
        5 => Ok(Kind::Error),
        _ => Err(prot_err(&format!("Invalid value for message::Kind::from_i8() detected: {}",val))),
    }}
}
