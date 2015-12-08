//! Since there is obviously no usecase for multiple segments in one request, we model message and segment together.
//! Instead we differentiate explicitly between request messages and reply messages.

use super::{PrtError,PrtResult,prot_err};
use super::conn_core::ConnRef;
use super::argument::Argument;
use super::function_code::FunctionCode;
use super::message_type::MessageType;
use super::part::Part;
use super::partkind::PartKind;
use super::resultset::ResultSet;
use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use chrono::Local;
use std::io::{self,BufRead};

const BUFFER_SIZE: u32 = 130000;
const MESSAGE_HEADER_SIZE: u32 = 32;
const SEGMENT_HEADER_SIZE: usize = 24; // same for in and out


// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct RequestMessage {
    pub session_id: i64,
    pub msg_type: MessageType,
    pub auto_commit: bool,
    pub command_options: u8,
    pub parts: Vec<Part>,
}
impl RequestMessage {
    pub fn new(session_id: i64, msg_type: MessageType, auto_commit: bool, command_options: u8) -> RequestMessage  {
        RequestMessage {
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
    -> PrtResult<ReplyMessage> {
        trace!("RequestMessage::send_and_receive()");
        let start = Local::now();
        try!(self.serialize(conn_ref));
        let delta1 = (Local::now() - start).num_milliseconds();

        let mut response = try!(ReplyMessage::parse(o_rs, conn_ref));
        try!(response.assert_no_error());
        try!(response.assert_expected_fc(expected_fc));
        let delta2 = (Local::now() - start).num_milliseconds();
        debug!("RequestMessage::send_and_receive() took {total} ms (send: {send} ms)", send=delta1, total=delta2);

        Ok(response)
    }


    fn serialize(&self, conn_ref: &ConnRef) -> PrtResult<()> {
        trace!("Entering Message::serialize()");

        let varpart_size = try!(self.varpart_size());
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing Message with total size {}", total_size);
        let mut remaining_bufsize = BUFFER_SIZE - MESSAGE_HEADER_SIZE;

        // MESSAGE HEADER
        let conn_ref_local = conn_ref.clone();
        let mut cs = conn_ref_local.borrow_mut();
        let session_id = cs.session_id;
        let seq_number = cs.next_seq_number();
        let w = &mut io::BufWriter::new(&mut cs.stream);
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

///
pub struct ReplyMessage {
    pub session_id: i64,
    pub function_code: FunctionCode,
    pub parts: Vec<Part>,
}
impl ReplyMessage {
    fn new(session_id: i64, function_code: FunctionCode) -> ReplyMessage {
        ReplyMessage {
            session_id: session_id,
            function_code: function_code,
            parts: Vec::<Part>::new(),
        }
    }

    ///
    fn parse(o_rs: &mut Option<&mut ResultSet>, conn_ref: &ConnRef) -> PrtResult<ReplyMessage> {
        trace!("ReplyMessage::parse()");
        let stream = &mut (conn_ref.borrow_mut().stream);
        let mut rdr = io::BufReader::new(stream);

        // MESSAGE HEADER: 32 bytes
        let session_id = try!(rdr.read_i64::<LittleEndian>());          // I8
        let packet_seq_number = try!(rdr.read_i32::<LittleEndian>());   // I4
        let varpart_size = try!(rdr.read_u32::<LittleEndian>());        // UI4  not needed?
        let remaining_bufsize = try!(rdr.read_u32::<LittleEndian>());   // UI4  not needed?
        let no_of_segs = try!(rdr.read_i16::<LittleEndian>());          // I2
        assert!(no_of_segs == 1);

        rdr.consume(10usize);                                           // (I1 + B[9])

        // SEGMENT HEADER: 24 bytes
        try!(rdr.read_i32::<LittleEndian>());                                       // I4 seg_size (BigEndian??)
        try!(rdr.read_i32::<LittleEndian>());                                       // I4 seg_offset (BigEndian??)
        let no_of_parts = try!(rdr.read_i16::<LittleEndian>());                     // I2
        try!(rdr.read_i16::<LittleEndian>());                                       // I2 seg_number
        try!(Kind::from_i8(try!(rdr.read_i8())));                                   // I1
        // match seg_kind {
            // Kind::Request => {
            //     let mt = try!(MessageType::from_i8(try!(rdr.read_i8())));        // I1
            //     let commit = try!(rdr.read_i8()) != 0_i8;                        // I1
            //     let command_options = try!(rdr.read_u8());                       // I1 command_options
            //     rdr.consume(8_usize);                                            // B[8] reserved1
            //     new_request_seg(mt, commit, command_options)
            // },
            // Kind::Reply | Kind::Error => {
        rdr.consume(1_usize);                                                       // I1 reserved2
        let fc = try!(FunctionCode::from_i16(try!(rdr.read_i16::<LittleEndian>())));// I2
        rdr.consume(8_usize);                                                       // B[8] reserved3
        //     },
        // };
        trace!("message and segment header: \
                {{ session_id = {}, packet_seq_number = {}, varpart_size = {}, remaining_bufsize = {}, no_of_parts = {} }}",
                session_id, packet_seq_number, varpart_size, remaining_bufsize, no_of_parts);
        let mut msg = ReplyMessage::new(session_id, fc);

        for _ in 0..no_of_parts {
            let part = try!(Part::parse(&mut (msg.parts), conn_ref, o_rs, &mut rdr));
            msg.push(part);
        }
        Ok(msg)
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

/// Specifies the layout of the remaining segment header structure
#[derive(Debug)]
enum Kind {
    Reply,
    Error,    // sp1sk_proccall, sp1sk_procreply ,sp1sk_last_segment_kind see api/Communication/Protocol/Layout.hpp
}
impl Kind {
    fn from_i8(val: i8) -> PrtResult<Kind> {match val {
        2 => Ok(Kind::Reply),
        5 => Ok(Kind::Error),
        _ => Err(PrtError::ProtocolError(format!("Invalid value for segment::Kind::from_i8() detected: {}",val))),
    }}
}



//
//
// #[cfg(test)]
// mod tests {
//     use super::parse;
//     use std::io;
//
//     // run exclusively with
//     // cargo test protocol::lowlevel::message::tests::test_parse_from_bstring -- --nocapture
//     #[test]
//     fn test_parse_from_bstring() {
//         // use flexi_logger;
//         // flexi_logger::init( flexi_logger::LogConfig::new(), Some("info".to_string()))
//         // .unwrap();
//
//         let bytes = b"\x5b\xd3\xf3\x17\x47\xa5\x04\x00\x02\x00\x00\x00\x06\x01\x00\x00\x10\x75\x00\x00\x01\x00\x00\x00\x00\x3a\x9b\x6c\x01\x00\x00\x00\x08\x01\x00\x00\x00\x00\x00\x00\x04\x00\x01\x00\x02\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x30\x00\x02\x00\x00\x00\x00\x00\x51\x00\x00\x00\xe8\x74\x00\x00\x02\x09\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x0c\x00\x00\x00\x0c\x00\x00\x00\x01\x0b\x00\x00\x00\x01\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x14\x00\x00\x00\x0b\x4d\x5f\x44\x41\x54\x41\x42\x41\x53\x45\x5f\x07\x56\x45\x52\x53\x49\x4f\x4e\x0c\x43\x55\x52\x52\x45\x4e\x54\x5f\x55\x53\x45\x52\x00\x00\x00\x13\x1c\x01\x15\x0d\x00\x01\x00\x00\x00\x00\x00\x08\x00\x00\x00\x80\x74\x00\x00\x80\x57\x7f\x62\x47\xa5\x04\x00\x27\x00\x02\x00\x00\x00\x00\x00\x2a\x00\x00\x00\x68\x74\x00\x00\x01\x21\x1c\x00\x01\x00\x00\x00\x00\x00\xad\xde\x38\xe2\x49\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x02\x04\x5f\x02\x00\x00\x00\x00\x00\x00\xf0\x3f\x06\x1c\x01\x07\x05\x11\x01\x00\x00\x00\x00\x00\x1e\x00\x00\x00\x28\x74\x00\x00\x16\x31\x2e\x35\x30\x2e\x30\x30\x30\x2e\x30\x31\x2e\x31\x34\x33\x37\x35\x38\x30\x31\x33\x31\x06\x53\x59\x53\x54\x45\x4d";
//         let mut reader = io::BufReader::new(io::Cursor::new(bytes.to_vec()));
//         info!("Got {:?}", parse(&mut None, &mut reader).unwrap());
//     }
// }
