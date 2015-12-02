use super::{PrtError,PrtResult};
use super::conn_core::ConnRef;
use super::argument::Argument;
use super::partkind::PartKind;
use super::resultset::ResultSet;
use super::segment::Segment;
use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::{self,BufRead,Write};


// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Message {
    pub session_id: i64,
    pub segments: Vec<Segment>,
}

/// Serialize to byte stream
impl Message {
    pub fn new() -> Message {
        Message::new_parsed(0_i64)
    }

    fn new_parsed(session_id: i64) -> Message {
        Message {
            session_id: session_id,
            segments: Vec::<Segment>::new(),
        }
    }

    pub fn send_and_receive(&mut self, o_rs: &mut Option<&mut ResultSet>, conn_ref: &ConnRef)
    -> PrtResult<Message> {
        trace!("Entering send_and_receive()");
        try!(self.serialize(conn_ref));
        debug!("send_and_receive: request data successfully sent");
        let mut msg = try!(Message::parse(o_rs, conn_ref));
        try!(msg.assert_no_error());
        Ok(msg)
    }


    fn serialize(&self, conn_ref: &ConnRef) -> PrtResult<()> {
        const BUFFER_SIZE: u32 = 130000;
        const MESSAGE_HEADER_SIZE: u32 = 32;

        trace!("Entering Message::serialize()");

        let varpart_size = try!(self.varpart_size());
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing Message with total size {}", total_size);
        let remaining_bufsize = BUFFER_SIZE - MESSAGE_HEADER_SIZE;

        // MESSAGE HEADER
        let conn_ref_local = conn_ref.clone();
        let mut cs = conn_ref_local.borrow_mut();
        let session_id = cs.session_id;
        let seq_number = cs.next_seq_number();
        let w = &mut cs.stream;
        try!(w.write_i64::<LittleEndian>(session_id));                  // I8
        try!(w.write_i32::<LittleEndian>(seq_number));                  // I4
        try!(w.write_u32::<LittleEndian>(varpart_size));                // UI4
        try!(w.write_u32::<LittleEndian>(remaining_bufsize));           // UI4
        try!(w.write_i16::<LittleEndian>(self.segments.len() as i16));  // I2
        try!(w.write_i8(0));                                            // I1    unused
        for _ in 0..9 { try!(w.write_u8(0)); }                          // B[9]  unused

        // SEGMENTS
        let mut osr = (0i32, 1i16, remaining_bufsize); // offset, segment_no, remaining_bufsize
        for ref segment in &self.segments {
            osr = try!(segment.serialize(osr.0, osr.1, osr.2, w));
        }
        try!(w.flush());
        Ok(())
    }

    /// Length in bytes of the variable part of the message, i.e. total message without the header
    fn varpart_size(&self) -> PrtResult<u32> {
        let mut len = 0_u32;
        for seg in &self.segments {
            len += try!(seg.size()) as u32;
        }
        trace!("varpart_size = {}",len);
        Ok(len)
    }

    fn assert_no_error(&mut self) -> PrtResult<()> {
        assert!(self.segments.len() == 1, "Wrong count of segments");

        for ref mut seg in &mut self.segments {
            if let Some(idx) = util::get_first_part_of_kind(PartKind::Error, &seg.parts) {
                let errpart = seg.parts.remove(idx);
                let vec = match errpart.arg {
                    Argument::Error(v) => v,
                    _ => return Err(PrtError::ProtocolError("Inconsistent error part found".to_string())),
                };
                let err = PrtError::DbMessage(vec);
                warn!("{}",err);
                return Err(err);
            }
        }
        Ok(())
    }

    ///
    fn parse(o_rs: &mut Option<&mut ResultSet>, conn_ref: &ConnRef)
    -> PrtResult<Message> {
        trace!("Entering parse()");

        let stream = &mut (conn_ref.borrow_mut().stream);
        let mut rdr = io::BufReader::new(stream);
        // MESSAGE HEADER: 32 bytes
        let session_id = try!(rdr.read_i64::<LittleEndian>());          // I8
        let packet_seq_number = try!(rdr.read_i32::<LittleEndian>());   // I4
        let varpart_size = try!(rdr.read_u32::<LittleEndian>());        // UI4  not needed?
        let remaining_bufsize = try!(rdr.read_u32::<LittleEndian>());   // UI4  not needed?
        let no_of_segs = try!(rdr.read_i16::<LittleEndian>());          // I2
        rdr.consume(10usize);                                           // (I1 + B[9])
        debug!("message_header = {{ session_id = {}, packet_seq_number = {}, \
                varpart_size = {}, remaining_bufsize = {}, no_of_segs = {} }}",
                session_id, packet_seq_number, varpart_size, remaining_bufsize, no_of_segs);

        let mut msg = Message::new_parsed(session_id);
        for _ in 0..no_of_segs { msg.segments.push(try!(Segment::parse(conn_ref, o_rs, &mut rdr))); }
        Ok(msg)
    }
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
