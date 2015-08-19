use super::segment::*;

use byteorder::{LittleEndian, WriteBytesExt};
use std::io::{Result,Write};

const BUFFER_SIZE: u32 = 130000;
const MESSAGE_HEADER_SIZE: u32 = 32;

// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Message {
    session_id: i64,
    packet_seq_number: i32,
    segments: Vec<Segment>,
}

/// Serialize to byte stream
impl Message {
    pub fn encode(&self, w: &mut Write) -> Result<()> {
        let remaining_bufsize = BUFFER_SIZE - MESSAGE_HEADER_SIZE;

        // MESSAGE HEADER
        try!(w.write_i64::<LittleEndian>(self.session_id));              // I8
        try!(w.write_i32::<LittleEndian>(self.packet_seq_number));       // I4
        try!(w.write_u32::<LittleEndian>(self.varpart_size()));          // UI4
        try!(w.write_u32::<LittleEndian>(remaining_bufsize));            // UI4
        try!(w.write_i16::<LittleEndian>(self.segments.len() as i16));   // I2
        try!(w.write_i8(0));                                             // I1    unused
        for _ in 0..9 { try!(w.write_u8(0)); }                           // B[9]  unused

        // SEGMENTS
        let mut osr = (0u32, 1i16, remaining_bufsize); // offset, segment_no, remaining_bufsize
        for ref segment in &self.segments {
            osr = try!(segment.encode(osr.0, osr.1, osr.2, w));
        }

        Ok(())
    }

    pub fn new(session_id: i64, packet_seq_number: i32) -> Message {
        Message {
            session_id: session_id,
            packet_seq_number: packet_seq_number,
            segments: Vec::<Segment>::new(),
        }
    }

    pub fn push(&mut self, segment: Segment){
        self.segments.push(segment);
    }

    /// Length in bytes of the variable part of the message, i.e. total message without the header
    fn varpart_size(&self) -> u32 {
        let mut len = 0;
        for seg in &self.segments {
            len += seg.size() as u32;
        }
        trace!("varpart_size = {}",len);
        len
    }
}
