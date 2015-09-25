use super::argument;
use super::partkind::*;
use super::segment;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::{BufRead,BufReader,Error,ErrorKind,Result,Write};
use std::net::TcpStream;


// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Message {
    pub session_id: i64,
    pub packet_seq_number: i32,
    pub segments: Vec<segment::Segment>,
}

pub fn new(session_id: i64, packet_seq_number: i32) -> Message {
    Message {
        session_id: session_id,
        packet_seq_number: packet_seq_number,
        segments: Vec::<segment::Segment>::new(),
    }
}


/// Serialize to byte stream
impl Message {
    pub fn send_and_receive(&mut self, stream: &mut TcpStream) -> Result<Message> {
        trace!("Entering DbStream::send_and_receive()");

        try!(self.serialize(stream));
        debug!("send_and_receive: request data successfully sent");

        let mut rdr = BufReader::new(stream);
        let msg = try!(parse(&mut rdr));
        try!(msg.assert_no_error());
        Ok(msg)
    }


    fn serialize(&self, w: &mut Write) -> Result<()> {
        const BUFFER_SIZE: u32 = 130000;
        const MESSAGE_HEADER_SIZE: u32 = 32;

        trace!("Entering Message::serialize()");

        let varpart_size = self.varpart_size();
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing Message with total size {}", total_size);
        let remaining_bufsize = BUFFER_SIZE - MESSAGE_HEADER_SIZE;

        // MESSAGE HEADER
        try!(w.write_i64::<LittleEndian>(self.session_id));             // I8
        try!(w.write_i32::<LittleEndian>(self.packet_seq_number));      // I4
        try!(w.write_u32::<LittleEndian>(varpart_size));                // UI4
        try!(w.write_u32::<LittleEndian>(remaining_bufsize));           // UI4
        try!(w.write_i16::<LittleEndian>(self.segments.len() as i16));  // I2
        try!(w.write_i8(0));                                            // I1    unused
        for _ in 0..9 { try!(w.write_u8(0)); }                          // B[9]  unused

        // SEGMENTS
        let mut osr = (0i32, 1i16, remaining_bufsize); // offset, segment_no, remaining_bufsize
        for ref segment in &self.segments {
            osr = try!(segment.encode(osr.0, osr.1, osr.2, w));
        }

        w.flush()
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

    fn assert_no_error(&self) -> Result<()> {
        for seg in &self.segments {
            for part in &seg.parts {
                match part.kind {
                    PartKind::Error => {
                        if let argument::Argument::Error(ref vec) = part.arg {
                            let mut s = String::new();
                            for hdberr in vec { s = format!("{} {:?}", s, hdberr); }   // FIXME improve formatting for multiple errors
                            return Err(Error::new(ErrorKind::Other, s));
                        }
                    },
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

///
fn parse(rdr: &mut BufRead) -> Result<Message> {
    trace!("Entering parse()");

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

    let mut msg = new(session_id,packet_seq_number);
    for _ in 0..no_of_segs { msg.segments.push(try!(segment::parse(rdr))); }
    Ok(msg)
}



#[cfg(test)]
mod tests {
    use super::parse;
    use std::io::{BufReader,Cursor};

    // run exclusively with
    // cargo test protocol::lowlevel::message::tests::test_parse_from_bstring -- --nocapture
    #[test]
    fn test_parse_from_bstring() {
        use flexi_logger;
        flexi_logger::init( flexi_logger::LogConfig::new(), Some("info".to_string()))
        .unwrap();

        let bytes = b"\x5b\xd3\xf3\x17\x47\xa5\x04\x00\x02\x00\x00\x00\x58\x00\x00\x00\xb0\xfb\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x58\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x01\x02\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x03\x00\x01\x00\x00\x00\x00\x00\x30\x00\x00\x00\x98\xfb\x01\x00\x53\x45\x4c\x45\x43\x54\x20\x56\x45\x52\x53\x49\x4f\x4e\x2c\x20\x43\x55\x52\x52\x45\x4e\x54\x5f\x55\x53\x45\x52\x20\x46\x52\x4f\x4d\x20\x53\x59\x53\x2e\x4d\x5f\x44\x41\x54\x41\x42\x41\x53\x45";
        let mut reader = BufReader::new(Cursor::new(bytes.to_vec()));
        info!("Got {:?}", parse(&mut reader).unwrap());
    }
}