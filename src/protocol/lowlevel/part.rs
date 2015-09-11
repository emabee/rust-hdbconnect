use super::argument;
use super::bufread::*;
use super::partkind::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::max;
use std::io::{Error,ErrorKind,Result,Write};
use std::net::TcpStream;


const PART_HEADER_SIZE: u32 = 16;

pub fn new(kind: PartKind) -> Part {
    Part{
        kind: kind,
        attributes: 0,
        arg: argument::Argument::Nil,
    }
}

#[derive(Debug)]
pub struct Part {
    kind: PartKind,
    attributes: i8,
    arg: argument::Argument,      // a.k.a. part data, or part buffer :-(
}

impl Part {
    /// Serialize to byte stream
    pub fn encode(&self, mut remaining_bufsize: u32, w: &mut Write) -> Result<u32> {
        // PART HEADER 16 bytes
        try!(w.write_i8(self.kind.to_i8()));                            // I1    Nature of part data
        try!(w.write_i8(self.attributes));                              // I1    Attributes of part
        try!(w.write_i16::<LittleEndian>(self.arg.count()));            // I2    Number of elements in arg
        try!(w.write_i32::<LittleEndian>(0));                           // I4    Number of elements in arg (where used)
        try!(w.write_i32::<LittleEndian>(self.arg.size(false) as i32)); // I4    Length of args in bytes
        try!(w.write_i32::<LittleEndian>(remaining_bufsize as i32));    // I4    Length in packet remaining without this part

        remaining_bufsize -= PART_HEADER_SIZE;

        remaining_bufsize = try!(self.arg.encode(remaining_bufsize, w));
        Ok(remaining_bufsize)
    }

    pub fn size(&self, with_padding: bool) -> u32 {
        let result = PART_HEADER_SIZE + self.arg.size(with_padding);
        trace!("Part_size = {}",result);
        result
    }

    pub fn set_arg(&mut self, arg: argument::Argument) {
        self.arg = arg;
    }
}

///
pub fn try_to_parse(rdr: &mut BufReader<&mut TcpStream>) -> Result<Part> {
    trace!("Entering try_to_parse()");

    loop {
        trace!("looping in try_to_parse()");
        match try_to_parse_header(rdr) {
            Ok(ParseResponse::PartHdr(mut part,no_of_args)) => {
                part.arg = try!(argument::try_to_parse(no_of_args, part.kind, rdr));
                return Ok(part);
            },
            Ok(ParseResponse::Incomplete) => {
                trace!("try_to_parse(): got Incomplete from try_to_parse_header()");
            },
            Err(e) => return Err(Error::from(e)),
        }
        match rdr.read_into_buf() {
            Ok(0) if rdr.get_buf().is_empty() => {
                return Err(Error::new(ErrorKind::Other, "Connection closed"));
            },
            Ok(0) => return Err(Error::new(ErrorKind::Other, "Response is bigger than expected")), // ???
            Ok(_) => (),
            Err(e) => return Err(Error::from(e))
        }
    }
}

///
fn try_to_parse_header(rdr: &mut BufReader<&mut TcpStream>) -> Result<ParseResponse> {
    trace!("Entering try_to_parse_header()");

    let l = rdr.get_buf().len();
    if  l >= (PART_HEADER_SIZE as usize) {
        // PART HEADER: 16 bytes
        let part_kind = try!(PartKind::from_i8(try!(rdr.read_i8())));       // I1
        let part_attributes = try!(rdr.read_i8());                          // I1
        let no_of_args = try!(rdr.read_i16::<LittleEndian>());              // I2
        let mut big_no_of_args =  try!(rdr.read_i32::<LittleEndian>());     // I4
        let part_size = try!(rdr.read_i32::<LittleEndian>());               // I4
        let remaining_packet_size = try!(rdr.read_i32::<LittleEndian>());   // I4

        debug!("try_to_parse_header() found part with attributes {:o} of size {} and remaining_packet_size {}",
                part_attributes, part_size, remaining_packet_size);

        big_no_of_args = max(no_of_args as i32, big_no_of_args);
        let part = new(part_kind);

        debug!("try_to_parse_header() returns Ok");
        Ok(ParseResponse::PartHdr(part, big_no_of_args))
    } else {
        trace!("try_to_parse_header() got only {} bytes", l);
        Ok(ParseResponse::Incomplete)
    }
}

enum ParseResponse {
    PartHdr(Part,i32),
    Incomplete,
}

// enum of bit positions
#[allow(dead_code)]
pub enum PartAttributes {
    LastPacket = 0,         // Last part in a sequence of parts (FETCH, array command EXECUTE)
    NextPacket = 1,         // Part in a sequence of parts
    FirstPacket = 2,        // First part in a sequence of parts
    RowNotFound = 3,        // Empty part, caused by “row not found” error
    ResultSetClosed = 4,    // The result set that produced this part is closed
}
