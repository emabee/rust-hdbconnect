use super::argument;
use super::partkind::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::max;
use std::io::{BufRead,Result,Write};


const PART_HEADER_SIZE: usize = 16;

pub fn new(kind: PartKind) -> Part {
    Part{
        kind: kind,
        attributes: 0,
        arg: argument::Argument::Nil,
    }
}

#[derive(Debug)]
pub struct Part {
    pub kind: PartKind,
    pub attributes: i8,
    pub arg: argument::Argument,      // a.k.a. part data, or part buffer :-(
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

        remaining_bufsize -= PART_HEADER_SIZE as u32;

        remaining_bufsize = try!(self.arg.encode(remaining_bufsize, w));
        Ok(remaining_bufsize)
    }

    pub fn size(&self, with_padding: bool) -> usize {
        let result = PART_HEADER_SIZE + self.arg.size(with_padding);
        trace!("Part_size = {}",result);
        result
    }

    pub fn set_arg(&mut self, arg: argument::Argument) {
        self.arg = arg;
    }
}

///
pub fn parse(rdr: &mut BufRead) -> Result<Part> {
    trace!("Entering parse()");

    // PART HEADER: 16 bytes
    let part_kind = try!(PartKind::from_i8(try!(rdr.read_i8())));       // I1
    let part_attributes = try!(rdr.read_i8());                          // I1
    let no_of_argsi16 = try!(rdr.read_i16::<LittleEndian>());           // I2
    let no_of_argsi32 =  try!(rdr.read_i32::<LittleEndian>());          // I4
    let arg_size = try!(rdr.read_i32::<LittleEndian>());                // I4
    let remaining_packet_size = try!(rdr.read_i32::<LittleEndian>());   // I4

    debug!("parse() found part with attributes {:o} of arg_size {} and remaining_packet_size {}",
            part_attributes, arg_size, remaining_packet_size);

    let mut part = new(part_kind);

    part.arg = try!(argument::parse( max(no_of_argsi16 as i32, no_of_argsi32), arg_size, part.kind, rdr));
    trace!("Got arg of kind {:?}", part.arg);
    Ok(part)
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
