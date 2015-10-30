use super::argument;
use super::partkind::PartKind;
use super::part_attributes::PartAttributes;
use super::resultset::ResultSet;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::max;
use std::io::{BufRead,Result,Write};


const PART_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub struct Part {
    pub kind: PartKind,
    pub arg: argument::Argument,      // a.k.a. part data, or part buffer :-(
}

impl Part {
    pub fn new(kind: PartKind, arg: argument::Argument) -> Part {
        Part{
            kind: kind,
            arg: arg,
        }
    }

    /// Serialize to byte stream
    pub fn encode(&self, mut remaining_bufsize: u32, w: &mut Write) -> Result<u32> {
        // PART HEADER 16 bytes
        try!(w.write_i8(self.kind.to_i8()));                            // I1    Nature of part data
        try!(w.write_u8(0));                                            // U1    (documented as I1) Attributes of part
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
}

///
pub fn parse(already_received_parts: &mut Vec<Part>, o_rs: &mut Option<&mut ResultSet>, rdr: &mut BufRead) -> Result<Part> {
    trace!("Entering parse()");

    // PART HEADER: 16 bytes
    let kind = try!(PartKind::from_i8( try!(rdr.read_i8()) ));          // I1
    let attributes = PartAttributes::new( try!(rdr.read_u8()) );        // U1    (documented as I1)
    let no_of_argsi16 = try!(rdr.read_i16::<LittleEndian>());           // I2
    let no_of_argsi32 = try!(rdr.read_i32::<LittleEndian>());           // I4
    let arg_size = try!(rdr.read_i32::<LittleEndian>());                // I4
    try!(rdr.read_i32::<LittleEndian>());                               // I4    remaining_packet_size

    let no_of_args =  max(no_of_argsi16 as i32, no_of_argsi32);
    debug!("parse() found part of kind {:?} with attributes {:?}({:b}) and no_of_args {}", kind, attributes, attributes, no_of_args);

    Ok(Part::new(kind, try!(argument::parse(kind, attributes, no_of_args, arg_size, already_received_parts, o_rs, rdr)) ))
}
