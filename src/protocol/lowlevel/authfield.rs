use super::bufread::*;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{Read,Write};
use std::iter::repeat;
use std::net::TcpStream;

#[derive(Debug)]
pub struct AuthField {
    pub v: Vec<u8>,
}
impl AuthField {
    pub fn encode (&self, w: &mut Write)  -> IoResult<()> {
        let len = self.v.len();
        if len < 245 {
            try!(w.write_u8(len as u8));                        // B1           LENGTH OF VALUE
        } else if len < i16::max_value() as usize {
            try!(w.write_u8(246u8));                            // B1           246
            try!(w.write_i16::<LittleEndian>(len as i16));      // I2           LENGTH OF VALUE
        } else {
            try!(w.write_u8(247u8));                            // B1           247
            try!(w.write_i32::<LittleEndian>(len as i32));      // I4           LENGTH OF VALUE
        }
        for b in &self.v {try!(w.write_u8(*b));}                // B variable   VALUE
        Ok(())
    }

    pub fn size(&self) -> usize {
        1 + self.v.len()
    }

    pub fn try_to_parse(rdr: &mut BufReader<&mut TcpStream>) -> IoResult<AuthField> {
        trace!("Entering try_to_parse()");
        let mut len = try!(rdr.read_u8())  as usize;            // B1
        if len == 246 {
            len = try!(rdr.read_i16::<LittleEndian>()) as usize;// (B1+)I2
        } else if len == 247 {
            len = try!(rdr.read_i32::<LittleEndian>()) as usize;// (B1+)I4
        }

        let mut vec: Vec<u8> = repeat(0u8).take(len).collect();
        try!(rdr.read(&mut vec[..]));
        trace!("Leaving try_to_parse()");
        Ok(AuthField{v: vec})
    }
}
