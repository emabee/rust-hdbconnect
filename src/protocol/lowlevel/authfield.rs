use super::bufread::*;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{Read,Write};
use std::net::TcpStream;

#[derive(Debug)]
pub struct AuthField {
    pub v: Vec<u8>,
}
impl AuthField {
    pub fn encode (&self, w: &mut Write)  -> IoResult<()> {
        try!(w.write_u8(self.v.len() as u8));                   // B1           LENGTH OF VALUE
        for b in &self.v {try!(w.write_u8(*b));}                // B variable   VALUE
        Ok(())
    }

    pub fn size(&self) -> usize {
        1 + self.v.len()
    }

    pub fn try_to_parse(rdr: &mut BufReader<&mut TcpStream>) -> IoResult<AuthField> {
        let len = try!(rdr.read_i16::<LittleEndian>());         // I2
        let mut vec = Vec::<u8>::with_capacity(len as usize);
        try!(rdr.read(&mut vec));                               // variable
        Ok(AuthField{v: vec})
    }
}
