use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{BufRead,Write};

#[derive(Debug)]
pub struct AuthField {
    pub v: Vec<u8>,
}
impl AuthField {
    pub fn encode (&self, w: &mut Write)  -> IoResult<()> {
        match self.v.len() {
            l if l <= 250usize => {
                try!(w.write_u8(l as u8));                              // B1           LENGTH OF VALUE
            },
            l if l <= 65535usize => {
                try!(w.write_u8(255));                                  // B1           247
                try!(w.write_u16::<LittleEndian>(l as u16));            // U2           LENGTH OF VALUE
            },
            l => {
                panic!("Value of AuthField is too big: {}",l);
            },
        }
        util::encode_bytes(&self.v, w)                                  // B variable   VALUE BYTES
    }

    pub fn size(&self) -> usize {
        1 + self.v.len()
    }

    pub fn parse(rdr: &mut BufRead) -> IoResult<AuthField> {
        let mut len = try!(rdr.read_u8())  as usize;                    // B1
        match len {
            255usize => {
                len = try!(rdr.read_u16::<LittleEndian>()) as usize;    // (B1+)I2
            },
            251...255 => {
                panic!("Unknown length indicator for AuthField: {}",len);
            },
            _ => {},
        }
        Ok(AuthField{v: try!(util::parse_bytes(len,rdr))})
    }
}
