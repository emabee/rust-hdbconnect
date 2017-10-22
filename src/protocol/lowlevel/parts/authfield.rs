use super::{PrtError, PrtResult, util};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

#[derive(Debug)]
pub struct AuthField(pub Vec<u8>);

impl AuthField {
    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        match self.0.len() {
            l if l <= 250_usize => w.write_u8(l as u8)?,                 // B1: length of value
            l if l <= 65_535_usize => {
                w.write_u8(255)?; // B1: 247
                w.write_u16::<LittleEndian>(l as u16)?; // U2: length of value
            }
            l => {
                return Err(
                    PrtError::ProtocolError(format!("Value of AuthField is too big: {}", l)),
                );
            }
        }
        util::serialize_bytes(&self.0, w) // B (varying) value
    }

    pub fn size(&self) -> usize {
        1 + self.0.len()
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<AuthField> {
        let mut len = rdr.read_u8()? as usize; // B1
        match len {
            255_usize => {
                len = rdr.read_u16::<LittleEndian>()? as usize; // (B1+)I2
            }
            251...254 => {
                return Err(PrtError::ProtocolError(
                    format!("Unknown length indicator for AuthField: {}", len),
                ));
            }
            _ => {}
        }
        Ok(AuthField(util::parse_bytes(len, rdr)?))
    }
}
