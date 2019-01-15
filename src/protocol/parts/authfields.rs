use crate::protocol::util;
use crate::{HdbError, HdbResult};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

#[derive(Debug, Default)]
pub struct AuthFields(Vec<AuthField>);
impl AuthFields {
    pub fn with_capacity(count: usize) -> AuthFields {
        AuthFields(Vec::<AuthField>::with_capacity(count))
    }
    pub fn parse(rdr: &mut io::BufRead) -> HdbResult<AuthFields> {
        let field_count = rdr.read_i16::<LittleEndian>()? as usize; // I2
        let mut auth_fields: AuthFields = AuthFields(Vec::<AuthField>::with_capacity(field_count));
        for _ in 0..field_count {
            auth_fields.0.push(AuthField::parse(rdr)?)
        }
        Ok(auth_fields)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        self.0.pop().map(|af| af.data())
    }

    pub fn size(&self) -> usize {
        let mut size = 2;
        for af in &self.0 {
            size += af.size();
        }
        size
    }

    pub fn serialize(&self, w: &mut io::Write) -> HdbResult<()> {
        w.write_i16::<LittleEndian>(self.0.len() as i16)?;
        for field in &self.0 {
            field.serialize(w)?;
        }
        Ok(())
    }

    pub fn push(&mut self, vec: Vec<u8>) {
        self.0.push(AuthField::new(vec))
    }
}

#[derive(Debug)]
struct AuthField(Vec<u8>);
impl AuthField {
    fn new(vec: Vec<u8>) -> AuthField {
        AuthField(vec)
    }

    fn data(self) -> Vec<u8> {
        self.0.to_vec()
    }

    fn serialize(&self, w: &mut io::Write) -> HdbResult<()> {
        match self.0.len() {
            l if l <= 250_usize => w.write_u8(l as u8)?, // B1: length of value
            l if l <= 65_535_usize => {
                w.write_u8(255)?; // B1: 247
                w.write_u16::<LittleEndian>(l as u16)?; // U2: length of value
            }
            l => {
                return Err(HdbError::Impl(format!(
                    "Value of AuthField is too big: {}",
                    l
                )));
            }
        }
        w.write_all(&self.0)?; // B (varying) value
        Ok(())
    }

    fn size(&self) -> usize {
        1 + self.0.len()
    }

    fn parse(rdr: &mut io::BufRead) -> HdbResult<AuthField> {
        let mut len = rdr.read_u8()? as usize; // B1
        match len {
            255 => {
                len = rdr.read_u16::<LittleEndian>()? as usize; // (B1+)I2
            }
            251...254 => {
                return Err(HdbError::Impl(format!(
                    "Unknown length indicator for AuthField: {}",
                    len
                )));
            }
            _ => {}
        }
        Ok(AuthField(util::parse_bytes(len, rdr)?))
    }
}
