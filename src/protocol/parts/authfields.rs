use crate::{
    protocol::util, LENGTH_INDICATOR_2BYTE, LENGTH_INDICATOR_4BYTE, LENGTH_INDICATOR_NULL,
    MAX_1_BYTE_LENGTH,
};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug, Default)]
pub(crate) struct AuthFields(Vec<AuthField>);
impl AuthFields {
    pub fn with_capacity(count: usize) -> Self {
        Self(Vec::<AuthField>::with_capacity(count))
    }
    pub fn parse(rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let field_count = rdr.read_u16::<LittleEndian>()? as usize; // I2
        let mut auth_fields: Self = Self(Vec::<AuthField>::with_capacity(field_count));
        for _ in 0..field_count {
            auth_fields.0.push(AuthField::parse(rdr)?);
        }
        Ok(auth_fields)
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        self.0.pop().map(AuthField::data)
    }

    pub fn size(&self) -> usize {
        let mut size = 2;
        for af in &self.0 {
            size += af.size();
        }
        size
    }

    pub fn emit(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_possible_wrap)]
        w.write_i16::<LittleEndian>(self.0.len() as i16)?;
        for field in &self.0 {
            field.emit(w)?;
        }
        Ok(())
    }

    pub fn push(&mut self, vec: Vec<u8>) {
        self.0.push(AuthField::new(vec));
    }
    pub fn push_string(&mut self, s: &str) {
        self.0.push(AuthField::new(s.as_bytes().to_vec()));
    }
}

#[derive(Debug)]
struct AuthField(Vec<u8>);
impl AuthField {
    fn new(vec: Vec<u8>) -> Self {
        Self(vec)
    }

    fn data(self) -> Vec<u8> {
        self.0
    }

    #[allow(clippy::cast_possible_truncation)]
    fn emit(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        match self.0.len() {
            l if l <= MAX_1_BYTE_LENGTH as usize => w.write_u8(l as u8)?, // B1: length of value
            l if l <= 0xFFFF => {
                w.write_u8(LENGTH_INDICATOR_2BYTE)?; // B1: 246
                w.write_u16::<LittleEndian>(l as u16)?; // U2: length of value
            }
            l if l <= 0xFFFFFFFF => {
                w.write_u8(LENGTH_INDICATOR_4BYTE)?; // B1: 247
                w.write_u32::<LittleEndian>(l as u32)?; // U4: length of value
            }
            l => {
                return Err(util::io_error(format!(
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

    fn parse(rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let len = Self::parse_length_of_bytes(rdr.read_u8()?, rdr)?;
        Ok(Self(util::parse_bytes(len, rdr)?))
    }

    fn parse_length_of_bytes(l8: u8, rdr: &mut dyn std::io::Read) -> std::io::Result<usize> {
        match l8 {
            0..=MAX_1_BYTE_LENGTH => Ok(l8 as usize),
            LENGTH_INDICATOR_2BYTE => Ok(rdr.read_u16::<LittleEndian>()? as usize), // U2
            LENGTH_INDICATOR_4BYTE => Ok(rdr.read_u32::<LittleEndian>()? as usize), // U4
            LENGTH_INDICATOR_NULL => Ok(rdr.read_u16::<BigEndian>()? as usize),     // U2 BigEndian
            _ => Err(util::io_error(format!(
                "Unknown length indicator for AuthField: {}",
                l8
            ))),
        }
    }
}
