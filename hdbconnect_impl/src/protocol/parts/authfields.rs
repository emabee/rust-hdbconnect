use crate::{
    HdbResult,
    protocol::{parts::length_indicator, util_sync},
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug, Default)]
pub(crate) struct AuthFields(Vec<AuthField>);
impl AuthFields {
    pub fn with_capacity(count: usize) -> Self {
        Self(Vec::<AuthField>::with_capacity(count))
    }

    pub(crate) fn parse(rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let field_count = rdr.read_u16::<LittleEndian>()? as usize; // I2
        let mut auth_fields: Self = Self(Vec::<AuthField>::with_capacity(field_count));
        for _ in 0..field_count {
            auth_fields.0.push(AuthField::parse(rdr)?);
        }
        Ok(auth_fields)
    }

    pub(crate) fn pop(&mut self) -> Option<Vec<u8>> {
        self.0.pop().map(AuthField::data)
    }

    pub(crate) fn size(&self) -> usize {
        let mut size = 2;
        for af in &self.0 {
            size += af.size();
        }
        size
    }

    pub(crate) fn emit(&self, w: &mut dyn std::io::Write) -> HdbResult<()> {
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_possible_wrap)]
        w.write_i16::<LittleEndian>(self.0.len() as i16)?;
        for field in &self.0 {
            field.emit(w)?;
        }
        Ok(())
    }

    pub(crate) fn push(&mut self, vec: Vec<u8>) {
        self.0.push(AuthField::new(vec));
    }
    pub(crate) fn push_string(&mut self, s: &str) {
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
    fn emit(&self, w: &mut dyn std::io::Write) -> HdbResult<()> {
        length_indicator::emit(self.0.len(), w)?;
        w.write_all(&self.0)?; // B (varying) value
        Ok(())
    }

    fn size(&self) -> usize {
        1 + self.0.len()
    }

    fn parse(rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let len = length_indicator::parse(rdr.read_u8()?, rdr)?;
        Ok(Self(util_sync::parse_bytes(len, rdr)?))
    }
}
