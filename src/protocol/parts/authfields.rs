use crate::protocol::{parts::length_indicator, util, util_async, util_sync};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug, Default)]
pub(crate) struct AuthFields(Vec<AuthField>);
impl AuthFields {
    pub fn with_capacity(count: usize) -> Self {
        Self(Vec::<AuthField>::with_capacity(count))
    }
    pub fn parse_sync(rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let field_count = rdr.read_u16::<LittleEndian>()? as usize; // I2
        let mut auth_fields: Self = Self(Vec::<AuthField>::with_capacity(field_count));
        for _ in 0..field_count {
            auth_fields.0.push(AuthField::parse_sync(rdr)?)
        }
        Ok(auth_fields)
    }

    pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        rdr: &mut R,
    ) -> std::io::Result<Self> {
        let field_count = util_async::read_u16(rdr).await? as usize; // I2
        let mut auth_fields: Self = Self(Vec::<AuthField>::with_capacity(field_count));
        for _ in 0..field_count {
            auth_fields.0.push(AuthField::parse_async(rdr).await?)
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

    pub fn emit_sync(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_possible_wrap)]
        w.write_i16::<LittleEndian>(self.0.len() as i16)?;
        for field in &self.0 {
            field.emit_sync(w)?;
        }
        Ok(())
    }

    pub async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        w: &mut W,
    ) -> std::io::Result<()> {
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_possible_wrap)]
        w.write_all(&((self.0.len() as i16).to_le_bytes())).await?;
        // w.write_i16::<LittleEndian>(self.0.len() as i16)?;
        for field in &self.0 {
            field.emit_async(w).await?;
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
    fn emit_sync(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        length_indicator::emit_sync(self.0.len(), w)?;
        w.write_all(&self.0)?; // B (varying) value
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        w: &mut W,
    ) -> std::io::Result<()> {
        // FIXME Adapt to sync method
        match self.0.len() {
            l if l <= 250_usize => w.write_u8(l as u8).await?, // B1: length of value
            l if l <= 65_535_usize => {
                w.write_u8(255).await?; // B1: 247
                w.write_all(&(l as u16).to_le_bytes()).await?; // U2: length of value
            }
            l => {
                return Err(util::io_error(format!(
                    "Value of AuthField is too big: {}",
                    l
                )));
            }
        }
        w.write_all(&self.0).await?; // B (varying) value
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        w: &mut W,
    ) -> std::io::Result<()> {
        match self.0.len() {
            l if l <= 250_usize => w.write_u8(l as u8).await?, // B1: length of value
            l if l <= 65_535_usize => {
                w.write_u8(255).await?; // B1: 247
                w.write_all(&(l as u16).to_le_bytes()).await?; // U2: length of value
            }
            l => {
                return Err(util::io_error(format!(
                    "Value of AuthField is too big: {}",
                    l
                )));
            }
        }
        w.write_all(&self.0).await?; // B (varying) value
        Ok(())
    }

    fn size(&self) -> usize {
        1 + self.0.len()
    }

    fn parse_sync(rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let len = length_indicator::parse_sync(rdr.read_u8()?, rdr)?;
        Ok(Self(util_sync::parse_bytes(len, rdr)?))
    }

    async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        rdr: &mut R,
    ) -> std::io::Result<Self> {
        let mut len = rdr.read_u8().await? as usize; // B1
        match len {
            255 => {
                len = util_async::read_u16(rdr).await? as usize; // (B1+)I2
            }
            251..=254 => {
                return Err(util::io_error(format!(
                    "Unknown length indicator for AuthField: {}",
                    len
                )));
            }
            _ => {}
        }
        Ok(Self(util_async::parse_bytes(len, rdr).await?))
    }
}
