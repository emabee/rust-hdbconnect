use crate::protocol::util_sync;
// #[cfg(feature = "sync")]
use byteorder::WriteBytesExt;

// #[cfg(feature = "async")]
// use crate::protocol::util_async;

use crate::{protocol::parts::length_indicator, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt};

#[derive(Debug, Default)]
pub struct AuthFields(Vec<AuthField>);
impl AuthFields {
    pub fn with_capacity(count: usize) -> Self {
        Self(Vec::<AuthField>::with_capacity(count))
    }

    // is also used in async context
    pub fn parse_sync(rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let field_count = rdr.read_u16::<LittleEndian>()? as usize; // I2
        let mut auth_fields: Self = Self(Vec::<AuthField>::with_capacity(field_count));
        for _ in 0..field_count {
            auth_fields.0.push(AuthField::parse_sync(rdr)?);
        }
        Ok(auth_fields)
    }

    // #[cfg(feature = "async")]
    // pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    //     rdr: &mut R,
    // ) -> HdbResult<Self> {
    //     let field_count = rdr.read_u16_le().await? as usize; // I2
    //     let mut auth_fields: Self = Self(Vec::<AuthField>::with_capacity(field_count));
    //     for _ in 0..field_count {
    //         auth_fields.0.push(AuthField::parse_async(rdr).await?);
    //     }
    //     Ok(auth_fields)
    // }

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

    // #[cfg(feature = "sync")]
    pub fn sync_emit(&self, w: &mut dyn std::io::Write) -> HdbResult<()> {
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_possible_wrap)]
        w.write_i16::<LittleEndian>(self.0.len() as i16)?;
        for field in &self.0 {
            field.sync_emit(w)?;
        }
        Ok(())
    }

    // #[cfg(feature = "async")]
    // pub async fn async_emit<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    //     &self,
    //     w: &mut W,
    // ) -> HdbResult<()> {
    //     #[allow(clippy::cast_possible_truncation)]
    //     #[allow(clippy::cast_possible_wrap)]
    //     w.write_i16_le(self.0.len() as i16).await?;
    //     for field in &self.0 {
    //         field.async_emit(w).await?;
    //     }
    //     Ok(())
    // }

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

    // #[cfg(feature = "sync")]
    #[allow(clippy::cast_possible_truncation)]
    fn sync_emit(&self, w: &mut dyn std::io::Write) -> HdbResult<()> {
        length_indicator::sync_emit(self.0.len(), w)?;
        w.write_all(&self.0)?; // B (varying) value
        Ok(())
    }

    // #[cfg(feature = "async")]
    // #[allow(clippy::cast_possible_truncation)]
    // async fn async_emit<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    //     &self,
    //     w: &mut W,
    // ) -> HdbResult<()> {
    //     length_indicator::async_emit(self.0.len(), w).await?;
    //     w.write_all(&self.0).await?; // B (varying) value
    //     Ok(())
    // }

    fn size(&self) -> usize {
        1 + self.0.len()
    }

    // is also used in async context
    fn parse_sync(rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        let len = length_indicator::parse_sync(rdr.read_u8()?, rdr)?;
        Ok(Self(util_sync::parse_bytes(len, rdr)?))
    }

    // #[cfg(feature = "async")]
    // async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    //     rdr: &mut R,
    // ) -> HdbResult<Self> {
    //     let len = length_indicator::parse_async(rdr.read_u8().await?, rdr).await?;
    //     Ok(Self(util_async::parse_bytes(len, rdr).await?))
    // }
}
