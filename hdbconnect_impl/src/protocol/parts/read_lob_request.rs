#[cfg(feature = "sync")]
use byteorder::{LittleEndian, WriteBytesExt};

#[derive(Debug)]
pub struct ReadLobRequest {
    locator_id: u64,
    offset: u64,
    length: u32,
}
impl ReadLobRequest {
    pub fn new(locator_id: u64, offset: u64, length: u32) -> Self {
        trace!("Offset = {}, length = {}", offset, length);
        Self {
            locator_id,
            offset,
            length,
        }
    }
    #[cfg(feature = "sync")]
    pub fn sync_emit(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        trace!("read_lob_request::emit() {:?}", self);
        w.write_u64::<LittleEndian>(self.locator_id)?;
        w.write_u64::<LittleEndian>(self.offset)?;
        w.write_u32::<LittleEndian>(self.length)?;
        w.write_u32::<LittleEndian>(0_u32)?; // FILLER
        Ok(())
    }
    #[cfg(feature = "async")]
    pub async fn async_emit<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        w: &mut W,
    ) -> std::io::Result<()> {
        trace!("read_lob_request::emit() {:?}", self);
        w.write_u64_le(self.locator_id).await?;
        w.write_u64_le(self.offset).await?;
        w.write_u32_le(self.length).await?;
        w.write_u32_le(0).await?; // FILLER
        Ok(())
    }
    pub fn size() -> usize {
        24
    }
}
