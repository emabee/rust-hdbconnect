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
    pub fn emit_sync(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        trace!("read_lob_request::emit() {:?}", self);
        w.write_u64::<LittleEndian>(self.locator_id)?;
        w.write_u64::<LittleEndian>(self.offset)?;
        w.write_u32::<LittleEndian>(self.length)?;
        w.write_u32::<LittleEndian>(0_u32)?; // FILLER
        Ok(())
    }
    pub async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        w: &mut W,
    ) -> std::io::Result<()> {
        trace!("read_lob_request::emit() {:?}", self);
        w.write_all(&self.locator_id.to_le_bytes()).await?;
        w.write_all(&self.offset.to_le_bytes()).await?;
        w.write_all(&self.length.to_le_bytes()).await?;
        w.write_all(&0_u32.to_le_bytes()).await?; // FILLER
        Ok(())
    }
    pub fn size() -> usize {
        24
    }
}
