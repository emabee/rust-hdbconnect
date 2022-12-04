use byteorder::{LittleEndian, WriteBytesExt};

#[derive(Debug)]
pub struct WriteLobRequest<'a> {
    locator_id: u64,
    offset: i64,
    buf: &'a [u8],
    last_data: bool,
}
impl<'a> WriteLobRequest<'a> {
    pub fn new(locator_id: u64, offset: i64, buf: &[u8], last_data: bool) -> WriteLobRequest {
        trace!("Offset = {}, buffer length = {}", offset, buf.len());
        WriteLobRequest {
            locator_id,
            offset,
            buf,
            last_data,
        }
    }
    pub fn sync_emit(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        // 1: NULL (not used here), 2: DATA_INCLUDED, 4: LASTDATA
        let options = if self.last_data { 6 } else { 2 };
        w.write_u64::<LittleEndian>(self.locator_id)?;
        w.write_u8(options)?;
        w.write_i64::<LittleEndian>(self.offset)?;

        #[allow(clippy::cast_possible_truncation)]
        w.write_u32::<LittleEndian>(self.buf.len() as u32)?;
        w.write_all(self.buf)?;

        Ok(())
    }
    pub fn size(&self) -> usize {
        21 + self.buf.len()
    }

    #[cfg(feature = "async")]
    pub async fn async_emit<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        w: &mut W,
    ) -> std::io::Result<()> {
        // 1: NULL (not used here), 2: DATA_INCLUDED, 4: LASTDATA
        let options = if self.last_data { 6 } else { 2 };
        w.write_all(&self.locator_id.to_le_bytes()).await?;
        w.write_u8(options).await?;
        w.write_all(&self.offset.to_le_bytes()).await?;

        #[allow(clippy::cast_possible_truncation)]
        w.write_all(&(self.buf.len() as u32).to_le_bytes()).await?;
        w.write_all(self.buf).await?;

        Ok(())
    }
}
