use crate::HdbResult;
use byteorder::{LittleEndian, WriteBytesExt};

#[derive(Debug)]
pub(crate) struct WriteLobRequest<'a> {
    locator_id: u64,
    offset: i64,
    buf: &'a [u8],
    last_data: bool,
}
impl WriteLobRequest<'_> {
    pub fn new(locator_id: u64, offset: i64, buf: &[u8], last_data: bool) -> WriteLobRequest<'_> {
        trace!(
            "Offset = {offset}, buffer length = {}, last_data: {last_data}",
            buf.len()
        );
        WriteLobRequest {
            locator_id,
            offset,
            buf,
            last_data,
        }
    }

    pub fn size(&self) -> usize {
        21 + self.buf.len()
    }

    pub fn emit(&self, w: &mut dyn std::io::Write) -> HdbResult<()> {
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
}
