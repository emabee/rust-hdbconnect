use crate::hdb_error::HdbResult;
use byteorder::{LittleEndian, WriteBytesExt};
use std::io;

#[derive(Debug)]
pub struct WriteLobRequest<'a> {
    locator_id: u64,
    offset: i64,
    buf: &'a [u8],
}
impl<'a> WriteLobRequest<'a> {
    pub fn new(locator_id: u64, offset: i64, buf: &[u8]) -> WriteLobRequest {
        trace!("Offset = {}, buffer length = {}", offset, buf.len());
        WriteLobRequest {
            locator_id,
            offset,
            buf,
        }
    }
    pub fn emit<T: io::Write>(&self, w: &mut T) -> HdbResult<()> {
        debug!("emit(): locator_id = {:?}", self.locator_id);
        let options = 2; // DATA_INCLUDED // LASTDATA would add 4
        w.write_u64::<LittleEndian>(self.locator_id)?;
        w.write_u8(options)?;
        w.write_i64::<LittleEndian>(self.offset)?;
        w.write_u32::<LittleEndian>(self.buf.len() as u32)?;
        w.write_all(self.buf)?;
        Ok(())
    }
    pub fn size(&self) -> usize {
        21 + self.buf.len()
    }
}
