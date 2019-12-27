use byteorder::{LittleEndian, ReadBytesExt};
use std::io::BufRead;

#[derive(Debug)]
pub(crate) struct WriteLobReply {
    locator_ids: Vec<u64>,
}
impl WriteLobReply {
    pub fn into_locator_ids(self) -> Vec<u64> {
        self.locator_ids
    }
}

impl WriteLobReply {
    pub fn parse<T: BufRead>(count: usize, rdr: &mut T) -> std::io::Result<Self> {
        debug!("called with count = {}", count);
        let mut locator_ids = Vec::<u64>::default();
        for _ in 0..count {
            let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
            locator_ids.push(locator_id);
        }

        Ok(Self { locator_ids })
    }
}
