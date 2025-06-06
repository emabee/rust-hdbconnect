use crate::HdbResult;
use byteorder::{LittleEndian, ReadBytesExt};

#[derive(Debug)]
pub(crate) struct WriteLobReply {
    #[allow(dead_code)] // TODO what are we supposed to do with this?
    locator_ids: Vec<u64>,
}
impl WriteLobReply {
    pub fn into_locator_ids(self) -> Vec<u64> {
        self.locator_ids
    }
}

impl WriteLobReply {
    pub fn parse(count: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        debug!("called with count = {count}");
        let mut locator_ids = Vec::<u64>::default();
        for _ in 0..count {
            let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
            locator_ids.push(locator_id);
        }

        Ok(Self { locator_ids })
    }
}
