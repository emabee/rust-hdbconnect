use crate::protocol::util_async;
use byteorder::{LittleEndian, ReadBytesExt};

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
    pub fn parse_sync(count: usize, rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        debug!("called with count = {}", count);
        let mut locator_ids = Vec::<u64>::default();
        for _ in 0..count {
            let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
            locator_ids.push(locator_id);
        }

        Ok(Self { locator_ids })
    }

    pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        count: usize,
        rdr: &mut R,
    ) -> std::io::Result<Self> {
        debug!("called with count = {}", count);
        let mut locator_ids = Vec::<u64>::default();
        for _ in 0..count {
            let locator_id = util_async::read_u64(rdr).await?; // I8
            locator_ids.push(locator_id);
        }

        Ok(Self { locator_ids })
    }
}
