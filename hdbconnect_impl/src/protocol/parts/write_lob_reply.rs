use crate::HdbResult;
#[cfg(feature = "sync")]
use byteorder::{LittleEndian, ReadBytesExt};

#[derive(Debug)]
pub struct WriteLobReply {
    locator_ids: Vec<u64>,
}
impl WriteLobReply {
    pub fn into_locator_ids(self) -> Vec<u64> {
        self.locator_ids
    }
}

impl WriteLobReply {
    #[cfg(feature = "sync")]
    pub fn parse_sync(count: usize, rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
        debug!("called with count = {}", count);
        let mut locator_ids = Vec::<u64>::default();
        for _ in 0..count {
            let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
            locator_ids.push(locator_id);
        }

        Ok(Self { locator_ids })
    }

    #[cfg(feature = "async")]
    pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        count: usize,
        rdr: &mut R,
    ) -> HdbResult<Self> {
        debug!("called with count = {}", count);
        let mut locator_ids = Vec::<u64>::default();
        for _ in 0..count {
            let locator_id = rdr.read_u64_le().await?; // I8
            locator_ids.push(locator_id);
        }

        Ok(Self { locator_ids })
    }
}
