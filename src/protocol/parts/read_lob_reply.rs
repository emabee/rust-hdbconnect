use crate::protocol::{util_async, util_sync};
use byteorder::{LittleEndian, ReadBytesExt};

#[derive(Debug)]
pub struct ReadLobReply {
    locator_id: u64,
    is_last_data: bool,
    data: Vec<u8>,
}
impl ReadLobReply {
    pub fn locator_id(&self) -> &u64 {
        &self.locator_id
    }
    pub fn into_data_and_last(self) -> (Vec<u8>, bool) {
        (self.data, self.is_last_data)
    }
}

impl ReadLobReply {
    pub fn parse_sync(rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
        let options = rdr.read_u8()?; // I1
        let is_last_data = (options & 0b100_u8) != 0;
        let chunk_length = rdr.read_i32::<LittleEndian>()?; // I4
        util_sync::skip_bytes(3, rdr)?; // B3 (filler)
        #[allow(clippy::cast_sign_loss)]
        let data = util_sync::parse_bytes(chunk_length as usize, rdr)?; // B[chunk_length]
        Ok(Self {
            locator_id,
            is_last_data,
            data,
        })
    }
    pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        rdr: &mut R,
    ) -> std::io::Result<Self> {
        let locator_id = util_async::read_u64(rdr).await?; // I8
        let options = rdr.read_u8().await?; // I1
        let is_last_data = (options & 0b_100_u8) != 0;
        let chunk_length = util_async::read_i32(rdr).await?; // I4
        util_async::skip_bytes(3, rdr).await?; // B3 (filler)
        #[allow(clippy::cast_sign_loss)]
        let data = util_async::parse_bytes(chunk_length as usize, rdr).await?; // B[chunk_length]
        Ok(Self {
            locator_id,
            is_last_data,
            data,
        })
    }
}
