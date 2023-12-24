// #[cfg(feature = "async")]
// use crate::protocol::util_async;
// #[cfg(feature = "sync")]
use crate::protocol::util_sync;
use crate::HdbResult;
// #[cfg(feature = "sync")]
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
    // #[cfg(feature = "sync")]
    pub fn parse_sync(rdr: &mut dyn std::io::Read) -> HdbResult<Self> {
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
    // #[cfg(feature = "async")]
    // pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    //     rdr: &mut R,
    // ) -> HdbResult<Self> {
    //     let locator_id = rdr.read_u64_le().await?; // I8
    //     let options = rdr.read_u8().await?; // I1
    //     let is_last_data = (options & 0b100_u8) != 0;
    //     let chunk_length = rdr.read_i32_le().await?; // I4
    //     util_async::skip_bytes(3, rdr).await?; // B3 (filler)
    //     #[allow(clippy::cast_sign_loss)]
    //     let data = util_async::parse_bytes(chunk_length as usize, rdr).await?; // B[chunk_length]
    //     Ok(Self {
    //         locator_id,
    //         is_last_data,
    //         data,
    //     })
    // }
}
