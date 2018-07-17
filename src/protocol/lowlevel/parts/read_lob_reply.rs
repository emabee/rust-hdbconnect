use protocol::lowlevel::util;
use HdbResult;

use byteorder::{LittleEndian, ReadBytesExt};
use std::io::BufRead;

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
    pub fn parse(rdr: &mut BufRead) -> HdbResult<ReadLobReply> {
        let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
        let options = rdr.read_u8()?; // I1
        let is_last_data = (options & 0b_100_u8) != 0;
        let chunk_length = rdr.read_i32::<LittleEndian>()?; // I4
        rdr.consume(3); // B3 (filler)
        let data = util::parse_bytes(chunk_length as usize, rdr)?; // B[chunk_length]
        Ok(ReadLobReply {
            locator_id,
            is_last_data,
            data,
        })
    }
}
