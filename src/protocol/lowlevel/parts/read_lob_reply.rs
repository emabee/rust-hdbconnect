use super::{PrtResult, util};

use byteorder::{LittleEndian, ReadBytesExt};
use std::io::BufRead;

pub struct ReadLobReply;
impl ReadLobReply {
    pub fn parse(rdr: &mut BufRead) -> PrtResult<(u64, bool, Vec<u8>)> {
        let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
        let options = rdr.read_u8()?; // I1
        let is_last_data = (options & 0b_100_u8) != 0;
        let chunk_length = rdr.read_i32::<LittleEndian>()?; // I4
        rdr.consume(3); // B3 (filler)
        let data = util::parse_bytes(chunk_length as usize, rdr)?; // B[chunk_length]
        Ok((locator_id, is_last_data, data))
    }
}
