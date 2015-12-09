use super::PrtResult;
use super::util;

use byteorder::{LittleEndian,ReadBytesExt};
use std::io::BufRead;

pub struct ReadLobReply;
impl ReadLobReply {
    pub fn parse(rdr: &mut BufRead) -> PrtResult<(u64,bool,Vec<u8>)>{
        let locator_id = try!(rdr.read_u64::<LittleEndian>());                  // I8
        let options = try!(rdr.read_u8());                                      // I1
        // let is_null = (options & 0b_1_u8) != 0;
        // let is_data_included = (options & 0b_10_u8) != 0;
        let is_last_data = (options & 0b_100_u8) != 0;
        let chunk_length = try!(rdr.read_i32::<LittleEndian>());                // I4
        rdr.consume(3);                                                         // B3 (filler)
        let data = try!(util::parse_bytes(chunk_length as usize,rdr));          // B[chunk_length]
        Ok((locator_id, is_last_data, data))
    }
}
