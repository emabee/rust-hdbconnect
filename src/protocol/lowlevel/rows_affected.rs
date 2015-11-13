use DbcResult;

use byteorder::{LittleEndian,ReadBytesExt};
use std::io;

#[derive(Debug)]
pub enum RowsAffected {
    Success(i32),
    SuccessNoInfo,   // -2
    ExecutionFailed, // -3
}
impl RowsAffected {
    pub fn parse(count: i32, rdr: &mut io::BufRead) -> DbcResult<Vec<RowsAffected>> {
        let mut vec = Vec::<RowsAffected>::with_capacity(count as usize);
        for _ in 0..count {
            match try!(rdr.read_i32::<LittleEndian>()) {
                -2 => vec.push(RowsAffected::SuccessNoInfo),
                -3 => vec.push(RowsAffected::ExecutionFailed),
                ra => vec.push(RowsAffected::Success(ra)),
            }
        }
        Ok(vec)
    }
}
