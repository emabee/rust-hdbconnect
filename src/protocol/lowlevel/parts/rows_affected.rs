use super::PrtResult;

use byteorder::{LittleEndian, ReadBytesExt};
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum RowsAffected {
    Count(usize),
    SuccessNoInfo, // -2
    ExecutionFailed, // -3
}
impl RowsAffected {
    pub fn parse(count: i32, rdr: &mut io::BufRead) -> PrtResult<Vec<RowsAffected>> {
        let mut vec = Vec::<RowsAffected>::with_capacity(count as usize);
        for _ in 0..count {
            match rdr.read_i32::<LittleEndian>()? {
                -2 => vec.push(RowsAffected::SuccessNoInfo),
                -3 => vec.push(RowsAffected::ExecutionFailed),
                i => vec.push(RowsAffected::Count(i as usize)),
            }
        }
        Ok(vec)
    }
}

impl fmt::Display for RowsAffected {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RowsAffected::Count(count) => writeln!(fmt, "Number of affected rows: {}, ", count)?,
            RowsAffected::SuccessNoInfo => {
                writeln!(fmt,
                         "Command successfully executed but number of affected rows cannot be \
                          determined")?
            }
            RowsAffected::ExecutionFailed => {
                writeln!(fmt, "Execution of statement or processing of row has failed")?
            }
        }
        Ok(())
    }
}
