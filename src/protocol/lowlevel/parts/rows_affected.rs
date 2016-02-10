use super::{PrtResult,prot_err};

use byteorder::{LittleEndian,ReadBytesExt};
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum RowsAffected {
    Success(i32),
    SuccessNoInfo,   // -2
}
impl RowsAffected {
    pub fn equals(&self, other: i32) -> bool {
        match *self {
            RowsAffected::Success(value) => value == other,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub struct VecRowsAffected(pub Vec<RowsAffected>);

impl VecRowsAffected {
    pub fn parse(count: i32, rdr: &mut io::BufRead) -> PrtResult<VecRowsAffected> {
        let mut vec = Vec::<RowsAffected>::with_capacity(count as usize);
        for _ in 0..count {
            match try!(rdr.read_i32::<LittleEndian>()) {
                -2 => vec.push(RowsAffected::SuccessNoInfo),
                -3 => {return Err(prot_err("Unexpected value -3 (= RowsAffected::ExecutionFailed)"))},
                ra => vec.push(RowsAffected::Success(ra)),
            }
        }
        Ok(VecRowsAffected(vec))
    }
}

impl fmt::Display for RowsAffected {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RowsAffected::Success(count) => try!(writeln!(fmt, "Number of affected rows: {}, ",count)),
            RowsAffected::SuccessNoInfo => try!(writeln!(fmt, "Command successfully executed")),
        }
        Ok(())
    }
}
