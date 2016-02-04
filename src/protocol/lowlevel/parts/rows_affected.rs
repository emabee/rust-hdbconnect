use super::PrtResult;

use byteorder::{LittleEndian,ReadBytesExt};
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum RowsAffected {
    Success(i32),
    SuccessNoInfo,   // -2
    ExecutionFailed, // -3
}
impl RowsAffected {
    // pub fn unwrap(self) -> i32 {
    //     match self {
    //         RowsAffected::Success(value) => value,
    //         _ => panic!("{:?} cannot be unwrapped",self),
    //     }
    // }
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
                -3 => vec.push(RowsAffected::ExecutionFailed),
                ra => vec.push(RowsAffected::Success(ra)),
            }
        }
        Ok(VecRowsAffected(vec))
    }
}

impl fmt::Display for VecRowsAffected {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        for ref rows_affected in &self.0 {
            match **rows_affected {
                RowsAffected::Success(count) => writeln!(fmt, "Number of affected rows: {}, ",count).unwrap(),
                RowsAffected::SuccessNoInfo => writeln!(fmt, "Number of affected rows: unknown, ").unwrap(),
                RowsAffected::ExecutionFailed => writeln!(fmt, "Execution failed, ").unwrap(),
            }
        }
        Ok(())
    }
}
