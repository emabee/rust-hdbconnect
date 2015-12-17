use super::{PrtError,PrtResult};
use super::option_value::OptionValue;

use byteorder::{ReadBytesExt,WriteBytesExt};
use std::io;

#[derive(Clone,Debug)]
pub struct CommitOption {
    pub id: CommitOptionId,
    pub value: OptionValue,
}
impl CommitOption {
    pub fn serialize (&self, w: &mut io::Write)  -> PrtResult<()> {
        try!(w.write_i8(self.id.to_i8()));                                      // I1
        self.value.serialize(w)
    }

    pub fn size(&self) -> usize {
        1 + self.value.size()
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<CommitOption> {
        let option_id = try!(CommitOptionId::from_i8(try!(rdr.read_i8())));    // I1
        let value = try!(OptionValue::parse(rdr));
        Ok(CommitOption{id: option_id, value: value})
    }
}


#[derive(Clone,Debug)]
pub enum CommitOptionId {
    HoldCursorsOverCommit,                // 1 //
}
impl CommitOptionId {
    fn to_i8(&self) -> i8 {
        match *self {
            CommitOptionId::HoldCursorsOverCommit =>  1,
        }
    }

    fn from_i8(val: i8) -> PrtResult<CommitOptionId> { match val {
        1 =>  Ok(CommitOptionId::HoldCursorsOverCommit),
        _ => Err(PrtError::ProtocolError(format!("Invalid value for CommitOptionId detected: {}",val))),
    }}
}
