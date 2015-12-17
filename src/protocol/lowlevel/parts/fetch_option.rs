use super::{PrtError,PrtResult};
use super::option_value::OptionValue;

use byteorder::{ReadBytesExt,WriteBytesExt};
use std::io;

#[derive(Clone,Debug)]
pub struct FetchOption {
    pub id: FetchOptionId,
    pub value: OptionValue,
}
impl FetchOption {
    pub fn serialize (&self, w: &mut io::Write)  -> PrtResult<()> {
        try!(w.write_i8(self.id.to_i8()));                                      // I1
        self.value.serialize(w)
    }

    pub fn size(&self) -> usize {
        1 + self.value.size()
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<FetchOption> {
        let option_id = try!(FetchOptionId::from_i8(try!(rdr.read_i8())));    // I1
        let value = try!(OptionValue::parse(rdr));
        Ok(FetchOption{id: option_id, value: value})
    }
}


#[derive(Clone,Debug)]
pub enum FetchOptionId {
    ResultsetPos,                         // 1 //
}
impl FetchOptionId {
    fn to_i8(&self) -> i8 {
        match *self {
            FetchOptionId::ResultsetPos =>  1,
        }
    }

    fn from_i8(val: i8) -> PrtResult<FetchOptionId> { match val {
        1 =>  Ok(FetchOptionId::ResultsetPos),
        _ => Err(PrtError::ProtocolError(format!("Invalid value for FetchOptionId detected: {}",val))),
    }}
}
