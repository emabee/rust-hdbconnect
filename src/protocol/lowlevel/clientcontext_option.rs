use super::{PrtError,PrtResult};
use super::option_value::OptionValue;

use byteorder::{ReadBytesExt,WriteBytesExt};
use std::io;

#[derive(Debug)]
pub struct CcOption {
    pub id: CcOptionId,
    pub value: OptionValue,
}
impl CcOption {
    pub fn serialize (&self, w: &mut io::Write)  -> PrtResult<()> {
        try!(w.write_i8(self.id.to_i8()));                                  // I1
        self.value.serialize(w)
    }

    pub fn size(&self) -> usize {
        1 + self.value.size()
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<CcOption> {
        let option_id = try!(CcOptionId::from_i8(try!(rdr.read_i8())));     // I1
        let value = try!(OptionValue::parse(rdr));
        Ok(CcOption{id: option_id, value: value})
    }
}


#[derive(Debug)]
pub enum CcOptionId {
    Version,
    ClientType,
    ClientApplicationProgram,
}
impl CcOptionId {
    pub fn to_i8(&self) -> i8 {
        match *self {
            CcOptionId::Version => 1,
            CcOptionId::ClientType => 2,
            CcOptionId::ClientApplicationProgram => 3,
        }
    }

    pub fn from_i8(val: i8) -> PrtResult<CcOptionId> { match val {
        1 => Ok(CcOptionId::Version),
        2 => Ok(CcOptionId::ClientType),
        3 => Ok(CcOptionId::ClientApplicationProgram),
        _ => Err(PrtError::ProtocolError(format!("Invalid value for CcOptionId detected: {}",val))),
    }}
}
