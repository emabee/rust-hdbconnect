use super::option_value::*;

use byteorder::{ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{BufRead,Error,ErrorKind,Write};

#[derive(Debug)]
pub struct StatementContextOption {
    pub id: ScOptionId,
    pub value: OptionValue,
}
impl StatementContextOption {
    pub fn encode (&self, w: &mut Write)  -> IoResult<()> {
        try!(w.write_i8(self.id.to_i8()));                                  // I1
        self.value.encode(w)
    }

    pub fn size(&self) -> usize {
        1 + self.value.size()
    }

    pub fn parse(rdr: &mut BufRead) -> IoResult<StatementContextOption> {
        let option_id = try!(ScOptionId::from_i8(try!(rdr.read_i8())));     // I1
        let value = try!(OptionValue::parse(rdr));
        Ok(StatementContextOption{id: option_id, value: value})
    }
}


#[derive(Debug)]
pub enum ScOptionId {
    StatementSequenceInfo,
    ServerProcessingTime,
    SchemaName,
}
impl ScOptionId {
    pub fn to_i8(&self) -> i8 {
        match *self {
            ScOptionId::StatementSequenceInfo => 1,
            ScOptionId::ServerProcessingTime => 2,
            ScOptionId::SchemaName => 3,
        }
    }

    pub fn from_i8(val: i8) -> IoResult<ScOptionId> { match val {
        1 => Ok(ScOptionId::StatementSequenceInfo),
        2 => Ok(ScOptionId::ServerProcessingTime),
        3 => Ok(ScOptionId::SchemaName),
        _ => Err(Error::new(ErrorKind::Other,format!("Invalid value for ScOptionId detected: {}",val))),
    }}
}
