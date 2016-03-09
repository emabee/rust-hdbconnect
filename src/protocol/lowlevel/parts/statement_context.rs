use super::{PrtError, PrtResult, prot_err};
use super::option_value::OptionValue;

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io;

#[derive(Debug)]
pub struct StatementContext {
    pub statement_sequence_info: Option<OptionValue>,
    pub server_processing_time: Option<OptionValue>,
    pub schema_name: Option<OptionValue>,
}

impl StatementContext {
    pub fn new() -> StatementContext {
        StatementContext {
            statement_sequence_info: None,
            server_processing_time: None,
            schema_name: None,
        }
    }

    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        match self.statement_sequence_info {
            Some(ref value) => {
                try!(w.write_i8(ScId::StatementSequenceInfo.to_i8()));              // I1
                try!(value.serialize(w));
                Ok(())
            }
            None => Err(prot_err("StatementContext::serialize(): statement_sequence_info is not filled")),
        }
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        if let Some(ref value) = self.statement_sequence_info {
            size += 1 + value.size();
        }
        if let Some(ref value) = self.server_processing_time {
            size += 1 + value.size();
        }
        if let Some(ref value) = self.schema_name {
            size += 1 + value.size();
        }
        size
    }

    pub fn count(&self) -> usize {
        let mut count = 0;
        if let Some(_) = self.statement_sequence_info {
            count += 1;
        }
        if let Some(_) = self.server_processing_time {
            count += 1;
        }
        if let Some(_) = self.schema_name {
            count += 1;
        }
        count
    }

    pub fn parse(count: i32, rdr: &mut io::BufRead) -> PrtResult<StatementContext> {
        trace!("StatementContext::parse()");
        let mut sc = StatementContext::new();
        for _ in 0..count {
            let sc_id = try!(ScId::from_i8(try!(rdr.read_i8())));               // I1
            let value = try!(OptionValue::parse(rdr));
            match sc_id {
                ScId::StatementSequenceInfo => sc.statement_sequence_info = Some(value),
                ScId::ServerProcessingTime => sc.server_processing_time = Some(value),
                ScId::SchemaName => sc.schema_name = Some(value),
            }
        }
        trace!("StatementContext::parse(): got {:?}", sc);
        Ok(sc)
    }
}


#[derive(Debug)]
pub enum ScId {
    StatementSequenceInfo,
    ServerProcessingTime,
    SchemaName,
}
impl ScId {
    pub fn to_i8(&self) -> i8 {
        match *self {
            ScId::StatementSequenceInfo => 1,
            ScId::ServerProcessingTime => 2,
            ScId::SchemaName => 3,
        }
    }

    pub fn from_i8(val: i8) -> PrtResult<ScId> {
        match val {
            1 => Ok(ScId::StatementSequenceInfo),
            2 => Ok(ScId::ServerProcessingTime),
            3 => Ok(ScId::SchemaName),
            _ => Err(PrtError::ProtocolError(format!("Invalid value for ScId detected: {}", val))),
        }
    }
}
