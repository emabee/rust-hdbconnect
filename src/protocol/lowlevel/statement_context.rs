use super::option_value::OptionValue;
use super::util;

use byteorder::{ReadBytesExt,WriteBytesExt};
use std::io;

#[derive(Debug)]
pub struct StatementContext {
    pub statement_sequence_info: Option<OptionValue>,
    pub server_processing_time: Option<OptionValue>,
    pub schema_name: Option<OptionValue>,
}

impl StatementContext {
    pub fn new() -> StatementContext {
        StatementContext { statement_sequence_info: None, server_processing_time: None, schema_name: None }
    }

    pub fn encode (&self, w: &mut io::Write)  -> io::Result<()> {
        if let Some(ref value) = self.statement_sequence_info {
            try!(w.write_i8(ScId::StatementSequenceInfo.to_i8()));              // I1
            try!(value.encode(w));
        }
        if let Some(ref value) = self.server_processing_time {
            try!(w.write_i8(ScId::ServerProcessingTime.to_i8()));               // I1
            try!(value.encode(w));
        }
        if let Some(ref value) = self.schema_name {
            try!(w.write_i8(ScId::SchemaName.to_i8()));                         // I1
            try!(value.encode(w));
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        if let Some(ref value) = self.statement_sequence_info { size += 1 + value.size(); }
        if let Some(ref value) = self.server_processing_time { size += 1 + value.size(); }
        if let Some(ref value) = self.schema_name { size += 1 + value.size(); }
        size
    }

    pub fn count(&self) -> i16 {
        let mut count = 0;
        if let Some(_) = self.statement_sequence_info { count += 1; }
        if let Some(_) = self.server_processing_time { count += 1; }
        if let Some(_) = self.schema_name { count += 1; }
        count
    }

    pub fn parse(count: i32, rdr: &mut io::BufRead) -> io::Result<StatementContext> {
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

    pub fn from_i8(val: i8) -> io::Result<ScId> { match val {
        1 => Ok(ScId::StatementSequenceInfo),
        2 => Ok(ScId::ServerProcessingTime),
        3 => Ok(ScId::SchemaName),
        _ => Err(util::io_error(&format!("Invalid value for ScId detected: {}",val))),
    }}
}
