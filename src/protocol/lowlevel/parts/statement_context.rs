use super::{prot_err, PrtResult};
use super::prt_option_value::PrtOptionValue;

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::i8;
use std::io;

#[derive(Debug, Default)]
pub struct StatementContext {
    pub statement_sequence_info: Option<PrtOptionValue>,
    pub server_processing_time: Option<PrtOptionValue>,
    pub schema_name: Option<PrtOptionValue>,
    pub flag_set: Option<PrtOptionValue>,
    pub query_rimeout: Option<PrtOptionValue>,
    pub client_reconnection_wait_timeout: Option<PrtOptionValue>,
}

impl StatementContext {
    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        match self.statement_sequence_info {
            Some(ref value) => {
                w.write_i8(StatementContextId::StatementSequenceInfo.to_i8())?; // I1
                value.serialize(w)?;
                Ok(())
            }
            None => Err(prot_err(
                "StatementContext::serialize(): statement_sequence_info is not filled",
            )),
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
        if self.statement_sequence_info.is_some() {
            count += 1;
        }
        if self.server_processing_time.is_some() {
            count += 1;
        }
        if self.schema_name.is_some() {
            count += 1;
        }
        count
    }

    pub fn parse(count: i32, rdr: &mut io::BufRead) -> PrtResult<StatementContext> {
        trace!("StatementContext::parse()");
        let mut sc = StatementContext::default();
        for _ in 0..count {
            let sc_id = StatementContextId::from_i8(rdr.read_i8()?); // I1
            let value = PrtOptionValue::parse(rdr)?;
            match sc_id {
                StatementContextId::StatementSequenceInfo => {
                    sc.statement_sequence_info = Some(value)
                }
                StatementContextId::ServerProcessingTime => sc.server_processing_time = Some(value),
                StatementContextId::SchemaName => sc.schema_name = Some(value),
                StatementContextId::FlagSet => sc.flag_set = Some(value),
                StatementContextId::QueryTimeout => sc.query_rimeout = Some(value),
                StatementContextId::ClientReconnectionWaitTimeout => {
                    sc.client_reconnection_wait_timeout = Some(value)
                }
                StatementContextId::__Unexpected__ => {
                    warn!(
                        "received value {:?} for unexpected StatementContextId",
                        value
                    );
                }
            }
        }
        trace!("StatementContext::parse(): got {:?}", sc);
        Ok(sc)
    }
}

#[derive(Debug)]
pub enum StatementContextId {
    StatementSequenceInfo,
    ServerProcessingTime,
    SchemaName,
    FlagSet,
    QueryTimeout,
    ClientReconnectionWaitTimeout,
    __Unexpected__,
}
impl StatementContextId {
    pub fn to_i8(&self) -> i8 {
        match *self {
            StatementContextId::StatementSequenceInfo => 1,
            StatementContextId::ServerProcessingTime => 2,
            StatementContextId::SchemaName => 3,
            StatementContextId::FlagSet => 4,
            StatementContextId::QueryTimeout => 5,
            StatementContextId::ClientReconnectionWaitTimeout => 6,
            StatementContextId::__Unexpected__ => i8::MAX,
        }
    }

    pub fn from_i8(val: i8) -> StatementContextId {
        match val {
            1 => StatementContextId::StatementSequenceInfo,
            2 => StatementContextId::ServerProcessingTime,
            3 => StatementContextId::SchemaName,
            4 => StatementContextId::FlagSet,
            5 => StatementContextId::QueryTimeout,
            6 => StatementContextId::ClientReconnectionWaitTimeout,
            val => {
                warn!("Invalid value for StatementContextId received: {}", val);
                StatementContextId::__Unexpected__
            }
        }
    }
}
