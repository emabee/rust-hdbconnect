use crate::protocol::parts::option_part::{OptionId, OptionPart};
use crate::protocol::parts::option_value::OptionValue;
use std::convert::TryInto;
use std::time::Duration;

// An options part that is populated from previously received statement context
// information. The binary option content is opaque to the client.
// Assumption: it's used to support automatic reconnects (but we don't make use of that: TODO)
pub(crate) type StatementContext = OptionPart<StatementContextId>;

impl StatementContext {
    pub fn statement_sequence_info(&self) -> Option<i64> {
        match self.get_value(&StatementContextId::StatementSequenceInfo) {
            Some(&OptionValue::BIGINT(value)) => Some(value),
            _ => None,
        }
    }

    pub fn set_statement_sequence_info(&mut self, value: i64) {
        self.set_value(
            StatementContextId::StatementSequenceInfo,
            OptionValue::BIGINT(value),
        );
    }

    pub fn server_processing_time(&self) -> Option<Duration> {
        match self.get_value(&StatementContextId::ServerProcessingTime) {
            Some(&OptionValue::BIGINT(value)) => {
                Some(Duration::from_micros(value.try_into().unwrap_or(0)))
            }
            _ => None,
        }
    }

    pub fn server_cpu_time(&self) -> Option<Duration> {
        match self.get_value(&StatementContextId::ServerCPUTime) {
            Some(&OptionValue::BIGINT(value)) => {
                Some(Duration::from_micros(value.try_into().unwrap_or(0)))
            }
            _ => None,
        }
    }

    pub fn server_memory_usage(&self) -> Option<u64> {
        match self.get_value(&StatementContextId::ServerMemoryUsage) {
            Some(&OptionValue::BIGINT(value)) => Some(value.try_into().unwrap_or(0)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum StatementContextId {
    StatementSequenceInfo,         // 1 // BIGINT?
    ServerProcessingTime,          // 2 // BIGINT
    SchemaName,                    // 3 // STRING
    FlagSet,                       // 4 // INT
    QueryTimeout,                  // 5 // BIGINT
    ClientReconnectionWaitTimeout, // 6 // INT
    ServerCPUTime,                 // 7 // BIGINT microseconds
    ServerMemoryUsage,             // 8 // BIGINT bytes
    __Unexpected__(u8),
}
impl OptionId<StatementContextId> for StatementContextId {
    fn to_u8(&self) -> u8 {
        match *self {
            StatementContextId::StatementSequenceInfo => 1,
            StatementContextId::ServerProcessingTime => 2,
            StatementContextId::SchemaName => 3,
            StatementContextId::FlagSet => 4,
            StatementContextId::QueryTimeout => 5,
            StatementContextId::ClientReconnectionWaitTimeout => 6,
            StatementContextId::ServerCPUTime => 7,
            StatementContextId::ServerMemoryUsage => 8,
            StatementContextId::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> StatementContextId {
        match val {
            1 => StatementContextId::StatementSequenceInfo,
            2 => StatementContextId::ServerProcessingTime,
            3 => StatementContextId::SchemaName,
            4 => StatementContextId::FlagSet,
            5 => StatementContextId::QueryTimeout,
            6 => StatementContextId::ClientReconnectionWaitTimeout,
            7 => StatementContextId::ServerCPUTime,
            8 => StatementContextId::ServerMemoryUsage,
            val => {
                warn!("Unknown value for StatementContextId received: {}", val);
                StatementContextId::__Unexpected__(val)
            }
        }
    }
}
