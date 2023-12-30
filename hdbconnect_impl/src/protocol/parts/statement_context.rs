use crate::protocol::parts::{
    option_part::{OptionId, OptionPart},
    option_value::OptionValue,
};
use std::{convert::TryInto, time::Duration};

// An options part that is populated from previously received statement context
// information. The binary option content is opaque to the client.
pub type StatementContext = OptionPart<StatementContextId>;

impl StatementContext {
    pub fn statement_sequence_info(&self) -> Option<i64> {
        match self.get(&StatementContextId::StatementSequenceInfo) {
            Ok(&OptionValue::BIGINT(value)) => Some(value),
            _ => None,
        }
    }

    pub fn set_statement_sequence_info(&mut self, value: i64) {
        self.insert(
            StatementContextId::StatementSequenceInfo,
            OptionValue::BIGINT(value),
        );
    }

    pub fn server_processing_time(&self) -> Option<Duration> {
        match self.get(&StatementContextId::ServerProcessingTime) {
            Ok(&OptionValue::BIGINT(value)) => {
                Some(Duration::from_micros(value.try_into().unwrap_or(0)))
            }
            _ => None,
        }
    }

    pub fn server_cpu_time(&self) -> Option<Duration> {
        match self.get(&StatementContextId::ServerCPUTime) {
            Ok(&OptionValue::BIGINT(value)) => {
                Some(Duration::from_micros(value.try_into().unwrap_or(0)))
            }
            _ => None,
        }
    }

    pub fn server_memory_usage(&self) -> Option<u64> {
        match self.get(&StatementContextId::ServerMemoryUsage) {
            Ok(&OptionValue::BIGINT(value)) => Some(value.try_into().unwrap_or(0)),
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
            Self::StatementSequenceInfo => 1,
            Self::ServerProcessingTime => 2,
            Self::SchemaName => 3,
            Self::FlagSet => 4,
            Self::QueryTimeout => 5,
            Self::ClientReconnectionWaitTimeout => 6,
            Self::ServerCPUTime => 7,
            Self::ServerMemoryUsage => 8,
            Self::__Unexpected__(val) => val,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            1 => Self::StatementSequenceInfo,
            2 => Self::ServerProcessingTime,
            3 => Self::SchemaName,
            4 => Self::FlagSet,
            5 => Self::QueryTimeout,
            6 => Self::ClientReconnectionWaitTimeout,
            7 => Self::ServerCPUTime,
            8 => Self::ServerMemoryUsage,
            val => {
                warn!("Unsupported value for StatementContextId received: {}", val);
                Self::__Unexpected__(val)
            }
        }
    }

    fn part_type(&self) -> &'static str {
        "StatementContext"
    }
}
