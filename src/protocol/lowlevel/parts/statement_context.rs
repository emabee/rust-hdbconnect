use protocol::lowlevel::parts::option_part::{OptionId, OptionPart};
use protocol::lowlevel::parts::option_value::OptionValue;

use std::u8;

// An options part that is populated from previously received statement context
// information. The binary option content is opaque to the client.
pub type StatementContext = OptionPart<StatementContextId>;

impl StatementContext {
    pub fn get_statement_sequence_info(&self) -> Option<i64> {
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

    pub fn get_server_processing_time(&self) -> i32 {
        match self.get_value(&StatementContextId::ServerProcessingTime) {
            Some(&OptionValue::INT(value)) => value,
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
//  FlagSet,                       // 4 // INT  // Unused?
pub enum StatementContextId {
    StatementSequenceInfo,         // 1 // BIGINT?
    ServerProcessingTime,          // 2 // INT
    SchemaName,                    // 3 // STRING
    QueryTimeout,                  // 5 // BIGINT
    ClientReconnectionWaitTimeout, // 6 // INT
    __Unexpected__,
}
impl OptionId<StatementContextId> for StatementContextId {
    fn to_u8(&self) -> u8 {
        match *self {
            StatementContextId::StatementSequenceInfo => 1,
            StatementContextId::ServerProcessingTime => 2,
            StatementContextId::SchemaName => 3,
            StatementContextId::QueryTimeout => 5,
            StatementContextId::ClientReconnectionWaitTimeout => 6,
            StatementContextId::__Unexpected__ => u8::MAX,
        }
    }

    fn from_u8(val: u8) -> StatementContextId {
        match val {
            1 => StatementContextId::StatementSequenceInfo,
            2 => StatementContextId::ServerProcessingTime,
            3 => StatementContextId::SchemaName,
            5 => StatementContextId::QueryTimeout,
            6 => StatementContextId::ClientReconnectionWaitTimeout,
            val => {
                warn!("Invalid value for StatementContextId received: {}", val);
                StatementContextId::__Unexpected__
            }
        }
    }
}
