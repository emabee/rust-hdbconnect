//! Handle for dealing with XA transactions.

mod resource_manager;
pub use self::resource_manager::{new_resource_manager, HdbResourceManager};

use hdb_error::HdbError;
use protocol::protocol_error::PrtError;
use dist_tx::rm::{Kind, RmError};
use std::error::Error;


impl From<PrtError> for RmError {
    fn from(error: PrtError) -> RmError {
        match error {
            PrtError::DbMessage(v) => {
                let mut s = String::new();
                for message in v {
                    s.push_str(&message.to_string())
                }
                RmError::new(Kind::RmError, s)
            }
            PrtError::Cesu8Error(err) => RmError::new(Kind::RmError, err.description().to_string()),
            PrtError::IoError(err) => RmError::new(Kind::RmError, err.description().to_string()),
            PrtError::ProtocolError(s) | PrtError::PoisonError(s) => RmError::new(Kind::RmError, s),
            PrtError::UsageError(s) => RmError::new(Kind::RmError, s.to_string()),
        }
    }
}

impl From<HdbError> for RmError {
    fn from(error: HdbError) -> RmError {
        match error {
            HdbError::ConversionError(e) => {
                RmError::new(Kind::RmError, e.description().to_string())
            }
            HdbError::DeserializationError(e) => {
                RmError::new(Kind::RmError, e.description().to_string())
            }
            HdbError::EvaluationError(s) | HdbError::PoisonError(s) => {
                RmError::new(Kind::RmError, s)
            }
            HdbError::FmtError(e) => RmError::new(Kind::RmError, e.description().to_string()),
            HdbError::InternalEvaluationError(s) => RmError::new(Kind::RmError, s.to_string()),
            HdbError::IoError(e) => RmError::new(Kind::RmError, e.description().to_string()),
            HdbError::ProtocolError(s) => RmError::new(Kind::RmError, s.to_string()),
            HdbError::SerializationError(e) => {
                RmError::new(Kind::RmError, e.description().to_string())
            }
            HdbError::UsageError(s) => RmError::new(Kind::RmError, s),
        }
    }
}
