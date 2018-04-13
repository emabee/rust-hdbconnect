//! Handle for dealing with XA transactions.

mod c_resource_manager;

pub use self::c_resource_manager::{new_resource_manager, HdbCResourceManager};

use HdbError;
use dist_tx::rm::{ErrorCode, RmError};
use std::error::Error;

impl From<HdbError> for RmError {
    fn from(error: HdbError) -> RmError {
        match error {
            HdbError::Cesu8(e) => RmError::new(ErrorCode::RmError, e.description().to_string()),
            HdbError::DbError(se) => RmError::new(ErrorCode::RmError, se.to_string()),
            HdbError::MultipleDbErrors(se) => RmError::new(ErrorCode::RmError, se[0].to_string()),
            HdbError::Conversion(e) => {
                RmError::new(ErrorCode::RmError, e.description().to_string())
            }
            HdbError::Deserialization(e) => {
                RmError::new(ErrorCode::RmError, e.description().to_string())
            }
            HdbError::Usage(s) | HdbError::Evaluation(s) | HdbError::Poison(s) => {
                RmError::new(ErrorCode::RmError, s)
            }
            HdbError::Impl(s) => RmError::new(ErrorCode::RmError, s.to_string()),
            HdbError::Io(e) => RmError::new(ErrorCode::RmError, e.description().to_string()),
            HdbError::Serialization(e) => {
                RmError::new(ErrorCode::RmError, e.description().to_string())
            }
        }
    }
}
