//! Handle for dealing with XA transactions.

mod c_resource_manager;

pub(crate) use self::c_resource_manager::new_resource_manager;

use dist_tx::rm::{ErrorCode, RmError};
use std::error::Error;
use HdbError;

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
            HdbError::Usage(s)
            | HdbError::Evaluation(s)
            | HdbError::Poison(s)
            | HdbError::DbIssue(s) => RmError::new(ErrorCode::RmError, s),
            HdbError::Impl(s) => RmError::new(ErrorCode::RmError, s.to_string()),
            HdbError::Io(e) => RmError::new(ErrorCode::RmError, e.description().to_string()),
            HdbError::Serialization(e) => {
                RmError::new(ErrorCode::RmError, e.description().to_string())
            }
        }
    }
}
