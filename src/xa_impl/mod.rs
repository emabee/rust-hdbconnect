//! Handle for dealing with XA transactions.

use crate::HdbError;
use dist_tx::rm::{ErrorCode, RmError};

mod c_resource_manager;
pub(crate) use self::c_resource_manager::new_resource_manager;

impl From<HdbError> for RmError {
    fn from(error: HdbError) -> Self {
        Self::new(ErrorCode::RmError, format!("{:?}", error))
    }
}
