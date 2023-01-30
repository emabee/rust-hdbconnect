//! Handle for dealing with XA transactions.
#[cfg(feature = "async")]
mod async_c_resource_manager;
#[cfg(feature = "sync")]
mod sync_c_resource_manager;

use crate::HdbError;
#[cfg(feature = "dist_tx")]
use dist_tx::{ErrorCode, RmError};

#[cfg(feature = "async")]
pub use self::async_c_resource_manager::async_new_resource_manager;
#[cfg(feature = "sync")]
pub use self::sync_c_resource_manager::sync_new_resource_manager;

impl From<HdbError> for RmError {
    fn from(error: HdbError) -> Self {
        Self::new(ErrorCode::RmError, format!("{error:?}"))
    }
}
