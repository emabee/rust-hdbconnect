#[cfg(feature = "async")]
use crate::protocol::parts::async_rs_state::AsyncResultSetCore;
#[cfg(feature = "sync")]
use crate::{protocol::parts::sync_rs_state::SyncResultSetCore, HdbResult};
use std::sync::Arc;

pub(crate) type AmRsCore = Arc<MRsCore>;

#[derive(Debug)]
pub(crate) enum MRsCore {
    #[cfg(feature = "sync")]
    Sync(std::sync::Mutex<SyncResultSetCore>),
    #[cfg(feature = "async")]
    Async(tokio::sync::Mutex<AsyncResultSetCore>),
}
impl MRsCore {
    #[cfg(feature = "sync")]
    pub(crate) fn sync_lock(&self) -> HdbResult<std::sync::MutexGuard<SyncResultSetCore>> {
        match self {
            MRsCore::Sync(m_rscore) => Ok(m_rscore.lock()?),
            #[cfg(feature = "async")]
            _ => unimplemented!("async not supported here"),
        }
    }
    #[cfg(feature = "async")]
    pub(crate) async fn async_lock(&self) -> tokio::sync::MutexGuard<AsyncResultSetCore> {
        match self {
            MRsCore::Async(m_rscore) => m_rscore.lock().await,
            #[cfg(feature = "sync")]
            _ => unimplemented!("sync not supported here"),
        }
    }
}
