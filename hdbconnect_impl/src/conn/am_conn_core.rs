#[cfg(feature = "async")]
use crate::a_sync::AsyncRsState;
#[cfg(feature = "sync")]
use crate::sync::SyncRsState;

use crate::{
    conn::ConnectionCore,
    protocol::{
        parts::ResultSetMetadata,
        {Reply, Request},
    },
    ConnectParams, HdbError, HdbResult, ParameterDescriptors,
};
use std::{sync::Arc, time::Instant};

#[derive(Clone, Debug)]
pub struct AmConnCore(Arc<MConnCore>);

#[derive(Debug)]
pub enum MConnCore {
    #[cfg(feature = "sync")]
    Sync(std::sync::Mutex<ConnectionCore>),
    #[cfg(feature = "async")]
    Async(tokio::sync::Mutex<ConnectionCore>),
}

// An encapsulation of the ConnectionCore.
impl AmConnCore {
    #[cfg(feature = "sync")]
    pub fn try_new_sync(conn_params: ConnectParams) -> HdbResult<Self> {
        trace!("trying to connect to {}", conn_params);
        let start = Instant::now();
        let conn_core = ConnectionCore::try_new_sync(conn_params)?;
        {
            debug!(
                "user \"{}\" successfully logged on ({} µs) to {:?} of {:?} (HANA version: {:?})",
                conn_core.connect_params().dbuser(),
                Instant::now().duration_since(start).as_micros(),
                conn_core.connect_options().get_database_name(),
                conn_core.connect_options().get_system_id(),
                conn_core.connect_options().get_full_version_string()
            );
        }
        Ok(Self(Arc::new(MConnCore::Sync(std::sync::Mutex::new(
            conn_core,
        )))))
    }

    #[cfg(feature = "async")]
    pub async fn async_try_new(conn_params: ConnectParams) -> HdbResult<Self> {
        trace!("trying to connect to {}", conn_params);
        let start = Instant::now();
        let conn_core = ConnectionCore::try_new_async(conn_params).await?;

        debug!(
            "user \"{}\" successfully logged on ({} µs) to {:?} of {:?} (HANA version: {:?})",
            conn_core.connect_params().dbuser(),
            Instant::now().duration_since(start).as_micros(),
            conn_core.connect_options().get_database_name(),
            conn_core.connect_options().get_system_id(),
            conn_core.connect_options().get_full_version_string()
        );
        Ok(Self(Arc::new(MConnCore::Async(tokio::sync::Mutex::new(
            conn_core,
        )))))
    }

    #[cfg(feature = "sync")]
    pub fn sync_lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<ConnectionCore>> {
        match *self.0 {
            MConnCore::Sync(ref m_conn_core) => m_conn_core.lock(),
            #[cfg(feature = "async")]
            _ => {
                unreachable!("async not supported here");
            }
        }
    }

    #[cfg(feature = "async")]
    pub async fn async_lock(&self) -> tokio::sync::MutexGuard<ConnectionCore> {
        match *self.0 {
            MConnCore::Async(ref m_conn_core) => m_conn_core.lock().await,
            #[cfg(feature = "sync")]
            _ => {
                unreachable!("sync not supported here");
            }
        }
    }

    #[cfg(feature = "sync")]
    pub fn sync_send(&self, request: Request) -> HdbResult<Reply> {
        self.sync_full_send(request, None, None, &mut None)
    }

    #[cfg(feature = "async")]
    pub async fn async_send(&self, request: Request<'_>) -> HdbResult<Reply> {
        self.async_full_send(request, None, None, &mut None).await
    }

    #[cfg(feature = "sync")]
    pub(crate) fn sync_full_send(
        &self,
        mut request: Request,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut SyncRsState>,
    ) -> HdbResult<Reply> {
        trace!(
            "AmConnCore::full_send() with requestType = {:?}",
            request.message_type(),
        );
        let start = Instant::now();
        let mut conn_core = self.sync_lock()?;
        conn_core.augment_request(&mut request);

        let reply = conn_core.roundtrip_sync(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs);
        match reply {
            Ok(reply) => {
                trace!(
                    "full_send_sync() took {} ms",
                    Instant::now().duration_since(start).as_millis(),
                );
                Ok(reply)
            }
            Err(HdbError::Io { source })
                if std::io::ErrorKind::ConnectionReset == source.kind() =>
            {
                warn!("full_send_sync(): reconnecting after error of kind ConnectionReset ...");
                conn_core.sync_reconnect()?;
                warn!("full_send_sync(): repeating request after reconnect...");
                conn_core.roundtrip_sync(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs)
            }
            Err(e) => Err(e),
        }
    }

    #[cfg(feature = "async")]
    pub(crate) async fn async_full_send(
        &self,
        mut request: Request<'_>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut AsyncRsState>,
    ) -> HdbResult<Reply> {
        trace!(
            "AmConnCore::full_send() with requestType = {:?}",
            request.message_type(),
        );
        let start = Instant::now();
        let mut conn_core = self.async_lock().await;
        conn_core.augment_request(&mut request);

        let reply = conn_core
            .roundtrip_async(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs)
            .await;
        match reply {
            Ok(reply) => {
                trace!(
                    "full_send_sync() took {} ms",
                    Instant::now().duration_since(start).as_millis(),
                );
                Ok(reply)
            }
            Err(HdbError::Io { source })
                if std::io::ErrorKind::ConnectionReset == source.kind() =>
            {
                debug!("full_send_sync(): reconnecting after ConnectionReset error...");
                conn_core.async_reconnect().await?;
                debug!("full_send_sync(): repeating request after reconnect...");
                conn_core
                    .roundtrip_async(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs)
                    .await
            }
            Err(e) => Err(e),
        }
    }
}

// impl Clone for SyncAmConnCore {
//     fn clone(&self) -> Self {
//         Self(self.0.clone())
//     }
// }

// impl Clone for AsyncAmConnCore {
//     fn clone(&self) -> Self {
//         Self(self.0.clone())
//     }
// }
