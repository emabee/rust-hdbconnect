use crate::{
    base::{RsState, AM},
    conn::{ConnectionConfiguration, ConnectionCore},
    protocol::{
        parts::ResultSetMetadata,
        {Reply, Request},
    },
    ConnectParams, HdbError, HdbResult, ParameterDescriptors,
};
use std::{sync::Arc, time::Instant};

#[derive(Clone, Debug)]
pub(crate) struct AmConnCore(AM<ConnectionCore>);
impl AmConnCore {
    #[cfg(feature = "sync")]
    pub fn try_new_sync(
        conn_params: ConnectParams,
        configuration: &ConnectionConfiguration,
    ) -> HdbResult<Self> {
        trace!("trying to connect to {conn_params}");
        let start = Instant::now();
        let conn_core = ConnectionCore::try_new_sync(conn_params, configuration)?;
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
        Ok(Self(crate::base::new_am_sync(conn_core)))
    }
    #[cfg(feature = "async")]
    pub async fn try_new_async(
        conn_params: ConnectParams,
        configuration: &ConnectionConfiguration,
    ) -> HdbResult<Self> {
        trace!("trying to connect to {conn_params}");
        let start = Instant::now();
        let conn_core = ConnectionCore::try_new_async(conn_params, configuration).await?;

        debug!(
            "user \"{}\" successfully logged on ({} µs) to {:?} of {:?} (HANA version: {:?})",
            conn_core.connect_params().dbuser(),
            Instant::now().duration_since(start).as_micros(),
            conn_core.connect_options().get_database_name(),
            conn_core.connect_options().get_system_id(),
            conn_core.connect_options().get_full_version_string()
        );
        Ok(Self(crate::base::new_am_async(conn_core)))
    }

    #[cfg(feature = "sync")]
    pub fn lock_sync(&self) -> std::sync::LockResult<std::sync::MutexGuard<ConnectionCore>> {
        self.0.lock_sync()
    }
    #[cfg(feature = "async")]
    pub async fn lock_async(&self) -> tokio::sync::MutexGuard<ConnectionCore> {
        self.0.lock_async().await
    }

    #[cfg(feature = "sync")]
    pub fn send_sync(&self, request: Request) -> HdbResult<Reply> {
        self.full_send_sync(request, None, None, &mut None)
    }
    #[cfg(feature = "async")]
    pub async fn send_async(&self, request: Request<'_>) -> HdbResult<Reply> {
        self.full_send_async(request, None, None, &mut None).await
    }

    #[cfg(feature = "sync")]
    pub(crate) fn full_send_sync(
        &self,
        mut request: Request,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
    ) -> HdbResult<Reply> {
        trace!(
            "AmConnCore::full_send_sync() with requestType = {:?}",
            request.message_type(),
        );
        let start = Instant::now();
        let mut conn_core = self.lock_sync()?;
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
                if request.message_type().is_repeatable()
                    && !conn_core.configuration().is_auto_commit() =>
            {
                warn!(
                    "full_send_sync(): reconnecting after error of kind {} {}...",
                    source.kind(),
                    if can_be_timeout(source.kind()) {
                        "which can be caused by a read timeout"
                    } else {
                        ""
                    }
                );
                conn_core.reconnect_sync()?;
                warn!("full_send_sync(): repeating request after reconnect...");
                conn_core
                    .roundtrip_sync(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs)
                    .map_err(|e2| HdbError::ErrorAfterReconnect {
                        source,
                        second: Box::new(e2),
                    })
            }
            Err(e) => Err(e),
        }
    }
    #[cfg(feature = "async")]
    pub(crate) async fn full_send_async(
        &self,
        mut request: Request<'_>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
    ) -> HdbResult<Reply> {
        trace!(
            "AmConnCore::full_send_async() with requestType = {:?}",
            request.message_type(),
        );
        let start = Instant::now();
        let mut conn_core = self.lock_async().await;
        conn_core.augment_request(&mut request);

        let reply = conn_core
            .roundtrip_async(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs)
            .await;
        match reply {
            Ok(reply) => {
                trace!(
                    "full_send_async() took {} ms",
                    Instant::now().duration_since(start).as_millis(),
                );
                Ok(reply)
            }
            Err(HdbError::Io { source })
                if request.message_type().is_repeatable()
                    && !conn_core.configuration().is_auto_commit() =>
            {
                warn!(
                    "full_send_async(): reconnecting after error of kind {} {}...",
                    source.kind(),
                    if can_be_timeout(source.kind()) {
                        "which can be caused by a read timeout"
                    } else {
                        ""
                    }
                );
                conn_core.reconnect_async().await?;
                warn!("full_send_sync(): repeating request after reconnect...");
                conn_core
                    .roundtrip_async(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs)
                    .await
                    .map_err(|e2| HdbError::ErrorAfterReconnect {
                        source,
                        second: Box::new(e2),
                    })
            }
            Err(e) => Err(e),
        }
    }
}

fn can_be_timeout(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::WouldBlock // typical read timeout on linux
            | std::io::ErrorKind::TimedOut // typical read timeout on windows
    )
}
