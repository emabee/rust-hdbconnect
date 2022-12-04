use crate::conn::ConnectionCore;
use crate::protocol::parts::{ResultSetMetadata, RsState};
use crate::protocol::{Reply, Request};
use crate::{ConnectParams, HdbError, HdbResult, ParameterDescriptors};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, MutexGuard};

// A thread-safe encapsulation of the ConnectionCore.
#[derive(Debug)]
pub struct AsyncAmConnCore(Arc<Mutex<ConnectionCore>>);

impl AsyncAmConnCore {
    pub async fn try_new(conn_params: ConnectParams) -> HdbResult<Self> {
        trace!("trying to connect to {}", conn_params);
        let start = Instant::now();
        let conn_core = ConnectionCore::try_new_async(conn_params).await?;
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
        Ok(Self(Arc::new(Mutex::new(conn_core))))
    }

    pub async fn lock(&self) -> MutexGuard<ConnectionCore> {
        self.0.lock().await
    }

    pub async fn send(&mut self, request: Request<'_>) -> HdbResult<Reply> {
        self.full_send(request, None, None, &mut None).await
    }
    pub async fn full_send(
        &mut self,
        mut request: Request<'_>,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
    ) -> HdbResult<Reply> {
        trace!(
            "AmConnCore::full_send() with requestType = {:?}",
            request.request_type,
        );
        let start = Instant::now();
        let mut conn_core = self.lock().await;
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
                conn_core.reconnect().await?;
                debug!("full_send_sync(): repeating request after reconnect...");
                conn_core
                    .roundtrip_async(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs)
                    .await
            }
            Err(e) => Err(e),
        }
    }
}

impl Clone for AsyncAmConnCore {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
