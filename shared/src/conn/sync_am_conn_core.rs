use crate::conn::ConnectionCore;
use crate::protocol::parts::{ResultSetMetadata, RsState};
use crate::protocol::{Reply, Request};
use crate::{ConnectParams, HdbError, HdbResult, ParameterDescriptors};
use std::sync::{Arc, LockResult, Mutex, MutexGuard};
use std::time::Instant;

// A thread-safe encapsulation of the ConnectionCore.
#[derive(Debug)]
pub struct SyncAmConnCore(Arc<Mutex<ConnectionCore>>);

impl SyncAmConnCore {
    pub fn try_new(conn_params: ConnectParams) -> HdbResult<Self> {
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
        Ok(Self(Arc::new(Mutex::new(conn_core))))
    }

    pub fn lock(&self) -> LockResult<MutexGuard<ConnectionCore>> {
        self.0.lock()
    }

    pub fn send(&mut self, request: Request) -> HdbResult<Reply> {
        self.full_send(request, None, None, &mut None)
    }

    pub fn full_send(
        &mut self,
        mut request: Request,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
    ) -> HdbResult<Reply> {
        trace!(
            "AmConnCore::full_send() with requestType = {:?}",
            request.request_type,
        );
        let start = Instant::now();
        let mut conn_core = self.lock()?;
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
                debug!("full_send_sync(): reconnecting after ConnectionReset error...");
                conn_core.reconnect()?;
                debug!("full_send_sync(): repeating request after reconnect...");
                conn_core.roundtrip_sync(&request, Some(self), o_a_rsmd, o_a_descriptors, o_rs)
            }
            Err(e) => Err(e),
        }
    }
}

impl Clone for SyncAmConnCore {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
