use crate::conn::ConnectionCore;
use crate::protocol::parts::{ResultSetMetadata, RsState};
use crate::protocol::{Reply, Request};
use crate::{ConnectParams, HdbError, HdbResult, ParameterDescriptors};
use chrono::Local;
use std::sync::{Arc, LockResult, Mutex, MutexGuard};

// A thread-safe encapsulation of the ConnectionCore.
#[derive(Debug)]
pub(crate) struct AmConnCore(Arc<Mutex<ConnectionCore>>);

impl AmConnCore {
    pub fn try_new(conn_params: ConnectParams) -> HdbResult<Self> {
        trace!("trying to connect to {}", conn_params);
        let start = Local::now();
        let conn_core = ConnectionCore::try_new(conn_params)?;
        {
            debug!(
                "user \"{}\" successfully logged on ({} µs) to {:?} of {:?} (HANA version: {:?})",
                conn_core.connect_params().dbuser(),
                Local::now()
                    .signed_duration_since(start)
                    .num_microseconds()
                    .unwrap_or(-1),
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

    pub fn send_sync(&mut self, request: Request) -> HdbResult<Reply> {
        self.full_send_sync(request, None, None, &mut None)
    }

    pub fn full_send_sync(
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
        let start = Local::now();
        let mut conn_core = self.lock()?;
        conn_core.augment_request(&mut request);

        match conn_core.roundtrip_sync(&request, Some(&self), o_a_rsmd, o_a_descriptors, o_rs) {
            Ok(reply) => {
                trace!(
                    "full_send_sync() took {} ms",
                    (Local::now().signed_duration_since(start)).num_milliseconds()
                );
                Ok(reply)
            }
            Err(HdbError::Tcp { source })
                if std::io::ErrorKind::ConnectionReset == source.kind() =>
            {
                debug!("full_send_sync(): reconnecting after ConnectionReset error...");
                conn_core.reconnect()?;
                debug!("full_send_sync(): repeating request after reconnect...");
                conn_core.roundtrip_sync(&request, Some(&self), o_a_rsmd, o_a_descriptors, o_rs)
            }
            Err(e) => Err(e),
        }
    }
}

impl Clone for AmConnCore {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
