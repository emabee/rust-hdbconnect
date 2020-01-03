use super::connection_core::ConnectionCore;
use crate::conn::ConnectParams;
use crate::hdb_error::HdbResult;
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::resultset::RsState;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::reply::Reply;
use crate::protocol::request::Request;
use chrono::Local;
use std::sync::{Arc, LockResult, Mutex, MutexGuard};

// A thread-safe encapsulation of the ConnectionCore.
#[derive(Debug)]
pub(crate) struct AmConnCore(Arc<Mutex<ConnectionCore>>);

impl AmConnCore {
    pub fn try_new(conn_params: ConnectParams) -> HdbResult<Self> {
        let conn_core = ConnectionCore::try_new(conn_params)?;
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

        match conn_core.statement_sequence() {
            Some(ssi_value) => request.add_statement_context(*ssi_value),
            None => {}
        }

        if conn_core.is_client_info_touched() {
            request.push(Part::new(
                PartKind::ClientInfo,
                Argument::ClientInfo(conn_core.get_client_info_for_sending()),
            ));
        }

        let reply = conn_core.roundtrip_sync(request, &self, o_a_rsmd, o_a_descriptors, o_rs)?;

        debug!(
            "AmConnCore::full_send() took {} ms",
            (Local::now().signed_duration_since(start)).num_milliseconds()
        );
        Ok(reply)
    }
}

impl Clone for AmConnCore {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
