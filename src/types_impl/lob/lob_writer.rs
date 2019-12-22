use crate::conn_core::AmConnCore;
use crate::hdb_response::InternalReturnValue;
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::parts::type_id::TypeId;
use crate::protocol::parts::write_lob_request::WriteLobRequest;
use crate::protocol::reply::Reply;
use crate::protocol::reply_type::ReplyType;
use crate::protocol::request::Request;
use crate::protocol::request_type::RequestType;
use crate::protocol::server_usage::ServerUsage;
use crate::protocol::util;
use crate::{HdbError, HdbResult};
use std::io::Write;
use std::sync::Arc;

pub(crate) struct LobWriter<'a> {
    locator_id: u64,
    type_id: TypeId,
    am_conn_core: AmConnCore,
    o_a_rsmd: Option<&'a Arc<ResultSetMetadata>>,
    o_a_descriptors: Option<&'a Arc<ParameterDescriptors>>,
    server_usage: ServerUsage,
    buffer: Vec<u8>,
    lob_write_length: usize,
    proc_result: Option<Vec<InternalReturnValue>>,
}
impl<'a> LobWriter<'a> {
    pub fn new(
        locator_id: u64,
        type_id: TypeId,
        am_conn_core: AmConnCore,
        o_a_rsmd: Option<&'a Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&'a Arc<ParameterDescriptors>>,
    ) -> HdbResult<LobWriter<'a>> {
        if let TypeId::BLOB | TypeId::CLOB | TypeId::NCLOB = type_id {
            // ok
        } else {
            return Err(HdbError::imp_detailed(format!(
                "Unsupported type-id {:?}",
                type_id
            )));
        }
        let lob_write_length = am_conn_core.lock()?.get_lob_write_length();
        Ok(LobWriter {
            locator_id,
            type_id,
            am_conn_core,
            o_a_rsmd,
            o_a_descriptors,
            server_usage: Default::default(),
            buffer: Vec::<u8>::with_capacity(lob_write_length + 8200),
            lob_write_length,
            proc_result: None,
        })
    }

    pub fn into_internal_return_values(self) -> Option<Vec<InternalReturnValue>> {
        self.proc_result
    }

    // Note that requested_length and offset count either bytes (for BLOB, CLOB),
    // or 1-2-3-chars (for NCLOB)
    fn write_a_lob_chunk(&mut self, lob_write_mode: LobWriteMode) -> HdbResult<Vec<u64>> {
        let mut request = Request::new(RequestType::WriteLob, 0);
        let write_lob_request = match lob_write_mode {
            // LobWriteMode::Offset(offset, buf) =>
            //     WriteLobRequest::new(locator_id, offset /* or offset + 1? */, buf, true),
            LobWriteMode::Append(buf) => WriteLobRequest::new(self.locator_id, -1_i64, buf, false),
            LobWriteMode::Last(buf) => WriteLobRequest::new(self.locator_id, -1_i64, buf, true),
        };
        request.push(Part::new(
            PartKind::WriteLobRequest,
            Argument::WriteLobRequest(write_lob_request),
        ));

        let reply =
            self.am_conn_core
                .full_send(request, self.o_a_rsmd, self.o_a_descriptors, &mut None)?;

        match reply.replytype {
            // regular response
            ReplyType::WriteLob => self.evaluate_write_lob_reply(reply),

            // last response of last IN parameter
            ReplyType::DbProcedureCall => self.evaluate_dbprocedure_call_reply(reply),

            _ => Err(HdbError::imp_detailed(format!(
                "LobWriter::write_a_lob_chunk got a reply of type {:?}",
                reply.replytype,
            ))),
        }
    }

    fn evaluate_write_lob_reply(&mut self, mut reply: Reply) -> HdbResult<Vec<u64>> {
        let (server_proc_time, server_cpu_time, server_memory_usage) = match reply
            .parts
            .pop_if_kind(PartKind::StatementContext)
            .map(Part::into_arg)
        {
            Some(Argument::StatementContext(stmt_ctx)) => (
                stmt_ctx.server_processing_time(),
                stmt_ctx.server_cpu_time(),
                stmt_ctx.server_memory_usage(),
            ),
            None => (None, None, None),
            _ => {
                return Err(HdbError::imp(
                    "Inconsistent StatementContext part found for ResultSet",
                ));
            }
        };
        self.server_usage
            .update(server_proc_time, server_cpu_time, server_memory_usage);

        if let Some(Argument::TransactionFlags(ta_flags)) = reply
            .parts
            .pop_if_kind(PartKind::TransactionFlags)
            .map(Part::into_arg)
        {
            if ta_flags.is_committed() {
                trace!("is committed");
            } else {
                trace!("is not committed");
            }
        }

        match reply
            .parts
            .pop_if_kind(PartKind::WriteLobReply)
            .map(Part::into_arg)
        {
            Some(Argument::WriteLobReply(write_lob_reply)) => Ok(write_lob_reply.into_locator_ids()),
            _ => Err(HdbError::imp_detailed(format!(
                "No WriteLobReply part found; parts = {:?}",
                reply.parts
            ))),
        }
    }

    fn evaluate_dbprocedure_call_reply(&mut self, mut reply: Reply) -> HdbResult<Vec<u64>> {
        let (server_proc_time, server_cpu_time, server_memory_usage) = match reply
            .parts
            .pop_if_kind(PartKind::StatementContext)
            .map(Part::into_arg)
        {
            Some(Argument::StatementContext(stmt_ctx)) => (
                stmt_ctx.server_processing_time(),
                stmt_ctx.server_cpu_time(),
                stmt_ctx.server_memory_usage(),
            ),
            None => (None, None, None),
            _ => {
                return Err(HdbError::imp("Inconsistent StatementContext found"));
            }
        };
        self.server_usage
            .update(server_proc_time, server_cpu_time, server_memory_usage);

        if let Some(Argument::TransactionFlags(ta_flags)) = reply
            .parts
            .pop_if_kind(PartKind::TransactionFlags)
            .map(Part::into_arg)
        {
            if ta_flags.is_committed() {
                trace!("is committed");
            } else {
                trace!("is not committed");
            }
        }

        let locator_ids = match reply
            .parts
            .pop_if_kind(PartKind::WriteLobReply)
            .map(Part::into_arg)
        {
            Some(Argument::WriteLobReply(write_lob_reply)) => write_lob_reply.into_locator_ids(),
            _ => Default::default(),
        };

        reply.parts.remove_first_of_kind(PartKind::WriteLobReply);

        let (internal_return_values, _) = reply
            .into_internal_return_values(&mut self.am_conn_core, Some(&mut self.server_usage))?;

        self.proc_result = Some(internal_return_values);
        Ok(locator_ids)
    }
}

impl<'a> Write for LobWriter<'a> {
    // Either buffers (in self.buffer) or writes buffer + input to the db
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        trace!("write() with input of len {}", input.len());
        if input.len() + self.buffer.len() < self.lob_write_length {
            self.buffer.append(&mut input.to_vec());
        } else {
            // concatenate buffer and input into payload_raw
            let payload_raw = if self.buffer.is_empty() {
                input.to_vec()
            } else {
                let mut payload_raw = Vec::<u8>::new();
                std::mem::swap(&mut payload_raw, &mut self.buffer);
                payload_raw.append(&mut input.to_vec());
                payload_raw
            };
            debug_assert!(self.buffer.is_empty());

            // if necessary, cut off new tail and convert to cesu8
            let payload = if let TypeId::CLOB | TypeId::NCLOB = self.type_id {
                let (cesu8, utf8_tail) = util::utf8_to_cesu8_and_utf8_tail(payload_raw)
                    .map_err(|e| util::io_error(e.to_string()))?;
                self.buffer = utf8_tail;
                cesu8
            } else {
                payload_raw
            };

            self.write_a_lob_chunk(LobWriteMode::Append(&payload))
                .map(|_locator_ids| ())
                .map_err(|e| util::io_error(e.to_string()))?;
        }
        Ok(input.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        trace!("flush(), with buffer of {} bytes", self.buffer.len());
        let mut payload_raw = Vec::<u8>::new();
        std::mem::swap(&mut payload_raw, &mut self.buffer);
        let payload = if let TypeId::CLOB | TypeId::NCLOB = self.type_id {
            let (cesu8, utf8_tail) = util::utf8_to_cesu8_and_utf8_tail(payload_raw)
                .map_err(|e| util::io_error(e.to_string()))?;
            if !utf8_tail.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "stream is ending with invalid utf-8",
                ));
            }
            cesu8
        } else {
            payload_raw
        };

        self.write_a_lob_chunk(LobWriteMode::Last(&payload))
            .map(|_locator_ids| ())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }
}

enum LobWriteMode<'a> {
    //Offset(i64, &'a [u8]),
    Append(&'a [u8]),
    Last(&'a [u8]),
}
