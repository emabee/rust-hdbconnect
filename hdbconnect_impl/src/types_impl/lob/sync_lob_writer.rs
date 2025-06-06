use super::lob_writer_util::{LobWriteMode, get_utf8_tail_len};
use crate::{
    HdbResult, ServerUsage,
    base::InternalReturnValue,
    conn::{AmConnCore, CommandOptions},
    impl_err,
    protocol::{
        MessageType, Part, PartKind, Reply, ReplyType, Request,
        parts::{ParameterDescriptors, ResultSetMetadata, TypeId, WriteLobRequest},
    },
};
use std::{io::Write, sync::Arc};

#[derive(Debug)]
pub(crate) struct SyncLobWriter<'a> {
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
impl<'a> SyncLobWriter<'a> {
    pub fn new(
        locator_id: u64,
        type_id: TypeId,
        am_conn_core: AmConnCore,
        o_a_rsmd: Option<&'a Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&'a Arc<ParameterDescriptors>>,
    ) -> HdbResult<SyncLobWriter<'a>> {
        if let TypeId::BLOB | TypeId::CLOB | TypeId::NCLOB = type_id {
            let lob_write_length =
                am_conn_core.lock_sync()?.configuration().lob_write_length() as usize;
            Ok(SyncLobWriter {
                locator_id,
                type_id,
                am_conn_core,
                o_a_rsmd,
                o_a_descriptors,
                server_usage: ServerUsage::default(),
                buffer: Vec::<u8>::with_capacity(lob_write_length + 8200),
                lob_write_length,
                proc_result: None,
            })
        } else {
            Err(impl_err!("Unsupported type-id {type_id:?}"))
        }
    }

    pub fn into_internal_return_values(self) -> Option<Vec<InternalReturnValue>> {
        self.proc_result
    }

    // Note that requested_length and offset count either bytes (for BLOB, CLOB),
    // or 1-2-3-chars (for NCLOB)
    fn write_a_lob_chunk(
        &mut self,
        buf: &[u8],
        locator_id: u64,
        lob_write_mode: &LobWriteMode,
    ) -> HdbResult<Vec<u64>> {
        let mut request = Request::new(MessageType::WriteLob, CommandOptions::EMPTY);
        let write_lob_request = WriteLobRequest::new(
            locator_id,
            -1_i64,
            buf,
            match lob_write_mode {
                LobWriteMode::Append => false,
                LobWriteMode::Last => true,
            },
        );
        // LobWriteMode::Offset(offset) =>
        //     WriteLobRequest::new(locator_id, offset /* or offset + 1? */, buf, true),
        request.push(Part::WriteLobRequest(write_lob_request));

        let reply = self.am_conn_core.full_send_sync(
            request,
            self.o_a_rsmd,
            self.o_a_descriptors,
            &mut None,
        )?;

        match reply.replytype {
            // regular response
            ReplyType::WriteLob => self.evaluate_write_lob_reply(reply),

            // last response of last IN parameter
            ReplyType::DbProcedureCall => self.evaluate_dbprocedure_call_reply(reply),

            _ => Err(impl_err!(
                "LobWriter::write_a_lob_chunk got a reply of type {:?}",
                reply.replytype,
            )),
        }
    }

    fn evaluate_write_lob_reply(&mut self, reply: Reply) -> HdbResult<Vec<u64>> {
        let mut result = None;

        for part in reply.parts {
            match part {
                Part::StatementContext(stmt_ctx) => {
                    self.server_usage.update(
                        stmt_ctx.server_processing_time(),
                        stmt_ctx.server_cpu_time(),
                        stmt_ctx.server_memory_usage(),
                    );
                }
                Part::TransactionFlags(ta_flags) => {
                    if ta_flags.is_committed() {
                        trace!("is committed");
                    } else {
                        trace!("is not committed");
                    }
                }
                Part::ExecutionResults(_) => {
                    //todo can we do better than just ignore this?
                }
                Part::WriteLobReply(write_lob_reply) => {
                    result = Some(write_lob_reply.into_locator_ids());
                }

                _ => warn!(
                    "evaluate_write_lob_reply: unexpected part {:?}",
                    part.kind()
                ),
            }
        }

        result.ok_or_else(|| impl_err!("No WriteLobReply part found"))
    }

    fn evaluate_dbprocedure_call_reply(&mut self, mut reply: Reply) -> HdbResult<Vec<u64>> {
        let locator_ids = self.evaluate_dbprocedure_call_reply1(&mut reply)?;
        let internal_return_values = reply
            .parts
            .into_internal_return_values_sync(&self.am_conn_core, Some(&mut self.server_usage))?;

        self.proc_result = Some(internal_return_values);
        Ok(locator_ids)
    }

    fn evaluate_dbprocedure_call_reply1(&mut self, reply: &mut Reply) -> HdbResult<Vec<u64>> {
        let (server_proc_time, server_cpu_time, server_memory_usage) =
            match reply.parts.pop_if_kind(PartKind::StatementContext) {
                Some(Part::StatementContext(stmt_ctx)) => (
                    stmt_ctx.server_processing_time(),
                    stmt_ctx.server_cpu_time(),
                    stmt_ctx.server_memory_usage(),
                ),
                None => (None, None, None),
                Some(_) => {
                    return Err(impl_err!("Inconsistent StatementContext found"));
                }
            };
        self.server_usage
            .update(server_proc_time, server_cpu_time, server_memory_usage);

        if let Some(Part::TransactionFlags(ta_flags)) =
            reply.parts.pop_if_kind(PartKind::TransactionFlags)
        {
            if ta_flags.is_committed() {
                trace!("is committed");
            } else {
                trace!("is not committed");
            }
        }

        let locator_ids = match reply.parts.pop_if_kind(PartKind::WriteLobReply) {
            Some(Part::WriteLobReply(write_lob_reply)) => write_lob_reply.into_locator_ids(),
            _ => Vec::default(),
        };

        reply.parts.remove_first_of_kind(PartKind::WriteLobReply);
        Ok(locator_ids)
    }
}

impl Write for SyncLobWriter<'_> {
    // Either buffers (in self.buffer) or writes buffer + input to the db
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        trace!("write() with input of len {}", input.len());
        if input.len() + self.buffer.len() < self.lob_write_length {
            self.buffer.extend_from_slice(input);
        } else {
            // concatenate buffer and input into payload_raw
            let payload_raw = if self.buffer.is_empty() {
                input.to_vec()
            } else {
                let mut payload_raw = Vec::<u8>::new();
                std::mem::swap(&mut payload_raw, &mut self.buffer);
                payload_raw.extend_from_slice(input);
                payload_raw
            };
            debug_assert!(self.buffer.is_empty());

            // if necessary, cut off new tail and convert to cesu8
            let payload = if let TypeId::CLOB | TypeId::NCLOB = self.type_id {
                let (cesu8, utf8_tail) = utf8_to_cesu8_and_utf8_tail(payload_raw)?;
                self.buffer = utf8_tail;
                cesu8
            } else {
                payload_raw
            };

            self.write_a_lob_chunk(&payload, self.locator_id, &LobWriteMode::Append)
                .map(|_locator_ids| ())
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        Ok(input.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        trace!("flush(), with buffer of {} bytes", self.buffer.len());
        let mut payload_raw = Vec::<u8>::new();
        std::mem::swap(&mut payload_raw, &mut self.buffer);
        let payload = if let TypeId::CLOB | TypeId::NCLOB = self.type_id {
            let (cesu8, utf8_tail) = utf8_to_cesu8_and_utf8_tail(payload_raw)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            if !utf8_tail.is_empty() {
                return Err(std::io::Error::other("stream ending with invalid utf-8"));
            }
            cesu8
        } else {
            payload_raw
        };

        self.write_a_lob_chunk(&payload, self.locator_id, &LobWriteMode::Last)
            .map(|_locator_ids| ())
            .map_err(std::io::Error::other)?;
        Ok(())
    }
}

fn utf8_to_cesu8_and_utf8_tail(mut utf8: Vec<u8>) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let tail_len = get_utf8_tail_len(&utf8).map_err(std::io::Error::other)?;
    let tail = utf8.split_off(utf8.len() - tail_len);
    Ok((
        cesu8::to_cesu8(&String::from_utf8(utf8).map_err(std::io::Error::other)?).to_vec(),
        tail,
    ))
}
