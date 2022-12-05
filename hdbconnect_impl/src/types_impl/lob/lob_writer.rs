#[cfg(feature = "async")]
use crate::conn::AsyncAmConnCore;
#[cfg(feature = "sync")]
use crate::conn::SyncAmConnCore;

use crate::hdb_response::InternalReturnValue;
use crate::protocol::parts::{ParameterDescriptors, ResultSetMetadata, TypeId, WriteLobRequest};
use crate::protocol::{util, Part, PartKind, Reply, ReplyType, Request, RequestType};
use crate::{HdbError, HdbResult, ServerUsage};
#[cfg(feature = "sync")]
use std::io::Write;
use std::sync::Arc;

#[derive(Debug)]
pub struct LobWriter<'a> {
    locator_id: u64,
    type_id: TypeId,
    #[cfg(feature = "sync")]
    am_conn_core: SyncAmConnCore,
    #[cfg(feature = "async")]
    am_conn_core: AsyncAmConnCore,
    o_a_rsmd: Option<&'a Arc<ResultSetMetadata>>,
    o_a_descriptors: Option<&'a Arc<ParameterDescriptors>>,
    server_usage: ServerUsage,
    buffer: Vec<u8>,
    lob_write_length: usize,
    proc_result: Option<Vec<InternalReturnValue>>,
}
impl<'a> LobWriter<'a> {
    #[cfg(feature = "sync")]
    pub fn new(
        locator_id: u64,
        type_id: TypeId,
        am_conn_core: SyncAmConnCore,
        o_a_rsmd: Option<&'a Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&'a Arc<ParameterDescriptors>>,
    ) -> HdbResult<LobWriter<'a>> {
        if let TypeId::BLOB | TypeId::CLOB | TypeId::NCLOB = type_id {
            let lob_write_length = am_conn_core.lock()?.get_lob_write_length();
            Ok(LobWriter {
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
            Err(HdbError::ImplDetailed(format!(
                "Unsupported type-id {type_id:?}"
            )))
        }
    }
    #[cfg(feature = "async")]
    pub async fn new(
        locator_id: u64,
        type_id: TypeId,
        am_conn_core: AsyncAmConnCore,
        o_a_rsmd: Option<&'a Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&'a Arc<ParameterDescriptors>>,
    ) -> HdbResult<LobWriter<'a>> {
        if let TypeId::BLOB | TypeId::CLOB | TypeId::NCLOB = type_id {
            let lob_write_length = am_conn_core.lock().await.get_lob_write_length();
            Ok(LobWriter {
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
            Err(HdbError::ImplDetailed(format!(
                "Unsupported type-id {type_id:?}"
            )))
        }
    }

    pub fn into_internal_return_values(self) -> Option<Vec<InternalReturnValue>> {
        self.proc_result
    }

    // Note that requested_length and offset count either bytes (for BLOB, CLOB),
    // or 1-2-3-chars (for NCLOB)
    #[cfg(feature = "sync")]
    fn sync_write_a_lob_chunk(&mut self, lob_write_mode: &LobWriteMode) -> HdbResult<Vec<u64>> {
        let mut request = Request::new(RequestType::WriteLob, 0);
        let write_lob_request = match lob_write_mode {
            // LobWriteMode::Offset(offset, buf) =>
            //     WriteLobRequest::new(locator_id, offset /* or offset + 1? */, buf, true),
            LobWriteMode::Append(buf) => WriteLobRequest::new(self.locator_id, -1_i64, buf, false),
            LobWriteMode::Last(buf) => WriteLobRequest::new(self.locator_id, -1_i64, buf, true),
        };
        request.push(Part::WriteLobRequest(write_lob_request));

        let reply =
            self.am_conn_core
                .full_send(request, self.o_a_rsmd, self.o_a_descriptors, &mut None)?;

        match reply.replytype {
            // regular response
            ReplyType::WriteLob => self.evaluate_write_lob_reply(reply),

            // last response of last IN parameter
            ReplyType::DbProcedureCall => self.sync_evaluate_dbprocedure_call_reply(reply),

            _ => Err(HdbError::ImplDetailed(format!(
                "LobWriter::write_a_lob_chunk got a reply of type {:?}",
                reply.replytype,
            ))),
        }
    }

    // Note that requested_length and offset count either bytes (for BLOB, CLOB),
    // or 1-2-3-chars (for NCLOB)
    #[cfg(feature = "async")]
    async fn async_write_a_lob_chunk(
        &mut self,
        lob_write_mode: &LobWriteMode<'_>,
    ) -> HdbResult<Vec<u64>> {
        let mut request = Request::new(RequestType::WriteLob, 0);
        let write_lob_request = match lob_write_mode {
            // LobWriteMode::Offset(offset, buf) =>
            //     WriteLobRequest::new(locator_id, offset /* or offset + 1? */, buf, true),
            LobWriteMode::Append(buf) => WriteLobRequest::new(self.locator_id, -1_i64, buf, false),
            LobWriteMode::Last(buf) => WriteLobRequest::new(self.locator_id, -1_i64, buf, true),
        };
        request.push(Part::WriteLobRequest(write_lob_request));

        let reply = self
            .am_conn_core
            .full_send(request, self.o_a_rsmd, self.o_a_descriptors, &mut None)
            .await?;

        match reply.replytype {
            // regular response
            ReplyType::WriteLob => self.evaluate_write_lob_reply(reply),

            // last response of last IN parameter
            ReplyType::DbProcedureCall => self.async_evaluate_dbprocedure_call_reply(reply).await,

            _ => Err(HdbError::ImplDetailed(format!(
                "LobWriter::write_a_lob_chunk got a reply of type {:?}",
                reply.replytype,
            ))),
        }
    }

    fn evaluate_write_lob_reply(&mut self, reply: Reply) -> HdbResult<Vec<u64>> {
        let mut result = None;

        for part in reply.parts.into_iter() {
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
                Part::ExecutionResult(_) => {
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

        result.ok_or_else(|| HdbError::Impl("No WriteLobReply part found"))
    }

    #[cfg(feature = "sync")]
    fn sync_evaluate_dbprocedure_call_reply(&mut self, mut reply: Reply) -> HdbResult<Vec<u64>> {
        let locator_ids = self.evaluate_dbprocedure_call_reply1(&mut reply)?;
        let internal_return_values = reply.parts.sync_into_internal_return_values(
            &mut self.am_conn_core,
            Some(&mut self.server_usage),
        )?;

        self.proc_result = Some(internal_return_values);
        Ok(locator_ids)
    }

    #[cfg(feature = "async")]
    async fn async_evaluate_dbprocedure_call_reply(
        &mut self,
        mut reply: Reply,
    ) -> HdbResult<Vec<u64>> {
        let locator_ids = self.evaluate_dbprocedure_call_reply1(&mut reply)?;
        let internal_return_values = reply
            .parts
            .async_into_internal_return_values(&mut self.am_conn_core, Some(&mut self.server_usage))
            .await?;

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
                    return Err(HdbError::Impl("Inconsistent StatementContext found"));
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

// FIXME add analogon for async
#[cfg(feature = "sync")]
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
                let (cesu8, utf8_tail) = utf8_to_cesu8_and_utf8_tail(payload_raw)?;
                self.buffer = utf8_tail;
                cesu8
            } else {
                payload_raw
            };

            self.sync_write_a_lob_chunk(&LobWriteMode::Append(&payload))
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
            let (cesu8, utf8_tail) = utf8_to_cesu8_and_utf8_tail(payload_raw)
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

        self.sync_write_a_lob_chunk(&LobWriteMode::Last(&payload))
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

fn utf8_to_cesu8_and_utf8_tail(mut utf8: Vec<u8>) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let tail_len = get_utf8_tail_len(&utf8)?;
    let tail = utf8.split_off(utf8.len() - tail_len);
    Ok((
        cesu8::to_cesu8(&String::from_utf8(utf8).map_err(util::io_error)?).to_vec(),
        tail,
    ))
}

fn get_utf8_tail_len(bytes: &[u8]) -> std::io::Result<usize> {
    match bytes.last() {
        None | Some(0..=127) => Ok(0),
        Some(0xC0..=0xDF) => Ok(1),
        Some(_) => {
            let len = bytes.len();
            for i in 0..len - 1 {
                let index = len - 2 - i;
                let utf8_char_start = get_utf8_char_start(&bytes[index..]);
                if let Some(char_len) = match utf8_char_start {
                    Utf8CharType::One => Some(1),
                    Utf8CharType::Two => Some(2),
                    Utf8CharType::Three => Some(3),
                    Utf8CharType::Four => Some(4),
                    Utf8CharType::NotAStart | Utf8CharType::Illegal | Utf8CharType::Empty => None,
                } {
                    return Ok(match (index + char_len).cmp(&len) {
                        std::cmp::Ordering::Greater => len - index,
                        std::cmp::Ordering::Equal => 0,
                        std::cmp::Ordering::Less => len - index - char_len,
                    });
                }
            }
            Err(util::io_error("no valid utf8 cutoff point found!"))
        }
    }
}
enum Utf8CharType {
    Empty,
    Illegal,
    NotAStart,
    One,   // ...plain ascii
    Two,   // ...two-byte char
    Three, // ...three-byte char
    Four,  // ...four-byte char
}

//   1: 0000_0000 to 0111_1111 (00 to 7F)
//cont: 1000_0000 to 1011_1111 (80 to BF)
//   2: 1100_0000 to 1101_1111 (C0 to DF)
//   3: 1110_0000 to 1110_1111 (E0 to EF)
//   4: 1111_0000 to 1111_0111 (F0 to F7)
// ill: 1111_1000 to 1111_1111 (F8 to FF)
fn get_utf8_char_start(bytes: &[u8]) -> Utf8CharType {
    match bytes.len() {
        0 => Utf8CharType::Empty,
        _ => match bytes[0] {
            0x00..=0x7F => Utf8CharType::One,
            0x80..=0xBF => Utf8CharType::NotAStart,
            0xC0..=0xDF => Utf8CharType::Two,
            0xE0..=0xEF => Utf8CharType::Three,
            0xF0..=0xF7 => Utf8CharType::Four,
            _ => Utf8CharType::Illegal,
        },
    }
}
