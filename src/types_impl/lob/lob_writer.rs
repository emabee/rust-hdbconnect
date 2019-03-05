use crate::conn_core::AmConnCore;
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::type_id::TypeId;
use crate::protocol::parts::write_lob_request::WriteLobRequest;
use crate::protocol::reply_type::ReplyType;
use crate::protocol::request::Request;
use crate::protocol::request_type::RequestType;
use crate::protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use crate::protocol::util::utf8_to_cesu8_and_utf8_tail;
use crate::{HdbError, HdbResult};
use std::io::{Error, ErrorKind, Result, Write};

pub(crate) struct LobWriter {
    locator_id: u64,
    type_id: TypeId,
    am_conn_core: AmConnCore,
    server_resource_consumption_info: ServerResourceConsumptionInfo,
    buffer: Vec<u8>,
    lob_write_length: usize,
}
impl LobWriter {
    pub fn new(locator_id: u64, type_id: TypeId, am_conn_core: AmConnCore) -> HdbResult<LobWriter> {
        if let TypeId::BLOB | TypeId::CLOB | TypeId::NCLOB = type_id {
            // ok
        } else {
            return Err(HdbError::Impl(format!("Unsupported type-id {:?}", type_id)));
        }
        let lob_write_length = am_conn_core.lock()?.get_lob_write_length();
        Ok(LobWriter {
            locator_id,
            type_id,
            am_conn_core,
            server_resource_consumption_info: Default::default(),
            buffer: Vec::<u8>::with_capacity(lob_write_length + 8200),
            lob_write_length,
        })
    }
}

impl Write for LobWriter {
    // Either buffers (in self.buffer) or writes buffer + input to the db
    fn write(&mut self, input: &[u8]) -> Result<usize> {
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
                let (cesu8, utf8_tail) = utf8_to_cesu8_and_utf8_tail(payload_raw)
                    .map_err(|e| Error::new(ErrorKind::Other, e))?;
                self.buffer = utf8_tail;
                cesu8
            } else {
                payload_raw
            };

            let locator_ids = write_a_lob_chunk(
                &mut self.am_conn_core,
                self.locator_id,
                LobWriteMode::Append(&payload),
                &mut self.server_resource_consumption_info,
            )
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
            debug_assert_eq!(locator_ids.len(), 1);
            debug_assert_eq!(locator_ids[0], self.locator_id);
        }
        Ok(input.len())
    }

    fn flush(&mut self) -> Result<()> {
        trace!("flush(), with buffer of {} bytes", self.buffer.len());
        let mut payload_raw = Vec::<u8>::new();
        std::mem::swap(&mut payload_raw, &mut self.buffer);
        let payload = if let TypeId::CLOB | TypeId::NCLOB = self.type_id {
            let (cesu8, utf8_tail) = utf8_to_cesu8_and_utf8_tail(payload_raw)
                .map_err(|e| Error::new(ErrorKind::Other, e))?;
            if !utf8_tail.is_empty() {
                return Err(Error::new(
                    ErrorKind::Other,
                    "stream is ending with invalid utf-8",
                ));
            }
            cesu8
        } else {
            payload_raw
        };

        let locator_ids = write_a_lob_chunk(
            &mut self.am_conn_core,
            self.locator_id,
            LobWriteMode::Last(&payload),
            &mut self.server_resource_consumption_info,
        )
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
        debug_assert_eq!(locator_ids.len(), 0);
        Ok(())
    }
}

// Note that requested_length and offset count either bytes (for BLOB, CLOB),
// or 1-2-3-chars (for NCLOB)
fn write_a_lob_chunk(
    am_conn_core: &mut AmConnCore,
    locator_id: u64,
    lob_write_mode: LobWriteMode,
    server_resource_consumption_info: &mut ServerResourceConsumptionInfo,
) -> HdbResult<Vec<u64>> {
    let mut request = Request::new(RequestType::WriteLob, 0);
    let write_lob_request = match lob_write_mode {
        // LobWriteMode::Offset(offset, buf) =>
        //     WriteLobRequest::new(locator_id, offset /* or offset + 1? */, buf, true),
        LobWriteMode::Append(buf) => WriteLobRequest::new(locator_id, -1_i64, buf, false),
        LobWriteMode::Last(buf) => WriteLobRequest::new(locator_id, -1_i64, buf, true),
    };
    request.push(Part::new(
        PartKind::WriteLobRequest,
        Argument::WriteLobRequest(write_lob_request),
    ));

    let mut reply = am_conn_core.send(request)?;
    reply.assert_expected_reply_type(&ReplyType::WriteLob)?;

    let (server_proc_time, server_cpu_time, server_memory_usage) =
        match reply.parts.pop_arg_if_kind(PartKind::StatementContext) {
            Some(Argument::StatementContext(stmt_ctx)) => (
                stmt_ctx.get_server_processing_time(),
                stmt_ctx.get_server_cpu_time(),
                stmt_ctx.get_server_memory_usage(),
            ),
            None => (None, None, None),
            _ => {
                return Err(HdbError::Impl(
                    "Inconsistent StatementContext part found for ResultSet".to_owned(),
                ));
            }
        };
    server_resource_consumption_info.update(server_proc_time, server_cpu_time, server_memory_usage);

    if let Some(Argument::TransactionFlags(ta_flags)) =
        reply.parts.pop_arg_if_kind(PartKind::TransactionFlags)
    {
        if ta_flags.is_committed() {
            trace!("is committed");
        } else {
            trace!("is not committed");
        }
    }

    match reply.parts.pop_arg_if_kind(PartKind::WriteLobReply) {
        Some(Argument::WriteLobReply(write_lob_reply)) => Ok(write_lob_reply.into_locator_ids()),
        _ => Err(HdbError::Impl(format!(
            "No WriteLobReply part found; parts = {:?}",
            reply.parts
        ))),
    }
}

enum LobWriteMode<'a> {
    //Offset(i64, &'a [u8]),
    Append(&'a [u8]),
    Last(&'a [u8]),
}
