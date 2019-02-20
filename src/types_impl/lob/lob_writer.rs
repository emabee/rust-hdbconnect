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

pub(crate) struct LobWriter {
    locator_id: u64,
    type_id: TypeId,
    am_conn_core: AmConnCore,
    server_resource_consumption_info: ServerResourceConsumptionInfo,
    tail: Vec<u8>,
}
impl LobWriter {
    pub fn new(locator_id: u64, type_id: TypeId, am_conn_core: AmConnCore) -> LobWriter {
        assert!(
            type_id == TypeId::BLOB || type_id == TypeId::CLOB || type_id == TypeId::NCLOB,
            "TypeId is {:?}",
            type_id
        );
        LobWriter {
            locator_id,
            type_id,
            am_conn_core,
            server_resource_consumption_info: Default::default(),
            tail: Default::default(),
        }
    }
}

// first implementation: do a roundtrip on every write, just appending
impl std::io::Write for LobWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        trace!("write() with buf of len {}", buf.len());
        match self.type_id {
            TypeId::CLOB | TypeId::NCLOB => {
                // concatenate previous tail and new buf
                let buf2 = if self.tail.is_empty() {
                    buf.to_vec()
                } else {
                    let mut buf2 = Vec::<u8>::new();
                    std::mem::swap(&mut buf2, &mut self.tail);
                    buf2.append(&mut buf.to_vec());
                    buf2
                };

                // cut off new tail and convert to cesu8
                let (cesu8, utf8_tail) = utf8_to_cesu8_and_utf8_tail(buf2)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                self.tail = utf8_tail;
                let locator_ids = write_a_lob_chunk(
                    &mut self.am_conn_core,
                    self.locator_id,
                    LobWriteMode::Append(&cesu8),
                    &mut self.server_resource_consumption_info,
                )
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                assert_eq!(locator_ids.len(), 1);
                assert_eq!(locator_ids[0], self.locator_id);
                Ok(buf.len())
            }
            TypeId::BLOB => {
                let locator_ids = write_a_lob_chunk(
                    &mut self.am_conn_core,
                    self.locator_id,
                    LobWriteMode::Append(buf),
                    &mut self.server_resource_consumption_info,
                )
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                assert_eq!(locator_ids.len(), 1);
                assert_eq!(locator_ids[0], self.locator_id);
                Ok(buf.len())
            }
            _ => panic!("unexpected TypeId {:?}", self.type_id),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        // concatenate previous tail and new buf
        if !self.tail.is_empty() {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "tail is not empty",
            ))
        } else {
            let locator_ids = write_a_lob_chunk(
                &mut self.am_conn_core,
                self.locator_id,
                LobWriteMode::Last,
                &mut self.server_resource_consumption_info,
            )
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            assert_eq!(locator_ids.len(), 0);
            Ok(())
        }
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
    let dummy_buf: Vec<u8> = vec![];
    let mut request = Request::new(RequestType::WriteLob, 0);
    let write_lob_request = match lob_write_mode {
        // LobWriteMode::Offset(offset, buf) =>
        //     WriteLobRequest::new(locator_id, offset /* or offset + 1? */, buf, true),
        LobWriteMode::Append(buf) => WriteLobRequest::new(locator_id, -1_i64, buf, false),
        LobWriteMode::Last => WriteLobRequest::new(locator_id, -1_i64, &dummy_buf, true),
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
    Last,
}
