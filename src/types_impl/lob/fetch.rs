use crate::conn_core::AmConnCore;
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::read_lob_request::ReadLobRequest;
use crate::protocol::parts::write_lob_request::WriteLobRequest;
use crate::protocol::reply_type::ReplyType;
use crate::protocol::request::Request;
use crate::protocol::request_type::RequestType;
use crate::protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use crate::{HdbError, HdbResult};

// Note that requested_length and offset count either bytes (BLOB, CLOB), or 1-2-3-chars (NCLOB)
pub(crate) fn fetch_a_lob_chunk(
    am_conn_core: &mut AmConnCore,
    locator_id: u64,
    offset: u64,
    length: u32,
    server_resource_consumption_info: &mut ServerResourceConsumptionInfo,
) -> HdbResult<(Vec<u8>, bool)> {
    let mut request = Request::new(RequestType::ReadLob, 0);
    let offset = offset + 1;
    request.push(Part::new(
        PartKind::ReadLobRequest,
        Argument::ReadLobRequest(ReadLobRequest::new(locator_id, offset, length)),
    ));

    let mut reply = am_conn_core.send(request)?;
    reply.assert_expected_reply_type(&ReplyType::ReadLob)?;

    let (reply_data, reply_is_last_data) = match reply.parts.pop_arg_if_kind(PartKind::ReadLobReply)
    {
        Some(Argument::ReadLobReply(read_lob_reply)) => {
            if *read_lob_reply.locator_id() != locator_id {
                return Err(HdbError::Impl(
                    "lob::fetch_a_lob_chunk(): locator ids do not match".to_owned(),
                ));
            }
            read_lob_reply.into_data_and_last()
        }
        _ => return Err(HdbError::Impl("No ReadLobReply part found".to_owned())),
    };

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

    Ok((reply_data, reply_is_last_data))
}

// Note that requested_length and offset count either bytes (BLOB, CLOB), or 1-2-3-chars (NCLOB)
pub(crate) fn write_a_lob_chunk(
    am_conn_core: &mut AmConnCore,
    locator_id: u64,
    offset: i64,
    buf: &[u8],
    server_resource_consumption_info: &mut ServerResourceConsumptionInfo,
) -> HdbResult<()> {
    let mut request = Request::new(RequestType::WriteLob, 0);
    let offset = offset + 1;
    request.push(Part::new(
        PartKind::WriteLobRequest,
        Argument::WriteLobRequest(WriteLobRequest::new(locator_id, offset, buf)),
    ));

    let mut reply = am_conn_core.send(request)?;
    reply.assert_expected_reply_type(&ReplyType::WriteLob)?;

    match reply.parts.pop_arg_if_kind(PartKind::WriteLobReply) {
        Some(Argument::WriteLobReply(write_lob_reply)) => {
            if write_lob_reply.locator_ids()[0] != locator_id {
                return Err(HdbError::Impl(
                    "lob::_write_a_lob_chunk(): locator ids do not match".to_owned(),
                ));
            }
        }
        _ => return Err(HdbError::Impl("No WriteLobReply part found".to_owned())),
    };

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

    Ok(())
}
