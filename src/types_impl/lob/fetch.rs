use crate::conn::AmConnCore;
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::read_lob_request::ReadLobRequest;
use crate::protocol::reply_type::ReplyType;
use crate::protocol::request::Request;
use crate::protocol::request_type::RequestType;
use crate::protocol::server_usage::ServerUsage;
use crate::{HdbError, HdbResult};

// Note that requested_length and offset count either bytes (BLOB, CLOB), or 1-2-3-chars (NCLOB)
pub(crate) fn fetch_a_lob_chunk(
    am_conn_core: &mut AmConnCore,
    locator_id: u64,
    offset: u64,
    length: u32,
    server_usage: &mut ServerUsage,
) -> HdbResult<(Vec<u8>, bool)> {
    let mut request = Request::new(RequestType::ReadLob, 0);
    let offset = offset + 1;
    request.push(Part::new(
        PartKind::ReadLobRequest,
        Argument::ReadLobRequest(ReadLobRequest::new(locator_id, offset, length)),
    ));

    let mut reply = am_conn_core.send_sync(request)?;
    reply.assert_expected_reply_type(ReplyType::ReadLob)?;

    let (reply_data, reply_is_last_data) = match reply
        .parts
        .pop_if_kind(PartKind::ReadLobReply)
        .map(Part::into_arg)
    {
        Some(Argument::ReadLobReply(read_lob_reply)) => {
            if *read_lob_reply.locator_id() != locator_id {
                return Err(HdbError::imp("locator ids do not match"));
            }
            read_lob_reply.into_data_and_last()
        }
        _ => return Err(HdbError::imp("No ReadLobReply part found")),
    };

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
    server_usage.update(server_proc_time, server_cpu_time, server_memory_usage);

    Ok((reply_data, reply_is_last_data))
}
