use conn_core::AmConnCore;
use protocol::argument::Argument;
use protocol::part::Part;
use protocol::partkind::PartKind;
use protocol::parts::read_lob_request::ReadLobRequest;
use protocol::reply::SkipLastSpace;
use protocol::reply_type::ReplyType;
use protocol::request::Request;
use protocol::request_type::RequestType;
use protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use std::cmp;
use {HdbError, HdbResult};

// Note that total_length and offset count either bytes (BLOB, CLOB), or 1-2-3-chars (NCLOB)
pub(crate) fn fetch_a_lob_chunk(
    o_am_conn_core: &mut Option<AmConnCore>,
    locator_id: u64,
    total_length: u64,
    offset: u64,
    server_resource_consumption_info: &mut ServerResourceConsumptionInfo,
) -> HdbResult<(Vec<u8>, bool)> {
    match *o_am_conn_core {
        None => Err(HdbError::Usage(
            "Fetching more LOB chunks is no more possible (connection already closed)".to_owned(),
        )),
        Some(ref mut am_conn_core) => {
            // build the request, provide StatementContext and length_to_read
            let mut request = Request::new(RequestType::ReadLob, 0);
            let length_to_read = {
                let guard = am_conn_core.lock()?;
                cmp::min((*guard).get_lob_read_length() as u64, total_length - offset) as i32
            };
            request.push(Part::new(
                PartKind::ReadLobRequest,
                Argument::ReadLobRequest(ReadLobRequest::new(
                    locator_id,
                    offset + 1,
                    length_to_read,
                )),
            ));

            trace!(
                "Sending ReadLobRequest with offset = {} and length_to_read = {}",
                offset + 1,
                length_to_read
            );

            let mut reply = request.send_and_get_reply_simplified(
                am_conn_core,
                Some(ReplyType::ReadLob),
                SkipLastSpace::No,
            )?;

            let (reply_data, reply_is_last_data) =
                match reply.parts.pop_arg_if_kind(PartKind::ReadLobReply) {
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
                        ))
                    }
                };
            server_resource_consumption_info.update(
                server_proc_time,
                server_cpu_time,
                server_memory_usage,
            );

            Ok((reply_data, reply_is_last_data))
        }
    }
}
