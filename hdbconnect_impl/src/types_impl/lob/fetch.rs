use crate::{
    HdbResult,
    conn::{AmConnCore, CommandOptions},
    impl_err,
    protocol::{
        MessageType, Part, ReplyType, Request, ServerUsage,
        parts::{ReadLobReply, ReadLobRequest},
    },
};

// Note that requested_length and offset count either bytes (BLOB, CLOB), or 1-2-3-chars (NCLOB)
#[cfg(feature = "sync")]
pub(crate) fn fetch_a_lob_chunk_sync(
    am_conn_core: &AmConnCore,
    locator_id: u64,
    offset: u64,
    length: u32,
    server_usage: &mut ServerUsage,
) -> HdbResult<(Vec<u8>, bool)> {
    let mut request = Request::new(MessageType::ReadLob, CommandOptions::EMPTY);
    let offset = offset + 1;
    request.push(Part::ReadLobRequest(ReadLobRequest::new(
        locator_id, offset, length,
    )));

    let reply = am_conn_core.send_sync(request)?;
    reply.assert_expected_reply_type(ReplyType::ReadLob)?;

    let mut o_read_lob_reply = None;
    for part in reply.parts {
        match part {
            Part::ReadLobReply(read_lob_reply) => {
                if *read_lob_reply.locator_id() != locator_id {
                    return Err(impl_err!("locator ids do not match"));
                }
                o_read_lob_reply = Some(read_lob_reply);
            }

            Part::StatementContext(stmt_ctx) => server_usage.update(
                stmt_ctx.server_processing_time(),
                stmt_ctx.server_cpu_time(),
                stmt_ctx.server_memory_usage(),
            ),
            x => warn!(
                "Unexpected part of kind {:?} received and ignored",
                x.kind()
            ),
        }
    }

    o_read_lob_reply
        .map(ReadLobReply::into_data_and_last)
        .ok_or_else(|| impl_err!("fetching a lob chunk failed"))
}

// Note that requested_length and offset count either bytes (BLOB, CLOB), or 1-2-3-chars (NCLOB)
#[cfg(feature = "async")]
pub(crate) async fn fetch_a_lob_chunk_async(
    am_conn_core: &AmConnCore,
    locator_id: u64,
    offset: u64,
    length: u32,
    server_usage: &mut ServerUsage,
) -> HdbResult<(Vec<u8>, bool)> {
    let mut request = Request::new(MessageType::ReadLob, CommandOptions::EMPTY);
    let offset = offset + 1;
    request.push(Part::ReadLobRequest(ReadLobRequest::new(
        locator_id, offset, length,
    )));

    let reply = am_conn_core.send_async(request).await?;
    reply.assert_expected_reply_type(ReplyType::ReadLob)?;

    let mut o_read_lob_reply = None;
    for part in reply.parts {
        match part {
            Part::ReadLobReply(read_lob_reply) => {
                if *read_lob_reply.locator_id() != locator_id {
                    return Err(impl_err!("locator ids do not match"));
                }
                o_read_lob_reply = Some(read_lob_reply);
            }

            Part::StatementContext(stmt_ctx) => server_usage.update(
                stmt_ctx.server_processing_time(),
                stmt_ctx.server_cpu_time(),
                stmt_ctx.server_memory_usage(),
            ),
            x => warn!(
                "Unexpected part of kind {:?} received and ignored",
                x.kind()
            ),
        }
    }

    o_read_lob_reply
        .map(ReadLobReply::into_data_and_last)
        .ok_or_else(|| impl_err!("fetching a lob chunk failed"))
}
