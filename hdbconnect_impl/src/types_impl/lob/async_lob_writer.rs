use crate::{
    base::InternalReturnValue,
    conn::AmConnCore,
    protocol::parts::{ParameterDescriptors, ResultSetMetadata, TypeId, WriteLobRequest},
    protocol::{util, MessageType, Part, PartKind, Reply, ReplyType, Request},
    types_impl::lob::lob_writer_util::{get_utf8_tail_len, LobWriteMode},
    {HdbError, HdbResult, ServerUsage},
};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};

pub async fn copy<'a, R>(
    reader: &'a mut R,
    am_conn_core: AmConnCore,
    locator_id: u64,
    internal_return_values: &mut Vec<InternalReturnValue>,
    type_id: TypeId,
    o_a_rsmd: Option<&'a Arc<ResultSetMetadata>>,
    o_a_descriptors: Option<&'a Arc<ParameterDescriptors>>,
) -> HdbResult<u64>
where
    R: AsyncRead + Unpin + ?Sized,
{
    match type_id {
        TypeId::BLOB | TypeId::CLOB | TypeId::NCLOB => {}
        _ => {
            return Err(HdbError::ImplDetailed(format!(
                "Unsupported type-id {type_id:?}"
            )))
        }
    }
    let lob_write_length = am_conn_core
        .lock_async()
        .await
        .configuration()
        .lob_write_length() as usize;
    let mut server_usage = ServerUsage::default();
    let mut read_done: bool = false;
    let mut buf = vec![0; lob_write_length].into_boxed_slice();
    let mut len = 0_usize;
    let mut utf8_tail = Vec::<u8>::with_capacity(5);
    let mut amount = 0_u64;

    while !read_done {
        // Fill buffer
        {
            debug_assert_eq!(len, 0);
            // transfer utf_tail to buf
            {
                for (i, b) in utf8_tail.iter().enumerate() {
                    buf[i] = *b;
                }
                len = utf8_tail.len();
                utf8_tail.clear();
            }

            debug_assert!(utf8_tail.is_empty());
            trace!("reading data");
            while len < lob_write_length && !read_done {
                let read = reader.read(&mut buf[len..]).await?;
                trace!("Read {read} bytes");
                len += read;
                amount += read as u64;
                read_done = read == 0;
            }
            trace!("Totally read data: {len}, (lob_write_length: {lob_write_length})");
        }

        let payload = if let TypeId::CLOB | TypeId::NCLOB = type_id {
            // transfer utf tail to utf_tail and convert the rest to cesu8
            let tail_len = get_utf8_tail_len(&buf[0..len])?;
            {
                for b in &buf[len - tail_len..] {
                    utf8_tail.push(*b);
                }
                len -= tail_len;
            }
            cesu8::to_cesu8(std::str::from_utf8(&buf[0..len]).map_err(util::io_error)?)
        } else {
            std::borrow::Cow::Borrowed(&buf[0..len])
        };
        trace!("before writing: {:?}", payload.as_ref());
        write_a_lob_chunk(
            &am_conn_core,
            payload.as_ref(),
            if read_done {
                LobWriteMode::Last
            } else {
                LobWriteMode::Append
            },
            locator_id,
            o_a_rsmd,
            o_a_descriptors,
            &mut server_usage,
            internal_return_values,
        )
        .await
        .map(|_locator_ids| ())?;
        trace!("after writing: {:?}", payload.as_ref());

        len = 0;
    }
    Ok(amount)
}

// Note that requested_length and offset count either bytes (for BLOB, CLOB),
// or 1-2-3-chars (for NCLOB)
#[allow(clippy::too_many_arguments)]
async fn write_a_lob_chunk<'a>(
    am_conn_core: &AmConnCore,
    buf: &[u8],
    lob_write_mode: LobWriteMode,
    locator_id: u64,
    o_a_rsmd: Option<&'a Arc<ResultSetMetadata>>,
    o_a_descriptors: Option<&'a Arc<ParameterDescriptors>>,
    server_usage: &mut ServerUsage,
    internal_return_values: &mut Vec<InternalReturnValue>,
) -> HdbResult<Vec<u64>> {
    let mut request = Request::new(MessageType::WriteLob, 0);
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

    let reply = am_conn_core
        .lock_async()
        .await
        .roundtrip_async(
            &request,
            Some(am_conn_core),
            o_a_rsmd,
            o_a_descriptors,
            &mut None,
        )
        .await?;

    match reply.replytype {
        // regular response
        ReplyType::WriteLob => evaluate_write_lob_reply(reply, server_usage),

        // last response of last IN parameter
        ReplyType::DbProcedureCall => {
            evaluate_dbprocedure_call_reply(
                am_conn_core,
                reply,
                server_usage,
                internal_return_values,
            )
            .await
        }

        _ => Err(HdbError::ImplDetailed(format!(
            "LobCopier::write_a_lob_chunk got a reply of type {:?}",
            reply.replytype,
        ))),
    }
}

fn evaluate_write_lob_reply(reply: Reply, server_usage: &mut ServerUsage) -> HdbResult<Vec<u64>> {
    let mut result = None;

    for part in reply.parts {
        match part {
            Part::StatementContext(stmt_ctx) => {
                server_usage.update(
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

async fn evaluate_dbprocedure_call_reply(
    am_conn_core: &AmConnCore,
    mut reply: Reply,
    server_usage: &mut ServerUsage,
    internal_return_values: &mut Vec<InternalReturnValue>,
) -> HdbResult<Vec<u64>> {
    let locator_ids = evaluate_dbprocedure_call_reply1(&mut reply, server_usage)?;
    let mut proc_result = reply
        .parts
        .into_internal_return_values_async(am_conn_core, Some(server_usage))
        .await?;

    internal_return_values.append(&mut proc_result);
    Ok(locator_ids)
}

fn evaluate_dbprocedure_call_reply1(
    reply: &mut Reply,
    server_usage: &mut ServerUsage,
) -> HdbResult<Vec<u64>> {
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
    server_usage.update(server_proc_time, server_cpu_time, server_memory_usage);

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
