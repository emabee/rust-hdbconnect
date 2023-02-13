#[cfg(feature = "async")]
use crate::a_sync::AsyncRsState;
#[cfg(feature = "sync")]
use crate::sync::SyncRsState;

#[cfg(feature = "async")]
use crate::protocol::util_async;

#[cfg(feature = "sync")]
use crate::protocol::util_sync;

#[cfg(feature = "sync")]
use byteorder::{LittleEndian, ReadBytesExt};

use crate::{
    conn::{AmConnCore, ConnectionCore},
    internal_returnvalue::InternalReturnValue,
    protocol::parts::{
        ExecutionResult, ParameterDescriptors, Parts, ResultSetMetadata, ServerError, Severity,
    },
    protocol::{Part, PartKind, ReplyType, ServerUsage},
    HdbError, HdbResult,
};
use std::sync::Arc;

// Since there is obviously no usecase for multiple segments in one request,
// we model message and segment together.
// But we differentiate explicitly between request messages and reply messages.
#[derive(Debug)]
pub struct Reply {
    session_id: i64,
    pub replytype: ReplyType,
    pub parts: Parts<'static>,
}
impl Reply {
    fn new(session_id: i64, replytype: ReplyType) -> Self {
        Self {
            session_id,
            replytype,
            parts: Parts::default(),
        }
    }

    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    // Parse a reply from the stream, building a Reply object.
    //
    // * `ResultSetMetadata` need to be injected in case of execute calls of
    //    prepared statements
    // * `ResultSet` needs to be injected (and is extended and returned)
    //    in case of fetch requests
    #[cfg(feature = "sync")]
    pub(crate) fn parse_sync(
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut SyncRsState>,
        o_am_conn_core: Option<&AmConnCore>,
        rdr: &mut dyn std::io::Read,
    ) -> HdbResult<Self> {
        trace!("Reply::parse()");
        let (no_of_parts, mut reply) = parse_msg_and_seq_header_sync(rdr)?;

        for i in 0..no_of_parts {
            let part = Part::parse_sync(
                &mut (reply.parts),
                o_am_conn_core,
                o_a_rsmd,
                o_a_descriptors,
                o_rs,
                i == no_of_parts - 1,
                rdr,
            )?;
            reply.push(part);
        }

        Ok(reply)
    }

    #[cfg(feature = "async")]
    // Parse a reply from the stream, building a Reply object.
    //
    // * `ResultSetMetadata` need to be injected in case of execute calls of
    //    prepared statements
    // * `ResultSet` needs to be injected (and is extended and returned)
    //    in case of fetch requests
    pub(crate) async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut AsyncRsState>,
        o_am_conn_core: Option<&AmConnCore>,
        rdr: &mut R,
    ) -> HdbResult<Self> {
        trace!("Reply::parse()");
        let (no_of_parts, mut reply) = parse_msg_and_seq_header_async(rdr).await?;

        for i in 0..no_of_parts {
            let part = Part::parse_async(
                &mut (reply.parts),
                o_am_conn_core,
                o_a_rsmd,
                o_a_descriptors,
                o_rs,
                i == no_of_parts - 1,
                rdr,
            )
            .await?;
            reply.push(part);
        }

        Ok(reply)
    }

    pub fn assert_expected_reply_type(&self, expected_reply_type: ReplyType) -> HdbResult<()> {
        if self.replytype == expected_reply_type {
            Ok(())
        } else {
            Err(HdbError::ImplDetailed(format!(
                "Expected reply type {:?}, got {:?}",
                expected_reply_type, self.replytype,
            )))
        }
    }

    pub fn push(&mut self, part: Part<'static>) {
        self.parts.push(part);
    }

    // digest parts, collect InternalReturnValues
    #[cfg(feature = "sync")]
    pub fn sync_into_internal_return_values(
        self,
        am_conn_core: &AmConnCore,
        o_additional_server_usage: Option<&mut ServerUsage>,
    ) -> HdbResult<(Vec<InternalReturnValue>, ReplyType)> {
        Ok((
            self.parts
                .sync_into_internal_return_values(am_conn_core, o_additional_server_usage)?,
            self.replytype,
        ))
    }

    #[cfg(feature = "async")]
    pub async fn async_into_internal_return_values(
        self,
        am_conn_core: &AmConnCore,
        o_additional_server_usage: Option<&mut ServerUsage>,
    ) -> HdbResult<(Vec<InternalReturnValue>, ReplyType)> {
        Ok((
            self.parts
                .async_into_internal_return_values(am_conn_core, o_additional_server_usage)
                .await?,
            self.replytype,
        ))
    }

    pub(crate) fn handle_db_error(&mut self, conn_core: &mut ConnectionCore) -> HdbResult<()> {
        conn_core.warnings.clear();

        // Retrieve server_errors from returned parts
        let mut server_errors = {
            match self.parts.remove_first_of_kind(PartKind::Error) {
                None => {
                    // No error part found, regular reply evaluation happens elsewhere
                    return Ok(());
                }
                Some(Part::Error(server_warnings_and_errors)) => {
                    let (mut warnings, server_errors): (Vec<ServerError>, Vec<ServerError>) =
                        server_warnings_and_errors
                            .into_iter()
                            .partition(|se| &Severity::Warning == se.severity());
                    std::mem::swap(&mut conn_core.warnings, &mut warnings);
                    if server_errors.is_empty() {
                        // Only warnings, so return Ok(())
                        return Ok(());
                    }
                    server_errors
                }
                Some(_non_error_part) => unreachable!("129837938423"),
            }
        };

        // Evaluate the other parts that can come with an error part
        let mut o_execution_results = None;
        self.parts.reverse(); // digest with pop
        while let Some(part) = self.parts.pop() {
            match part {
                Part::StatementContext(ref stmt_ctx) => {
                    conn_core.evaluate_statement_context(stmt_ctx);
                }
                Part::TransactionFlags(ta_flags) => {
                    conn_core.evaluate_ta_flags(ta_flags)?;
                }
                Part::ExecutionResult(vec) => {
                    o_execution_results = Some(vec);
                }
                part => warn!(
                    "Reply::handle_db_error(): ignoring unexpected part of kind {:?}",
                    part.kind()
                ),
            }
        }

        match o_execution_results {
            Some(execution_results) => {
                // mix server_errors into execution results
                let mut err_iter = server_errors.into_iter();
                let mut execution_results = execution_results
                    .into_iter()
                    .map(|er| match er {
                        ExecutionResult::Failure(_) => ExecutionResult::Failure(err_iter.next()),
                        _ => er,
                    })
                    .collect::<Vec<ExecutionResult>>();
                for e in err_iter {
                    warn!(
                        "Reply::handle_db_error(): \
                         found more server_errors than instances of ExecutionResult::Failure"
                    );
                    execution_results.push(ExecutionResult::Failure(Some(e)));
                }
                Err(HdbError::ExecutionResults(execution_results))
            }
            None => {
                if server_errors.len() == 1 {
                    Err(HdbError::from(server_errors.remove(0)))
                } else {
                    unreachable!("hopefully...")
                }
            }
        }
    }
}

#[cfg(feature = "sync")]
fn parse_msg_and_seq_header_sync(rdr: &mut dyn std::io::Read) -> HdbResult<(i16, Reply)> {
    // MESSAGE HEADER: 32 bytes
    let session_id: i64 = rdr.read_i64::<LittleEndian>()?; // I8
    let packet_seq_number: i32 = rdr.read_i32::<LittleEndian>()?; // I4
    let varpart_size: u32 = rdr.read_u32::<LittleEndian>()?; // UI4  not needed?
    let remaining_bufsize: u32 = rdr.read_u32::<LittleEndian>()?; // UI4  not needed?
    let no_of_segs = rdr.read_i16::<LittleEndian>()?; // I2
    if no_of_segs == 0 {
        return Err(HdbError::Impl("empty response (is ok for drop connection)"));
    }

    if no_of_segs > 1 {
        return Err(HdbError::ImplDetailed(format!(
            "no_of_segs = {no_of_segs} > 1"
        )));
    }

    util_sync::skip_bytes(10, rdr)?; // (I1 + B[9])

    // SEGMENT HEADER: 24 bytes
    rdr.read_i32::<LittleEndian>()?; // I4 seg_size
    rdr.read_i32::<LittleEndian>()?; // I4 seg_offset
    let no_of_parts: i16 = rdr.read_i16::<LittleEndian>()?; // I2
    rdr.read_i16::<LittleEndian>()?; // I2 seg_number
    let seg_kind = Kind::from_i8(rdr.read_i8()?)?; // I1

    trace!(
        "message and segment header: {{ packet_seq_number = {}, varpart_size = {}, \
         remaining_bufsize = {}, no_of_parts = {} }}",
        packet_seq_number,
        varpart_size,
        remaining_bufsize,
        no_of_parts
    );

    match seg_kind {
        Kind::Request => Err(HdbError::Impl("Cannot _parse_ a request")),
        Kind::Reply | Kind::Error => {
            util_sync::skip_bytes(1, rdr)?; // I1 reserved2
            let reply_type = ReplyType::from_i16(rdr.read_i16::<LittleEndian>()?)?; // I2
            util_sync::skip_bytes(8, rdr)?; // B[8] reserved3
            debug!(
                "Reply::parse(): got reply of type {:?} and seg_kind {:?} for session_id {}",
                reply_type, seg_kind, session_id
            );
            Ok((no_of_parts, Reply::new(session_id, reply_type)))
        }
    }
}

#[cfg(feature = "async")]
async fn parse_msg_and_seq_header_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> HdbResult<(i16, Reply)> {
    // MESSAGE HEADER: 32 bytes
    let session_id: i64 = rdr.read_i64_le().await?; // I8
    let packet_seq_number: i32 = rdr.read_i32_le().await?; // I4
    let varpart_size: u32 = rdr.read_u32_le().await?; // UI4  not needed?
    let remaining_bufsize: u32 = rdr.read_u32_le().await?; // UI4  not needed?
    let no_of_segs = rdr.read_i16_le().await?; // I2
    if no_of_segs == 0 {
        return Err(HdbError::Impl("empty response (is ok for drop connection)"));
    }

    if no_of_segs > 1 {
        return Err(HdbError::ImplDetailed(format!(
            "no_of_segs = {no_of_segs} > 1"
        )));
    }

    util_async::skip_bytes(10, rdr).await?; // (I1 + B[9])

    // SEGMENT HEADER: 24 bytes
    rdr.read_i32_le().await?; // I4 seg_size
    rdr.read_i32_le().await?; // I4 seg_offset
    let no_of_parts: i16 = rdr.read_i16_le().await?; // I2
    rdr.read_i16_le().await?; // I2 seg_number
    let seg_kind = Kind::from_i8(rdr.read_i8().await?)?; // I1

    trace!(
        "message and segment header: {{ packet_seq_number = {}, varpart_size = {}, \
         remaining_bufsize = {}, no_of_parts = {} }}",
        packet_seq_number,
        varpart_size,
        remaining_bufsize,
        no_of_parts
    );

    match seg_kind {
        Kind::Request => Err(HdbError::Impl("Cannot _parse_ a request")),
        Kind::Reply | Kind::Error => {
            util_async::skip_bytes(1, rdr).await?; // I1 reserved2
            let reply_type = ReplyType::from_i16(rdr.read_i16_le().await?)?; // I2
            util_async::skip_bytes(8, rdr).await?; // B[8] reserved3
            debug!(
                "Reply::parse(): got reply of type {:?} and seg_kind {:?} for session_id {}",
                reply_type, seg_kind, session_id
            );
            Ok((no_of_parts, Reply::new(session_id, reply_type)))
        }
    }
}

/// Specifies the layout of the remaining segment header structure
#[derive(Debug)]
enum Kind {
    Request,
    Reply,
    Error,
}
impl Kind {
    fn from_i8(val: i8) -> HdbResult<Self> {
        match val {
            1 => Ok(Self::Request),
            2 => Ok(Self::Reply),
            5 => Ok(Self::Error),
            _ => Err(HdbError::ImplDetailed(format!(
                "reply::Kind {val} not implemented",
            ))),
        }
    }
}
