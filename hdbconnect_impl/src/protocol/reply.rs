use crate::{
    base::{InternalReturnValue, RsState},
    conn::{AmConnCore, ConnectionCore, ConnectionStatistics},
    protocol::parts::{
        ExecutionResult, ParameterDescriptors, Parts, ResultSetMetadata, ServerError, Severity,
    },
    protocol::{util_sync, Part, PartKind, ReplyType, ServerUsage},
    HdbError, HdbResult,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{io::Cursor, sync::Arc, time::Instant};

use super::{MESSAGE_AND_SEGMENT_HEADER_SIZE, SEGMENT_HEADER_SIZE};

// Since there is obviously no usecase for multiple segments in one request,
// we model message and segment together.
// But we differentiate explicitly between request messages and reply messages.
#[derive(Debug)]
pub(crate) struct Reply {
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
    // * `ResultSetMetadata` needs to be injected for execute calls of prepared statements
    // * `ResultSet` needs to be injected (and is extended and returned) for fetch requests
    #[cfg(feature = "sync")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn parse_sync(
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        o_am_conn_core: Option<&AmConnCore>,
        statistics: &mut ConnectionStatistics,
        start: std::time::Instant,
        io_buffer: &mut Cursor<Vec<u8>>,
        rdr: &mut dyn std::io::Read,
    ) -> HdbResult<Self> {
        trace!("Reply::parse_sync()");
        let packet_header = {
            read_into_buffer_sync(MESSAGE_AND_SEGMENT_HEADER_SIZE, io_buffer, rdr)?;
            statistics.add_wait_time(Instant::now().duration_since(start));
            parse_packet_header(io_buffer)?
        };

        // read rest of reply into buffer and decompress if necessary
        read_into_buffer_sync(packet_header.part_buffer_size, io_buffer, rdr)?;
        let mut o_cursor = packet_header
            .o_uncompressed_size
            .map(|uncompressed_size| {
                trace!("received compressed reply");
                statistics.add_compressed_reply(packet_header.part_buffer_size, uncompressed_size);
                lz4_flex::block::decompress(io_buffer.get_ref(), uncompressed_size)
            })
            .transpose()?
            .map(Cursor::new);

        // parse the parts and build the reply object
        let mut reply = Self::new(packet_header.session_id, packet_header.reply_type);
        for i in 0..packet_header.no_of_parts {
            let part = Part::parse_sync(
                &mut (reply.parts),
                o_am_conn_core,
                o_a_rsmd,
                o_a_descriptors,
                o_rs,
                i == packet_header.no_of_parts - 1,
                o_cursor.as_mut().unwrap_or(io_buffer),
            )?;
            reply.push(part);
        }
        Ok(reply)
    }

    // Parse a reply from the stream, building a Reply object.
    //
    // * `ResultSetMetadata` need to be injected in case of execute calls of
    //    prepared statements
    // * `ResultSet` needs to be injected (and is extended and returned)
    //    in case of fetch requests
    #[cfg(feature = "async")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        o_am_conn_core: Option<&AmConnCore>,
        start: std::time::Instant,
        statistics: &mut ConnectionStatistics,
        io_buffer: &mut Cursor<Vec<u8>>,
        rdr: &mut R,
    ) -> HdbResult<Self> {
        trace!("Reply::parse_async()");
        let packet_header = {
            read_into_buffer_async(MESSAGE_AND_SEGMENT_HEADER_SIZE, io_buffer, rdr).await?;
            statistics.add_wait_time(Instant::now().duration_since(start));
            parse_packet_header(io_buffer)?
        };

        // read rest of reply into buffer and decompress if necessary
        read_into_buffer_async(packet_header.part_buffer_size, io_buffer, rdr).await?;
        let mut o_cursor = packet_header
            .o_uncompressed_size
            .map(|uncompressed_size| {
                trace!("received compressed reply");
                statistics.add_compressed_reply(packet_header.part_buffer_size, uncompressed_size);
                lz4_flex::block::decompress(io_buffer.get_ref(), uncompressed_size)
            })
            .transpose()?
            .map(Cursor::new);

        // parse the parts and build the reply object
        let mut reply = Self::new(packet_header.session_id, packet_header.reply_type);
        for i in 0..packet_header.no_of_parts {
            let part = Part::parse_async(
                &mut (reply.parts),
                o_am_conn_core,
                o_a_rsmd,
                o_a_descriptors,
                o_rs,
                i == packet_header.no_of_parts - 1,
                o_cursor.as_mut().unwrap_or(io_buffer),
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
    pub fn into_internal_return_values_sync(
        self,
        am_conn_core: &AmConnCore,
        o_additional_server_usage: Option<&mut ServerUsage>,
    ) -> HdbResult<(Vec<InternalReturnValue>, ReplyType)> {
        Ok((
            self.parts
                .into_internal_return_values_sync(am_conn_core, o_additional_server_usage)?,
            self.replytype,
        ))
    }

    #[cfg(feature = "async")]
    pub async fn into_internal_return_values_async(
        self,
        am_conn_core: &AmConnCore,
        o_additional_server_usage: Option<&mut ServerUsage>,
    ) -> HdbResult<(Vec<InternalReturnValue>, ReplyType)> {
        Ok((
            self.parts
                .into_internal_return_values_async(am_conn_core, o_additional_server_usage)
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

fn parse_packet_header(rdr: &mut dyn std::io::Read) -> HdbResult<ReplyPacketHeader> {
    // TODO validate session_id against ConnectionCore::session_id
    // TODO session_id and packet_count must be 0 for exactly the first roundtrip
    // TODO validate assumptions about seg_size, seg_offset, seg_number being always = (varpart_size, 0, 1)

    // MESSAGE HEADER: 32 bytes
    let session_id: i64 = rdr.read_i64::<LittleEndian>()?; // I8
    let packet_seq_number: i32 = rdr.read_i32::<LittleEndian>()?; // I4
    let parts_and_segment_header_size: u32 = rdr.read_u32::<LittleEndian>()?; // UI4
    let remaining_bufsize: u32 = rdr.read_u32::<LittleEndian>()?; // UI4
    let no_of_segs = rdr.read_i16::<LittleEndian>()?; // I2
    match no_of_segs {
        1 => {}
        0 => return Err(HdbError::Impl("empty response (is ok for drop connection)")),
        _ => {
            return Err(HdbError::ImplDetailed(format!(
                "hdbconnect is not prepared for no_of_segs = {no_of_segs} > 1"
            )))
        }
    }

    let compressed = match rdr.read_u8()? {
        0 => false,
        2 => true,
        v => {
            return Err(HdbError::ImplDetailed(format!(
                "unexpected value for compression control: {v}"
            )));
        }
    };
    util_sync::skip_bytes(1, rdr)?; // filler1byte
    let uncompressed_size = rdr.read_u32::<LittleEndian>()?;
    util_sync::skip_bytes(4, rdr)?; // m_filler4byte

    // SEGMENT HEADER: 24 bytes
    let seg_size = rdr.read_i32::<LittleEndian>()?; // I4 seg_size
    let seg_offset = rdr.read_i32::<LittleEndian>()?; // I4 seg_offset
    let no_of_parts: i16 = rdr.read_i16::<LittleEndian>()?; // I2
    let seg_number = rdr.read_i16::<LittleEndian>()?; // I2 seg_number
    let seg_kind = Kind::from_i8(rdr.read_i8()?)?; // I1

    trace!(
        "REPLY, message and segment header: {{\
            \n  session_id = {session_id}, \
            \n  packet_seq_number = {packet_seq_number}, \
            \n  parts_and_segment_header_size = {parts_and_segment_header_size}, \
            \n  remaining_bufsize = {remaining_bufsize}, \
            \n  no_of_segs = {no_of_segs}, \
            \n  compressed = {compressed}, \
            \n  uncompressed_size = {uncompressed_size}, \
            \n\n  seg_size = {seg_size}, \
            \n  seg_offset = {seg_offset}, \
            \n  no_of_parts = {no_of_parts}, \
            \n  seg_number = {seg_number}, \
            \n  seg_kind = {seg_kind:?} \
        }}"
    );

    match seg_kind {
        Kind::Request => Err(HdbError::Impl("Cannot _parse_ a request")),
        Kind::Reply | Kind::Error => {
            util_sync::skip_bytes(1, rdr)?; // I1
            let reply_type = ReplyType::from_i16(rdr.read_i16::<LittleEndian>()?)?; // I2
            util_sync::skip_bytes(8, rdr)?; // B[8]

            debug!(
                "Reply::parse(): got reply of type {:?} and seg_kind {:?} for session_id {}",
                reply_type, seg_kind, session_id
            );
            Ok(ReplyPacketHeader {
                no_of_parts,
                o_uncompressed_size: if compressed {
                    Some(uncompressed_size as usize)
                } else {
                    None
                },
                session_id,
                part_buffer_size: (parts_and_segment_header_size - SEGMENT_HEADER_SIZE) as usize,
                reply_type,
            })
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

struct ReplyPacketHeader {
    reply_type: ReplyType,
    session_id: i64,
    o_uncompressed_size: Option<usize>,
    part_buffer_size: usize,
    no_of_parts: i16,
}

#[cfg(feature = "sync")]
fn read_into_buffer_sync(
    len: usize,
    buf: &mut Cursor<Vec<u8>>,
    rdr: &mut dyn std::io::Read,
) -> HdbResult<()> {
    buf.set_position(0);
    buf.get_mut().resize(len, 0);
    rdr.read_exact(buf.get_mut())?;
    Ok(())
}

#[cfg(feature = "async")]
async fn read_into_buffer_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    len: usize,
    buf: &mut Cursor<Vec<u8>>,
    rdr: &mut R,
) -> HdbResult<()> {
    buf.set_position(0);
    buf.get_mut().resize(len, 0);
    rdr.read_exact(buf.get_mut()).await?;
    Ok(())
}
