use crate::conn_core::AmConnCore;
use crate::hdb_response::InternalReturnValue;
use crate::protocol::argument::Argument;
use crate::protocol::part::{Part, Parts};
use crate::protocol::part_attributes::PartAttributes;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::parts::resultset::RsState;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::reply_type::ReplyType;
use crate::protocol::server_usage::ServerUsage;
use crate::protocol::util;
use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt};
use std::sync::Arc;

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
    fn new(session_id: i64, replytype: ReplyType) -> Reply {
        Reply {
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
    pub fn parse<T: std::io::BufRead>(
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        o_rs: &mut Option<&mut RsState>,
        o_am_conn_core: Option<&AmConnCore>,
        rdr: &mut T,
    ) -> HdbResult<Reply> {
        trace!("Reply::parse()");
        let (no_of_parts, mut reply) = parse_message_and_sequence_header(rdr)?;

        for i in 0..no_of_parts {
            let part = Part::parse(
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

        // Make sure that here (after parsing) the buffer is empty.
        // The following only works with nightly, because `.buffer()`
        // is on its way, but not yet in stable (https://github.com/rust-lang/rust/pull/49139)
        // and needs additionally to activate feature(bufreader_buffer) in lib.rs
        #[cfg(feature = "check_buffer")]
        {
            use std::io::BufRead;

            let buf_len = {
                let buf = rdr.buffer();
                if !buf.is_empty() {
                    error!(
                        "Buffer is not empty after Reply::parse() \'{:?}\'",
                        buf.to_vec()
                    );
                } else {
                    debug!("Reply::parse(): buffer is empty");
                }
                buf.len()
            };
            rdr.consume(buf_len);
        }

        Ok(reply)
    }

    pub fn assert_expected_reply_type(&self, reply_type: ReplyType) -> HdbResult<()> {
        if self.replytype == reply_type {
            Ok(()) // we got what we expected
        } else {
            Err(HdbError::Impl(format!(
                "got unexpected reply_type {:?} instead of {:?}",
                self.replytype, reply_type
            )))
        }
    }

    pub fn push(&mut self, part: Part<'static>) {
        self.parts.push(part);
    }

    // digest parts, collect InternalReturnValues
    pub fn into_internal_return_values(
        mut self,
        am_conn_core: &mut AmConnCore,
        mut o_additional_server_usage: Option<&mut ServerUsage>,
    ) -> HdbResult<(Vec<InternalReturnValue>, ReplyType)> {
        let mut conn_core = am_conn_core.lock()?;
        let mut int_return_values = Vec::<InternalReturnValue>::new();
        self.parts.reverse(); // digest the last part first
        while let Some(part) = self.parts.pop() {
            let (kind, arg) = part.into_elements();
            debug!("Reply::into_hdbresponse(): found part of kind {:?}", kind);
            match arg {
                Argument::StatementContext(ref stmt_ctx) => {
                    (*conn_core).evaluate_statement_context(stmt_ctx)?;
                    if let Some(ref mut server_usage) = o_additional_server_usage {
                        server_usage.update(
                            stmt_ctx.server_processing_time(),
                            stmt_ctx.server_cpu_time(),
                            stmt_ctx.server_memory_usage(),
                        );
                    }
                }
                Argument::TransactionFlags(ta_flags) => {
                    (*conn_core).evaluate_ta_flags(ta_flags)?;
                }

                Argument::OutputParameters(op) => {
                    int_return_values.push(InternalReturnValue::OutputParameters(op));
                }
                Argument::ParameterMetadata(pm) => {
                    int_return_values.push(InternalReturnValue::ParameterMetadata(Arc::new(pm)));
                }
                Argument::ResultSet(Some(rs)) => {
                    int_return_values.push(InternalReturnValue::ResultSet(rs));
                }
                Argument::ResultSetMetadata(rsmd) => match self.parts.pop() {
                    Some(part) => match part.into_arg() {
                        Argument::ResultSetId(rs_id) => {
                            let rs = ResultSet::new(
                                am_conn_core,
                                PartAttributes::new(0b_0000_0100),
                                rs_id,
                                Arc::new(rsmd),
                                None,
                            );
                            int_return_values.push(InternalReturnValue::ResultSet(rs));
                        }
                        _ => panic!("impossible: wrong Argument variant: ResultSetID expected"),
                    },
                    _ => {
                        return Err(HdbError::Impl(
                            "Missing required part ResultSetID".to_owned(),
                        ));
                    }
                },
                Argument::ExecutionResult(vra) => {
                    int_return_values.push(InternalReturnValue::AffectedRows(vra));
                }
                Argument::WriteLobReply(wlr) => {
                    int_return_values.push(InternalReturnValue::WriteLobReply(wlr));
                }
                _ => warn!(
                    "Reply::into_hdbresponse(): \
                     ignoring unexpected part of kind {:?}, , arg = {:?}, reply-type is {:?}",
                    kind, arg, self.replytype
                ),
            }
        }
        Ok((int_return_values, self.replytype))
    }
}

impl Drop for Reply {
    fn drop(&mut self) {
        for part in self.parts.ref_inner() {
            warn!(
                "reply of type {:?} is dropped, not all parts were evaluated: part-kind = {:?}",
                self.replytype,
                part.kind()
            );
        }
    }
}

fn parse_message_and_sequence_header<T: std::io::BufRead>(rdr: &mut T) -> HdbResult<(i16, Reply)> {
    // MESSAGE HEADER: 32 bytes
    let session_id: i64 = rdr.read_i64::<LittleEndian>()?; // I8
    let packet_seq_number: i32 = rdr.read_i32::<LittleEndian>()?; // I4
    let varpart_size: u32 = rdr.read_u32::<LittleEndian>()?; // UI4  not needed?
    let remaining_bufsize: u32 = rdr.read_u32::<LittleEndian>()?; // UI4  not needed?
    let no_of_segs = rdr.read_i16::<LittleEndian>()?; // I2
    if no_of_segs == 0 {
        return Err(HdbError::Impl(
            "empty response (is ok for drop connection)".to_owned(),
        ));
    }

    if no_of_segs > 1 {
        return Err(HdbError::Impl(format!("no_of_segs = {} > 1", no_of_segs)));
    }

    util::skip_bytes(10, rdr)?; // (I1 + B[9])

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
        Kind::Request => Err(HdbError::Usage("Cannot _parse_ a request".to_string())),
        Kind::Reply | Kind::Error => {
            util::skip_bytes(1, rdr)?; // I1 reserved2
            let reply_type = ReplyType::from_i16(rdr.read_i16::<LittleEndian>()?)?; // I2
            util::skip_bytes(8, rdr)?; // B[8] reserved3
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
    fn from_i8(val: i8) -> HdbResult<Kind> {
        match val {
            1 => Ok(Kind::Request),
            2 => Ok(Kind::Reply),
            5 => Ok(Kind::Error),
            _ => Err(HdbError::Impl(format!(
                "reply::Kind {} not implemented",
                val
            ))),
        }
    }
}
