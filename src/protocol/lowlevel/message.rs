//! Since there is obviously no usecase for multiple segments in one request,
//! we model message and segment together.
//! But we differentiate explicitly between request messages and reply messages.
use super::argument::Argument;
use super::conn_core::AmConnCore;
use super::part::{Part, Parts};
use super::part_attributes::PartAttributes;
use super::partkind::PartKind;
use super::parts::parameter_descriptor::ParameterDescriptor;
use super::parts::resultset::factory as ResultSetFactory;
use super::parts::resultset::ResultSet;
use super::parts::resultset_metadata::ResultSetMetadata;
use super::parts::server_error::{ServerError, Severity};
use super::parts::statement_context::StatementContext;
use super::reply_type::ReplyType;
use super::request_type::RequestType;
use super::util;
use hdb_response::factory as HdbResponseFactory;
use hdb_response::factory::InternalReturnValue;
use {HdbError, HdbResponse, HdbResult};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::Local;
use std::io::{self, BufRead, Write};
use std::net::TcpStream;

const MESSAGE_HEADER_SIZE: u32 = 32;
const SEGMENT_HEADER_SIZE: usize = 24; // same for in and out

#[derive(Debug)]
pub enum Message {
    Request(Request),
    Reply(Reply),
}

// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Request {
    request_type: RequestType,
    command_options: u8,
    parts: Parts,
}
impl Request {
    pub fn new(request_type: RequestType, command_options: u8) -> Request {
        Request {
            request_type,
            command_options,
            parts: Parts::default(),
        }
    }

    pub fn new_for_disconnect() -> Request {
        Request::new(RequestType::Disconnect, 0)
    }

    pub fn send_and_get_response(
        &mut self,
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        am_conn_core: &mut AmConnCore,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
    ) -> HdbResult<HdbResponse> {
        let mut reply = self.send_and_receive_detailed(
            o_rs_md,
            o_par_md,
            &mut None,
            am_conn_core,
            expected_reply_type,
            skip,
        )?;

        // digest parts, collect InternalReturnValues
        let mut int_return_values = Vec::<InternalReturnValue>::new();
        reply.parts.reverse(); // digest the last part first
        while let Some(part) = reply.parts.pop() {
            let (kind, arg) = part.into_elements();
            debug!("digesting a part of kind {:?}", kind);
            match arg {
                Argument::RowsAffected(vra) => {
                    int_return_values.push(InternalReturnValue::AffectedRows(vra));
                }
                Argument::StatementContext(stmt_ctx) => {
                    trace!(
                        "Received StatementContext with sequence_info = {:?}",
                        stmt_ctx.get_statement_sequence_info()
                    );
                    let mut guard = am_conn_core.lock()?;
                    (*guard).set_statement_sequence(stmt_ctx.get_statement_sequence_info());
                    (*guard).add_server_proc_time(stmt_ctx.get_server_processing_time());
                }
                Argument::TransactionFlags(ref ta_flags) => {
                    let mut guard = am_conn_core.lock()?;
                    (*guard).update_session_state(ta_flags)?;
                }
                Argument::ResultSet(Some(rs)) => {
                    int_return_values.push(InternalReturnValue::ResultSet(rs));
                }
                Argument::OutputParameters(op) => {
                    int_return_values.push(InternalReturnValue::OutputParameters(op));
                }
                Argument::ResultSetMetadata(rsm) => match reply.parts.pop() {
                    Some(part) => match *part.arg() {
                        Argument::ResultSetId(rs_id) => {
                            let rs = ResultSetFactory::resultset_new(
                                am_conn_core,
                                PartAttributes::new(0b_0000_0100),
                                rs_id,
                                rsm,
                                None,
                            );
                            int_return_values.push(InternalReturnValue::ResultSet(rs));
                        }
                        _ => panic!("wrong Argument variant: ResultSetID expected"),
                    },
                    _ => panic!("Missing required part ResultSetID"),
                },
                _ => warn!(
                    "send_and_get_response: found unexpected part of kind {:?}",
                    kind
                ),
            }
        }

        // re-pack InternalReturnValues into appropriate HdbResponse
        debug!(
            "Building HdbResponse for a reply of type {:?}",
            reply.replytype
        );
        trace!(
            "The found InternalReturnValues are: {:?}",
            int_return_values
        );
        match reply.replytype {
            ReplyType::Select |
            ReplyType::SelectForUpdate => HdbResponseFactory::resultset(int_return_values),

            
            ReplyType::Ddl |
            ReplyType::Commit |
            ReplyType::Rollback => HdbResponseFactory::success(int_return_values),

            ReplyType::Nil | 
            ReplyType::Insert |
            ReplyType::Update |
            ReplyType::Delete => HdbResponseFactory::rows_affected(int_return_values),

            ReplyType::DbProcedureCall |
            ReplyType::DbProcedureCallWithResult =>
                HdbResponseFactory::multiple_return_values(int_return_values),

            
            // dedicated ReplyTypes that are handled elsewhere and that
            // should not go through this method:
            ReplyType::Connect | ReplyType::Fetch | ReplyType::ReadLob |
            ReplyType::CloseCursor | ReplyType::Disconnect |
            ReplyType::XAControl | ReplyType::XARecover |

            // FIXME: 2 ReplyTypes that occur only in not yet implemented calls:
            ReplyType::FindLob |
            ReplyType::WriteLob |

            // FIXME: 4 ReplyTypes where it is unclear when they occur and what to return:
            ReplyType::Explain |
            ReplyType::XaStart |
            ReplyType::XaJoin |
            ReplyType::XAPrepare => {
                let s = format!(
                    "unexpected reply type {:?} in send_and_get_response(), \
                     with these internal return values: {:?}", 
                    reply.replytype, int_return_values);
                error!("{}",s);
                error!("Reply: {:?}",reply);
                Err(HdbError::impl_(s))
            },
        }
    }

    // simplified interface
    pub fn send_and_receive(
        &mut self,
        am_conn_core: &mut AmConnCore,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
    ) -> HdbResult<Reply> {
        self.send_and_receive_detailed(
            None,
            None,
            &mut None,
            am_conn_core,
            expected_reply_type,
            skip,
        )
    }

    pub fn send_and_receive_detailed(
        &mut self,
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        am_conn_core: &mut AmConnCore,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
    ) -> HdbResult<Reply> {
        trace!(
            "Request::send_and_receive_detailed() with requestType = {:?}",
            self.request_type,
        );
        let _start = Local::now();
        self.add_statement_sequence(am_conn_core)?;

        self.serialize(am_conn_core)?;

        let mut reply = Reply::parse(o_rs_md, o_par_md, o_rs, am_conn_core, skip)?;
        reply.assert_expected_reply_type(expected_reply_type)?;
        // FIXME digest StatementContext and TransactionFlags here,
        // so that they are not ignored in case of errors
        reply.assert_no_error(am_conn_core)?;

        debug!(
            "Request::send_and_receive_detailed() took {} ms",
            (Local::now().signed_duration_since(_start)).num_milliseconds()
        );
        Ok(reply)
    }

    fn add_statement_sequence(&mut self, am_conn_core: &AmConnCore) -> HdbResult<()> {
        let guard = am_conn_core.lock()?;
        match *(*guard).statement_sequence() {
            None => {}
            Some(ssi_value) => {
                let mut stmt_ctx = StatementContext::default();
                stmt_ctx.set_statement_sequence_info(ssi_value);
                trace!(
                    "Sending StatementContext with sequence_info = {:?}",
                    ssi_value
                );
                self.parts.push(Part::new(
                    PartKind::StatementContext,
                    Argument::StatementContext(stmt_ctx),
                ));
            }
        }
        Ok(())
    }

    // FIXME: this should be a method on the ConnectionCore, with the request as
    // argument
    fn serialize(&self, am_conn_core: &mut AmConnCore) -> HdbResult<()> {
        trace!("Entering Message::serialize()");
        let mut guard = am_conn_core.lock()?;
        let auto_commit_flag: i8 = if (*guard).is_auto_commit() { 1 } else { 0 };
        let conn_core = &mut *guard;
        self.serialize_impl(
            conn_core.session_id(),
            conn_core.next_seq_number(),
            auto_commit_flag,
            &mut conn_core.writer(),
        )
    }

    pub fn serialize_impl(
        &self,
        session_id: i64,
        seq_number: i32,
        auto_commit_flag: i8,
        w: &mut io::BufWriter<TcpStream>,
    ) -> HdbResult<()> {
        let varpart_size = self.varpart_size()?;
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing Message with total size {}", total_size);
        let mut remaining_bufsize = total_size - MESSAGE_HEADER_SIZE;

        debug!(
            "Request::serialize() for session_id = {}, seq_number = {}, request_type = {:?}",
            session_id, seq_number, self.request_type
        );
        // MESSAGE HEADER
        w.write_i64::<LittleEndian>(session_id)?; // I8
        w.write_i32::<LittleEndian>(seq_number)?; // I4
        w.write_u32::<LittleEndian>(varpart_size)?; // UI4
        w.write_u32::<LittleEndian>(remaining_bufsize)?; // UI4
        w.write_i16::<LittleEndian>(1)?; // I2    Number of segments
        for _ in 0..10 {
            w.write_u8(0)?;
        } // I1+ B[9]  unused

        // SEGMENT HEADER
        let parts_len = self.parts.len() as i16;
        let size = self.seg_size()? as i32;
        w.write_i32::<LittleEndian>(size)?; // I4  Length including the header
        w.write_i32::<LittleEndian>(0)?; // I4 Offset within the message buffer
        w.write_i16::<LittleEndian>(parts_len)?; // I2 Number of contained parts
        w.write_i16::<LittleEndian>(1)?; // I2 Number of this segment, starting with 1
        w.write_i8(1)?; // I1 Segment kind: always 1 = Request
        w.write_i8(self.request_type.to_i8())?; // I1 "Message type"
        w.write_i8(auto_commit_flag)?; // I1 auto_commit on/off
        w.write_u8(self.command_options)?; // I1 Bit set for options
        for _ in 0..8 {
            w.write_u8(0)?;
        } // [B;8] Reserved, do not use

        remaining_bufsize -= SEGMENT_HEADER_SIZE as u32;
        trace!("Headers are written");
        // PARTS
        for part in &(self.parts) {
            remaining_bufsize = part.serialize(remaining_bufsize, w)?;
        }
        w.flush()?;
        trace!("Parts are written");
        Ok(())
    }

    pub fn push(&mut self, part: Part) {
        self.parts.push(part);
    }

    /// Length in bytes of the variable part of the message, i.e. total message
    /// without the header
    fn varpart_size(&self) -> HdbResult<u32> {
        let mut len = 0_u32;
        len += self.seg_size()? as u32;
        trace!("varpart_size = {}", len);
        Ok(len)
    }

    fn seg_size(&self) -> HdbResult<usize> {
        let mut len = SEGMENT_HEADER_SIZE;
        for part in &self.parts {
            len += part.size(true)?;
        }
        Ok(len)
    }
}

#[derive(Debug)]
pub struct Reply {
    session_id: i64,
    replytype: ReplyType,
    pub parts: Parts,
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

    /// parse a response from the stream, building a Reply object
    /// `ResultSetMetadata` need to be injected in case of execute calls of
    /// prepared statements `ResultSet` needs to be injected (and is
    /// extended and returned) in case of fetch requests `am_conn_core`
    /// needs to be injected in case we get an incomplete resultset or lob
    /// (so that they later can fetch)
    fn parse(
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        am_conn_core: &AmConnCore,
        skip: SkipLastSpace,
    ) -> HdbResult<Reply> {
        trace!("Reply::parse()");
        let mut guard = am_conn_core.lock()?;

        let rdr = (*guard).reader();
        trace!("Reply::parse(): got the reader");

        let (no_of_parts, msg) = parse_message_and_sequence_header(rdr)?;
        trace!("Reply::parse(): parsed the header");
        match msg {
            Message::Request(_) => Err(HdbError::Impl("Reply::parse() found Request".to_owned())),
            Message::Reply(mut msg) => {
                for i in 0..no_of_parts {
                    let (part, padsize) = Part::parse(
                        &mut (msg.parts),
                        Some(am_conn_core),
                        o_rs_md,
                        o_par_md,
                        o_rs,
                        rdr,
                    )?;
                    msg.push(part);

                    if i < no_of_parts - 1 {
                        util::skip_bytes(padsize, rdr)?;
                    } else {
                        match skip {
                            SkipLastSpace::Soft => util::dont_use_soft_consume_bytes(padsize, rdr)?,
                            SkipLastSpace::Hard => util::skip_bytes(padsize, rdr)?,
                            SkipLastSpace::No => {}
                        }
                    }
                }
                Ok(msg)
            }
        }
    }

    fn assert_expected_reply_type(&self, expected_reply_type: Option<ReplyType>) -> HdbResult<()> {
        match expected_reply_type {
            None => Ok(()), // we had no clear expectation
            Some(fc) => {
                if self.replytype.to_i16() == fc.to_i16() {
                    Ok(()) // we got what we expected
                } else {
                    Err(HdbError::Impl(format!(
                        "unexpected reply_type (function code) {:?}",
                        self.replytype
                    )))
                }
            }
        }
    }

    fn assert_no_error(&mut self, am_conn_core: &mut AmConnCore) -> HdbResult<()> {
        let mut guard = am_conn_core.lock()?;
        (*guard).warnings.clear();
        let err_code = PartKind::Error.to_i8();
        match (&self.parts)
            .into_iter()
            .position(|p| p.kind().to_i8() == err_code)
        {
            None => Ok(()),
            Some(idx) => {
                let err_part = self.parts.swap_remove(idx);
                match err_part.into_elements() {
                    (_, Argument::Error(vec)) => {
                        // warnings are filtered out and added
                        let mut errors: Vec<ServerError> = vec.into_iter()
                            .filter_map(|se| match se.severity() {
                                Severity::Warning => {
                                    (*guard).warnings.push(se);
                                    None
                                }
                                _ => Some(se),
                            })
                            .collect();
                        if errors.is_empty() {
                            Ok(())
                        } else if errors.len() == 1 {
                            Err(HdbError::DbError(errors.remove(0)))
                        } else {
                            // FIXME is this appropriate?
                            self.parts.clear();
                            Err(HdbError::MultipleDbErrors(errors))
                        }
                    }
                    _ => Err(HdbError::Impl(
                        "assert_no_error: inconsistent error part found".to_owned(),
                    )),
                }
            }
        }
    }

    pub fn push(&mut self, part: Part) {
        self.parts.push(part);
    }
}

#[derive(Clone, Copy)]
pub enum SkipLastSpace {
    Hard,
    Soft,
    No,
}

impl Drop for Reply {
    fn drop(&mut self) {
        for part in &self.parts {
            warn!(
                "reply is dropped, but not all parts were evaluated: part-kind = {:?}",
                part.kind()
            );
        }
    }
}

///
pub fn parse_message_and_sequence_header(rdr: &mut BufRead) -> HdbResult<(i16, Message)> {
    // MESSAGE HEADER: 32 bytes
    let session_id: i64 = rdr.read_i64::<LittleEndian>()?; // I8
    let packet_seq_number: i32 = rdr.read_i32::<LittleEndian>()?; // I4
    let varpart_size: u32 = rdr.read_u32::<LittleEndian>()?; // UI4  not needed?
    let remaining_bufsize: u32 = rdr.read_u32::<LittleEndian>()?; // UI4  not needed?
    let no_of_segs = rdr.read_i16::<LittleEndian>()?; // I2
    assert_eq!(no_of_segs, 1);

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
        Kind::Request => {
            // only for read_wire
            let request_type = RequestType::from_i8(rdr.read_i8()?)?; // I1
            let _auto_commit = rdr.read_i8()? != 0_i8; // I1
            let command_options = rdr.read_u8()?; // I1 command_options
            util::skip_bytes(8, rdr)?; // B[8] reserved1
            Ok((
                no_of_parts,
                Message::Request(Request {
                    request_type,
                    command_options,
                    parts: Parts::default(),
                }),
            ))
        }
        Kind::Reply | Kind::Error => {
            util::skip_bytes(1, rdr)?; // I1 reserved2
            let rt = ReplyType::from_i16(rdr.read_i16::<LittleEndian>()?)?; // I2
            util::skip_bytes(8, rdr)?; // B[8] reserved3
            debug!(
                "Reply::parse(): got reply of type {:?} and seg_kind {:?} for session_id {}",
                rt, seg_kind, session_id
            );
            Ok((no_of_parts, Message::Reply(Reply::new(session_id, rt))))
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
                "Invalid value for message::Kind::from_i8() detected: {}",
                val
            ))),
        }
    }
}
