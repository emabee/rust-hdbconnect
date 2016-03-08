//! Since there is obviously no usecase for multiple segments in one request, we model message and segment together.
//! But we differentiate explicitly between request messages and reply messages.
use {DbcError,DbcResult,DbResponse};
use db_response::factory as DbResponseFactory;
use db_response::factory::InternalReturnValue;
use super::{PrtError,PrtResult,prot_err};
use super::conn_core::ConnRef;
use super::argument::Argument;
use super::reply_type::ReplyType;
use super::request_type::RequestType;
use super::part::{Part,Parts};
use super::part_attributes::PartAttributes;
use super::partkind::PartKind;
use super::parts::resultset::ResultSet;
use super::parts::option_value::OptionValue;
use super::parts::parameter_metadata::ParameterMetadata;
use super::parts::statement_context::StatementContext;
use super::parts::resultset::factory as ResultSetFactory;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use chrono::Local;
use std::io::{self,BufRead};
use std::net::TcpStream;

const BUFFER_SIZE: u32 = 130000;
const MESSAGE_HEADER_SIZE: u32 = 32;
const SEGMENT_HEADER_SIZE: usize = 24; // same for in and out


#[derive(Debug)]
pub enum MsgType {
    //Request,              // only required for read_wire
    Reply,
}

#[derive(Debug)]
pub enum Message {
    Request(Request),
    Reply(Reply),
}

// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Request {
    pub request_type: RequestType,
    pub auto_commit: bool,
    pub command_options: u8,
    pub parts: Parts,
}
impl Request {
    pub fn new(conn_ref: &ConnRef,request_type: RequestType, auto_commit: bool, command_options: u8)
    -> PrtResult<Request> {
        let mut request = Request {
                        request_type: request_type,
                        auto_commit: auto_commit,
                        command_options: command_options,
                        parts: Parts::new(),
        };
        try!(request.add_ssi(conn_ref));
        Ok(request)
    }

    pub fn new_for_disconnect() -> Request {
        Request {
            request_type: RequestType::Disconnect,
            auto_commit: false,
            command_options: 0,
            parts: Parts::new(),
        }
    }

    pub fn send_and_get_response(&mut self,
                                    o_par_md: &mut Option<ParameterMetadata>,
                                    conn_ref: &ConnRef,
                                    expected_fc: Option<ReplyType>,
                                    acc_server_proc_time: &mut i32)
    -> DbcResult<DbResponse> {
        let mut reply = try!(self.send_and_receive_detailed(o_par_md, &mut None, conn_ref, expected_fc));

        // digest parts, collect InternalReturnValues
        let mut int_return_values = Vec::<InternalReturnValue>::new();
        reply.parts.0.reverse(); //digest the last part first
        while let Some(part) = reply.parts.0.pop() {
            debug!("digesting a part of kind {:?}",part.kind);
            match part.arg {
                Argument::RowsAffected(vra) => {
                    int_return_values.push(InternalReturnValue::AffectedRows(vra));
                },
                Argument::StatementContext(stmt_ctx) => {
                    trace!("Received StatementContext with sequence_info = {:?}",stmt_ctx.statement_sequence_info);
                    conn_ref.borrow_mut().ssi = stmt_ctx.statement_sequence_info;
                    *acc_server_proc_time += match stmt_ctx.server_processing_time {
                        Some(OptionValue::INT(i)) => i,
                        _ => 0,
                    };
                },
                Argument::TransactionFlags(vec) => {
                    for ta_flag in vec {
                        try!(conn_ref.borrow_mut().set_transaction_state(ta_flag));
                    }
                },
                Argument::ResultSet(Some(rs)) => {
                    int_return_values.push(InternalReturnValue::ResultSet(rs));
                },
                Argument::OutputParameters(op) => {
                    int_return_values.push(InternalReturnValue::OutputParameters(op));
                },
                Argument::ResultSetMetadata(rsm) => {
                    match reply.parts.0.pop() {
                        Some(part) => {
                            match part.arg {
                                Argument::ResultSetId(rs_id) => {
                                    let rs = ResultSetFactory::resultset_new(
                                        Some(conn_ref), PartAttributes::new(0b_0000_0100), rs_id, rsm, None,
                                    );
                                    int_return_values.push(InternalReturnValue::ResultSet(rs));
                                },
                                _ => panic!("wrong Argument variant: ResultSetID expected"),
                            }
                        },
                        _ => panic!("Missing required part ResultSetID"),
                    }
                }
                _ => warn!("send_and_get_response: found unexpected part of kind {:?}",part.kind),
            }
        }

        // re-pack InternalReturnValues into appropriate DbResponse
        debug!("Building DbResponse for a reply of type {:?}",reply.type_);
        trace!("The found InternalReturnValue are: {:?}",int_return_values);
        match reply.type_ {
            ReplyType::Select => DbResponseFactory::resultset(int_return_values),

            ReplyType::Ddl |
            ReplyType::Commit |
            ReplyType::Rollback => DbResponseFactory::success(int_return_values),

            ReplyType::Insert |
            ReplyType::Update |
            ReplyType::Delete => DbResponseFactory::rows_affected(int_return_values),

            ReplyType::DbProcedureCall |
            ReplyType::DbProcedureCallWithResult =>
                DbResponseFactory::multiple_return_values(int_return_values),

            // dedicated ReplyTypes that are handled elsewhere and that should not go through this method
            ReplyType::Nil | ReplyType::Connect | ReplyType::Fetch | ReplyType::ReadLob |
            ReplyType::CloseCursor | ReplyType::Disconnect |

            // FIXME: 2 ReplyTypes that occur only in not yet implemented calls
            ReplyType::FindLob |
            ReplyType::WriteLob |

            // FIXME: 4 ReplyTypes where it is unclear when they occur and what to return
            ReplyType::SelectForUpdate |
            ReplyType::Explain |
            ReplyType::XaStart |
            ReplyType::XaJoin => {
                let s = format!("unexpected reply type {:?} in send_and_get_response()", reply.type_);
                error!("{}",s);
                error!("Reply: {:?}",reply);
                Err(DbcError::EvaluationError("send_and_get_response(): unexpected reply type"))
            },
        }
    }

    // simplified interface
    pub fn send_and_receive(&mut self, conn_ref: &ConnRef, expected_fc: Option<ReplyType>) -> PrtResult<Reply> {
        self.send_and_receive_detailed(&mut None, &mut None, conn_ref, expected_fc)
    }

    pub fn send_and_receive_detailed(&mut self,
                            o_par_md: &mut Option<ParameterMetadata>,
                            o_rs: &mut Option<&mut ResultSet>,
                            conn_ref: &ConnRef,
                            expected_fc: Option<ReplyType>)
    -> PrtResult<Reply> {
        trace!("Request::send_and_receive_detailed() with requestType = {:?}, auto_commit = {}",
                self.request_type,self.auto_commit);
        let _start = Local::now();

        try!(self.serialize(conn_ref));

        let mut reply = try!(Reply::parse(o_par_md, o_rs, conn_ref));
        try!(reply.assert_no_error());
        try!(reply.assert_expected_fc(expected_fc));

        debug!("Request::send_and_receive_detailed() took {} ms", (Local::now() - _start).num_milliseconds());
        Ok(reply)
    }

    fn add_ssi(&mut self, conn_ref: &ConnRef)  -> PrtResult<()> {
        match conn_ref.borrow().ssi {
            None => {},
            Some(ref ssi) => {
                let mut stmt_ctx = StatementContext::new();
                stmt_ctx.statement_sequence_info = Some(ssi.clone());
                trace!("Sending StatementContext with sequence_info = {:?}",stmt_ctx.statement_sequence_info);
                self.parts.0.push(Part::new(PartKind::StatementContext, Argument::StatementContext(stmt_ctx)));
            }
        }
        Ok(())
    }


    fn serialize(&self, conn_ref: &ConnRef) -> PrtResult<()> {
        trace!("Entering Message::serialize()");
        let mut conn_core = conn_ref.borrow_mut();
        self.serialize_impl(conn_core.session_id, conn_core.next_seq_number(), &mut conn_core.stream)
    }

    pub fn serialize_impl(&self, session_id: i64,  seq_number: i32, stream: &mut TcpStream) -> PrtResult<()> {
        let varpart_size = try!(self.varpart_size());
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing Message with total size {}", total_size);
        let mut remaining_bufsize = BUFFER_SIZE - MESSAGE_HEADER_SIZE;

        let w = &mut io::BufWriter::with_capacity(total_size as usize, stream);
        debug!(
            "Request::serialize() for session_id = {}, seq_number = {}, request_type = {:?}",
            session_id, seq_number, self.request_type
        );
        // MESSAGE HEADER
        try!(w.write_i64::<LittleEndian>(session_id));                  // I8
        try!(w.write_i32::<LittleEndian>(seq_number));                  // I4
        try!(w.write_u32::<LittleEndian>(varpart_size));                // UI4
        try!(w.write_u32::<LittleEndian>(remaining_bufsize));           // UI4
        try!(w.write_i16::<LittleEndian>(1));                           // I2    Number of segments
        for _ in 0..10 { try!(w.write_u8(0)); }                         // I1+ B[9]  unused

        // SEGMENT HEADER
        let size = try!(self.seg_size()) as i32;
        try!(w.write_i32::<LittleEndian>(size));                        // I4    Length including the header
        try!(w.write_i32::<LittleEndian>(0));                           // I4    Offset within the message buffer
        try!(w.write_i16::<LittleEndian>(self.parts.0.len() as i16));   // I2    Number of contained parts
        try!(w.write_i16::<LittleEndian>(1));                           // I2    Number of this segment, starting with 1
        try!(w.write_i8(1));                                            // I1    Segment kind: always 1 = Request
        try!(w.write_i8(self.request_type.to_i8()));                    // I1    "Message type"
        try!(w.write_i8(match self.auto_commit {true => 1, _ => 0}));   // I1    auto_commit on/off
        try!(w.write_u8(self.command_options));                         // I1    Bit set for options
        for _ in 0..8 { try!(w.write_u8(0)); }                          // [B;8] Reserved, do not use

        remaining_bufsize -= SEGMENT_HEADER_SIZE as u32;
        trace!("Headers are written");
        // PARTS
        for ref part in &self.parts.0 {
            remaining_bufsize = try!(part.serialize(remaining_bufsize, w));
        }
        trace!("Parts are written");
        Ok(())
    }

    pub fn push(&mut self, part: Part){
        self.parts.0.push(part);
    }

    /// Length in bytes of the variable part of the message, i.e. total message without the header
    fn varpart_size(&self) -> PrtResult<u32> {
        let mut len = 0_u32;
        len += try!(self.seg_size()) as u32;
        trace!("varpart_size = {}",len);
        Ok(len)
    }

    fn seg_size(&self) -> PrtResult<usize> {
        let mut len = SEGMENT_HEADER_SIZE;
        for part in &self.parts.0 {
            len += try!(part.size(true));
        }
        Ok(len)
    }
}


#[derive(Debug)]
pub struct Reply {
    pub session_id: i64,
    pub type_: ReplyType,
    pub parts: Parts,
    pub server_processing_time: Option<OptionValue>,
}
impl Reply {
    fn new(session_id: i64, type_: ReplyType) -> Reply {
        Reply {
            session_id: session_id,
            type_: type_,
            parts: Parts::new(),
            server_processing_time: None,
        }
    }

    /// parse a response from the stream, building a Reply object
    /// `ResultSetMetadata` need to be injected in case of execute calls of prepared statements
    /// `ResultSet` needs to be injected (and is extended and returned) in case of fetch requests
    /// `conn_ref` needs to be injected in case we get an incomplete resultset or lob (so that they later can fetch)
    fn parse(o_par_md: &mut Option<ParameterMetadata>, o_rs: &mut Option<&mut ResultSet>, conn_ref: &ConnRef)
    -> PrtResult<Reply> {
        trace!("Reply::parse()");
        let stream = &mut (conn_ref.borrow_mut().stream);
        let mut rdr = io::BufReader::new(stream);

        let (no_of_parts, msg) = try!(parse_message_and_sequence_header(&mut rdr));
        match msg {
            Message::Request(_) => Err(prot_err("Reply::parse() found Request")),
            Message::Reply(mut msg) => {
                for _ in 0..no_of_parts {
                    let part = try!(Part::parse(
                        MsgType::Reply, &mut (msg.parts), Some(conn_ref), o_par_md, o_rs, &mut rdr
                    ));
                    msg.push(part);
                }
                Ok(msg)
            }
        }
    }

    fn assert_expected_fc(&self, expected_fc: Option<ReplyType>) -> PrtResult<()> {
        match expected_fc {
            None => Ok(()),     // we had no clear expectation
            Some(fc) => {
                if self.type_.to_i16() == fc.to_i16() {
                    Ok(())      // we got what we expected
                } else {
                    Err(PrtError::ProtocolError(format!("unexpected reply_type (function code) {:?}", self.type_)))
                }
            },
        }
    }

    fn assert_no_error(&mut self) -> PrtResult<()> {
        let err_code = PartKind::Error.to_i8();
        match self.parts.0.iter().position(|p| p.kind.to_i8() == err_code) {
            None => Ok(()),
            Some(idx) => {
                let err_part = self.parts.0.swap_remove(idx);
                match err_part.arg {
                    Argument::Error(vec) => {
                        let err = PrtError::DbMessage(vec);
                        warn!("{}",err);
                        Err(err)
                    },
                    _ => Err(prot_err("assert_no_error: inconsistent error part found")),
                }
            },
        }
    }

    pub fn push(&mut self, part: Part){
        self.parts.0.push(part);
    }
}
impl Drop for Reply {
    fn drop(&mut self) {
        for ref part in &self.parts.0 {
            warn!("reply is dropped, but not all parts were evaluated: part-kind = {:?}", part.kind);
        }
    }
}


///
pub fn parse_message_and_sequence_header(rdr: &mut BufRead) -> PrtResult<(i16,Message)> {
    // MESSAGE HEADER: 32 bytes
    let session_id: i64 = try!(rdr.read_i64::<LittleEndian>());                             // I8
    let packet_seq_number: i32 = try!(rdr.read_i32::<LittleEndian>());                      // I4
    let varpart_size: u32 = try!(rdr.read_u32::<LittleEndian>());                           // UI4  not needed?
    let remaining_bufsize: u32 = try!(rdr.read_u32::<LittleEndian>());                      // UI4  not needed?
    let no_of_segs = try!(rdr.read_i16::<LittleEndian>());                                  // I2
    assert!(no_of_segs == 1);

    rdr.consume(10usize);                                                                   // (I1 + B[9])

    // SEGMENT HEADER: 24 bytes
    try!(rdr.read_i32::<LittleEndian>());                                                   // I4 seg_size
    try!(rdr.read_i32::<LittleEndian>());                                                   // I4 seg_offset
    let no_of_parts: i16 = try!(rdr.read_i16::<LittleEndian>());                            // I2
    try!(rdr.read_i16::<LittleEndian>());                                                   // I2 seg_number
    let seg_kind = try!(Kind::from_i8(try!(rdr.read_i8())));                                // I1

    trace!("message and segment header: \
            {{ packet_seq_number = {}, varpart_size = {}, remaining_bufsize = {}, no_of_parts = {} }}",
            packet_seq_number, varpart_size, remaining_bufsize, no_of_parts);

    match seg_kind {
        Kind::Request => {  // only for read_wire
            let request_type = try!(RequestType::from_i8(try!(rdr.read_i8())));             // I1
            let auto_commit = try!(rdr.read_i8()) != 0_i8;                                  // I1
            let command_options = try!(rdr.read_u8());                                      // I1 command_options
            rdr.consume(8_usize);                                                           // B[8] reserved1
            Ok((no_of_parts, Message::Request( Request {
                                                    request_type: request_type,
                                                    auto_commit: auto_commit,
                                                    command_options: command_options,
                                                    parts: Parts::new(),
            })))
        },
        Kind::Reply | Kind::Error => {
            rdr.consume(1_usize);                                                           // I1 reserved2
            let rt = try!(ReplyType::from_i16(try!(rdr.read_i16::<LittleEndian>())));       // I2
            rdr.consume(8_usize);                                                           // B[8] reserved3
            debug!(
                "Reply::parse(): found reply of type {:?} and seg_kind {:?} for session_id {}",
                rt, seg_kind, session_id
            );
            Ok((no_of_parts, Message::Reply(Reply::new(session_id, rt))))
        },
    }
}

/// Specifies the layout of the remaining segment header structure
#[derive(Debug)]
enum Kind {
    Request,
    Reply,
    Error,    // sp1sk_proccall, sp1sk_procreply ,sp1sk_last_segment_kind see api/Communication/Protocol/Layout.hpp
}
impl Kind {
    fn from_i8(val: i8) -> PrtResult<Kind> {match val {
        1 => Ok(Kind::Request),
        2 => Ok(Kind::Reply),
        5 => Ok(Kind::Error),
        _ => Err(prot_err(&format!("Invalid value for message::Kind::from_i8() detected: {}",val))),
    }}
}
