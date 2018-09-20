//! Since there is obviously no usecase for multiple segments in one request,
//! we model message and segment together.
//! But we differentiate explicitly between request messages and reply messages.
use super::argument::Argument;
use super::part::{Part, Parts};
use super::partkind::PartKind;
use super::parts::parameter_descriptor::ParameterDescriptor;
use super::parts::resultset::ResultSet;
use super::parts::resultset_metadata::ResultSetMetadata;
use super::parts::statement_context::StatementContext;
use super::reply_type::ReplyType;
use super::request_type::RequestType;
use byteorder::{LittleEndian, WriteBytesExt};
use chrono::Local;
use conn_core::AmConnCore;
use protocol::reply::Reply;
use protocol::reply::SkipLastSpace;
use std::io;
use {HdbResponse, HdbResult};

const MESSAGE_HEADER_SIZE: u32 = 32;
const SEGMENT_HEADER_SIZE: usize = 24; // same for in and out

// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Request {
    pub request_type: RequestType,
    command_options: u8,
    parts: Parts,
}
// Methods for defining a request
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

    pub fn push(&mut self, part: Part) {
        self.parts.push(part);
    }
}

// Methods for sending the request
impl Request {
    pub fn send_and_get_hdbresponse(
        self,
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        am_conn_core: &mut AmConnCore,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
    ) -> HdbResult<HdbResponse> {
        let reply = self.send_and_get_reply(
            o_rs_md,
            o_par_md,
            &mut None,
            am_conn_core,
            expected_reply_type,
            skip,
        )?;

        reply.into_hdbresponse(am_conn_core)
    }

    // simplified interface
    pub fn send_and_get_reply_simplified(
        self,
        am_conn_core: &mut AmConnCore,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
    ) -> HdbResult<Reply> {
        self.send_and_get_reply(
            None,
            None,
            &mut None,
            am_conn_core,
            expected_reply_type,
            skip,
        )
    }

    pub fn send_and_get_reply(
        mut self,
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        am_conn_core: &mut AmConnCore,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
    ) -> HdbResult<Reply> {
        trace!(
            "Request::send_and_get_reply() with requestType = {:?}",
            self.request_type,
        );
        let _start = Local::now();
        self.add_statement_sequence(am_conn_core)?;

        let mut reply = self.roundtrip(
            o_rs_md,
            o_par_md,
            o_rs,
            am_conn_core,
            expected_reply_type,
            skip,
        )?;

        reply.handle_db_error(am_conn_core)?;

        debug!(
            "Request::send_and_get_reply() took {} ms",
            (Local::now().signed_duration_since(_start)).num_milliseconds()
        );
        Ok(reply)
    }

    fn add_statement_sequence(&mut self, am_conn_core: &AmConnCore) -> HdbResult<()> {
        let guard = am_conn_core.lock()?;
        match *(*guard).statement_sequence() {
            None => {}
            Some(ssi_value) => {
                let mut stmt_ctx: StatementContext = Default::default();
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

    fn roundtrip(
        self,
        o_rs_md: Option<&ResultSetMetadata>,
        o_par_md: Option<&Vec<ParameterDescriptor>>,
        o_rs: &mut Option<&mut ResultSet>,
        am_conn_core: &AmConnCore,
        expected_reply_type: Option<ReplyType>,
        skip: SkipLastSpace,
    ) -> HdbResult<Reply> {
        trace!("request::roundtrip()");
        let mut conn_core = am_conn_core.lock()?;
        conn_core.roundtrip(
            self,
            am_conn_core,
            o_rs_md,
            o_par_md,
            o_rs,
            expected_reply_type,
            skip,
        )
    }

    pub fn serialize(
        self,
        session_id: i64,
        seq_number: i32,
        auto_commit_flag: i8,
        w: &mut io::Write,
    ) -> HdbResult<()> {
        let varpart_size = self.varpart_size()?;
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing request with total size {}", total_size);
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
