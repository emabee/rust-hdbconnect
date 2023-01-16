//! Since there is obviously no usecase for multiple segments in one request,
//! we model message and segment together.
//! But we differentiate explicitly between request messages and reply messages.
use crate::protocol::parts::{ParameterDescriptors, Parts, StatementContext};
use crate::protocol::{Part, RequestType};
#[cfg(feature = "sync")]
use byteorder::{LittleEndian, WriteBytesExt};
use std::sync::Arc;

#[cfg(feature = "async")]
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::Mutex,
};

const MESSAGE_HEADER_SIZE: u32 = 32;
const SEGMENT_HEADER_SIZE: usize = 24; // same for in and out
pub const HOLD_CURSORS_OVER_COMMIT: u8 = 8;

// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Request<'a> {
    pub request_type: RequestType,
    command_options: u8,
    parts: Parts<'a>,
}
// Methods for defining a request
impl<'a> Request<'a> {
    pub fn new(request_type: RequestType, command_options: u8) -> Request<'a> {
        Request {
            request_type,
            command_options,
            parts: Parts::default(),
        }
    }

    pub fn new_for_disconnect() -> Request<'a> {
        Request::new(RequestType::Disconnect, 0)
    }

    pub fn push(&mut self, part: Part<'a>) {
        self.parts.push(part);
    }

    pub fn add_statement_context(&mut self, ssi_value: i64) {
        let mut stmt_ctx = StatementContext::default();
        stmt_ctx.set_statement_sequence_info(ssi_value);
        trace!(
            "Sending StatementContext with sequence_info = {:?}",
            ssi_value
        );
        self.push(Part::StatementContext(stmt_ctx));
    }

    #[cfg(feature = "sync")]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_possible_wrap)]
    pub fn sync_emit(
        &self,
        session_id: i64,
        seq_number: i32,
        auto_commit: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        let varpart_size = self.varpart_size(o_a_descriptors)?;
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing request with total size {}", total_size);
        let mut remaining_bufsize = total_size - MESSAGE_HEADER_SIZE;

        debug!(
            "Request::emit() of type {:?} for session_id = {}, seq_number = {}",
            self.request_type, session_id, seq_number
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
        let size = self.seg_size(o_a_descriptors)? as i32;
        w.write_i32::<LittleEndian>(size)?; // I4  Length including the header
        w.write_i32::<LittleEndian>(0)?; // I4 Offset within the message buffer
        w.write_i16::<LittleEndian>(parts_len)?; // I2 Number of contained parts
        w.write_i16::<LittleEndian>(1)?; // I2 Number of this segment, starting with 1
        w.write_i8(1)?; // I1 Segment kind: always 1 = Request
        w.write_i8(self.request_type as i8)?; // I1 "Message type"
        w.write_i8(auto_commit.into())?; // I1 auto_commit on/off
        w.write_u8(self.command_options)?; // I1 Bit set for options
        for _ in 0..8 {
            w.write_u8(0)?;
        } // [B;8] Reserved, do not use

        remaining_bufsize -= SEGMENT_HEADER_SIZE as u32;
        trace!("Headers are written");
        // PARTS
        for part in self.parts.ref_inner() {
            remaining_bufsize = part.sync_emit(remaining_bufsize, o_a_descriptors, w)?;
        }
        w.flush()?;
        trace!("Parts are written");
        Ok(())
    }

    #[cfg(feature = "async")]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_possible_wrap)]
    pub async fn async_emit(
        &self,
        session_id: i64,
        seq_number: i32,
        auto_commit: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        am_w: Arc<Mutex<BufWriter<TcpStream>>>,
    ) -> std::io::Result<()> {
        let varpart_size = self.varpart_size(o_a_descriptors)?;
        let total_size = MESSAGE_HEADER_SIZE + varpart_size;
        trace!("Writing request with total size {}", total_size);
        let mut remaining_bufsize = total_size - MESSAGE_HEADER_SIZE;

        debug!(
            "Request::emit() of type {:?} for session_id = {}, seq_number = {}",
            self.request_type, session_id, seq_number
        );

        let mut writer = am_w.lock().await;
        let w = &mut *writer;
        // MESSAGE HEADER
        w.write_i64_le(session_id).await?; // I8 <LittleEndian>
        w.write_i32_le(seq_number).await?; // I4
        w.write_u32_le(varpart_size).await?; // UI4
        w.write_u32_le(remaining_bufsize).await?; // UI4
        w.write_i16_le(1).await?; // I2    Number of segments
        for _ in 0..10_u8 {
            w.write_u8(0).await?;
        } // I1+ B[9]  unused

        // SEGMENT HEADER
        let parts_len = self.parts.len() as i16;
        let size = self.seg_size(o_a_descriptors)? as i32;
        w.write_i32_le(size).await?; // I4  Length including the header
        w.write_i32_le(0).await?; // I4 Offset within the message buffer
        w.write_i16_le(parts_len).await?; // I2 Number of contained parts
        w.write_i16_le(1).await?; // I2 Number of this segment, starting with 1
        w.write_i8(1).await?; // I1 Segment kind: always 1 = Request
        w.write_i8(self.request_type as i8).await?; // I1 "Message type"
        w.write_i8(auto_commit.into()).await?; // I1 auto_commit on/off
        w.write_u8(self.command_options).await?; // I1 Bit set for options
        for _ in 0..8_u8 {
            w.write_u8(0).await?;
        } // [B;8] Reserved, do not use

        remaining_bufsize -= SEGMENT_HEADER_SIZE as u32;
        trace!("Headers are written");
        // PARTS
        for part in self.parts.ref_inner() {
            remaining_bufsize = part
                .async_emit(remaining_bufsize, o_a_descriptors, w)
                .await?;
        }
        w.flush().await?;
        trace!("Parts are written");
        Ok(())
    }

    // Length in bytes of the variable part of the message, i.e. total message
    // without the header
    #[allow(clippy::cast_possible_truncation)]
    fn varpart_size(
        &self,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
    ) -> std::io::Result<u32> {
        let mut len = 0_u32;
        len += self.seg_size(o_a_descriptors)? as u32;
        trace!("varpart_size = {}", len);
        Ok(len)
    }

    fn seg_size(
        &self,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
    ) -> std::io::Result<usize> {
        let mut len = SEGMENT_HEADER_SIZE;
        for part in self.parts.ref_inner() {
            len += part.size(true, o_a_descriptors)?;
        }
        Ok(len)
    }
}
