//! Since there is obviously no usecase for multiple segments in one request,
//! we model message and segment together.
//! But we differentiate explicitly between request messages and reply messages.
#[cfg(feature = "sync")]
use byteorder::{LittleEndian, WriteBytesExt};

use std::{io::Cursor, sync::Arc};

use super::SEGMENT_HEADER_SIZE;
use crate::{
    protocol::{
        parts::{ParameterDescriptors, Parts, StatementContext},
        MessageType, Part,
    },
    HdbResult,
};

const MIN_COMPRESSION_SIZE: u32 = 10 * 1024;
const ONE_AS_NUMBER_OF_SEGMENTS: i16 = 1;
const ONE_AS_ORDINAL_OF_THIS_SEGMENT: i16 = 1;
const ZERO_AS_OFFSET: i32 = 0;
const SEGMENT_KIND_REQUEST: i8 = 1;

const PACKET_OPTION_COMPRESS: u8 = 2;

const FILLER_1: u8 = 0;
const FILLER_4: u32 = 0;
const FILLER_8: u64 = 0;
const FILLER_10: [u8; 10] = [0; 10];

pub const HOLD_CURSORS_OVER_COMMIT: u8 = 8;

// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub struct Request<'a> {
    message_type: MessageType,
    command_options: u8,
    parts: Parts<'a>,
}
// Methods for defining a request
impl<'a> Request<'a> {
    pub fn new(request_type: MessageType, command_options: u8) -> Request<'a> {
        Request {
            message_type: request_type,
            command_options,
            parts: Parts::default(),
        }
    }

    pub fn new_for_disconnect() -> Request<'a> {
        Request::new(MessageType::Disconnect, 0)
    }
    pub fn message_type(&self) -> MessageType {
        self.message_type
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
    pub fn emit_sync(
        &self,
        session_id: i64,
        packet_seq_number: i32,
        auto_commit: bool,
        compress: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        w: &mut dyn std::io::Write,
    ) -> HdbResult<()> {
        let no_of_parts = self.parts.len() as i16;
        let uncompressed_parts_size = self.parts.size(true, o_a_descriptors) as u32;

        debug!(
            "Request::sync_emit() of type {:?} for session_id = {session_id}, \
             packet_seq_number = {packet_seq_number}, parts_size = {uncompressed_parts_size}",
            self.message_type,
        );

        // FIXME Avoid use of buffer for small requests
        // serialize parts into buffer  // TODO use pool of buffers
        let mut remaining_bufsize = uncompressed_parts_size;
        let mut cursor = Cursor::new(Vec::<u8>::with_capacity(remaining_bufsize as usize));
        for part in self.parts.ref_inner() {
            remaining_bufsize = part.sync_emit(remaining_bufsize, o_a_descriptors, &mut cursor)?;
        }
        let uncompressed_parts = cursor.into_inner();

        // decide if parts should be sent in compressed form
        let o_compressed_parts = if compress && uncompressed_parts_size > MIN_COMPRESSION_SIZE {
            let compressed_parts = lz4_flex::block::compress(&uncompressed_parts);
            if shrunk_by_at_least_five_percent(&compressed_parts, &uncompressed_parts) {
                Some(compressed_parts)
            } else {
                None
            }
        } else {
            None
        };

        // send the header and the parts
        if let Some(compressed_parts) = o_compressed_parts {
            trace!("using compression");
            self.emit_packet_header_sync(
                session_id,
                packet_seq_number,
                auto_commit,
                no_of_parts,
                PartsSizes::Compressed {
                    uncompressed: uncompressed_parts_size,
                    compressed: compressed_parts.len().try_into().unwrap(/*OK*/),
                },
                w,
            )?;

            w.write_all(&*compressed_parts)?;
        } else {
            self.emit_packet_header_sync(
                session_id,
                packet_seq_number,
                auto_commit,
                no_of_parts,
                PartsSizes::Uncompressed {
                    uncompressed: uncompressed_parts_size,
                },
                w,
            )?;
            w.write_all(&*uncompressed_parts)?;
        }
        w.flush()?;
        trace!("Parts are written");

        Ok(())
    }

    fn emit_packet_header_sync(
        &self,
        session_id: i64,
        packet_seq_number: i32,
        auto_commit: bool,
        no_of_parts: i16,
        parts_sizes: PartsSizes,
        w: &mut dyn std::io::Write,
    ) -> HdbResult<()> {
        let (compress, uncompressed, compressed) = match parts_sizes {
            PartsSizes::Compressed {
                uncompressed,
                compressed,
            } => (true, uncompressed, compressed),
            PartsSizes::Uncompressed { uncompressed } => (false, uncompressed, 0),
        };

        // MESSAGE HEADER
        w.write_i64::<LittleEndian>(session_id)?; // I8
        w.write_i32::<LittleEndian>(packet_seq_number)?; // I4

        w.write_u32::<LittleEndian>(
            if compress { compressed } else { uncompressed } + SEGMENT_HEADER_SIZE,
        )?; // UI4

        w.write_u32::<LittleEndian>(uncompressed + SEGMENT_HEADER_SIZE)?; // UI4
        w.write_i16::<LittleEndian>(ONE_AS_NUMBER_OF_SEGMENTS)?; // I2

        if compress {
            w.write_u8(PACKET_OPTION_COMPRESS)?; // I1
            w.write_u8(FILLER_1)?; // I1
            w.write_u32::<LittleEndian>(uncompressed + SEGMENT_HEADER_SIZE)?; // UI4
            w.write_u32::<LittleEndian>(FILLER_4)?; // UI4
        } else {
            w.write_all(&FILLER_10)?;
        }

        // SEGMENT HEADER
        w.write_u32::<LittleEndian>(uncompressed + SEGMENT_HEADER_SIZE)?; // I4
        w.write_i32::<LittleEndian>(ZERO_AS_OFFSET)?; // I4 Offset within the message buffer
        w.write_i16::<LittleEndian>(no_of_parts)?; // I2 Number of contained parts
        w.write_i16::<LittleEndian>(ONE_AS_ORDINAL_OF_THIS_SEGMENT)?; // I2
        w.write_i8(SEGMENT_KIND_REQUEST)?; // I1
        w.write_i8(self.message_type as i8)?; // I1
        w.write_i8(auto_commit.into())?; // I1
        w.write_u8(self.command_options)?; // I1
        w.write_u64::<LittleEndian>(FILLER_8)?; // [B;8]

        trace!(
            "REQUEST, message and segment header: {{\
                \n  session_id = {session_id}, \
                \n  packet_seq_number = {packet_seq_number}, \
                \n  compressed = {compressed}, \
                \n  uncompressed = {uncompressed}, \
                \n  no_of_parts = {no_of_parts}, \
            }}"
        );

        trace!("Headers are written");
        Ok(())
    }

    #[cfg(feature = "async")]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_possible_wrap)]
    pub async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        session_id: i64,
        seq_number: i32,
        auto_commit: bool,
        _compress: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        w: &mut W,
    ) -> HdbResult<()> {
        let number_of_parts = self.parts.len() as i16;
        let parts_size = self.parts.size(true, o_a_descriptors) as u32;

        debug!(
            "Request::sync_emit() of type {:?} for session_id = {session_id}, \
             seq_number = {seq_number}, parts_size = {parts_size}",
            self.message_type,
        );

        // FIXME make use of compress!!!

        // MESSAGE HEADER
        w.write_i64_le(session_id).await?; // I8 <LittleEndian>
        w.write_i32_le(seq_number).await?; // I4
        w.write_u32_le(parts_size + SEGMENT_HEADER_SIZE).await?; // UI4
        w.write_u32_le(parts_size + SEGMENT_HEADER_SIZE).await?; // UI4
        w.write_i16_le(1).await?; // I2    Number of segments
        for _ in 0..10_u8 {
            w.write_u8(0).await?;
        } // I1+ B[9]  unused

        // SEGMENT HEADER
        w.write_u32_le(parts_size + SEGMENT_HEADER_SIZE).await?; // I4  Length including the header
        w.write_i32_le(0).await?; // I4 Offset within the message buffer
        w.write_i16_le(number_of_parts).await?; // I2 Number of contained parts
        w.write_i16_le(1).await?; // I2 Number of this segment, starting with 1
        w.write_i8(1).await?; // I1 Segment kind: always 1 = Request
        w.write_i8(self.message_type as i8).await?; // I1 "Message type"
        w.write_i8(auto_commit.into()).await?; // I1 auto_commit on/off
        w.write_u8(self.command_options).await?; // I1 Bit set for options
        for _ in 0..8_u8 {
            w.write_u8(0).await?;
        } // [B;8] Reserved, do not use

        trace!("Headers are written");

        // PARTS
        let mut remaining_bufsize = parts_size;
        for part in self.parts.ref_inner() {
            remaining_bufsize = part
                .async_emit(remaining_bufsize, o_a_descriptors, w)
                .await?;
        }
        w.flush().await?;
        trace!("Parts are written");
        Ok(())
    }
}

enum PartsSizes {
    Compressed { uncompressed: u32, compressed: u32 },
    Uncompressed { uncompressed: u32 },
}
fn shrunk_by_at_least_five_percent(compressed: &[u8], uncompressed: &[u8]) -> bool {
    let c = compressed.len();
    let u = uncompressed.len();
    c < u && u - c > u / 20
}
