use crate::{
    HdbResult,
    conn::{CommandOptions, ConnectionConfiguration, ConnectionStatistics},
    protocol::{
        MESSAGE_AND_SEGMENT_HEADER_SIZE, MessageType, Part, SEGMENT_HEADER_SIZE,
        parts::{ParameterDescriptors, Parts, StatementContext},
    },
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{io::Cursor, sync::Arc};

const ONE_AS_NUMBER_OF_SEGMENTS: i16 = 1;
const ONE_AS_ORDINAL_OF_THIS_SEGMENT: i16 = 1;
const ZERO_AS_OFFSET: i32 = 0;
const SEGMENT_KIND_REQUEST: i8 = 1;

const PACKET_OPTION_COMPRESS: u8 = 2;

const FILLER_1: u8 = 0;
const FILLER_4: u32 = 0;
const FILLER_8: u64 = 0;
const FILLER_10: [u8; 10] = [0; 10];

// Packets having the same sequence number belong to one request/response pair.
#[derive(Debug)]
pub(crate) struct Request<'a> {
    message_type: MessageType,
    command_options: CommandOptions,
    parts: Parts<'a>,
}
// Methods for defining a request
impl<'a> Request<'a> {
    pub fn new(message_type: MessageType, command_options: CommandOptions) -> Request<'a> {
        Request {
            message_type,
            command_options,
            parts: Parts::default(),
        }
    }

    pub fn new_for_disconnect() -> Request<'a> {
        Request::new(MessageType::Disconnect, CommandOptions::EMPTY)
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
        trace!("Sending StatementContext with sequence_info = {ssi_value:?}");
        self.push(Part::StatementContext(stmt_ctx));
    }

    #[cfg(feature = "sync")]
    #[allow(clippy::too_many_arguments)]
    pub fn emit_sync(
        &self,
        session_id: i64,
        packet_seq_number: u32,
        config: &ConnectionConfiguration,
        compress: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        statistics: &mut ConnectionStatistics,
        io_buffer: &mut Cursor<Vec<u8>>,
        w: &mut dyn std::io::Write,
    ) -> HdbResult<std::time::Instant> {
        io_buffer.get_mut().clear();
        let uncompressed_parts_size = self.parts.size(o_a_descriptors);

        //  write uncompressed parts to buffer, and leave space for the packet header
        let capa = io_buffer.get_ref().capacity();
        if capa < MESSAGE_AND_SEGMENT_HEADER_SIZE + uncompressed_parts_size {
            io_buffer
                .get_mut()
                .reserve(MESSAGE_AND_SEGMENT_HEADER_SIZE + uncompressed_parts_size - capa);
        }
        io_buffer.set_position(MESSAGE_AND_SEGMENT_HEADER_SIZE as u64);
        let mut remaining_bufsize = u32::try_from(uncompressed_parts_size).unwrap(/*OK*/);
        for part in self.parts.ref_inner() {
            remaining_bufsize = part.emit(remaining_bufsize, o_a_descriptors, io_buffer)?;
        }

        // decide if parts should be sent in compressed form, and compress if necessary
        let o_compressed_parts = if compress
            && uncompressed_parts_size > config.min_compression_size()
        {
            io_buffer.set_position(MESSAGE_AND_SEGMENT_HEADER_SIZE as u64);
            let compressed_parts =
                lz4_flex::block::compress(&io_buffer.get_ref()[MESSAGE_AND_SEGMENT_HEADER_SIZE..]);

            if shrunk_by_at_least_five_percent(compressed_parts.len(), uncompressed_parts_size) {
                Some(compressed_parts)
            } else {
                None
            }
        } else {
            None
        };

        // write header to beginning of buffer
        io_buffer.set_position(0);
        self.emit_packet_header(
            session_id,
            packet_seq_number,
            config.is_auto_commit(),
            u32::try_from(uncompressed_parts_size).expect("uncompressed parts too big"),
            o_compressed_parts.as_ref().map(|data| {
                trace!("sending compressed request");
                u32::try_from(data.len()).expect("compressed parts too big")
            }),
            io_buffer,
        )?;

        // serialize request to stream
        let start = std::time::Instant::now();
        if let Some(compressed_parts) = o_compressed_parts {
            // serialize header to stream
            io_buffer.set_position(0);
            w.write_all(&io_buffer.get_ref()[0..MESSAGE_AND_SEGMENT_HEADER_SIZE])?;

            // serialize compressed parts to stream
            w.write_all(&compressed_parts)?;
            statistics.add_compressed_request(compressed_parts.len(), uncompressed_parts_size);
        } else {
            // serialize header and uncompressed parts to stream
            io_buffer.set_position(0);
            w.write_all(io_buffer.get_ref())?;
        }
        w.flush()?;
        trace!("Parts are written");

        io_buffer.get_mut().clear();
        Ok(start)
    }

    fn emit_packet_header(
        &self,
        session_id: i64,
        packet_sequence_number: u32,
        auto_commit: bool,
        uncompressed_size: u32,
        o_compressed_size: Option<u32>,
        w: &mut dyn std::io::Write,
    ) -> HdbResult<()> {
        let (compress, compressed_size) = match o_compressed_size {
            Some(compressed_size) => (true, compressed_size),
            None => (false, 0),
        };

        trace!(
            "REQUEST, message and segment header: {{\
                \n  session_id = {session_id}, \
                \n  packet_seq_number = {packet_sequence_number}, \
                \n  compressed = {compressed_size}, \
                \n  uncompressed = {uncompressed_size}, \
            }}"
        );

        // MESSAGE HEADER
        w.write_i64::<LittleEndian>(session_id)?; // I8
        w.write_u32::<LittleEndian>(packet_sequence_number)?; // I4

        w.write_u32::<LittleEndian>(
            if compress {
                compressed_size
            } else {
                uncompressed_size
            } + SEGMENT_HEADER_SIZE,
        )?; // UI4

        w.write_u32::<LittleEndian>(uncompressed_size + SEGMENT_HEADER_SIZE)?; // UI4
        w.write_i16::<LittleEndian>(ONE_AS_NUMBER_OF_SEGMENTS)?; // I2

        if compress {
            w.write_u8(PACKET_OPTION_COMPRESS)?; // I1
            w.write_u8(FILLER_1)?; // I1
            w.write_u32::<LittleEndian>(uncompressed_size + SEGMENT_HEADER_SIZE)?; // UI4
            w.write_u32::<LittleEndian>(FILLER_4)?; // UI4
        } else {
            w.write_all(&FILLER_10)?;
        }

        // (first and only) SEGMENT HEADER
        w.write_u32::<LittleEndian>(uncompressed_size + SEGMENT_HEADER_SIZE)?; // I4
        w.write_i32::<LittleEndian>(ZERO_AS_OFFSET)?; // I4
        w.write_u16::<LittleEndian>(u16::try_from(self.parts.len()).unwrap(/*OK*/))?; // I2 Number of contained parts
        w.write_i16::<LittleEndian>(ONE_AS_ORDINAL_OF_THIS_SEGMENT)?; // I2
        w.write_i8(SEGMENT_KIND_REQUEST)?; // I1
        w.write_i8(self.message_type as i8)?; // I1
        w.write_i8(auto_commit.into())?; // I1
        w.write_u8(self.command_options.as_u8())?; // I1
        w.write_u64::<LittleEndian>(FILLER_8)?; // [B;8]

        trace!("Headers are written");
        Ok(())
    }

    #[cfg(feature = "async")]
    #[allow(clippy::too_many_arguments)]
    pub async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        session_id: i64,
        packet_sequence_number: u32,
        config: &ConnectionConfiguration,
        compress: bool,
        o_a_descriptors: Option<&Arc<ParameterDescriptors>>,
        statistics: &mut ConnectionStatistics,
        io_buffer: &mut Cursor<Vec<u8>>,
        w: &mut W,
    ) -> HdbResult<std::time::Instant> {
        io_buffer.get_mut().clear();
        let uncompressed_parts_size = self.parts.size(o_a_descriptors);

        //  write uncompressed parts to buffer, and leave space for the packet header
        let capa = io_buffer.get_ref().capacity();
        if capa < MESSAGE_AND_SEGMENT_HEADER_SIZE + uncompressed_parts_size {
            io_buffer
                .get_mut()
                .reserve(MESSAGE_AND_SEGMENT_HEADER_SIZE + uncompressed_parts_size - capa);
        }
        io_buffer.set_position(MESSAGE_AND_SEGMENT_HEADER_SIZE as u64);
        let mut remaining_bufsize = u32::try_from(uncompressed_parts_size).unwrap(/*OK*/);
        for part in self.parts.ref_inner() {
            remaining_bufsize = part.emit(remaining_bufsize, o_a_descriptors, io_buffer)?;
        }

        // decide if parts should be sent in compressed form, and compress if necessary
        let o_compressed_parts = if compress
            && uncompressed_parts_size > config.min_compression_size()
        {
            io_buffer.set_position(MESSAGE_AND_SEGMENT_HEADER_SIZE as u64);
            let compressed_parts =
                lz4_flex::block::compress(&io_buffer.get_ref()[MESSAGE_AND_SEGMENT_HEADER_SIZE..]);

            if shrunk_by_at_least_five_percent(compressed_parts.len(), uncompressed_parts_size) {
                Some(compressed_parts)
            } else {
                None
            }
        } else {
            None
        };

        // write header to beginning of buffer
        io_buffer.set_position(0);
        self.emit_packet_header(
            session_id,
            packet_sequence_number,
            config.is_auto_commit(),
            u32::try_from(uncompressed_parts_size).expect("uncompressed parts too big"),
            o_compressed_parts.as_ref().map(|data| {
                trace!("sending compressed request");
                u32::try_from(data.len()).expect("compressed parts too big")
            }),
            io_buffer,
        )?;

        // serialize request to stream
        let start = std::time::Instant::now();
        if let Some(compressed_parts) = o_compressed_parts {
            // serialize header to stream
            io_buffer.set_position(0);
            w.write_all(&io_buffer.get_ref()[0..MESSAGE_AND_SEGMENT_HEADER_SIZE])
                .await?;

            // serialize compressed parts to stream
            w.write_all(&compressed_parts).await?;
            statistics.add_compressed_request(compressed_parts.len(), uncompressed_parts_size);
        } else {
            // serialize header and uncompressed parts to stream
            io_buffer.set_position(0);
            w.write_all(io_buffer.get_ref()).await?;
        }
        w.flush().await?;
        trace!("Parts are written");

        io_buffer.get_mut().clear();
        Ok(start)
    }
}

fn shrunk_by_at_least_five_percent(c: usize, u: usize) -> bool {
    c < u && u - c > u / 20
}
