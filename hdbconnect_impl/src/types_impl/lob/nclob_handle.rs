#[cfg(feature = "async")]
use super::fetch::fetch_a_lob_chunk_async;

#[cfg(feature = "sync")]
use super::fetch::fetch_a_lob_chunk_sync;

use super::{CharLobSlice, LobBuf, UTF_BUFFER_SIZE};
use crate::{
    base::{RsCore, OAM},
    conn::AmConnCore,
    protocol::util,
    {HdbError, HdbResult, ServerUsage},
};
use debug_ignore::DebugIgnore;
use std::io::{Cursor, Write};

// `NCLobHandle` is used for NCLOBs that we receive from the database.
// The data are often not transferred completely, so we carry internally
// a database connection and the necessary controls to support fetching remaining data on demand.
// The data stream can be cut into chunks between valid 1-, 2-, or 3-byte sequences.
// Since surrogate pairs can be cut in two halfs (two 3-byte sequences), we may need to buffer
// an orphaned surrogate between two fetches.
#[derive(Clone, Debug)]
pub(crate) struct NCLobHandle {
    pub(crate) am_conn_core: AmConnCore,
    o_am_rscore: OAM<RsCore>,
    is_data_complete: bool,
    total_char_length: u64,
    total_byte_length: u64,
    locator_id: u64,
    cesu8: DebugIgnore<LobBuf>,
    utf8: DebugIgnore<LobBuf>,
    acc_byte_length: usize,
    acc_char_length: usize,
    pub(crate) server_usage: ServerUsage,
}
impl NCLobHandle {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &OAM<RsCore>,
        is_data_complete: bool,
        total_char_length: u64,
        total_byte_length: u64,
        locator_id: u64,
        cesu8: Vec<u8>,
    ) -> Self {
        let acc_char_length = count_1_2_3_sequence_starts(&cesu8);
        let acc_byte_length = cesu8.len();

        let nclob_handle = Self {
            am_conn_core: am_conn_core.clone(),
            o_am_rscore: o_am_rscore.clone(),
            total_char_length,
            total_byte_length,
            is_data_complete,
            locator_id,
            cesu8: DebugIgnore::from(LobBuf::with_initial_content(cesu8)),
            utf8: DebugIgnore::from(LobBuf::with_capacity(UTF_BUFFER_SIZE)),
            acc_byte_length,
            acc_char_length,
            server_usage: ServerUsage::default(),
        };

        trace!(
            "new() with: is_data_complete = {}, total_char_length = {}, total_byte_length = {}, \
             locator_id = {}, cesu8.len() = {}",
            nclob_handle.is_data_complete,
            nclob_handle.total_char_length,
            nclob_handle.total_byte_length,
            nclob_handle.locator_id,
            nclob_handle.cesu8.len()
        );
        nclob_handle
    }

    #[cfg(feature = "sync")]
    pub(crate) fn read_slice_sync(&mut self, offset: u64, length: u32) -> HdbResult<CharLobSlice> {
        let (reply_data, _reply_is_last_data) = fetch_a_lob_chunk_sync(
            &self.am_conn_core,
            self.locator_id,
            offset,
            length,
            &mut self.server_usage,
        )?;
        debug!("read_slice(): got {} bytes", reply_data.len());
        util::split_off_orphaned_surrogates(reply_data)
    }

    #[cfg(feature = "async")]
    pub(crate) async fn read_slice_async(
        &mut self,
        offset: u64,
        length: u32,
    ) -> HdbResult<CharLobSlice> {
        let (reply_data, _reply_is_last_data) = fetch_a_lob_chunk_async(
            &self.am_conn_core,
            self.locator_id,
            offset,
            length,
            &mut self.server_usage,
        )
        .await?;
        debug!("read_slice(): got {} bytes", reply_data.len());
        util::split_off_orphaned_surrogates(reply_data)
    }

    pub(crate) fn total_byte_length(&self) -> u64 {
        self.total_byte_length
    }

    pub(crate) fn total_char_length(&self) -> u64 {
        self.total_char_length
    }

    pub(crate) fn cur_buf_len(&self) -> usize {
        self.cesu8.len()
    }

    #[cfg(feature = "sync")]
    #[allow(clippy::cast_possible_truncation)]
    fn fetch_next_chunk_sync(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::Impl("fetch_next_chunk_sync: already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core
                .lock_sync()?
                .configuration()
                .lob_read_length(),
            (self.total_char_length - self.acc_char_length as u64) as u32,
        );

        let (reply_data, reply_is_last_data) = fetch_a_lob_chunk_sync(
            &self.am_conn_core,
            self.locator_id,
            self.acc_char_length as u64,
            read_length,
            &mut self.server_usage,
        )?;

        self.acc_byte_length += reply_data.len();
        self.acc_char_length += count_1_2_3_sequence_starts(&reply_data);
        self.cesu8.append(&reply_data);

        if reply_is_last_data {
            self.is_data_complete = true;
            self.o_am_rscore = None;
        }

        assert_eq!(
            self.is_data_complete,
            self.total_byte_length == self.acc_byte_length as u64
        );
        trace!(
            "fetch_next_chunk_sync: is_data_complete = {}, cesu8.len() = {}",
            self.is_data_complete,
            self.cesu8.len()
        );
        Ok(())
    }

    #[cfg(feature = "async")]
    #[allow(clippy::cast_possible_truncation)]
    async fn fetch_next_chunk_async(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::Impl("fetch_next_chunk_async(): already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core
                .lock_async()
                .await
                .configuration()
                .lob_read_length(),
            (self.total_char_length - self.acc_char_length as u64) as u32,
        );

        let (reply_data, reply_is_last_data) = fetch_a_lob_chunk_async(
            &self.am_conn_core,
            self.locator_id,
            self.acc_char_length as u64,
            read_length,
            &mut self.server_usage,
        )
        .await?;

        self.acc_byte_length += reply_data.len();
        self.acc_char_length += count_1_2_3_sequence_starts(&reply_data);
        self.cesu8.append(&reply_data);

        if reply_is_last_data {
            self.is_data_complete = true;
            self.o_am_rscore = None;
        }

        assert_eq!(
            self.is_data_complete,
            self.total_byte_length == self.acc_byte_length as u64
        );
        trace!(
            "fetch_next_chunk_async(): is_data_complete = {}, cesu8.len() = {}",
            self.is_data_complete,
            self.cesu8.len()
        );
        Ok(())
    }

    #[cfg(feature = "sync")]
    pub(crate) fn load_complete_sync(&mut self) -> HdbResult<()> {
        trace!("NCLobHandle::load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk_sync()?;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub(crate) async fn load_complete_async(&mut self) -> HdbResult<()> {
        trace!("NCLobHandle::load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk_async().await?;
        }
        Ok(())
    }

    // Converts a NCLobHandle into a String containing its data, if it is fully loaded
    pub(crate) fn into_string_if_complete(self) -> HdbResult<String> {
        trace!("NCLobHandle::into_string()");
        if self.is_data_complete {
            Ok(util::string_from_cesu8(self.cesu8.0.into_inner())?)
        } else {
            Err(HdbError::Usage(
                "NCLob must be loaded completely before 'into_string' can be called",
            ))
        }
    }

    // assumption: utf8 is empty
    // fills utf8 buffer from cesu8: drain 8k from cesu8, convert, store as utf8
    #[cfg(feature = "sync")]
    fn fill_utf8_buffer_sync(&mut self) -> std::io::Result<()> {
        // refill cesu8 if necessary
        if self.cesu8.len() < UTF_BUFFER_SIZE && !self.is_data_complete {
            self.fetch_next_chunk_sync()
                .map_err(|e| util::io_error(e.to_string()))?;
        }

        // now refill utf8
        let mut chunk_size = std::cmp::min(UTF_BUFFER_SIZE, self.cesu8.len());
        chunk_size -= util::get_cesu8_tail_len(&*self.cesu8, chunk_size)?;
        self.utf8.append(
            cesu8::from_cesu8(self.cesu8.drain(chunk_size)?)
                .map_err(util::io_error)?
                .as_bytes(),
        );
        Ok(())
    }
    #[cfg(feature = "async")]
    async fn fill_utf8_buffer_async(&mut self) -> std::io::Result<()> {
        // refill cesu8 if necessary
        if self.cesu8.len() < UTF_BUFFER_SIZE && !self.is_data_complete {
            self.fetch_next_chunk_async()
                .await
                .map_err(|e| util::io_error(e.to_string()))?;
        }

        // now refill utf8
        let mut chunk_size = std::cmp::min(UTF_BUFFER_SIZE, self.cesu8.len());
        chunk_size -= util::get_cesu8_tail_len(&*self.cesu8, chunk_size)?;
        self.utf8.append(
            cesu8::from_cesu8(self.cesu8.drain(chunk_size)?)
                .map_err(util::io_error)?
                .as_bytes(),
        );
        Ok(())
    }
}

// Read from the DB chunks of lob_read_size into self.cesu8,
// then drain from there chunks of 8k (shortened if necessary to make the chunk valid cesu8)
// convert each into utf8 and store it as self.utf8,
// and drain utf8 into the external buffer.
#[cfg(feature = "sync")]
// Support for NCLOB streaming
impl std::io::Read for NCLobHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        trace!("NCLobHandle::read() called with buffer of size {buf_len}");
        let mut cursor = Cursor::new(buf);
        let mut written = 0;

        while written < buf_len {
            if self.utf8.is_empty() {
                self.fill_utf8_buffer_sync()?;
                if self.utf8.is_empty() {
                    break;
                }
            }

            let chunk_size = std::cmp::min(buf_len - written, self.utf8.len());
            cursor.write_all(self.utf8.drain(chunk_size)?)?;
            written += chunk_size;
        }
        Ok(written)
    }
}

#[cfg(feature = "async")]
impl NCLobHandle {
    pub(crate) async fn read_async(&mut self, buf: &mut [u8]) -> HdbResult<usize> {
        let buf_len = buf.len();
        trace!("CLobHandle::read called with buffer of size {buf_len}");
        let mut cursor = Cursor::new(buf);
        let mut written = 0;

        while written < buf_len {
            if self.utf8.is_empty() {
                self.fill_utf8_buffer_async().await?;
                if self.utf8.is_empty() {
                    break;
                }
            }

            let chunk_size = std::cmp::min(buf_len - written, self.utf8.len());
            cursor.write_all(self.utf8.drain(chunk_size)?)?;
            written += chunk_size;
        }
        Ok(written)
    }
}

fn count_1_2_3_sequence_starts(cesu8: &[u8]) -> usize {
    cesu8.iter().filter(|b| is_utf8_char_start(**b)).count()
}
fn is_utf8_char_start(b: u8) -> bool {
    matches!(b, 0x00..=0x7F | 0xC0..=0xDF | 0xE0..=0xEF | 0xF0..=0xF7)
}
