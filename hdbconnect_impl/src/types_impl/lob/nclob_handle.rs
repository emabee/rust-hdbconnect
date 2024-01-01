#[cfg(feature = "async")]
use super::fetch::fetch_a_lob_chunk_async;
#[cfg(feature = "async")]
use tokio::io::ReadBuf;

#[cfg(feature = "sync")]
use super::fetch::fetch_a_lob_chunk_sync;
#[cfg(feature = "sync")]
use std::io::Write;

use crate::{
    base::{RsCore, OAM},
    conn::AmConnCore,
    protocol::util,
    types::CharLobSlice,
    {HdbError, HdbResult, ServerUsage},
};
use std::collections::VecDeque;

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
    cesu8: VecDeque<u8>,
    cesu8_tail_len: usize,
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
    ) -> HdbResult<Self> {
        let acc_char_length = count_1_2_3_sequence_starts(&cesu8);
        let cesu8 = VecDeque::from(cesu8);
        let acc_byte_length = cesu8.len();

        let cesu8_tail_len = util::get_cesu8_tail_len(&cesu8, cesu8.len())?;

        let nclob_handle = Self {
            am_conn_core: am_conn_core.clone(),
            o_am_rscore: o_am_rscore.as_ref().cloned(),
            total_char_length,
            total_byte_length,
            is_data_complete,
            locator_id,
            cesu8,
            cesu8_tail_len,
            acc_byte_length,
            acc_char_length,
            server_usage: ServerUsage::default(),
        };

        trace!(
            "new() with: is_data_complete = {}, total_char_length = {}, total_byte_length = {}, \
             locator_id = {}, cesu8_tail_len = {:?}, cesu8.len() = {}",
            nclob_handle.is_data_complete,
            nclob_handle.total_char_length,
            nclob_handle.total_byte_length,
            nclob_handle.locator_id,
            nclob_handle.cesu8_tail_len,
            nclob_handle.cesu8.len()
        );
        Ok(nclob_handle)
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
            return Err(HdbError::Impl("already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core.lock_sync()?.lob_read_length(),
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

        self.cesu8.append(&mut VecDeque::from(reply_data));
        self.cesu8_tail_len = util::get_cesu8_tail_len(&self.cesu8, self.cesu8.len())?;
        if reply_is_last_data {
            self.is_data_complete = true;
            self.o_am_rscore = None;
        }

        assert_eq!(
            self.is_data_complete,
            self.total_byte_length == self.acc_byte_length as u64
        );
        trace!(
            "fetch_next_chunk: is_data_complete = {}, cesu8.len() = {}",
            self.is_data_complete,
            self.cesu8.len()
        );
        Ok(())
    }

    #[cfg(feature = "async")]
    #[allow(clippy::cast_possible_truncation)]
    async fn fetch_next_chunk_async(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::Impl("already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core.lock_async().await.lob_read_length(),
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

        self.cesu8.append(&mut VecDeque::from(reply_data));
        self.cesu8_tail_len = util::get_cesu8_tail_len(&self.cesu8, self.cesu8.len())?;
        if reply_is_last_data {
            self.is_data_complete = true;
            self.o_am_rscore = None;
        }

        assert_eq!(
            self.is_data_complete,
            self.total_byte_length == self.acc_byte_length as u64
        );
        trace!(
            "fetch_next_chunk: is_data_complete = {}, cesu8.len() = {}",
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
            Ok(util::string_from_cesu8(Vec::from(self.cesu8))?)
        } else {
            Err(HdbError::Usage(
                "NCLob must be loaded completely before 'into_string' can be called",
            ))
        }
    }
}

#[cfg(feature = "sync")]
// Support for NCLOB streaming
impl std::io::Read for NCLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        trace!("read() with buf of len {}", buf.len());

        while !self.is_data_complete && (buf.len() > self.cesu8.len() - self.cesu8_tail_len) {
            self.fetch_next_chunk_sync()
                .map_err(|e| util::io_error(e.to_string()))?;
        }

        // we want to write only clean UTF-8 into buf, so we cut off at good places only;
        // utf8 is equally long as cesu8, or shorter (6->4 bytes for BMP1)
        // so we cut of at the latest char start before buf-len
        let drain_len = std::cmp::min(buf.len(), self.cesu8.len());
        let cesu8_buf: Vec<u8> = self.cesu8.drain(0..drain_len).collect();
        let cut_off_position =
            cesu8_buf.len() - util::get_cesu8_tail_len(&cesu8_buf, cesu8_buf.len())?;

        // convert the valid part to utf-8 and push the tail back
        let utf8 = cesu8::from_cesu8(&cesu8_buf[0..cut_off_position]).map_err(util::io_error)?;
        for i in (cut_off_position..cesu8_buf.len()).rev() {
            self.cesu8.push_front(cesu8_buf[i]);
        }

        buf.write_all(utf8.as_bytes())?;
        Ok(utf8.len())
    }
}

#[cfg(feature = "async")]
impl NCLobHandle {
    pub(crate) async fn read_async(&mut self, buf: &mut [u8]) -> HdbResult<usize> {
        let mut buf = ReadBuf::new(buf);
        let buf_len = buf.capacity();
        debug_assert!(buf.filled().is_empty());
        trace!("read() with buf of len {}", buf_len);

        while !self.is_data_complete && (buf_len > self.cesu8.len() - self.cesu8_tail_len) {
            self.fetch_next_chunk_async().await?;
        }

        // we want to write only clean UTF-8 into buf, so we cut off at good places only;
        // utf8 is equally long as cesu8, or shorter (6->4 bytes for BMP1)
        // so we cut of at the latest char start before buf-len
        let drain_len = std::cmp::min(buf_len, self.cesu8.len());
        let cesu8_buf: Vec<u8> = self.cesu8.drain(0..drain_len).collect();
        let cut_off_position =
            cesu8_buf.len() - util::get_cesu8_tail_len(&cesu8_buf, cesu8_buf.len())?;

        // convert the valid part to utf-8 and push the tail back
        let utf8 =
            cesu8::from_cesu8(&cesu8_buf[0..cut_off_position]).map_err(|_| HdbError::Cesu8)?;
        for i in (cut_off_position..cesu8_buf.len()).rev() {
            self.cesu8.push_front(cesu8_buf[i]);
        }

        buf.put_slice(utf8.as_bytes());
        Ok(utf8.len())
    }
}

fn count_1_2_3_sequence_starts(cesu8: &[u8]) -> usize {
    cesu8.iter().filter(|b| is_utf8_char_start(**b)).count()
}
fn is_utf8_char_start(b: u8) -> bool {
    matches!(b, 0x00..=0x7F | 0xC0..=0xDF | 0xE0..=0xEF | 0xF0..=0xF7)
}
