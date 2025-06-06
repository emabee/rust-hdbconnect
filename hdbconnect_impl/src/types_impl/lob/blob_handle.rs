#[cfg(feature = "async")]
use crate::usage_err;

use super::LobBuf;
#[cfg(feature = "async")]
use super::fetch::fetch_a_lob_chunk_async;
#[cfg(feature = "sync")]
use super::fetch::fetch_a_lob_chunk_sync;
use crate::{
    HdbResult,
    base::{OAM, RsCore, XMutexed},
    conn::AmConnCore,
    impl_err,
    protocol::ServerUsage,
};
use debug_ignore::DebugIgnore;
use std::{
    io::{Cursor, Write},
    sync::Arc,
};

// `BLobHandle` is used for blobs that we receive from the database.
// The data are often not transferred completely, so we carry internally
// a database connection and the necessary controls to support fetching
// remaining data on demand.
#[derive(Clone, Debug)]
pub(crate) struct BLobHandle {
    pub(crate) am_conn_core: AmConnCore,
    o_am_rscore: Option<Arc<XMutexed<RsCore>>>,
    is_data_complete: bool,
    total_byte_length: u64,
    locator_id: u64,
    data: DebugIgnore<LobBuf>,
    acc_byte_length: usize,
    pub(crate) server_usage: ServerUsage,
}
impl BLobHandle {
    #[allow(clippy::ref_option)]
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &OAM<RsCore>,
        is_data_complete: bool,
        total_byte_length: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> Self {
        let data = DebugIgnore::from(LobBuf::with_initial_content(data));
        Self {
            am_conn_core: am_conn_core.clone(),
            o_am_rscore: o_am_rscore.clone(),
            total_byte_length,
            is_data_complete,
            locator_id,
            acc_byte_length: data.len(),
            data,
            server_usage: ServerUsage::default(),
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn read_slice_sync(&mut self, offset: u64, length: u32) -> HdbResult<Vec<u8>> {
        let (reply_data, _reply_is_last_data) = fetch_a_lob_chunk_sync(
            &self.am_conn_core,
            self.locator_id,
            offset,
            length,
            &mut self.server_usage,
        )?;
        debug!("read_slice(): got {} bytes", reply_data.len());
        Ok(reply_data)
    }

    #[cfg(feature = "async")]
    pub(crate) async fn read_slice_async(
        &mut self,
        offset: u64,
        length: u32,
    ) -> HdbResult<Vec<u8>> {
        let (reply_data, _reply_is_last_data) = fetch_a_lob_chunk_async(
            &self.am_conn_core,
            self.locator_id,
            offset,
            length,
            &mut self.server_usage,
        )
        .await?;
        debug!("read_slice(): got {} bytes", reply_data.len());
        Ok(reply_data)
    }

    pub(crate) fn total_byte_length(&self) -> u64 {
        self.total_byte_length
    }

    pub(crate) fn cur_buf_len(&self) -> usize {
        self.data.len()
    }

    #[allow(clippy::cast_possible_truncation)]
    #[cfg(feature = "sync")]
    fn fetch_next_chunk_sync(&mut self) -> HdbResult<usize> {
        if self.is_data_complete {
            return Err(impl_err!("fetch_next_chunk(): already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core
                .lock_sync()?
                .configuration()
                .lob_read_length(),
            (self.total_byte_length - self.acc_byte_length as u64) as u32,
        );

        let (reply_data, reply_is_last_data) = fetch_a_lob_chunk_sync(
            &self.am_conn_core,
            self.locator_id,
            self.acc_byte_length as u64,
            read_length,
            &mut self.server_usage,
        )?;
        let reply_len = reply_data.len();
        self.acc_byte_length += reply_len;

        self.data.append(&reply_data);
        if reply_is_last_data {
            self.is_data_complete = true;
            self.o_am_rscore = None;
        }
        assert_eq!(
            self.is_data_complete,
            self.total_byte_length == self.acc_byte_length as u64
        );
        trace!(
            "fetch_next_chunk: is_data_complete = {}, data.len() = {}",
            self.is_data_complete,
            self.data.len()
        );
        Ok(reply_len)
    }

    #[allow(clippy::cast_possible_truncation)]
    #[cfg(feature = "async")]
    async fn fetch_next_chunk_async(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(impl_err!("fetch_next_chunk(): already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core
                .lock_async()
                .await
                .configuration()
                .lob_read_length(),
            (self.total_byte_length - self.acc_byte_length as u64) as u32,
        );

        let (reply_data, reply_is_last_data) = fetch_a_lob_chunk_async(
            &self.am_conn_core,
            self.locator_id,
            self.acc_byte_length as u64,
            read_length,
            &mut self.server_usage,
        )
        .await?;

        self.acc_byte_length += reply_data.len();
        self.data.append(&reply_data);
        if reply_is_last_data {
            self.is_data_complete = true;
            self.o_am_rscore = None;
        }

        assert_eq!(
            self.is_data_complete,
            self.total_byte_length == self.acc_byte_length as u64
        );
        trace!(
            "fetch_next_chunk: is_data_complete = {}, data.len() = {}",
            self.is_data_complete,
            self.data.len()
        );
        Ok(())
    }

    #[cfg(feature = "sync")]
    pub(crate) fn load_complete_sync(&mut self) -> HdbResult<()> {
        trace!("load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk_sync()?;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub(crate) async fn load_complete_async(&mut self) -> HdbResult<()> {
        trace!("load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk_async().await?;
        }
        Ok(())
    }

    // Converts a BLobHandle into a Vec<u8> containing its data.
    #[cfg(feature = "sync")]
    pub(crate) fn into_bytes_sync(mut self) -> HdbResult<Vec<u8>> {
        trace!("into_bytes()");
        self.load_complete_sync()?;
        Ok(self.data.0.into_inner())
    }

    // Converts a BLobHandle into a Vec<u8> containing its data.
    #[cfg(feature = "async")]
    pub(crate) fn into_bytes_if_complete_async(self) -> HdbResult<Vec<u8>> {
        trace!("into_bytes_if_complete()");
        if self.is_data_complete {
            Ok(self.data.0.into_inner())
        } else {
            Err(usage_err!(
                "Can't convert BLob that is not not completely loaded"
            ))
        }
    }

    #[cfg(feature = "async")]
    pub(crate) async fn read_async(&mut self, buf: &mut [u8]) -> HdbResult<usize> {
        let buf_len = buf.len();
        trace!("BLobHandle::read() with buffer of size {buf_len}");
        let mut cursor = Cursor::new(buf);
        let mut written = 0;

        while written < buf_len {
            if self.data.is_empty() {
                if !self.is_data_complete {
                    self.fetch_next_chunk_async()
                        .await
                        .map_err(std::io::Error::other)?;
                }
                if self.data.is_empty() {
                    break;
                }
            }

            let chunk_size = std::cmp::min(buf_len - written, self.data.len());
            cursor.write_all(self.data.drain(chunk_size)?)?;
            written += chunk_size;
        }
        Ok(written)
    }
}

// Read from the DB chunks of lob_read_size into self.data,
// and drain data into the external buffer.
#[cfg(feature = "sync")]
// Support for streaming
impl std::io::Read for BLobHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        trace!("BLobHandle::read() with buffer of size {buf_len}");
        let mut cursor = Cursor::new(buf);
        let mut written = 0;

        while written < buf_len {
            if self.data.is_empty() {
                if !self.is_data_complete {
                    self.fetch_next_chunk_sync()
                        .map_err(std::io::Error::other)?;
                }
                if self.data.is_empty() {
                    break;
                }
            }

            let chunk_size = std::cmp::min(buf_len - written, self.data.len());
            cursor.write_all(self.data.drain(chunk_size)?)?;
            written += chunk_size;
        }
        Ok(written)
    }
}
