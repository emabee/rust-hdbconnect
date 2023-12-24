use crate::{
    base::{RsCore, XMutexed, OAM},
    conn::AmConnCore,
    protocol::ServerUsage,
    HdbError, HdbResult,
};
use std::{collections::VecDeque, sync::Arc};

#[cfg(feature = "async")]
use super::fetch::async_fetch_a_lob_chunk;
#[cfg(feature = "sync")]
use super::fetch::sync_fetch_a_lob_chunk;
#[cfg(feature = "sync")]
use std::io::Write;
#[cfg(feature = "async")]
use tokio::io::ReadBuf;

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
    data: VecDeque<u8>,
    acc_byte_length: usize,
    pub(crate) server_usage: ServerUsage,
}
impl BLobHandle {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &OAM<RsCore>,
        is_data_complete: bool,
        total_byte_length: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> Self {
        let data = VecDeque::from(data);
        Self {
            am_conn_core: am_conn_core.clone(),
            o_am_rscore: o_am_rscore.as_ref().cloned(),
            total_byte_length,
            is_data_complete,
            locator_id,
            acc_byte_length: data.len(),
            data,
            server_usage: ServerUsage::default(),
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn sync_read_slice(&mut self, offset: u64, length: u32) -> HdbResult<Vec<u8>> {
        let (reply_data, _reply_is_last_data) = sync_fetch_a_lob_chunk(
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
    pub(crate) async fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<Vec<u8>> {
        let (reply_data, _reply_is_last_data) = async_fetch_a_lob_chunk(
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
    fn fetch_next_chunk_sync(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::Impl("fetch_next_chunk(): already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core.sync_lock()?.lob_read_length(),
            (self.total_byte_length - self.acc_byte_length as u64) as u32,
        );

        let (reply_data, reply_is_last_data) = sync_fetch_a_lob_chunk(
            &self.am_conn_core,
            self.locator_id,
            self.acc_byte_length as u64,
            read_length,
            &mut self.server_usage,
        )?;

        self.acc_byte_length += reply_data.len();
        self.data.append(&mut VecDeque::from(reply_data));
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

    #[allow(clippy::cast_possible_truncation)]
    #[cfg(feature = "async")]
    async fn fetch_next_chunk_async(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::Impl("fetch_next_chunk(): already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core.async_lock().await.lob_read_length(),
            (self.total_byte_length - self.acc_byte_length as u64) as u32,
        );

        let (reply_data, reply_is_last_data) = async_fetch_a_lob_chunk(
            &self.am_conn_core,
            self.locator_id,
            self.acc_byte_length as u64,
            read_length,
            &mut self.server_usage,
        )
        .await?;

        self.acc_byte_length += reply_data.len();
        self.data.append(&mut VecDeque::from(reply_data));
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
    pub(crate) fn sync_load_complete(&mut self) -> HdbResult<()> {
        trace!("load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk_sync()?;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub(crate) async fn async_load_complete(&mut self) -> HdbResult<()> {
        trace!("load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk_async().await?;
        }
        Ok(())
    }

    // Converts a BLobHandle into a Vec<u8> containing its data.
    #[cfg(feature = "sync")]
    pub(crate) fn sync_into_bytes(mut self) -> HdbResult<Vec<u8>> {
        trace!("into_bytes()");
        self.sync_load_complete()?;
        Ok(Vec::from(self.data))
    }

    // Converts a BLobHandle into a Vec<u8> containing its data.
    #[cfg(feature = "async")]
    pub(crate) fn into_bytes_if_complete(self) -> HdbResult<Vec<u8>> {
        trace!("into_bytes_if_complete()");
        if self.is_data_complete {
            Ok(Vec::from(self.data))
        } else {
            Err(HdbError::Usage(
                "Can't convert BLob that is not not completely loaded",
            ))
        }
    }
}

// Support for streaming
#[cfg(feature = "sync")]
impl std::io::Read for BLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        trace!("BLobHandle::read() with buf of len {}", buf_len);

        while !self.is_data_complete && (buf_len > self.data.len()) {
            self.fetch_next_chunk_sync().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e.to_string())
            })?;
        }

        let count = std::cmp::min(self.data.len(), buf_len);
        let (s1, s2) = self.data.as_slices();
        if count <= s1.len() {
            // write count bytes from s1
            buf.write_all(&s1[0..count])?;
        } else {
            // write s1 completely, then take the rest from s2
            buf.write_all(s1)?;
            buf.write_all(&s2[0..(count - s1.len())])?;
        }
        self.data.drain(0..count);
        Ok(count)
    }
}

#[cfg(feature = "async")]
impl<'a> BLobHandle {
    pub(crate) async fn async_read(&mut self, buf: &'a mut [u8]) -> HdbResult<usize> {
        let mut buf = ReadBuf::new(buf);
        let buf_len = buf.capacity();
        debug_assert!(buf.filled().is_empty());
        trace!("BLobHandle::read() with buf of len {}", buf_len);

        while !self.is_data_complete && (self.data.len() < buf_len) {
            self.fetch_next_chunk_async().await?;
        }

        let count = std::cmp::min(self.data.len(), buf_len);
        let (s1, s2) = self.data.as_slices();
        if count <= s1.len() {
            // write count bytes from s1
            buf.put_slice(&s1[0..count]);
        } else {
            // write s1 completely, then take the rest from s2
            buf.put_slice(s1);
            buf.put_slice(&s2[0..(count - s1.len())]);
        }
        self.data.drain(0..count);
        Ok(count)
    }
}
