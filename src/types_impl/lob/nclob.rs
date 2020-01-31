use super::fetch::fetch_a_lob_chunk;
use super::CharLobSlice;
use crate::conn::AmConnCore;
use crate::protocol::parts::resultset::AmRsCore;
use crate::protocol::server_usage::ServerUsage;
use crate::protocol::util;
use crate::{HdbError, HdbResult};
use std::boxed::Box;
use std::io::Write;

/// Unicode LOB implementation that is used with `HdbValue::NCLOB`.
#[derive(Clone, Debug)]
pub struct NCLob(Box<NCLobHandle>);

impl NCLob {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        is_data_complete: bool,
        total_char_length: u64,
        total_byte_length: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> Self {
        Self(Box::new(NCLobHandle::new(
            am_conn_core,
            o_am_rscore,
            is_data_complete,
            total_char_length,
            total_byte_length,
            locator_id,
            data,
        )))
    }

    /// Converts the `NCLob` into the contained String.
    ///
    /// All outstanding data (data that were not yet fetched from the server) are fetched
    /// _into_ this `NCLob` object,
    /// before the complete data, as far as they were not yet read _from_ this `NCLob` object,
    /// are returned.
    ///
    ///
    /// ## Example
    ///
    /// ```rust, no-run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
    /// # fn foo() -> HdbResult<()> {
    /// # let params = "".into_connect_params()?;
    /// # let mut connection = Connection::new(params)?;
    /// # let query = "";
    ///  let mut resultset = connection.query(query)?;
    ///  let mut nclob = resultset.into_single_row()?.into_single_value()?.try_into_nclob()?;
    ///  let s = nclob.into_string(); // String, can be huge
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Alternative
    ///
    /// For larger objects, a streaming approach using the `Read` implementation of `NCLob`
    /// might by more appropriate, to avoid total allocation of the large object.
    ///
    /// ## Example
    ///
    /// ```rust, no-run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
    /// # fn foo() -> HdbResult<()> {
    /// # let params = "".into_connect_params()?;
    /// # let mut connection = Connection::new(params)?;
    ///  let mut writer;
    ///  // ... writer gets instantiated, is an implementation of std::io::Write;
    ///  # writer = Vec::<u8>::new();
    ///
    ///  # let query = "";
    ///  # let mut resultset = connection.query(query)?;
    ///  # let mut nclob = resultset.into_single_row()?.into_single_value()?.try_into_nclob()?;
    ///  std::io::copy(&mut nclob, &mut writer)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn into_string(self) -> HdbResult<String> {
        self.0.into_string()
    }

    /// Reads from given offset and the given length, in number of 123-byte sequences.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<CharLobSlice> {
        self.0.read_slice(offset, length)
    }

    /// Total length of data, in bytes.
    pub fn total_byte_length(&self) -> u64 {
        self.0.total_byte_length()
    }

    /// Returns true if the `NCLob` does not contain data.
    pub fn is_empty(&self) -> bool {
        self.total_byte_length() == 0
    }

    /// Returns the maximum size of the internal buffers, in bytes.
    ///
    /// With streaming, this value should not exceed `lob_read_size` plus
    /// the buffer size used by the reader.
    pub fn max_buf_len(&self) -> usize {
        self.0.max_buf_len()
    }

    /// Current size of the internal buffer, in bytes.
    pub fn cur_buf_len(&self) -> usize {
        self.0.cur_buf_len() as usize
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `NCBLob` object.
    pub fn server_usage(&self) -> ServerUsage {
        self.0.server_usage
    }
}

// Support for NCLob streaming.
impl std::io::Read for NCLob {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

// `NCLobHandle` is used for NCLOBs that we receive from the database.
// The data are often not transferred completely, so we carry internally
// a database connection and the necessary controls to support fetching remaining data on demand.
// The data stream can be cut into chunks between valid 1-, 2-, or 3-byte sequences.
// Since surrogate pairs can be cut in two halfs (two 3-byte sequences), we may need to buffer
// an orphaned surrogate between two fetches.
#[derive(Clone, Debug)]
struct NCLobHandle {
    am_conn_core: AmConnCore,
    o_am_rscore: Option<AmRsCore>,
    is_data_complete: bool,
    total_char_length: u64,
    total_byte_length: u64,
    locator_id: u64,
    surrogate_buf: Option<Vec<u8>>,
    utf8: String,
    max_buf_len: usize,
    acc_byte_length: usize,
    acc_char_length: usize,
    server_usage: ServerUsage,
}
impl NCLobHandle {
    fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        is_data_complete: bool,
        total_char_length: u64,
        total_byte_length: u64,
        locator_id: u64,
        cesu8: Vec<u8>,
    ) -> Self {
        let acc_byte_length = cesu8.len();
        let acc_char_length = util::count_1_2_3_sequence_starts(&cesu8);

        let (utf8, surrogate_buf) = util::to_string_and_surrogate(cesu8).unwrap(/* yes */);

        let nclob_handle = Self {
            am_conn_core: am_conn_core.clone(),
            o_am_rscore: match o_am_rscore {
                Some(ref am_rscore) => Some(am_rscore.clone()),
                None => None,
            },
            total_char_length,
            total_byte_length,
            is_data_complete,
            locator_id,
            max_buf_len: utf8.len() + if surrogate_buf.is_some() { 3 } else { 0 },
            surrogate_buf,
            utf8,
            acc_byte_length,
            acc_char_length,
            server_usage: ServerUsage::default(),
        };

        trace!(
            "new() with: is_data_complete = {}, total_char_length = {}, total_byte_length = {}, \
             locator_id = {}, surrogate_buf = {:?}, utf8.len() = {}",
            nclob_handle.is_data_complete,
            nclob_handle.total_char_length,
            nclob_handle.total_byte_length,
            nclob_handle.locator_id,
            nclob_handle.surrogate_buf,
            nclob_handle.utf8.len()
        );
        nclob_handle
    }

    fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<CharLobSlice> {
        let (reply_data, _reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.am_conn_core,
            self.locator_id,
            offset,
            length,
            &mut self.server_usage,
        )?;
        debug!("read_slice(): got {} bytes", reply_data.len());
        Ok(util::split_off_orphaned_surrogates(reply_data)?)
    }

    fn total_byte_length(&self) -> u64 {
        self.total_byte_length
    }

    fn cur_buf_len(&self) -> usize {
        self.utf8.len()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn fetch_next_chunk(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::Impl("already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core.lock()?.get_lob_read_length() as u32,
            (self.total_char_length - self.acc_char_length as u64) as u32,
        );

        let (mut reply_data, reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.am_conn_core,
            self.locator_id,
            self.acc_char_length as u64,
            read_length,
            &mut self.server_usage,
        )?;

        self.acc_byte_length += reply_data.len();
        self.acc_char_length += util::count_1_2_3_sequence_starts(&reply_data);

        let (utf8, surrogate_buf) = match self.surrogate_buf {
            Some(ref buf) => {
                let mut temp = buf.to_vec();
                temp.append(&mut reply_data);
                util::to_string_and_surrogate(temp).unwrap(/* yes */)
            }
            None => util::to_string_and_surrogate(reply_data).unwrap(/* yes */),
        };

        self.utf8.push_str(&utf8);
        self.surrogate_buf = surrogate_buf;
        self.is_data_complete = reply_is_last_data;
        self.max_buf_len = std::cmp::max(
            self.utf8.len() + if self.surrogate_buf.is_some() { 3 } else { 0 },
            self.max_buf_len,
        );

        assert_eq!(
            self.is_data_complete,
            self.total_byte_length == self.acc_byte_length as u64
        );
        trace!(
            "fetch_next_chunk: is_data_complete = {}, utf8.len() = {}",
            self.is_data_complete,
            self.utf8.len()
        );
        Ok(())
    }

    fn load_complete(&mut self) -> HdbResult<()> {
        trace!("NCLobHandle::load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk()?;
        }
        Ok(())
    }

    fn max_buf_len(&self) -> usize {
        self.max_buf_len
    }

    // Converts a NCLobHandle into a String containing its data.
    fn into_string(mut self) -> HdbResult<String> {
        trace!("NCLobHandle::into_string()");
        self.load_complete()?;
        Ok(self.utf8)
    }
}

// Support for NCLOB streaming
impl std::io::Read for NCLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        trace!("read() with buf of len {}", buf.len());

        while !self.is_data_complete && (buf.len() > self.utf8.len()) {
            self.fetch_next_chunk()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        }

        // we want to keep clean UTF-8 in utf8, so we cut off at good places only
        let count: usize = if self.utf8.len() < buf.len() {
            self.utf8.len()
        } else {
            let mut tmp = buf.len();
            while !util::is_utf8_char_start(self.utf8.as_bytes()[tmp]) {
                tmp -= 1;
            }
            tmp
        };

        buf.write_all(&self.utf8.as_bytes()[0..count])?;
        self.utf8.drain(0..count);
        Ok(count)
    }
}
