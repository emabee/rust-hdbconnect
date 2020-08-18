use super::fetch::fetch_a_lob_chunk;
use super::CharLobSlice;
use crate::conn::AmConnCore;
use crate::protocol::parts::AmRsCore;
use crate::protocol::{util, ServerUsage};
use crate::{HdbError, HdbResult};
use std::boxed::Box;
use std::collections::VecDeque;
use std::io::Write;

/// LOB implementation for unicode Strings that is used with `HdbValue::CLOB` (which is deprecated).
///
/// Note that the CLOB type is not recommended for use.
/// CLOB fields are supposed to only store ASCII7, but HANA doesn't check this.
///
/// `CLob` respects the Connection's lob read length
/// (see [`Connection::set_lob_read_length`](struct.Connection.html#method.set_lob_read_length)),
/// by transferring per fetch request `lob_read_length` bytes.
#[derive(Clone, Debug)]
pub struct CLob(Box<CLobHandle>);

impl CLob {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        is_data_complete: bool,
        total_char_length: u64,
        total_byte_length: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> HdbResult<Self> {
        Ok(Self(Box::new(CLobHandle::new(
            am_conn_core,
            o_am_rscore,
            is_data_complete,
            total_char_length,
            total_byte_length,
            locator_id,
            data,
        )?)))
    }

    /// Converts the `CLob` into the contained String.
    ///
    /// All outstanding data (data that were not yet fetched from the server) are fetched
    /// _into_ this `CLob` object, before the complete data,
    /// as far as they were not yet read _from_ this `CLob` object, are returned.
    ///
    /// ## Example
    ///
    /// ```rust, no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
    /// # fn foo() -> HdbResult<()> {
    /// # let params = "".into_connect_params()?;
    /// # let mut connection = Connection::new(params)?;
    /// # let query = "";
    ///  let mut resultset = connection.query(query)?;
    ///  let mut clob = resultset.into_single_row()?.into_single_value()?.try_into_clob()?;
    ///  let s = clob.into_string(); // String, can be huge
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Alternative
    ///
    /// For larger objects, a streaming approach using the `Read` implementation of `CLob`
    /// might by more appropriate, to avoid total allocation of the large object.
    ///
    /// ## Example
    ///
    /// ```rust, no_run
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
    ///  # let mut clob = resultset.into_single_row()?.into_single_value()?.try_into_clob()?;
    ///  std::io::copy(&mut clob, &mut writer)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn into_string(self) -> HdbResult<String> {
        trace!("CLob::into_string()");
        self.0.into_string()
    }

    /// Reads from given offset and the given length, in bytes.
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

    /// Returns true if the `CLob` does not contain data.
    pub fn is_empty(&self) -> bool {
        self.total_byte_length() == 0
    }

    /// Returns the maximum size of the internal buffer, in bytes.
    ///
    /// This method exists mainly for debugging purposes. With streaming, the returned value is
    /// not supposed to exceed `lob_read_length` (see
    /// [`Connection::set_lob_read_length`](../struct.Connection.html#method.set_lob_read_length))
    /// plus the buffer size used by the reader.
    pub fn max_buf_len(&self) -> usize {
        self.0.max_buf_len()
    }

    /// Current size of the internal buffer, in bytes.
    pub fn cur_buf_len(&self) -> usize {
        self.0.cur_buf_len() as usize
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `CBLob` object.
    pub fn server_usage(&self) -> ServerUsage {
        self.0.server_usage
    }
}

// Support for CLob streaming
impl std::io::Read for CLob {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

// `CLobHandle` is used for CLOBs that we receive from the database.
// The data are often not transferred completely, so we carry internally
// a database connection and the necessary controls to support fetching
// remaining data on demand.
// Since the data stream can be cut into chunks anywhere in the byte stream,
// we may need to buffer an orphaned part of a multi-byte sequence between two fetches.
#[derive(Clone, Debug)]
struct CLobHandle {
    am_conn_core: AmConnCore,
    o_am_rscore: Option<AmRsCore>,
    is_data_complete: bool,
    total_char_length: u64,
    total_byte_length: u64,
    locator_id: u64,
    cesu8: VecDeque<u8>,
    cesu8_tail_len: usize,
    max_buf_len: usize,
    acc_byte_length: usize,
    server_usage: ServerUsage,
}
impl CLobHandle {
    fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        is_data_complete: bool,
        total_char_length: u64,
        total_byte_length: u64,
        locator_id: u64,
        cesu8: Vec<u8>,
    ) -> HdbResult<Self> {
        let cesu8 = VecDeque::from(cesu8);
        let acc_byte_length = cesu8.len();

        let cesu8_tail_len = util::get_cesu8_tail_len(&cesu8, cesu8.len())?;

        let clob_handle = Self {
            am_conn_core: am_conn_core.clone(),
            o_am_rscore: match o_am_rscore {
                Some(ref am_rscore) => Some(am_rscore.clone()),
                None => None,
            },
            total_char_length,
            total_byte_length,
            is_data_complete,
            locator_id,
            max_buf_len: cesu8.len(),
            cesu8,
            cesu8_tail_len,
            acc_byte_length,
            server_usage: ServerUsage::default(),
        };
        debug!(
            "CLobHandle::new() with: is_data_complete = {}, total_char_length = {}, total_byte_length = {}, \
             locator_id = {}, cesu8_tail_len = {}, cesu8.len() = {}",
            clob_handle.is_data_complete,
            clob_handle.total_char_length,
            clob_handle.total_byte_length,
            clob_handle.locator_id,
            clob_handle.cesu8_tail_len,
            clob_handle.cesu8.len()
        );
        Ok(clob_handle)
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
        Ok(util::split_off_orphaned_bytes(&reply_data)?)
    }

    fn total_byte_length(&self) -> u64 {
        self.total_byte_length
    }

    fn cur_buf_len(&self) -> usize {
        self.cesu8.len()
    }

    #[allow(clippy::cast_possible_truncation)]
    fn fetch_next_chunk(&mut self) -> HdbResult<()> {
        if self.is_data_complete {
            return Err(HdbError::Impl("fetch_next_chunk(): already complete"));
        }

        let read_length = std::cmp::min(
            self.am_conn_core.lock()?.get_lob_read_length() as u32,
            (self.total_byte_length - self.acc_byte_length as u64) as u32,
        );

        let (reply_data, reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.am_conn_core,
            self.locator_id,
            self.acc_byte_length as u64,
            read_length,
            &mut self.server_usage,
        )?;

        self.acc_byte_length += reply_data.len();

        self.cesu8.append(&mut VecDeque::from(reply_data));
        self.cesu8_tail_len = util::get_cesu8_tail_len(&self.cesu8, self.cesu8.len())?;

        self.is_data_complete = reply_is_last_data;
        self.max_buf_len = std::cmp::max(self.cesu8.len(), self.max_buf_len);

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

    fn load_complete(&mut self) -> HdbResult<()> {
        trace!("load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk()?;
        }
        Ok(())
    }

    fn max_buf_len(&self) -> usize {
        self.max_buf_len
    }

    // Converts a CLobHandle into a String containing its data.
    fn into_string(mut self) -> HdbResult<String> {
        trace!("into_string()");
        self.load_complete()?;
        Ok(util::string_from_cesu8(Vec::from(self.cesu8))?)
    }
}

// Support for CLOB streaming
impl std::io::Read for CLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        trace!("CLobHandle::read() with buf of len {}", buf.len());

        while !self.is_data_complete && (buf.len() > self.cesu8.len() - self.cesu8_tail_len) {
            self.fetch_next_chunk()
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
