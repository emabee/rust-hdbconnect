use super::fetch::fetch_a_lob_chunk;
use super::CharLobSlice;
use crate::conn::AmConnCore;
use crate::protocol::parts::AmRsCore;
use crate::protocol::util;
use crate::{HdbError, HdbResult, ServerUsage};
use std::boxed::Box;
use std::collections::VecDeque;
use std::io::Write;

/// LOB implementation for unicode Strings that is used with `HdbValue::NCLOB` instances coming
/// from the database.
///
/// Bigger LOBs are not transferred completely in the first roundtrip, instead more data are
/// fetched in subsequent roundtrips when they are needed.
///
/// `NCLob` respects the Connection's lob read length
/// (see [`Connection::set_lob_read_length`](crate::Connection::set_lob_read_length)),
/// by transferring per fetch request `lob_read_length` unicode characters (rather than bytes).
/// Note that due to the way how HANA represents unicode internally,
/// all BMP-0 characters count as 1, non-BMP-0 characters count as 2.
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
    ) -> HdbResult<Self> {
        Ok(Self(Box::new(NCLobHandle::new(
            am_conn_core,
            o_am_rscore,
            is_data_complete,
            total_char_length,
            total_byte_length,
            locator_id,
            data,
        )?)))
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
    /// ```rust, no_run
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

    /// Reads from given offset and the given length, in number of unicode characters.
    ///
    /// Note that due to the way how HANA represents unicode internally,
    /// all BMP-0 characters count as 1, non-BMP-0 characters count as 2.
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

    /// Total length of data, in characters.
    ///
    /// Note that due to the way how HANA represents unicode internally,
    /// all BMP-0 characters count as 1, non-BMP-0 characters count as 2.
    pub fn total_char_length(&self) -> u64 {
        self.0.total_char_length()
    }

    /// Returns true if the `NCLob` does not contain data.
    pub fn is_empty(&self) -> bool {
        self.total_byte_length() == 0
    }

    /// Returns the maximum size the internal buffers ever had, in bytes.
    ///
    /// This method exists mainly for debugging purposes. With streaming, the returned value is
    /// not supposed to exceed 3 times `lob_read_length` (see
    /// [`Connection::set_lob_read_length`](crate::Connection::set_lob_read_length))
    /// plus the buffer size used by the reader.
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
    cesu8: VecDeque<u8>,
    cesu8_tail_len: usize,
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
            max_buf_len: cesu8.len(),
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

    fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<CharLobSlice> {
        let (reply_data, _reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.am_conn_core,
            self.locator_id,
            offset,
            length,
            &mut self.server_usage,
        )?;
        debug!("read_slice(): got {} bytes", reply_data.len());
        util::split_off_orphaned_surrogates(reply_data)
    }

    fn total_byte_length(&self) -> u64 {
        self.total_byte_length
    }

    fn total_char_length(&self) -> u64 {
        self.total_char_length
    }

    fn cur_buf_len(&self) -> usize {
        self.cesu8.len()
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

        let (reply_data, reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.am_conn_core,
            self.locator_id,
            self.acc_char_length as u64,
            read_length,
            &mut self.server_usage,
        )?;

        self.acc_byte_length += reply_data.len();
        self.acc_char_length += count_1_2_3_sequence_starts(&reply_data);

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
        Ok(util::string_from_cesu8(Vec::from(self.cesu8))?)
    }
}

// Support for NCLOB streaming
impl std::io::Read for NCLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        trace!("read() with buf of len {}", buf.len());

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

fn count_1_2_3_sequence_starts(cesu8: &[u8]) -> usize {
    cesu8.iter().filter(|b| is_utf8_char_start(**b)).count()
}
fn is_utf8_char_start(b: u8) -> bool {
    matches!(b, 0x00..=0x7F | 0xC0..=0xDF | 0xE0..=0xEF | 0xF0..=0xF7)
}
