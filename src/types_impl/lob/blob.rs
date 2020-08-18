use super::fetch::fetch_a_lob_chunk;
use crate::conn::AmConnCore;
use crate::protocol::parts::AmRsCore;
use crate::protocol::ServerUsage;
use crate::{HdbError, HdbResult};
use std::boxed::Box;
use std::collections::VecDeque;
use std::io::Write;

/// LOB implementation for binary values that is used within `HdbValue::BLOB` instances coming
/// from the database.
///
/// Bigger LOBs are not transferred completely in the first roundtrip, instead more data are
/// fetched in subsequent roundtrips when they are needed.
///
/// `BLob` respects the Connection's lob read length
/// (see [`Connection::set_lob_read_length`](struct.Connection.html#method.set_lob_read_length)),
/// by transferring per fetch request `lob_read_length` bytes.
#[derive(Clone, Debug)]
pub struct BLob(Box<BLobHandle>);

impl BLob {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        is_data_complete: bool,
        total_byte_length: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> Self {
        Self(Box::new(BLobHandle::new(
            am_conn_core,
            o_am_rscore,
            is_data_complete,
            total_byte_length,
            locator_id,
            data,
        )))
    }

    /// Converts the `BLob` into a Vec<u8>.
    ///
    /// All outstanding data (data that were not yet fetched from the server) are fetched
    /// _into_ this `BLob` object,
    /// before the complete data, as far as they were not yet read _from_ this `BLob` object,
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
    ///  let mut blob = resultset.into_single_row()?.into_single_value()?.try_into_blob()?;
    ///
    ///  let b = blob.into_bytes()?; // Vec<u8>, can be huge
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Alternative
    ///
    /// For larger objects, a streaming approach using the `Read` implementation of `BLob`
    /// might by more appropriate, to avoid total allocation of the large object.
    ///
    /// ## Example
    ///
    /// ```rust, no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
    /// # fn foo() -> HdbResult<()> {
    /// # let params = "".into_connect_params()?;
    /// # let mut connection = Connection::new(params)?;
    /// # let mut writer = Vec::<u8>::new();
    /// # let query = "select chardata from TEST_NCLOBS";
    /// # let mut resultset = connection.query(query)?;
    /// # let mut blob = resultset.into_single_row()?.into_single_value()?.try_into_blob()?;
    ///  std::io::copy(&mut blob, &mut writer)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn into_bytes(self) -> HdbResult<Vec<u8>> {
        trace!("BLob::into_bytes()");
        self.0.into_bytes()
    }

    /// Reads from given offset and the given length, in bytes.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<Vec<u8>> {
        self.0.read_slice(offset, length)
    }

    /// Total length of data, in bytes.
    pub fn total_byte_length(&self) -> u64 {
        self.0.total_byte_length()
    }

    /// Returns true if the `BLob` does not contain data.
    pub fn is_empty(&self) -> bool {
        self.total_byte_length() == 0
    }

    /// Returns the maximum size the internal buffers ever had, in bytes.
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
    /// is related to this `BLob` object.
    pub fn server_usage(&self) -> ServerUsage {
        self.0.server_usage
    }
}

// Support for BLob streaming
impl std::io::Read for BLob {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

// `BLobHandle` is used for BLobs that we receive from the database.
// The data are often not transferred completely, so we carry internally
// a database connection and the necessary controls to support fetching
// remaining data on demand.
#[derive(Clone, Debug)]
struct BLobHandle {
    am_conn_core: AmConnCore,
    o_am_rscore: Option<AmRsCore>,
    is_data_complete: bool,
    total_byte_length: u64,
    locator_id: u64,
    data: VecDeque<u8>,
    max_buf_len: usize,
    acc_byte_length: usize,
    server_usage: ServerUsage,
}
impl BLobHandle {
    fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        is_data_complete: bool,
        total_byte_length: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> Self {
        let data = VecDeque::from(data);
        Self {
            am_conn_core: am_conn_core.clone(),
            o_am_rscore: match o_am_rscore {
                Some(ref am_rscore) => Some(am_rscore.clone()),
                None => None,
            },
            total_byte_length,
            is_data_complete,
            locator_id,
            max_buf_len: data.len(),
            acc_byte_length: data.len(),
            data,
            server_usage: ServerUsage::default(),
        }
    }

    fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<Vec<u8>> {
        let (reply_data, _reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.am_conn_core,
            self.locator_id,
            offset,
            length,
            &mut self.server_usage,
        )?;
        debug!("read_slice(): got {} bytes", reply_data.len());
        Ok(reply_data)
    }

    fn total_byte_length(&self) -> u64 {
        self.total_byte_length
    }

    fn cur_buf_len(&self) -> usize {
        self.data.len()
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
        self.data.append(&mut VecDeque::from(reply_data));
        self.is_data_complete = reply_is_last_data;
        self.max_buf_len = std::cmp::max(self.data.len(), self.max_buf_len);

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

    // Converts a BLobHandle into a Vec<u8> containing its data.
    fn into_bytes(mut self) -> HdbResult<Vec<u8>> {
        trace!("into_bytes()");
        self.load_complete()?;
        Ok(Vec::from(self.data))
    }
}

// Support for streaming
impl std::io::Read for BLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        trace!("BLobHandle::read() with buf of len {}", buf_len);

        while !self.is_data_complete && (buf_len > self.data.len()) {
            self.fetch_next_chunk().map_err(|e| {
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
            buf.write_all(&s1)?;
            buf.write_all(&s2[0..(count - s1.len())])?;
        }
        self.data.drain(0..count);
        Ok(count)
    }
}
