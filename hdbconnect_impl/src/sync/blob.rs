use crate::{
    base::{RsCore, OAM},
    conn::AmConnCore,
    protocol::ServerUsage,
    types_impl::lob::BLobHandle,
    HdbResult,
};
use std::io::Read;

/// LOB implementation for binary values.
///
/// `BLob` is used within [`HdbValue::BLOB`](../enum.HdbValue.html#variant.BLOB)
/// instances received from the database.
///
/// Bigger LOBs are not transferred completely in the first roundtrip, instead more data is
/// fetched in subsequent roundtrips when needed.
///
/// `BLob` respects the Connection's lob read length
/// (see [`Connection::set_lob_read_length`](crate::Connection::set_lob_read_length)).
#[derive(Clone, Debug)]
pub struct BLob(Box<BLobHandle>);

impl BLob {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &OAM<RsCore>,
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
    ///  let mut result_set = connection.query(query)?;
    ///  let mut blob = result_set.into_single_row()?.into_single_value()?.try_into_blob()?;
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
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn into_bytes(self) -> HdbResult<Vec<u8>> {
        trace!("BLob::into_bytes()");
        self.0.into_bytes_sync()
    }

    /// Writes the content into the given writer.
    ///
    /// Reads outstanding data in chunks of size
    /// [`Connection::lob_read_length`](../struct.Connection.html#method.lob_read_length) from the database
    /// and writes them immediately into the writer,
    /// thus avoiding that all data are materialized within this `NCLob`.
    ///
    /// # Errors
    ///
    /// Various errors can occur.
    pub fn write_into(mut self, writer: &mut dyn std::io::Write) -> HdbResult<()> {
        let lob_read_length: usize = self
            .0
            .am_conn_core
            .lock_sync()?
            .configuration()
            .lob_read_length() as usize;
        let mut buf = vec![0_u8; lob_read_length].into_boxed_slice();

        loop {
            let read = self.0.read(&mut buf)?;
            if read == 0 {
                break;
            }
            writer.write_all(&buf[0..read])?;
        }
        writer.flush()?;
        Ok(())
    }

    pub(crate) fn into_bytes_if_complete(self) -> HdbResult<Vec<u8>> {
        trace!("BLob::into_bytes_if_complete()");
        self.0.into_bytes_sync()
    }

    pub(crate) fn load_complete(&mut self) -> HdbResult<()> {
        self.0.load_complete_sync()
    }

    /// Reads from given offset and the given length, in bytes.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<Vec<u8>> {
        self.0.read_slice_sync(offset, length)
    }

    /// Total length of data, in bytes.
    #[must_use]
    pub fn total_byte_length(&self) -> u64 {
        self.0.total_byte_length()
    }

    /// Returns true if the `BLob` does not contain data.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_byte_length() == 0
    }

    /// Current size of the internal buffer, in bytes.
    #[must_use]
    pub fn cur_buf_len(&self) -> usize {
        self.0.cur_buf_len()
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `BLob` object.
    #[must_use]
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
