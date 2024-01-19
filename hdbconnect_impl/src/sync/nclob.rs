use crate::{
    base::{RsCore, OAM},
    conn::AmConnCore,
    types::CharLobSlice,
    types_impl::lob::NCLobHandle,
    {HdbResult, ServerUsage},
};

use std::{boxed::Box, io::Read};

/// LOB implementation for unicode Strings.
///
/// `NCLob` is used within [`HdbValue::NCLOB`](../enum.HdbValue.html#variant.NCLOB)
/// instances received from the database.
///
/// Bigger LOBs are not transferred completely in the first roundtrip, instead more data is
/// fetched in subsequent roundtrips when needed.
///
/// `NCLob` respects the Connection's lob read length
/// (see [`Connection::set_lob_read_length`](crate::Connection::set_lob_read_length))
/// by transferring per fetch request `lob_read_length` unicode characters (rather than bytes).
/// Note that due to the way how HANA represents unicode internally,
/// all BMP-0 characters count as 1, non-BMP-0 characters count as 2.
#[derive(Clone, Debug)]
pub struct NCLob(Box<NCLobHandle>);

impl NCLob {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &OAM<RsCore>,
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
    pub fn into_string(mut self) -> HdbResult<String> {
        trace!("NCLob::into_string()");
        self.load_complete()?;
        self.0.into_string_if_complete()
    }

    /// Writes the content into the given writer.
    ///
    /// Reads outstanding data in chunks of size
    /// [`Connection::lob_read_length`](../struct.Connection.html#method.lob_read_length) from the database
    /// and writes them immediately into the writer,
    /// thus avoiding that all data are materialized within this `NCLob`.
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

    // Converts a NCLobHandle into a String containing its data.
    pub(crate) fn into_string_if_complete(mut self) -> HdbResult<String> {
        self.0.load_complete_sync()?;
        self.0.into_string_if_complete()
    }

    pub(crate) fn load_complete(&mut self) -> HdbResult<()> {
        self.0.load_complete_sync()
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
        self.0.read_slice_sync(offset, length)
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

    /// Current size of the internal buffer, in bytes.
    pub fn cur_buf_len(&self) -> usize {
        self.0.cur_buf_len()
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
