use crate::{
    base::{RsCore, OAM},
    conn::AmConnCore,
    types::CharLobSlice,
    types_impl::lob::CLobHandle,
    {HdbResult, ServerUsage},
};
use std::boxed::Box;

/// LOB implementation for unicode Strings (deprecated).
///
/// `CLob` is used within [`HdbValue::CLOB`](../enum.HdbValue.html#variant.CLOB) (deprecated)
/// instances received from the database.
///
/// Bigger LOBs are not transferred completely in the first roundtrip, instead more data is
/// fetched in subsequent roundtrips when needed.
///
/// `CLob` respects the Connection's lob read length
/// (see [`Connection::set_lob_read_length`](crate::Connection::set_lob_read_length)).
#[derive(Clone, Debug)]
pub struct CLob(Box<CLobHandle>);

impl CLob {
    pub(crate) fn new(
        am_conn_core: &AmConnCore,
        o_am_rscore: &OAM<RsCore>,
        is_data_complete: bool,
        total_char_length: u64,
        total_byte_length: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> Self {
        Self(Box::new(CLobHandle::new(
            am_conn_core,
            o_am_rscore,
            is_data_complete,
            total_char_length,
            total_byte_length,
            locator_id,
            data,
        )))
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
    ///  let mut clob = resultset.into_single_row().await?.into_single_value()?.try_into_clob()?;
    ///  let s = clob.into_string(); // String, can be huge
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Alternative
    ///
    /// For larger objects, a streaming approach using [`CLob::write_into`]
    /// might by more appropriate, to avoid total allocation of the large object.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn into_string(mut self) -> HdbResult<String> {
        trace!("CLob::into_string()");
        self.0.load_complete_async().await?;
        self.0.into_string_if_complete()
    }

    /// Writes the content into the given writer.
    ///
    /// Reads outstanding data in chunks of size
    /// [`Connection::lob_read_length`](../struct.Connection.html#method.lob_read_length) from the database
    /// and writes them immediately into the writer,
    /// thus avoiding that all data are materialized within this `CLob`.
    pub async fn write_into<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        mut self,
        writer: &mut W,
    ) -> HdbResult<()> {
        let lob_read_length: usize = self
            .0
            .am_conn_core
            .lock_async()
            .await
            .configuration()
            .lob_read_length() as usize;
        let mut buf = vec![0_u8; lob_read_length].into_boxed_slice();

        loop {
            let read = self.0.read_async(&mut buf).await?;
            if read == 0 {
                break;
            }
            writer.write_all(&buf[0..read]).await?;
        }
        writer.flush().await?;
        Ok(())
    }

    #[allow(unused_mut)]
    pub(crate) fn into_string_if_complete(mut self) -> HdbResult<String> {
        #[cfg(feature = "sync")]
        {
            self.0.load_complete_sync()?;
        }
        self.0.into_string_if_complete()
    }

    pub(crate) async fn load_complete(&mut self) -> HdbResult<()> {
        self.0.load_complete_async().await
    }

    /// Reads from given offset and the given length, in bytes.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn read_slice(&mut self, offset: u64, length: u32) -> HdbResult<CharLobSlice> {
        self.0.read_slice_async(offset, length).await
    }

    /// Total length of data, in bytes.
    pub fn total_byte_length(&self) -> u64 {
        self.0.total_byte_length()
    }

    /// Returns true if the `CLob` does not contain data.
    pub fn is_empty(&self) -> bool {
        self.total_byte_length() == 0
    }

    /// Current size of the internal buffer, in bytes.
    pub fn cur_buf_len(&self) -> usize {
        self.0.cur_buf_len()
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `CBLob` object.
    pub fn server_usage(&self) -> ServerUsage {
        self.0.server_usage
    }
}
