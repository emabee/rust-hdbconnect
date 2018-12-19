use crate::conn_core::AmConnCore;
use crate::protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use crate::types_impl::lob::fetch_a_lob_chunk;
use crate::{HdbError, HdbResult};
use std::cell::RefCell;
use std::cmp;
use std::io::{self, Write};
use std::sync::Arc;

/// BLob implementation that is used within `HdbValue::BLOB`.
#[derive(Clone, Debug, Serialize)]
pub struct BLob(BLobEnum);

#[derive(Clone, Debug, Serialize)]
enum BLobEnum {
    FromDB(RefCell<BLobHandle>),
    ToDB(Vec<u8>),
}

pub(crate) fn new_blob_from_db(
    am_conn_core: &AmConnCore,
    is_data_complete: bool,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
) -> BLob {
    BLob(BLobEnum::FromDB(RefCell::new(BLobHandle::new(
        am_conn_core,
        is_data_complete,
        length_b,
        locator_id,
        data,
    ))))
}

// Factory method for BLobs that are to be sent to the database.
pub(crate) fn new_blob_to_db(vec: Vec<u8>) -> BLob {
    BLob(BLobEnum::ToDB(vec))
}

impl BLob {
    /// Length of contained data.
    pub fn len_alldata(&self) -> usize {
        match self.0 {
            BLobEnum::FromDB(ref handle) => handle.borrow_mut().len_alldata() as usize,
            BLobEnum::ToDB(ref vec) => vec.len(),
        }
    }

    /// Length of read data.
    pub fn len_readdata(&self) -> usize {
        match self.0 {
            BLobEnum::FromDB(ref handle) => handle.borrow_mut().len_readdata() as usize,
            BLobEnum::ToDB(ref vec) => vec.len(),
        }
    }

    /// Is container empty
    pub fn is_empty(&self) -> HdbResult<bool> {
        Ok(self.len_alldata() == 0)
    }

    /// Ref to the contained Vec<u8>.
    pub fn ref_to_bytes(&self) -> HdbResult<&Vec<u8>> {
        trace!("BLob::ref_to_bytes()");
        match self.0 {
            BLobEnum::FromDB(_) => Err(HdbError::impl_("cannot serialize BLobHandle")),
            BLobEnum::ToDB(ref vec) => Ok(vec),
        }
    }

    /// Converts into the contained Vec<u8>.
    pub fn into_bytes(self) -> HdbResult<Vec<u8>> {
        trace!("BLob::into_bytes()");
        match self.0 {
            BLobEnum::FromDB(handle) => handle.into_inner().into_bytes(),
            BLobEnum::ToDB(vec) => Ok(vec),
        }
    }

    /// Returns the maximum size of the internal buffers.
    ///
    /// Tests can verify that this value does not exceed `lob_read_size` +
    /// `buf.len()`.
    pub fn max_size(&self) -> usize {
        match self.0 {
            BLobEnum::FromDB(ref handle) => handle.borrow().max_size(),
            BLobEnum::ToDB(ref v) => v.len(),
        }
    }
}

// Support for BLob streaming
impl io::Read for BLob {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.0 {
            BLobEnum::FromDB(ref blob_handle) => blob_handle.borrow_mut().read(buf),
            BLobEnum::ToDB(ref v) => v.as_slice().read(buf),
        }
    }
}

// `BLobHandle` is used for BLobs that we receive from the database.
// The data are often not transferred completely,
// so we carry internally a database connection and the
// necessary controls to support fetching remaining data on demand.
#[derive(Clone, Debug, Serialize)]
struct BLobHandle {
    #[serde(skip)]
    o_am_conn_core: Option<AmConnCore>,
    is_data_complete: bool,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
    max_size: usize,
    acc_byte_length: usize,
    #[serde(skip)]
    server_resource_consumption_info: ServerResourceConsumptionInfo,
}
impl BLobHandle {
    pub fn new(
        am_conn_core: &AmConnCore,
        is_data_complete: bool,
        length_b: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> BLobHandle {
        trace!(
            "BLobHandle::new() with length_b = {}, is_data_complete = {}, data.length() = {}",
            length_b,
            is_data_complete,
            data.len()
        );
        BLobHandle {
            o_am_conn_core: Some(Arc::clone(am_conn_core)),
            length_b,
            is_data_complete,
            locator_id,
            max_size: data.len(),
            acc_byte_length: data.len(),
            data,
            server_resource_consumption_info: Default::default(),
        }
    }

    pub fn len_alldata(&mut self) -> u64 {
        self.length_b
    }

    pub fn len_readdata(&mut self) -> usize {
        self.data.len()
    }

    fn fetch_next_chunk(&mut self) -> HdbResult<()> {
        let (mut reply_data, reply_is_last_data) = fetch_a_lob_chunk(
            &mut self.o_am_conn_core,
            self.locator_id,
            self.length_b,
            self.acc_byte_length as u64,
            &mut self.server_resource_consumption_info,
        )?;

        self.acc_byte_length += reply_data.len();
        self.data.append(&mut reply_data);
        self.is_data_complete = reply_is_last_data;
        self.max_size = cmp::max(self.data.len(), self.max_size);

        assert_eq!(
            self.is_data_complete,
            self.length_b == self.acc_byte_length as u64
        );
        trace!(
            "fetch_next_chunk: is_data_complete = {}, data.len() = {}",
            self.is_data_complete,
            self.data.len()
        );
        Ok(())
    }

    /// Converts a BLob into a Vec<u8> containing its data.
    fn load_complete(&mut self) -> HdbResult<()> {
        trace!("load_complete()");
        while !self.is_data_complete {
            self.fetch_next_chunk()?;
        }
        Ok(())
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Converts a BLob into a Vec<u8> containing its data.
    pub fn into_bytes(mut self) -> HdbResult<Vec<u8>> {
        trace!("into_bytes()");
        self.load_complete()?;
        Ok(self.data)
    }
}

// Support for streaming
impl io::Read for BLobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        trace!("BLobHandle::read() with buf of len {}", buf.len());

        while !self.is_data_complete && (buf.len() > self.data.len()) {
            self.fetch_next_chunk()
                .map_err(|e| io::Error::new(io::ErrorKind::UnexpectedEof, e))?;
        }

        let count = cmp::min(self.data.len(), buf.len());
        buf.write_all(&self.data[0..count])?;
        self.data.drain(0..count);
        Ok(count)
    }
}
