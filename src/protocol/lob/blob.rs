use protocol::conn_core::AmConnCore;
use protocol::lob::fetch_a_lob_chunk::fetch_a_lob_chunk;
use protocol::server_resource_consumption_info::ServerResourceConsumptionInfo;
use std::cell::RefCell;
use {HdbError, HdbResult};

use std::cmp;
use std::io::{self, Write};
use std::sync::Arc;

/// BLOB implementation that is used within `TypedValue::BLOB`.
///
/// BLOB comes in two flavors, depending on
/// whether we read it from the database or write it to the database.
#[derive(Clone, Debug)]
pub struct BLOB(BlobEnum);

#[derive(Clone, Debug)]
enum BlobEnum {
    /// A BlobHandle represents a CLOB that was read from the database.
    FromDB(RefCell<BlobHandle>),
    /// A mere newtype-struct around the data.
    ToDB(Vec<u8>),
}

pub fn new_blob_from_db(
    am_conn_core: &AmConnCore,
    is_data_complete: bool,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
) -> BLOB {
    BLOB(BlobEnum::FromDB(RefCell::new(BlobHandle::new(
        am_conn_core,
        is_data_complete,
        length_b,
        locator_id,
        data,
    ))))
}

/// Factory method for BLOBs that are to be sent to the database.
pub fn new_blob_to_db(vec: Vec<u8>) -> BLOB {
    BLOB(BlobEnum::ToDB(vec))
}

impl BLOB {
    /// Length of contained data.
    pub fn len_alldata(&self) -> usize {
        match self.0 {
            BlobEnum::FromDB(ref handle) => handle.borrow_mut().len_alldata() as usize,
            BlobEnum::ToDB(ref vec) => vec.len(),
        }
    }

    /// Length of read data.
    pub fn len_readdata(&self) -> usize {
        match self.0 {
            BlobEnum::FromDB(ref handle) => handle.borrow_mut().len_readdata() as usize,
            BlobEnum::ToDB(ref vec) => vec.len(),
        }
    }

    /// Is container empty
    pub fn is_empty(&self) -> HdbResult<bool> {
        Ok(self.len_alldata() == 0)
    }

    /// Ref to the contained Vec<u8>.
    pub fn ref_to_bytes(&self) -> HdbResult<&Vec<u8>> {
        trace!("BLOB::ref_to_bytes()");
        match self.0 {
            BlobEnum::FromDB(_) => Err(HdbError::impl_("cannot serialize BlobHandle")),
            BlobEnum::ToDB(ref vec) => Ok(vec),
        }
    }

    /// Converts into the contained Vec<u8>.
    pub fn into_bytes(self) -> HdbResult<Vec<u8>> {
        trace!("BLOB::into_bytes()");
        match self.0 {
            BlobEnum::FromDB(handle) => handle.into_inner().into_bytes(),
            BlobEnum::ToDB(vec) => Ok(vec),
        }
    }

    /// Returns the maximum size of the internal buffers.
    ///
    /// Tests can verify that this value does not exceed `lob_read_size` +
    /// `buf.len()`.
    pub fn max_size(&self) -> usize {
        match self.0 {
            BlobEnum::FromDB(ref handle) => handle.borrow().max_size(),
            BlobEnum::ToDB(ref v) => v.len(),
        }
    }
}

// Support for BLOB streaming
impl io::Read for BLOB {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.0 {
            BlobEnum::FromDB(ref blob_handle) => blob_handle.borrow_mut().read(buf),
            BlobEnum::ToDB(ref v) => v.as_slice().read(buf),
        }
    }
}

/// `BlobHandle` is used for BLOBs that we receive from the database.
/// The data are often not transferred completely,
/// so we carry internally a database connection and the
/// necessary controls to support fetching remaining data on demand.
#[derive(Clone, Debug)]
pub struct BlobHandle {
    o_am_conn_core: Option<AmConnCore>,
    is_data_complete: bool,
    length_b: u64,
    locator_id: u64,
    data: Vec<u8>,
    max_size: usize,
    acc_byte_length: usize,
    server_resource_consumption_info: ServerResourceConsumptionInfo,
}
impl BlobHandle {
    pub fn new(
        am_conn_core: &AmConnCore,
        is_data_complete: bool,
        length_b: u64,
        locator_id: u64,
        data: Vec<u8>,
    ) -> BlobHandle {
        trace!(
            "BlobHandle::new() with length_b = {}, is_data_complete = {}, data.length() = {}",
            length_b,
            is_data_complete,
            data.len()
        );
        BlobHandle {
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
            "After BlobHandle fetch: is_data_complete = {}, data.len() = {}",
            self.is_data_complete,
            self.data.len()
        );
        Ok(())
    }

    /// Converts a BLOB into a Vec<u8> containing its data.
    fn fetch_all(&mut self) -> HdbResult<()> {
        trace!("BlobHandle::fetch_all()");
        while !self.is_data_complete {
            self.fetch_next_chunk()?;
        }
        Ok(())
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Converts a BLOB into a Vec<u8> containing its data.
    pub fn into_bytes(mut self) -> HdbResult<Vec<u8>> {
        trace!("BlobHandle::into_bytes()");
        self.fetch_all()?;
        Ok(self.data)
    }
}

// Support for streaming
impl io::Read for BlobHandle {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        trace!("BlobHandle::read() with buf of len {}", buf.len());

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
