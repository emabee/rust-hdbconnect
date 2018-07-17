use protocol::lowlevel::conn_core::AmConnCore;
use protocol::lowlevel::parts::blob_handle::BlobHandle;
use protocol::lowlevel::parts::clob_handle::ClobHandle;
use std::cell::RefCell;
use std::io;
use {HdbError, HdbResult};

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

// ////////////////////////////////////////////////////////////////////////////////////////

/// CLOB implementation that is used with `TypedValue::CLOB` and
/// `TypedValue::NCLOB`.
#[derive(Clone, Debug)]
pub struct CLOB(RefCell<ClobHandle>);

pub fn new_clob_from_db(
    am_conn_core: &AmConnCore,
    is_data_complete: bool,
    length_c: u64,
    length_b: u64,
    locator_id: u64,
    data: &[u8],
) -> CLOB {
    CLOB(RefCell::new(ClobHandle::new(
        am_conn_core,
        is_data_complete,
        length_c,
        length_b,
        locator_id,
        data,
    )))
}

impl CLOB {
    /// Length of contained String
    pub fn len(&self) -> HdbResult<usize> {
        self.0.borrow_mut().len()
    }

    /// Is container empty
    pub fn is_empty(&self) -> HdbResult<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns the maximum size of the internal buffers.
    ///
    /// Tests can verify that this value does not exceed `lob_read_size` +
    /// `buf.len()`.
    pub fn max_size(&self) -> usize {
        self.0.borrow().max_size()
    }

    /// Returns the contained String.
    pub fn into_string(self) -> HdbResult<String> {
        trace!("CLOB::into_string()");
        self.0.into_inner().into_string()
    }
}

// Support for CLOB streaming
impl io::Read for CLOB {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.borrow_mut().read(buf)
    }
}
