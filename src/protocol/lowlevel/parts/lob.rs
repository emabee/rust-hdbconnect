use protocol::lowlevel::conn_core::ConnCoreRef;
use protocol::lowlevel::parts::lob_handles::{BlobHandle, ClobHandle};
use protocol::protocol_error::{PrtError, PrtResult};

use std::cell::RefCell;

/// BLOB implementation that is used within TypedValue::BLOB.
///
/// BLOB comes in two flavors, depending on
/// whether we read it from the database or write it to the database.
#[derive(Clone,Debug)]
pub struct BLOB(BlobEnum);

#[derive(Clone,Debug)]
enum BlobEnum {
    /// A BlobHandle represents a CLOB that was read from the database.
    FromDB(RefCell<BlobHandle>),
    /// A mere newtype-struct around the data.
    ToDB(Vec<u8>),
}

pub fn new_blob_from_db(conn_ref: &ConnCoreRef, is_data_complete: bool, length_b: u64,
                        locator_id: u64, data: Vec<u8>)
                        -> BLOB {
    BLOB(BlobEnum::FromDB(RefCell::new(BlobHandle::new(conn_ref,
                                                       is_data_complete,
                                                       length_b,
                                                       locator_id,
                                                       data))))
}

/// Factory method for BLOBs that are to be sent to the database.
pub fn new_blob_to_db(vec: Vec<u8>) -> BLOB {
    BLOB(BlobEnum::ToDB(vec))
}

impl BLOB {
    /// Length of contained data.
    pub fn len(&self) -> PrtResult<usize> {
        match self.0 {
            BlobEnum::FromDB(ref handle) => handle.borrow_mut().len(),
            BlobEnum::ToDB(ref vec) => Ok(vec.len()),
        }
    }

    /// Ref to the contained Vec<u8>.
    pub fn ref_to_bytes(&self) -> PrtResult<&Vec<u8>> {
        trace!("BLOB::into_bytes()");
        match self.0 {
            BlobEnum::FromDB(_) => {
                Err(PrtError::ProtocolError("cannot serialize BlobHandle".to_string()))
            }
            BlobEnum::ToDB(ref vec) => Ok(vec),
        }
    }

    /// Converts into the contained Vec<u8>.
    pub fn into_bytes(self) -> PrtResult<Vec<u8>> {
        trace!("BLOB::into_bytes()");
        match self.0 {
            BlobEnum::FromDB(handle) => handle.into_inner().into_bytes(),
            BlobEnum::ToDB(vec) => Ok(vec),
        }
    }
}

// ////////////////////////////////////////////////////////////////////////////////////////

/// CLOB implementation that is used with TypedValue::CLOB and TypedValue::NCLOB.
///
/// CLOB comes in two flavors, depending on
/// whether we read it from the database or want to write it to the database.
#[derive(Clone,Debug)]
pub struct CLOB(ClobEnum);

#[derive(Clone,Debug)]
enum ClobEnum {
    /// A ClobHandle represents a CLOB that was read from the database.
    FromDB(RefCell<ClobHandle>),
    /// A mere newtype-struct around the data.
    ToDB(String),
}

pub fn new_clob_from_db(conn_ref: &ConnCoreRef, is_data_complete: bool, length_c: u64,
                        length_b: u64, char_count: u64, locator_id: u64, data: String)
                        -> CLOB {
    CLOB(ClobEnum::FromDB(RefCell::new(ClobHandle::new(conn_ref,
                                                       is_data_complete,
                                                       length_c,
                                                       length_b,
                                                       char_count,
                                                       locator_id,
                                                       data))))
}

/// Factory method for CLOBs that are to be sent to the database.
pub fn new_clob_to_db(s: String) -> CLOB {
    CLOB(ClobEnum::ToDB(s))
}

impl CLOB {
    /// Length of contained String
    pub fn len(&self) -> PrtResult<usize> {
        match self.0 {
            ClobEnum::FromDB(ref handle) => handle.borrow_mut().len(),
            ClobEnum::ToDB(ref s) => Ok(s.len()),
        }
    }

    /// Ref to the contained String.
    pub fn ref_to_string(&self) -> PrtResult<&String> {
        trace!("CLOB::ref_to_string()");
        match self.0 {
            ClobEnum::FromDB(_) => {
                Err(PrtError::ProtocolError("cannot serialize ClobHandle".to_string()))
            }
            ClobEnum::ToDB(ref s) => Ok(s),
        }
    }

    /// Returns the contained String.
    pub fn into_string(self) -> PrtResult<String> {
        trace!("CLOB::into_string()");
        match self.0 {
            ClobEnum::FromDB(handle) => handle.into_inner().into_string(),
            ClobEnum::ToDB(s) => Ok(s),
        }
    }
}
