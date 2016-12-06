use protocol::lowlevel::parts::lob_handles::{BlobHandle, ClobHandle};
use protocol::protocol_error::PrtResult;

/// BLOB implementation that is used within TypedValue::BLOB.
///
/// BLOB comes in two flavors, depending on
/// whether we read it from the database or write it to the database.
#[derive(Clone,Debug)]
pub enum BLOB {
    /// A BlobHandle represents a CLOB that was read from the database.
    FromDB(BlobHandle),
    /// A mere newtype-struct around the data.
    ToDB(Vec<u8>),
}

impl BLOB {
    /// Returns the contained Vec<u8>.
    pub fn into_bytes(self) -> PrtResult<Vec<u8>> {
        trace!("BLOB::into_bytes()");
        match self {
            BLOB::FromDB(handle) => handle.into_bytes(),
            BLOB::ToDB(vec) => Ok(vec),
        }
    }
}

// ////////////////////////////////////////////////////////////////////////////////////////

/// CLOB implementation that is used with TypedValue::CLOB and TypedValue::NCLOB.
///
/// CLOB comes in two flavors, depending on
/// whether we read it from the database or want to write it to the database.
#[derive(Clone,Debug)]
pub enum CLOB {
    /// A ClobHandle represents a CLOB that was read from the database.
    FromDB(ClobHandle),
    /// A mere newtype-struct around the data.
    ToDB(String),
}

impl CLOB {
    /// Returns the contained String.
    pub fn into_string(self) -> PrtResult<String> {
        trace!("CLOB::into_string()");
        match self {
            CLOB::FromDB(handle) => handle.into_string(),
            CLOB::ToDB(s) => Ok(s),
        }
    }
}
