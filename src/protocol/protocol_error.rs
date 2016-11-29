use protocol::lowlevel::parts::server_error::ServerError;
use protocol::lowlevel::util::Cesu8DecodingError;

use std::error;
use std::fmt;
use std::io;
use std::result;

/// This type represents all possible errors that can occur in hdbconnect
#[derive(Debug)]
pub enum PrtError {
    /// Database server responded with an error
    DbMessage(Vec<ServerError>),
    /// Some error occured while reading CESU-8.
    Cesu8Error(Cesu8DecodingError),
    /// IO error occured in communication with the database
    IoError(io::Error),
    /// Protocol error occured in communication with the database
    ProtocolError(String),
}

pub type PrtResult<T> = result::Result<T, PrtError>;

pub fn prot_err(s: &str) -> PrtError {
    PrtError::ProtocolError(String::from(s))
}

impl error::Error for PrtError {
    fn description(&self) -> &str {
        match *self {
            PrtError::Cesu8Error(ref error) => error.description(),
            PrtError::IoError(ref error) => error.description(),
            PrtError::ProtocolError(ref s) => s,
            PrtError::DbMessage(_) => "HANA returned at least one error",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            PrtError::Cesu8Error(ref error) => Some(error),
            PrtError::IoError(ref error) => Some(error),
            PrtError::ProtocolError(_) |
            PrtError::DbMessage(_) => None,
        }
    }
}

impl fmt::Display for PrtError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PrtError::Cesu8Error(ref error) => fmt::Display::fmt(error, fmt),
            PrtError::IoError(ref error) => fmt::Display::fmt(error, fmt),
            PrtError::ProtocolError(ref s) => write!(fmt, "{}", s),
            PrtError::DbMessage(ref vec) => {
                for hdberr in vec {
                    try!(fmt::Display::fmt(hdberr, fmt));
                }
                Ok(())
            }
        }
    }
}

impl From<io::Error> for PrtError {
    fn from(error: io::Error) -> PrtError {
        PrtError::IoError(error)
    }
}

impl From<Cesu8DecodingError> for PrtError {
    fn from(error: Cesu8DecodingError) -> PrtError {
        PrtError::Cesu8Error(error)
    }
}
