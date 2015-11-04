use protocol::lowlevel::hdberror::HdbError;
use protocol::lowlevel::util::Cesu8DecodingError;

use std::error;
use std::fmt;
use std::io;
use std::result;
// use std::string::FromUtf8Error;

use byteorder;
use serde;

/// This type represents all possible errors that can occur in hdbconnect
#[derive(Debug)]
pub enum DbcError {
    /// Error occured in deserialization of data into an application-defined structure
    DeserializationError(DCode),

    // /// Some UTF8 error occurred while serializing or deserializing a value.
    // FromUtf8Error(FromUtf8Error),

    /// Some error occured while reading CESU-8.
    Cesu8Error(Cesu8DecodingError),

    /// IO error occured in communication with the database
    IoError(io::Error),

    /// Protocol error occured in communication with the database
    ProtocolError(String),

    /// Database server responded with an error
    DbMessage(Vec<HdbError>),
}

pub type DbcResult<T> = result::Result<T, DbcError>;

impl DbcError {
    pub fn deserialization_error(code: DCode) -> DbcError {
        DbcError::DeserializationError(code)
    }
    pub fn protocol_error(s: String) -> DbcError {
        DbcError::ProtocolError(s)
    }
}

impl error::Error for DbcError {
    fn description(&self) -> &str {
        match *self {
            DbcError::DeserializationError(ref dcode) => dcode.description(),
            // DbcError::FromUtf8Error(ref error) => error.description(),
            DbcError::Cesu8Error(ref error) => error.description(),
            DbcError::IoError(ref error) => error.description(),
            DbcError::ProtocolError(ref s) => s,
            DbcError::DbMessage(_) => "HANA returned at least one error",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            // DbcError::FromUtf8Error(ref error) => Some(error),
            DbcError::Cesu8Error(ref error) => Some(error),
            DbcError::IoError(ref error) => Some(error),
            _ => None,
        }
    }
}

impl fmt::Display for DbcError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DbcError::DeserializationError(ref code) => write!(fmt, "{:?} ", code),
            // DbcError::FromUtf8Error(ref error) => fmt::Display::fmt(error, fmt),
            DbcError::Cesu8Error(ref error) => fmt::Display::fmt(error, fmt),
            DbcError::IoError(ref error) => fmt::Display::fmt(error, fmt),
            DbcError::ProtocolError(ref s) => write!(fmt, "{}", s),
            DbcError::DbMessage(ref vec) => {
                for hdberr in vec {
                    try!(fmt::Display::fmt(hdberr, fmt));
                }
                Ok(())
            },
        }
    }
}

impl From<io::Error> for DbcError {
    fn from(error: io::Error) -> DbcError {
        DbcError::IoError(error)
    }
}

// impl From<FromUtf8Error> for DbcError {
//     fn from(error: FromUtf8Error) -> DbcError {
//         DbcError::FromUtf8Error(error)
//     }
// }
//
impl From<Cesu8DecodingError> for DbcError {
    fn from(error: Cesu8DecodingError) -> DbcError {
        DbcError::Cesu8Error(error)
    }
}

impl From<byteorder::Error> for DbcError {
    fn from(err: byteorder::Error) -> DbcError {
        match err {
            byteorder::Error::Io(err) => DbcError::IoError(err),
            byteorder::Error::UnexpectedEOF =>
                DbcError::IoError(io::Error::new(io::ErrorKind::Other,"unexpected EOF"))
        }
    }
}


impl serde::de::Error for DbcError {
    fn syntax(s: &str) -> DbcError {
        DbcError::DeserializationError(DCode::ProgramError(String::from(s)))
    }

    fn end_of_stream() -> DbcError {
        DbcError::DeserializationError(DCode::TrailingRows)
    }

    fn unknown_field(field: &str) -> DbcError {
        DbcError::DeserializationError(DCode::UnknownField(String::from(field)))
    }

    fn missing_field(field: &'static str) -> DbcError {
        DbcError::DeserializationError(DCode::MissingField(field))
    }
}


/// The error codes that can arise while consuming a ResultSet
#[derive(Clone, PartialEq)]
pub enum DCode {
    ///
    ProgramError(String),

    /// Unknown field in struct.
    UnknownField(String),

    /// Struct is missing a field.
    MissingField(&'static str),

    ///
    NoMoreRows,

    /// Not all rows were evaluated
    TrailingRows,

    ///
    NoMoreCols,

    /// Not all rows were evaluated
    TrailingCols,

    /// No value found
    NoValueForRowColumn(usize,usize),

    ///
    KvnNothing,
}

impl DCode {
    fn description(&self) -> &str {
        match *self {
            DCode::ProgramError(_) => "Syntax error",
            DCode::UnknownField(_) => "UnknownField",
            DCode::MissingField(_) => "MissingField: {}",
            DCode::TrailingRows => "TrailingRows",
            DCode::NoMoreRows => "NoMoreRows",
            DCode::TrailingCols => "TrailingCols",
            DCode::NoMoreCols => "NoMoreCols",
            DCode::NoValueForRowColumn(_,_) => "No value found for (row, column)",
            DCode::KvnNothing => "Program error: got KVN::NOTHING",
        }
    }
}

impl fmt::Debug for DCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::fmt::Debug;

        match *self {
            DCode::ProgramError(ref s) => write!(f, "Syntax error: {}", s),
            DCode::UnknownField(ref s) => write!(f, "UnknownField: {}", s),
            DCode::MissingField(ref s) => write!(f, "MissingField: {}", s),
            DCode::TrailingRows => "TrailingRows".fmt(f),
            DCode::NoMoreRows => "NoMoreRows".fmt(f),
            DCode::TrailingCols => "TrailingCols".fmt(f),
            DCode::NoMoreCols => "NoMoreCols".fmt(f),
            DCode::NoValueForRowColumn(r,c) => write!(f, "No value found for row {} and column {}",r,c),
            DCode::KvnNothing => "Program error: got KVN::NOTHING".fmt(f),
        }
    }
}
