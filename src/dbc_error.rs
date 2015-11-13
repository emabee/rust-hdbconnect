use protocol::lowlevel::hdberror::HdbError;
use protocol::lowlevel::util::Cesu8DecodingError;

use std::error;
use std::fmt;
use std::io;
use std::result;

use byteorder;
use serde;

/// This type represents all possible errors that can occur in hdbconnect
#[derive(Debug)]
pub enum DbcError {
    /// Error occured in deserialization of data into an application-defined structure
    DeserializationError(DCode),

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
            DbcError::Cesu8Error(ref error) => error.description(),
            DbcError::IoError(ref error) => error.description(),
            DbcError::ProtocolError(ref s) => s,
            DbcError::DbMessage(_) => "HANA returned at least one error",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
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

    fn missing_field(field: &str) -> DbcError {
        DbcError::DeserializationError(DCode::MissingField(String::from(field)))
    }
}


/// The error codes that can arise while consuming a ResultSet
#[derive(Clone, PartialEq)]
pub enum DCode {
    /// Indicates an error in the deserialization-code
    ProgramError(String),

    /// Unknown field in struct.
    UnknownField(String),

    /// Struct is missing a field.
    MissingField(String),

    /// Unknown field in struct.
    WrongValueType(String),

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
}

impl DCode {
    fn description(&self) -> &str {
        match *self {
            DCode::ProgramError(_) => "error in the implementation of hdbconnect",
            DCode::UnknownField(_) => "the target structure contains a field for which no data are provided",
            DCode::MissingField(_) => "the target structure misses a field",
            DCode::WrongValueType(_) => "value types do not match",
            DCode::TrailingRows => "trailing rows",
            DCode::NoMoreRows => "no more rows",
            DCode::TrailingCols => "trailing columns",
            DCode::NoMoreCols => "no more columns",
            DCode::NoValueForRowColumn(_,_) => "vo value found for (row, column)",
        }
    }
}

impl fmt::Debug for DCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DCode::ProgramError(ref s)
            | DCode::UnknownField(ref s)
            | DCode::WrongValueType(ref s) => write!(f, "{}: {}", self.description(), s),
            DCode::MissingField(ref s) =>
                write!(f, "{} \"{}\"; note that the field mapping is case-sensitive, \
                           and partial deserialization is not supported", self.description(), s),
            DCode::TrailingRows
            | DCode::NoMoreRows
            | DCode::TrailingCols
            | DCode::NoMoreCols => write!(f, "{}", self.description()),
            DCode::NoValueForRowColumn(r,c) =>  write!(f, "{} = ({},{})", self.description(), r, c),
        }
    }
}
