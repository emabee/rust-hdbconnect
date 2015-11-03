use std::error;
use std::fmt;
use std::io;
use std::result;
use std::string::FromUtf8Error;

use serde;

pub fn rs_error(s: &String) -> RsError {
    RsError::RsError(Code::Syntax(s.clone()))
}

/// This type represents all possible errors that can occur when deserializing a ResultSet
#[derive(Debug)]
pub enum RsError {
    RsError(Code),

    /// Some UTF8 error occurred while serializing or deserializing a value.
    FromUtf8Error(FromUtf8Error),
}

/// The error codes that can arise while consuming a ResultSet
#[derive(Clone, PartialEq)]
pub enum Code {
    ///
    Syntax(String),

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

impl fmt::Debug for Code {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::fmt::Debug;

        match *self {
            Code::Syntax(ref s) => write!(f, "Syntax error: {}", s),
            Code::UnknownField(ref s) => write!(f, "UnknownField: {}", s),
            Code::MissingField(ref s) => write!(f, "MissingField: {}", s),
            Code::TrailingRows => "TrailingRows".fmt(f),
            Code::NoMoreRows => "NoMoreRows".fmt(f),
            Code::TrailingCols => "TrailingCols".fmt(f),
            Code::NoMoreCols => "NoMoreCols".fmt(f),
            Code::NoValueForRowColumn(r,c) => write!(f, "No value found for row {} and column {}",r,c),
            Code::KvnNothing => "Program error: got KVN::NOTHING".fmt(f),
        }
    }
}

impl error::Error for RsError {
    fn description(&self) -> &str {
        match *self {
            RsError::RsError(..) => "result evaluation error",
            RsError::FromUtf8Error(ref error) => error.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            RsError::FromUtf8Error(ref error) => Some(error),
            _ => None,
        }
    }

}

impl fmt::Display for RsError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RsError::RsError(ref code) => write!(fmt, "{:?} ", code),
            RsError::FromUtf8Error(ref error) => fmt::Display::fmt(error, fmt),
        }
    }
}

impl From<RsError> for io::Error {
    fn from(error: RsError) -> io::Error {
        io::Error::new(io::ErrorKind::Other, error)
    }
}

impl From<FromUtf8Error> for RsError {
    fn from(error: FromUtf8Error) -> RsError {
        RsError::FromUtf8Error(error)
    }
}

impl serde::de::Error for RsError {
    fn syntax(s: &str) -> RsError {
        RsError::RsError(Code::Syntax(String::from(s)))
    }

    fn end_of_stream() -> RsError {
        RsError::RsError(Code::TrailingRows)
    }

    fn unknown_field(field: &str) -> RsError {
        RsError::RsError(Code::UnknownField(String::from(field)))
    }

    fn missing_field(field: &'static str) -> RsError {
        RsError::RsError(Code::MissingField(field))
    }
}

pub type RsResult<T> = result::Result<T, RsError>;
