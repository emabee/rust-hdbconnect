use std::error::Error;
use std::fmt;

pub enum ConversionError {
    ValueType(String),
    NumberRange(String),
    IncompleteLob(String),
}

impl Error for ConversionError {
    fn description(&self) -> &str {
        match *self {
            ConversionError::ValueType(_) => "value types do not match",
            ConversionError::NumberRange(_) => "number range exceeded",
            ConversionError::IncompleteLob(_) => "incomplete LOB",
        }
    }
}

impl fmt::Debug for ConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConversionError::ValueType(ref s) => {
                write!(formatter, "{}: (\"{}\")", self.description(), s)
            }
            ConversionError::NumberRange(ref s) => {
                write!(formatter, "{}: (\"{}\")", self.description(), s)
            }
            ConversionError::IncompleteLob(ref s) => {
                write!(formatter, "{}: (\"{}\")", self.description(), s)
            }
        }
    }
}

impl fmt::Display for ConversionError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConversionError::ValueType(ref s) => write!(fmt, "{} ", s),
            ConversionError::NumberRange(ref s) => write!(fmt, "{} ", s),
            ConversionError::IncompleteLob(ref s) => write!(fmt, "{} ", s),
        }
    }
}
