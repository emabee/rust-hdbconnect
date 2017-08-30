use protocol::lowlevel::parts::typed_value::TypedValue;
use protocol::protocol_error::PrtError;
use types::LongDate;

use chrono::{NaiveDateTime, NaiveDate, NaiveTime};
use std::{u8, u16, u32, i8, i16, i32};

///
pub trait DbValue<T> {
    ///
    fn try_into(self) -> Result<T, ConversionError>;
}

impl DbValue<bool> for TypedValue {
    fn try_into(self) -> Result<bool, ConversionError> {
        match self {
            TypedValue::BOOLEAN(b) |
            TypedValue::N_BOOLEAN(Some(b)) => Ok(b),
            value => Err(wrong_type(&value, "bool")),
        }
    }
}

impl DbValue<u8> for TypedValue {
    fn try_into(self) -> Result<u8, ConversionError> {
        match self {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => Ok(u),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if (i >= 0) && (i <= u8::MAX as i16) {
                    Ok(i as u8)
                } else {
                    Err(number_range(&(i as i64), "u8"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if (i >= 0) && (i <= u8::MAX as i32) {
                    Ok(i as u8)
                } else {
                    Err(number_range(&(i as i64), "u8"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= u8::MAX as i64) {
                    Ok(i as u8)
                } else {
                    Err(number_range(&i, "u8"))
                }
            }

            value => Err(wrong_type(&value, "u8")),
        }
    }
}

impl DbValue<u16> for TypedValue {
    fn try_into(self) -> Result<u16, ConversionError> {
        match self {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => Ok(u as u16),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if i >= 0 {
                    Ok(i as u16)
                } else {
                    Err(number_range(&(i as i64), "u16"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if (i >= 0) && (i <= u16::MAX as i32) {
                    Ok(i as u16)
                } else {
                    Err(number_range(&(i as i64), "u16"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= u16::MAX as i64) {
                    Ok(i as u16)
                } else {
                    Err(number_range(&i, "u16"))
                }
            }

            value => Err(wrong_type(&value, "u16")),
        }
    }
}

impl DbValue<u32> for TypedValue {
    fn try_into(self) -> Result<u32, ConversionError> {
        match self {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => Ok(u as u32),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if i >= 0 {
                    Ok(i as u32)
                } else {
                    Err(number_range(&(i as i64), "u32"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if i >= 0 {
                    Ok(i as u32)
                } else {
                    Err(number_range(&(i as i64), "u32"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= u32::MAX as i64) {
                    Ok(i as u32)
                } else {
                    Err(number_range(&i, "u32"))
                }
            }

            value => Err(wrong_type(&value, "u32")),
        }
    }
}

impl DbValue<u64> for TypedValue {
    fn try_into(self) -> Result<u64, ConversionError> {
        match self {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => Ok(u as u64),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if i >= 0 {
                    Ok(i as u64)
                } else {
                    Err(number_range(&(i as i64), "u64"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if i >= 0 {
                    Ok(i as u64)
                } else {
                    Err(number_range(&(i as i64), "u64"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if i >= 0 {
                    Ok(i as u64)
                } else {
                    Err(number_range(&i, "u64"))
                }
            }

            value => Err(wrong_type(&value, "u64")),
        }
    }
}

impl DbValue<i8> for TypedValue {
    fn try_into(self) -> Result<i8, ConversionError> {
        match self {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => {
                if u <= i8::MAX as u8 {
                    Ok(u as i8)
                } else {
                    Err(number_range(&(u as i64), "i8"))
                }
            }

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => {
                if (i >= i8::MIN as i16) && (i <= i8::MAX as i16) {
                    Ok(i as i8)
                } else {
                    Err(number_range(&(i as i64), "i8"))
                }
            }

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if (i >= i8::MIN as i32) && (i <= i8::MAX as i32) {
                    Ok(i as i8)
                } else {
                    Err(number_range(&(i as i64), "i8"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i8::MIN as i64) && (i <= i8::MAX as i64) {
                    Ok(i as i8)
                } else {
                    Err(number_range(&i, "i8"))
                }
            }

            value => Err(wrong_type(&value, "i8")),
        }
    }
}

impl DbValue<i16> for TypedValue {
    fn try_into(self) -> Result<i16, ConversionError> {
        match self {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => Ok(u as i16),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => Ok(i),

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => {
                if (i >= i16::MIN as i32) && (i <= i16::MAX as i32) {
                    Ok(i as i16)
                } else {
                    Err(number_range(&(i as i64), "i16"))
                }
            }

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i16::MIN as i64) && (i <= i16::MAX as i64) {
                    Ok(i as i16)
                } else {
                    Err(number_range(&i, "i16"))
                }
            }

            value => Err(wrong_type(&value, "i16")),
        }
    }
}

impl DbValue<i32> for TypedValue {
    fn try_into(self) -> Result<i32, ConversionError> {
        match self {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => Ok(u as i32),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => Ok(i as i32),

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => Ok(i),

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i32::MIN as i64) && (i <= i32::MAX as i64) {
                    Ok(i as i32)
                } else {
                    Err(number_range(&i, "i32"))
                }
            }
            value => Err(wrong_type(&value, "i32")),
        }
    }
}

impl DbValue<i64> for TypedValue {
    fn try_into(self) -> Result<i64, ConversionError> {
        match self {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => Ok(u as i64),

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => Ok(i as i64),

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => Ok(i as i64),

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) |
            TypedValue::LONGDATE(LongDate(i)) |
            TypedValue::N_LONGDATE(Some(LongDate(i))) => Ok(i),

            value => return Err(wrong_type(&value, "i64")),
        }
    }
}

impl DbValue<f32> for TypedValue {
    fn try_into(self) -> Result<f32, ConversionError> {
        match self {
            TypedValue::REAL(f) |
            TypedValue::N_REAL(Some(f)) => Ok(f),
            value => return Err(wrong_type(&value, "f32")),
        }
    }
}

impl DbValue<f64> for TypedValue {
    fn try_into(self) -> Result<f64, ConversionError> {
        match self {
            TypedValue::DOUBLE(f) |
            TypedValue::N_DOUBLE(Some(f)) => Ok(f),
            value => return Err(wrong_type(&value, "f64")),
        }
    }
}

impl DbValue<Option<i32>> for TypedValue {
    fn try_into(self) -> Result<Option<i32>, ConversionError> {
        match self {
            TypedValue::INT(i) => Ok(Some(i)),
            TypedValue::N_INT(o_i) => Ok(o_i),
            tv => Err(ConversionError::ValueType(format!("Not a Option<i32> value: {:?}", tv))),
        }
    }
}

impl DbValue<String> for TypedValue {
    fn try_into(self) -> Result<String, ConversionError> {
        match self {
            TypedValue::CHAR(s) |
            TypedValue::VARCHAR(s) |
            TypedValue::NCHAR(s) |
            TypedValue::NVARCHAR(s) |
            TypedValue::STRING(s) |
            TypedValue::NSTRING(s) |
            TypedValue::TEXT(s) |
            TypedValue::SHORTTEXT(s) |
            TypedValue::N_CHAR(Some(s)) |
            TypedValue::N_VARCHAR(Some(s)) |
            TypedValue::N_NCHAR(Some(s)) |
            TypedValue::N_NVARCHAR(Some(s)) |
            TypedValue::N_STRING(Some(s)) |
            TypedValue::N_NSTRING(Some(s)) |
            TypedValue::N_SHORTTEXT(Some(s)) |
            TypedValue::N_TEXT(Some(s)) => Ok(s),

            TypedValue::LONGDATE(ld) |
            TypedValue::N_LONGDATE(Some(ld)) => Ok(str_from_longdate(&ld)),

            TypedValue::CLOB(clob) |
            TypedValue::NCLOB(clob) |
            TypedValue::N_CLOB(Some(clob)) |
            TypedValue::N_NCLOB(Some(clob)) => Ok(clob.into_string()?),

            value => return Err(wrong_type(&value, "String")),
        }
    }
}

impl DbValue<NaiveDateTime> for TypedValue {
    fn try_into(self) -> Result<NaiveDateTime, ConversionError> {
        match self {
            TypedValue::LONGDATE(ld) |
            TypedValue::N_LONGDATE(Some(ld)) => {
                let (y, m, d, h, min, s, f) = ld.as_ymd_hms_f();
                Ok(NaiveDateTime::new(NaiveDate::from_ymd(y, m, d),
                                      NaiveTime::from_hms_nano(h, min, s, f * 100)))
            }
            _ => Err(ConversionError::ValueType("Not a LongDate value".to_owned())),
        }
    }
}

fn wrong_type(tv: &TypedValue, ovt: &str) -> ConversionError {
    ConversionError::ValueType(format!("The value {:?} cannot be converted into type {}", tv, ovt))
}

fn number_range(value: &i64, ovt: &str) -> ConversionError {
    ConversionError::NumberRange(format!("The value {:?} exceeds \
                                                                      the number range of type \
                                                                      {}",
                                         value,
                                         ovt))
}


/// Deserializes a LongDate into a String format.
fn str_from_longdate(ld: &LongDate) -> String {
    format!("{}", ld)
}



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

impl From<PrtError> for ConversionError {
    fn from(error: PrtError) -> ConversionError {
        ConversionError::IncompleteLob(error.description().to_owned())
    }
}
