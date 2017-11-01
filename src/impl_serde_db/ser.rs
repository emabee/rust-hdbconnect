use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};

use protocol::lowlevel::parts::longdate::LongDate;
use protocol::lowlevel::parts::lob::new_blob_to_db;
use protocol::lowlevel::parts::parameter_metadata::ParameterDescriptor;
use protocol::lowlevel::parts::typed_value::TypedValue;
use protocol::lowlevel::parts::type_id::*;

use serde_db::ser::{DbvFactory, SerializationError};

use std::{i16, i32, i64, i8, u16, u32, u8};




impl DbvFactory for ParameterDescriptor {
    type DBV = TypedValue;

    fn from_bool(&self, value: bool) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_BOOLEAN => TypedValue::BOOLEAN(value),
            TYPEID_N_BOOLEAN => TypedValue::N_BOOLEAN(Some(value)),
            _ => return Err(SerializationError::TypeMismatch("boolean", self.descriptor())),
        })
    }

    fn from_i8(&self, value: i8) -> Result<TypedValue, SerializationError> {
        let input_type = "i8";
        Ok(match self.value_type {
            TYPEID_TINYINT => if value >= 0 {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_TINYINT => if value >= 0 {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_SMALLINT => TypedValue::SMALLINT(i16::from(value)),
            TYPEID_N_SMALLINT => TypedValue::N_SMALLINT(Some(i16::from(value))),
            TYPEID_INT => TypedValue::INT(i32::from(value)),
            TYPEID_N_INT => TypedValue::N_INT(Some(i32::from(value))),
            TYPEID_BIGINT => TypedValue::BIGINT(i64::from(value)),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i16(&self, value: i16) -> Result<TypedValue, SerializationError> {
        let input_type = "i16";
        Ok(match self.value_type {
            TYPEID_TINYINT => if (value >= 0) && (value <= i16::from(u8::MAX)) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_TINYINT => if (value >= 0) && (value <= i16::from(u8::MAX)) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_SMALLINT => TypedValue::SMALLINT(value),
            TYPEID_N_SMALLINT => TypedValue::N_SMALLINT(Some(value)),
            TYPEID_INT => TypedValue::INT(i32::from(value)),
            TYPEID_N_INT => TypedValue::N_INT(Some(i32::from(value))),
            TYPEID_BIGINT => TypedValue::BIGINT(i64::from(value)),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i32(&self, value: i32) -> Result<TypedValue, SerializationError> {
        let input_type = "i32";
        Ok(match self.value_type {
            TYPEID_TINYINT => if (value >= 0) && (value <= i32::from(u8::MAX)) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_TINYINT => if (value >= 0) && (value <= i32::from(u8::MAX)) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_SMALLINT => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, self.descriptor()));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, self.descriptor()));
                }
            }
            TYPEID_INT => TypedValue::INT(value),
            TYPEID_N_INT => TypedValue::N_INT(Some(value)),
            TYPEID_BIGINT => TypedValue::BIGINT(i64::from(value)),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i64(&self, value: i64) -> Result<TypedValue, SerializationError> {
        let input_type = "i64";
        Ok(match self.value_type {
            TYPEID_TINYINT => if (value >= 0) && (value <= i64::from(u8::MAX)) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_TINYINT => if (value >= 0) && (value <= i64::from(u8::MAX)) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_SMALLINT => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, self.descriptor()));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, self.descriptor()));
                }
            }
            TYPEID_INT => if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                TypedValue::INT(value as i32)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_INT => if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                TypedValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_BIGINT => TypedValue::BIGINT(value),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(value)),
            TYPEID_LONGDATE => TypedValue::LONGDATE(LongDate(value)),
            TYPEID_N_LONGDATE => TypedValue::N_LONGDATE(Some(LongDate(value))),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u8(&self, value: u8) -> Result<TypedValue, SerializationError> {
        let input_type = "u8";
        Ok(match self.value_type {
            TYPEID_TINYINT => TypedValue::TINYINT(value),
            TYPEID_N_TINYINT => TypedValue::N_TINYINT(Some(value)),
            TYPEID_SMALLINT => TypedValue::SMALLINT(i16::from(value)),
            TYPEID_N_SMALLINT => TypedValue::N_SMALLINT(Some(i16::from(value))),
            TYPEID_INT => TypedValue::INT(i32::from(value)),
            TYPEID_N_INT => TypedValue::N_INT(Some(i32::from(value))),
            TYPEID_BIGINT => TypedValue::BIGINT(i64::from(value)),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u16(&self, value: u16) -> Result<TypedValue, SerializationError> {
        let input_type = "u16";
        Ok(match self.value_type {
            TYPEID_TINYINT => if value <= u16::from(u8::MAX) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_TINYINT => if value <= u16::from(u8::MAX) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_SMALLINT => if value <= i16::MAX as u16 {
                TypedValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_SMALLINT => if value <= i16::MAX as u16 {
                TypedValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_INT => TypedValue::INT(i32::from(value)),
            TYPEID_N_INT => TypedValue::N_INT(Some(i32::from(value))),
            TYPEID_BIGINT => TypedValue::BIGINT(i64::from(value)),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u32(&self, value: u32) -> Result<TypedValue, SerializationError> {
        let input_type = "u32";
        Ok(match self.value_type {
            TYPEID_TINYINT => if value <= u32::from(u8::MAX) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_TINYINT => if value <= u32::from(u8::MAX) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_SMALLINT => if value <= i16::MAX as u32 {
                TypedValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_SMALLINT => if value <= i16::MAX as u32 {
                TypedValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_INT => if value <= i32::MAX as u32 {
                TypedValue::INT(value as i32)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_INT => if value <= i32::MAX as u32 {
                TypedValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_BIGINT => TypedValue::BIGINT(i64::from(value)),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u64(&self, value: u64) -> Result<TypedValue, SerializationError> {
        let input_type = "u64";
        Ok(match self.value_type {
            TYPEID_TINYINT => if value <= u64::from(u8::MAX) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_TINYINT => if value <= u64::from(u8::MAX) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_SMALLINT => if value <= i16::MAX as u64 {
                TypedValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_SMALLINT => if value <= i16::MAX as u64 {
                TypedValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_INT => if value <= i32::MAX as u64 {
                TypedValue::INT(value as i32)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_INT => if value <= i32::MAX as u64 {
                TypedValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_BIGINT => if value <= i64::MAX as u64 {
                TypedValue::BIGINT(value as i64)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            TYPEID_N_BIGINT => if value <= i64::MAX as u64 {
                TypedValue::N_BIGINT(Some(value as i64))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_f32(&self, value: f32) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_REAL => TypedValue::REAL(value),
            TYPEID_N_REAL => TypedValue::N_REAL(Some(value)),
            _ => return Err(SerializationError::TypeMismatch("f32", self.descriptor())),
        })
    }
    fn from_f64(&self, value: f64) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_DOUBLE => TypedValue::DOUBLE(value),
            TYPEID_N_DOUBLE => TypedValue::N_DOUBLE(Some(value)),
            _ => return Err(SerializationError::TypeMismatch("f64", self.descriptor())),
        })
    }
    fn from_char(&self, value: char) -> Result<TypedValue, SerializationError> {
        let mut s = String::new();
        s.push(value);
        Ok(match self.value_type {
            TYPEID_CHAR |
            TYPEID_VARCHAR |
            TYPEID_NCHAR |
            TYPEID_NVARCHAR |
            TYPEID_STRING |
            TYPEID_NSTRING |
            TYPEID_TEXT |
            TYPEID_SHORTTEXT => TypedValue::STRING(s),
            _ => return Err(SerializationError::TypeMismatch("char", self.descriptor())),
        })
    }
    fn from_str(&self, value: &str) -> Result<TypedValue, SerializationError> {
        let s = String::from(value);
        Ok(match self.value_type {
            TYPEID_CHAR |
            TYPEID_VARCHAR |
            TYPEID_NCHAR |
            TYPEID_NVARCHAR |
            TYPEID_STRING |
            TYPEID_NSTRING |
            TYPEID_TEXT |
            TYPEID_SHORTTEXT |
            TYPEID_N_CLOB |
            TYPEID_N_NCLOB |
            TYPEID_NCLOB |
            TYPEID_CLOB => TypedValue::STRING(s),
            TYPEID_LONGDATE => TypedValue::LONGDATE(longdate_from_str(value)?),

            _ => return Err(SerializationError::TypeMismatch("&str", self.descriptor())),
        })
    }
    fn from_bytes(&self, value: &[u8]) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_BLOB => TypedValue::BLOB(new_blob_to_db((*value).to_vec())),
            TYPEID_N_BLOB => TypedValue::N_BLOB(Some(new_blob_to_db((*value).to_vec()))),
            _ => return Err(SerializationError::TypeMismatch("bytes", self.descriptor())),
        })
    }
    fn from_none(&self) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_N_TINYINT => TypedValue::N_TINYINT(None),
            TYPEID_N_SMALLINT => TypedValue::N_SMALLINT(None),
            TYPEID_N_INT => TypedValue::N_INT(None),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(None),
            TYPEID_N_REAL => TypedValue::N_REAL(None),
            TYPEID_N_DOUBLE => TypedValue::N_DOUBLE(None),
            TYPEID_N_CHAR => TypedValue::N_CHAR(None),
            TYPEID_N_VARCHAR => TypedValue::N_VARCHAR(None),
            TYPEID_N_NCHAR => TypedValue::N_NCHAR(None),
            TYPEID_N_NVARCHAR => TypedValue::N_NVARCHAR(None),
            TYPEID_N_BINARY => TypedValue::N_BINARY(None),
            TYPEID_N_VARBINARY => TypedValue::N_VARBINARY(None),
            TYPEID_N_CLOB => TypedValue::N_CLOB(None),
            TYPEID_N_NCLOB => TypedValue::N_NCLOB(None),
            TYPEID_N_BLOB => TypedValue::N_BLOB(None),
            TYPEID_N_BOOLEAN => TypedValue::N_BOOLEAN(None),
            TYPEID_N_STRING => TypedValue::N_STRING(None),
            TYPEID_N_NSTRING => TypedValue::N_NSTRING(None),
            TYPEID_N_BSTRING => TypedValue::N_BSTRING(None),
            TYPEID_N_TEXT => TypedValue::N_TEXT(None),
            TYPEID_N_SHORTTEXT => TypedValue::N_SHORTTEXT(None),
            TYPEID_N_LONGDATE => TypedValue::N_LONGDATE(None),
            _ => return Err(SerializationError::TypeMismatch("none", self.descriptor())),
        })
    }

    fn descriptor(&self) -> String {
        String::from(match self.value_type {
            TYPEID_N_TINYINT => "Nullable TINYINT",
            TYPEID_TINYINT => "TINYINT",
            TYPEID_N_SMALLINT => "Nullable SMALLINT",
            TYPEID_SMALLINT => "SMALLINT",
            TYPEID_N_INT => "Nullable INT",
            TYPEID_INT => "INT",
            TYPEID_N_BIGINT => "Nullable BIGINT",
            TYPEID_BIGINT => "BIGINT",
            TYPEID_N_REAL => "Nullable REAL",
            TYPEID_REAL => "REAL",
            TYPEID_N_DOUBLE => "Nullable DOUBLE",
            TYPEID_DOUBLE => "DOUBLE",
            TYPEID_N_CHAR => "Nullable CHAR",
            TYPEID_CHAR => "CHAR",
            TYPEID_N_VARCHAR => "Nullable VARCHAR",
            TYPEID_VARCHAR => "VARCHAR",
            TYPEID_N_NCHAR => "Nullable NCHAR",
            TYPEID_NCHAR => "NCHAR",
            TYPEID_N_NVARCHAR => "Nullable NVARCHAR",
            TYPEID_NVARCHAR => "NVARCHAR",
            TYPEID_N_BINARY => "Nullable BINARY",
            TYPEID_BINARY => "BINARY",
            TYPEID_N_VARBINARY => "Nullable VARBINARY",
            TYPEID_VARBINARY => "VARBINARY",
            TYPEID_N_CLOB => "Nullable CLOB",
            TYPEID_CLOB => "CLOB",
            TYPEID_N_NCLOB => "Nullable NCLOB",
            TYPEID_NCLOB => "NCLOB",
            TYPEID_N_BLOB => "Nullable BLOB",
            TYPEID_BLOB => "BLOB",
            TYPEID_N_BOOLEAN => "Nullable BOOLEAN",
            TYPEID_BOOLEAN => "BOOLEAN",
            TYPEID_N_STRING => "Nullable STRING",
            TYPEID_STRING => "STRING",
            TYPEID_N_NSTRING => "Nullable NSTRING",
            TYPEID_NSTRING => "NSTRING",
            TYPEID_N_BSTRING => "Nullable BSTRING",
            TYPEID_BSTRING => "BSTRING",
            TYPEID_N_TEXT => "Nullable TEXT",
            TYPEID_TEXT => "TEXT",
            TYPEID_N_SHORTTEXT => "Nullable SHORTTEXT",
            TYPEID_SHORTTEXT => "SHORTTEXT",
            TYPEID_N_LONGDATE => "Nullable LONGDATE",
            TYPEID_LONGDATE => "LONGDATE",
            i => return format!("no descriptor available for {}", i),
        })
    }
}


// Serializes a date string into a `LongDate`.
//
// Chrono types serialize as formatted Strings. We try to parse such a string
// to convert back into the type we had originally, and construct a `LongDate`.
fn longdate_from_str(s: &str) -> Result<LongDate, SerializationError> {
    #[allow(unknown_lints)]
    #[allow(type_complexity)]
    let funcs: Vec<fn(&str) -> Result<LongDate, ()>> = vec![
        from_naivedt_string_full,
        from_naivedt_string_second,
        from_naivedt_string_day,
        from_utc_string,
    ];

    for func in funcs {
        if let Ok(longdate) = func(s) {
            return Ok(longdate);
        }
    }
    Err(SerializationError::GeneralError("Cannot serialize date-string to LongDate".to_string()))
}

// 2012-02-02T02:02:02.200
fn from_naivedt_string_full(s: &str) -> Result<LongDate, ()> {
    trace!("from_naivedt_string_full");
    match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(ndt_parsed) => {
            let ld = LongDate::from_ymd_hms_n(
                ndt_parsed.year(),
                ndt_parsed.month(),
                ndt_parsed.day(),
                ndt_parsed.hour(),
                ndt_parsed.minute(),
                ndt_parsed.second(),
                ndt_parsed.nanosecond(),
            ).or(Err(()))?;
            trace!("NaiveDateTime::from_naivedt_string_full(): OK with ld = {}", ld);
            Ok(ld)
        }
        Err(e) => {
            trace!("{:?}", e);
            Err(())
        }
    }
}

fn from_naivedt_string_second(s: &str) -> Result<LongDate, ()> {
    trace!("from_naivedt_string_second");
    match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        Ok(ndt_parsed) => {
            let ld = LongDate::from_ymd_hms(
                ndt_parsed.year(),
                ndt_parsed.month(),
                ndt_parsed.day(),
                ndt_parsed.hour(),
                ndt_parsed.minute(),
                ndt_parsed.second(),
            ).or(Err(()))?;
            trace!("NaiveDateTime::from_naivedt_string_second(): OK with ld = {}", ld);
            Ok(ld)
        }
        Err(e) => {
            trace!("{:?}", e);
            Err(())
        }
    }
}

fn from_naivedt_string_day(s: &str) -> Result<LongDate, ()> {
    trace!("from_naivedt_string_day with {}", s);
    match NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        Ok(ndt_parsed) => {
            let ld = try!(
                LongDate::from_ymd(ndt_parsed.year(), ndt_parsed.month(), ndt_parsed.day()).or(
                    Err(())
                )
            );
            trace!("NaiveDateTime::from_naivedt_string_day(): OK with ld = {}", ld);
            Ok(ld)
        }
        Err(e) => {
            trace!("{:?}", e);
            Err(())
        }
    }
}

// 2012-02-02T02:02:02.200Z
fn from_utc_string(s: &str) -> Result<LongDate, ()> {
    trace!("from_utc_string");
    match DateTime::parse_from_rfc3339(s) {
        Ok(dt) => {
            trace!("DateTime::parse_from_rfc3339(s): {}", dt);
            let ndt = dt.naive_utc();
            let ld = try!(
                LongDate::from_ymd_hms_n(
                    ndt.year(),
                    ndt.month(),
                    ndt.day(),
                    ndt.hour(),
                    ndt.minute(),
                    ndt.second(),
                    ndt.nanosecond()
                ).or(Err(()))
            );
            trace!("DateTime::parse_from_rfc3339(): OK with ld = {}", ld);
            Ok(ld)
        }
        Err(e) => {
            trace!("{:?}", e);
            Err(())
        }
    }
}
