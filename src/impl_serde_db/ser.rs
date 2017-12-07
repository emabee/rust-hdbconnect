use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};

use protocol::lowlevel::parts::hdb_decimal::HdbDecimal;
use protocol::lowlevel::parts::longdate::LongDate;
use protocol::lowlevel::parts::lob::new_blob_to_db;
use protocol::lowlevel::parts::parameter_descriptor::ParameterDescriptor;
use protocol::lowlevel::parts::typed_value::TypedValue;
use protocol::lowlevel::parts::type_id;

use num::FromPrimitive;
use rust_decimal::Decimal;
use serde_db::ser::{DbvFactory, SerializationError};
use std::{i16, i32, i64, i8, u16, u32, u8};
use std::str::FromStr;


#[doc(hidden)]
impl DbvFactory for ParameterDescriptor {
    type DBV = TypedValue;

    fn from_bool(&self, value: bool) -> Result<TypedValue, SerializationError> {
        Ok(match self.type_id() {
            type_id::BOOLEAN => TypedValue::BOOLEAN(value),
            type_id::N_BOOLEAN => TypedValue::N_BOOLEAN(Some(value)),
            _ => return Err(SerializationError::TypeMismatch("boolean", self.descriptor())),
        })
    }

    fn from_i8(&self, value: i8) -> Result<TypedValue, SerializationError> {
        let input_type = "i8";
        Ok(match self.type_id() {
            type_id::TINYINT => if value >= 0 {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if value >= 0 {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::SMALLINT => TypedValue::SMALLINT(i16::from(value)),
            type_id::N_SMALLINT => TypedValue::N_SMALLINT(Some(i16::from(value))),
            type_id::INT => TypedValue::INT(i32::from(value)),
            type_id::N_INT => TypedValue::N_INT(Some(i32::from(value))),
            type_id::BIGINT => TypedValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_i8(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_i8(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i16(&self, value: i16) -> Result<TypedValue, SerializationError> {
        let input_type = "i16";
        Ok(match self.type_id() {
            type_id::TINYINT => if (value >= 0) && (value <= i16::from(u8::MAX)) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if (value >= 0) && (value <= i16::from(u8::MAX)) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::SMALLINT => TypedValue::SMALLINT(value),
            type_id::N_SMALLINT => TypedValue::N_SMALLINT(Some(value)),
            type_id::INT => TypedValue::INT(i32::from(value)),
            type_id::N_INT => TypedValue::N_INT(Some(i32::from(value))),
            type_id::BIGINT => TypedValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_i16(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_i16(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i32(&self, value: i32) -> Result<TypedValue, SerializationError> {
        let input_type = "i32";
        Ok(match self.type_id() {
            type_id::TINYINT => if (value >= 0) && (value <= i32::from(u8::MAX)) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if (value >= 0) && (value <= i32::from(u8::MAX)) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::SMALLINT => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, self.descriptor()));
                }
            }
            type_id::N_SMALLINT => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, self.descriptor()));
                }
            }
            type_id::INT => TypedValue::INT(value),
            type_id::N_INT => TypedValue::N_INT(Some(value)),
            type_id::BIGINT => TypedValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_i32(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_i32(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i64(&self, value: i64) -> Result<TypedValue, SerializationError> {
        let input_type = "i64";
        Ok(match self.type_id() {
            type_id::TINYINT => if (value >= 0) && (value <= i64::from(u8::MAX)) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if (value >= 0) && (value <= i64::from(u8::MAX)) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::SMALLINT => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, self.descriptor()));
                }
            }
            type_id::N_SMALLINT => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, self.descriptor()));
                }
            }
            type_id::INT => if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                TypedValue::INT(value as i32)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_INT => if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                TypedValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::BIGINT => TypedValue::BIGINT(value),
            type_id::N_BIGINT => TypedValue::N_BIGINT(Some(value)),
            type_id::LONGDATE => TypedValue::LONGDATE(LongDate::new(value)),
            type_id::N_LONGDATE => TypedValue::N_LONGDATE(Some(LongDate::new(value))),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_i64(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_i64(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u8(&self, value: u8) -> Result<TypedValue, SerializationError> {
        let input_type = "u8";
        Ok(match self.type_id() {
            type_id::TINYINT => TypedValue::TINYINT(value),
            type_id::N_TINYINT => TypedValue::N_TINYINT(Some(value)),
            type_id::SMALLINT => TypedValue::SMALLINT(i16::from(value)),
            type_id::N_SMALLINT => TypedValue::N_SMALLINT(Some(i16::from(value))),
            type_id::INT => TypedValue::INT(i32::from(value)),
            type_id::N_INT => TypedValue::N_INT(Some(i32::from(value))),
            type_id::BIGINT => TypedValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_u8(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_u8(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u16(&self, value: u16) -> Result<TypedValue, SerializationError> {
        let input_type = "u16";
        Ok(match self.type_id() {
            type_id::TINYINT => if value <= u16::from(u8::MAX) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if value <= u16::from(u8::MAX) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::SMALLINT => if value <= i16::MAX as u16 {
                TypedValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_SMALLINT => if value <= i16::MAX as u16 {
                TypedValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::INT => TypedValue::INT(i32::from(value)),
            type_id::N_INT => TypedValue::N_INT(Some(i32::from(value))),
            type_id::BIGINT => TypedValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_u16(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_u16(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u32(&self, value: u32) -> Result<TypedValue, SerializationError> {
        let input_type = "u32";
        Ok(match self.type_id() {
            type_id::TINYINT => if value <= u32::from(u8::MAX) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if value <= u32::from(u8::MAX) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::SMALLINT => if value <= i16::MAX as u32 {
                TypedValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_SMALLINT => if value <= i16::MAX as u32 {
                TypedValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::INT => if value <= i32::MAX as u32 {
                TypedValue::INT(value as i32)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_INT => if value <= i32::MAX as u32 {
                TypedValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::BIGINT => TypedValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => TypedValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_u32(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_u32(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u64(&self, value: u64) -> Result<TypedValue, SerializationError> {
        let input_type = "u64";
        Ok(match self.type_id() {
            type_id::TINYINT => if value <= u64::from(u8::MAX) {
                TypedValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if value <= u64::from(u8::MAX) {
                TypedValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::SMALLINT => if value <= i16::MAX as u64 {
                TypedValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_SMALLINT => if value <= i16::MAX as u64 {
                TypedValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::INT => if value <= i32::MAX as u64 {
                TypedValue::INT(value as i32)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_INT => if value <= i32::MAX as u64 {
                TypedValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::BIGINT => if value <= i64::MAX as u64 {
                TypedValue::BIGINT(value as i64)
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::N_BIGINT => if value <= i64::MAX as u64 {
                TypedValue::N_BIGINT(Some(value as i64))
            } else {
                return Err(SerializationError::RangeErr(input_type, self.descriptor()));
            },
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_u64(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_u64(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_f32(&self, value: f32) -> Result<TypedValue, SerializationError> {
        let input_type = "f32";
        Ok(match self.type_id() {
            type_id::REAL => TypedValue::REAL(value),
            type_id::N_REAL => TypedValue::N_REAL(Some(value)),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_f32(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_f32(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch("f32", self.descriptor())),
        })
    }
    fn from_f64(&self, value: f64) -> Result<TypedValue, SerializationError> {
        let input_type = "f64";
        Ok(match self.type_id() {
            type_id::DOUBLE => TypedValue::DOUBLE(value),
            type_id::N_DOUBLE => TypedValue::N_DOUBLE(Some(value)),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::from_decimal(
                Decimal::from_f64(value).ok_or_else(|| decimal_range(input_type))?,
            )?),
            type_id::N_DECIMAL => TypedValue::N_DECIMAL(Some(HdbDecimal::from_decimal(
                Decimal::from_f64(value).ok_or_else(|| decimal_range(input_type))?,
            )?)),
            _ => return Err(SerializationError::TypeMismatch("f64", self.descriptor())),
        })
    }
    fn from_char(&self, value: char) -> Result<TypedValue, SerializationError> {
        let mut s = String::new();
        s.push(value);
        Ok(match self.type_id() {
            type_id::CHAR |
            type_id::VARCHAR |
            type_id::NCHAR |
            type_id::NVARCHAR |
            type_id::STRING |
            type_id::NSTRING |
            type_id::TEXT |
            type_id::SHORTTEXT => TypedValue::STRING(s),
            _ => return Err(SerializationError::TypeMismatch("char", self.descriptor())),
        })
    }
    fn from_str(&self, value: &str) -> Result<TypedValue, SerializationError> {
        let maperr1 = |_| SerializationError::TypeMismatch("&str", self.descriptor());
        let maperr2 = |_| SerializationError::TypeMismatch("&str", self.descriptor());
        Ok(match self.type_id() {
            type_id::TINYINT => TypedValue::TINYINT(u8::from_str(value).map_err(maperr1)?),
            type_id::SMALLINT => TypedValue::SMALLINT(i16::from_str(value).map_err(maperr1)?),
            type_id::INT => TypedValue::INT(i32::from_str(value).map_err(maperr1)?),
            type_id::BIGINT => TypedValue::BIGINT(i64::from_str(value).map_err(maperr1)?),
            type_id::REAL => TypedValue::REAL(f32::from_str(value).map_err(maperr2)?),
            type_id::DOUBLE => TypedValue::DOUBLE(f64::from_str(value).map_err(maperr2)?),
            type_id::CHAR |
            type_id::VARCHAR |
            type_id::NCHAR |
            type_id::NVARCHAR |
            type_id::STRING |
            type_id::NSTRING |
            type_id::TEXT |
            type_id::SHORTTEXT |
            type_id::N_CLOB |
            type_id::N_NCLOB |
            type_id::NCLOB |
            type_id::CLOB => TypedValue::STRING(String::from(value)),
            type_id::DECIMAL => TypedValue::DECIMAL(HdbDecimal::parse_from_str(value)?),
            type_id::LONGDATE => TypedValue::LONGDATE(longdate_from_str(value)?),

            _ => return Err(SerializationError::TypeMismatch("&str", self.descriptor())),
        })
    }
    fn from_bytes(&self, value: &[u8]) -> Result<TypedValue, SerializationError> {
        Ok(match self.type_id() {
            type_id::BLOB => TypedValue::BLOB(new_blob_to_db((*value).to_vec())),
            type_id::N_BLOB => TypedValue::N_BLOB(Some(new_blob_to_db((*value).to_vec()))),
            _ => return Err(SerializationError::TypeMismatch("bytes", self.descriptor())),
        })
    }
    fn from_none(&self) -> Result<TypedValue, SerializationError> {
        Ok(match self.type_id() {
            type_id::N_TINYINT => TypedValue::N_TINYINT(None),
            type_id::N_SMALLINT => TypedValue::N_SMALLINT(None),
            type_id::N_INT => TypedValue::N_INT(None),
            type_id::N_BIGINT => TypedValue::N_BIGINT(None),
            type_id::N_REAL => TypedValue::N_REAL(None),
            type_id::N_DOUBLE => TypedValue::N_DOUBLE(None),
            type_id::N_CHAR => TypedValue::N_CHAR(None),
            type_id::N_VARCHAR => TypedValue::N_VARCHAR(None),
            type_id::N_NCHAR => TypedValue::N_NCHAR(None),
            type_id::N_NVARCHAR => TypedValue::N_NVARCHAR(None),
            type_id::N_BINARY => TypedValue::N_BINARY(None),
            type_id::N_VARBINARY => TypedValue::N_VARBINARY(None),
            type_id::N_CLOB => TypedValue::N_CLOB(None),
            type_id::N_NCLOB => TypedValue::N_NCLOB(None),
            type_id::N_BLOB => TypedValue::N_BLOB(None),
            type_id::N_BOOLEAN => TypedValue::N_BOOLEAN(None),
            type_id::N_STRING => TypedValue::N_STRING(None),
            type_id::N_NSTRING => TypedValue::N_NSTRING(None),
            type_id::N_BSTRING => TypedValue::N_BSTRING(None),
            type_id::N_TEXT => TypedValue::N_TEXT(None),
            type_id::N_SHORTTEXT => TypedValue::N_SHORTTEXT(None),
            type_id::N_LONGDATE => TypedValue::N_LONGDATE(None),
            _ => return Err(SerializationError::TypeMismatch("none", self.descriptor())),
        })
    }

    fn descriptor(&self) -> String {
        String::from(match self.type_id() {
            type_id::N_TINYINT => "Nullable TINYINT",
            type_id::TINYINT => "TINYINT",
            type_id::N_SMALLINT => "Nullable SMALLINT",
            type_id::SMALLINT => "SMALLINT",
            type_id::N_INT => "Nullable INT",
            type_id::INT => "INT",
            type_id::N_BIGINT => "Nullable BIGINT",
            type_id::BIGINT => "BIGINT",
            type_id::N_DECIMAL => "Nullable DECIMAL",
            type_id::DECIMAL => "DECIMAL",
            type_id::N_REAL => "Nullable REAL",
            type_id::REAL => "REAL",
            type_id::N_DOUBLE => "Nullable DOUBLE",
            type_id::DOUBLE => "DOUBLE",
            type_id::N_CHAR => "Nullable CHAR",
            type_id::CHAR => "CHAR",
            type_id::N_VARCHAR => "Nullable VARCHAR",
            type_id::VARCHAR => "VARCHAR",
            type_id::N_NCHAR => "Nullable NCHAR",
            type_id::NCHAR => "NCHAR",
            type_id::N_NVARCHAR => "Nullable NVARCHAR",
            type_id::NVARCHAR => "NVARCHAR",
            type_id::N_BINARY => "Nullable BINARY",
            type_id::BINARY => "BINARY",
            type_id::N_VARBINARY => "Nullable VARBINARY",
            type_id::VARBINARY => "VARBINARY",
            type_id::N_CLOB => "Nullable CLOB",
            type_id::CLOB => "CLOB",
            type_id::N_NCLOB => "Nullable NCLOB",
            type_id::NCLOB => "NCLOB",
            type_id::N_BLOB => "Nullable BLOB",
            type_id::BLOB => "BLOB",
            type_id::N_BOOLEAN => "Nullable BOOLEAN",
            type_id::BOOLEAN => "BOOLEAN",
            type_id::N_STRING => "Nullable STRING",
            type_id::STRING => "STRING",
            type_id::N_NSTRING => "Nullable NSTRING",
            type_id::NSTRING => "NSTRING",
            type_id::N_BSTRING => "Nullable BSTRING",
            type_id::BSTRING => "BSTRING",
            type_id::N_TEXT => "Nullable TEXT",
            type_id::TEXT => "TEXT",
            type_id::N_SHORTTEXT => "Nullable SHORTTEXT",
            type_id::SHORTTEXT => "SHORTTEXT",
            type_id::N_LONGDATE => "Nullable LONGDATE",
            type_id::LONGDATE => "LONGDATE",
            i => return format!("[no descriptor available for {}]", i),
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

fn decimal_range(ovt: &'static str) -> SerializationError {
    SerializationError::RangeErr(ovt, "Decimal".to_string())
}
