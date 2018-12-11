use bigdecimal::ParseBigDecimalError;
use hdb_error::HdbError;
use std::num::ParseFloatError;
use std::num::ParseIntError;
use types_impl::lob::new_blob_to_db;

use protocol::parts::hdb_value::HdbValue;
use protocol::parts::parameter_descriptor::ParameterDescriptor;
use protocol::parts::type_id::BaseTypeId;
use types::DayDate;
use types::LongDate;
use types::SecondDate;
use types::SecondTime;

use bigdecimal::BigDecimal;
use bigdecimal::FromPrimitive;
use serde_db::ser::{parse_error, DbvFactory, SerializationError};
use std::str::FromStr;
use std::{i16, i32, i64, i8, u16, u32, u8};

#[doc(hidden)]
impl DbvFactory for ParameterDescriptor {
    type DBV = HdbValue;

    fn from_bool(&self, value: bool) -> Result<HdbValue, SerializationError> {
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::BOOLEAN, false) => HdbValue::BOOLEAN(value),
            (BaseTypeId::BOOLEAN, true) => HdbValue::N_BOOLEAN(Some(value)),
            _ => {
                return Err(SerializationError::Type {
                    value_type: "boolean",
                    db_type: self.descriptor(),
                })
            }
        })
    }

    fn from_i8(&self, value: i8) -> Result<HdbValue, SerializationError> {
        let input_type = "i8";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::TINYINT, false) => {
                if value >= 0 {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::TINYINT, true) => {
                if value >= 0 {
                    HdbValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, false) => HdbValue::SMALLINT(i16::from(value)),
            (BaseTypeId::SMALLINT, true) => HdbValue::N_SMALLINT(Some(i16::from(value))),
            (BaseTypeId::INT, false) => HdbValue::INT(i32::from(value)),
            (BaseTypeId::INT, true) => HdbValue::N_INT(Some(i32::from(value))),
            (BaseTypeId::BIGINT, false) => HdbValue::BIGINT(i64::from(value)),
            (BaseTypeId::BIGINT, true) => HdbValue::N_BIGINT(Some(i64::from(value))),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_i8(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_i8(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: input_type,
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_i16(&self, value: i16) -> Result<HdbValue, SerializationError> {
        let input_type = "i16";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::TINYINT, false) => {
                if (value >= 0) && (value <= i16::from(u8::MAX)) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::TINYINT, true) => {
                if (value >= 0) && (value <= i16::from(u8::MAX)) {
                    HdbValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, false) => HdbValue::SMALLINT(value),
            (BaseTypeId::SMALLINT, true) => HdbValue::N_SMALLINT(Some(value)),
            (BaseTypeId::INT, false) => HdbValue::INT(i32::from(value)),
            (BaseTypeId::INT, true) => HdbValue::N_INT(Some(i32::from(value))),
            (BaseTypeId::BIGINT, false) => HdbValue::BIGINT(i64::from(value)),
            (BaseTypeId::BIGINT, true) => HdbValue::N_BIGINT(Some(i64::from(value))),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_i16(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_i16(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: input_type,
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_i32(&self, value: i32) -> Result<HdbValue, SerializationError> {
        let input_type = "i32";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::TINYINT, false) => {
                if (value >= 0) && (value <= i32::from(u8::MAX)) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::TINYINT, true) => {
                if (value >= 0) && (value <= i32::from(u8::MAX)) {
                    HdbValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, false) => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, true) => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    HdbValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::INT, false) => HdbValue::INT(value),
            (BaseTypeId::INT, true) => HdbValue::N_INT(Some(value)),
            (BaseTypeId::BIGINT, false) => HdbValue::BIGINT(i64::from(value)),
            (BaseTypeId::BIGINT, true) => HdbValue::N_BIGINT(Some(i64::from(value))),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_i32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_i32(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            (BaseTypeId::DAYDATE, false) => HdbValue::DAYDATE(DayDate::new(value)),
            (BaseTypeId::DAYDATE, true) => HdbValue::N_DAYDATE(Some(DayDate::new(value))),
            (BaseTypeId::SECONDTIME, false) => HdbValue::SECONDTIME(SecondTime::new(value)),
            (BaseTypeId::SECONDTIME, true) => HdbValue::N_SECONDTIME(Some(SecondTime::new(value))),
            _ => {
                return Err(SerializationError::Type {
                    value_type: input_type,
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_i64(&self, value: i64) -> Result<HdbValue, SerializationError> {
        let input_type = "i64";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::TINYINT, false) => {
                if (value >= 0) && (value <= i64::from(u8::MAX)) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::TINYINT, true) => {
                if (value >= 0) && (value <= i64::from(u8::MAX)) {
                    HdbValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, false) => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, true) => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    HdbValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::INT, false) => {
                if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                    HdbValue::INT(value as i32)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::INT, true) => {
                if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                    HdbValue::N_INT(Some(value as i32))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::BIGINT, false) => HdbValue::BIGINT(value),
            (BaseTypeId::BIGINT, true) => HdbValue::N_BIGINT(Some(value)),
            (BaseTypeId::LONGDATE, false) => HdbValue::LONGDATE(LongDate::new(value)),
            (BaseTypeId::LONGDATE, true) => HdbValue::N_LONGDATE(Some(LongDate::new(value))),
            (BaseTypeId::SECONDDATE, false) => HdbValue::SECONDDATE(SecondDate::new(value)),
            (BaseTypeId::SECONDDATE, true) => HdbValue::N_SECONDDATE(Some(SecondDate::new(value))),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_i64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_i64(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: input_type,
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_u8(&self, value: u8) -> Result<HdbValue, SerializationError> {
        let input_type = "u8";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::TINYINT, false) => HdbValue::TINYINT(value),
            (BaseTypeId::TINYINT, true) => HdbValue::N_TINYINT(Some(value)),
            (BaseTypeId::SMALLINT, false) => HdbValue::SMALLINT(i16::from(value)),
            (BaseTypeId::SMALLINT, true) => HdbValue::N_SMALLINT(Some(i16::from(value))),
            (BaseTypeId::INT, false) => HdbValue::INT(i32::from(value)),
            (BaseTypeId::INT, true) => HdbValue::N_INT(Some(i32::from(value))),
            (BaseTypeId::BIGINT, false) => HdbValue::BIGINT(i64::from(value)),
            (BaseTypeId::BIGINT, true) => HdbValue::N_BIGINT(Some(i64::from(value))),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_u8(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_u8(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: input_type,
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_u16(&self, value: u16) -> Result<HdbValue, SerializationError> {
        let input_type = "u16";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::TINYINT, false) => {
                if value <= u16::from(u8::MAX) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::TINYINT, true) => {
                if value <= u16::from(u8::MAX) {
                    HdbValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, false) => {
                if value <= i16::MAX as u16 {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, true) => {
                if value <= i16::MAX as u16 {
                    HdbValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::INT, false) => HdbValue::INT(i32::from(value)),
            (BaseTypeId::INT, true) => HdbValue::N_INT(Some(i32::from(value))),
            (BaseTypeId::BIGINT, false) => HdbValue::BIGINT(i64::from(value)),
            (BaseTypeId::BIGINT, true) => HdbValue::N_BIGINT(Some(i64::from(value))),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_u16(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_u16(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: input_type,
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_u32(&self, value: u32) -> Result<HdbValue, SerializationError> {
        let input_type = "u32";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::TINYINT, false) => {
                if value <= u32::from(u8::MAX) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::TINYINT, true) => {
                if value <= u32::from(u8::MAX) {
                    HdbValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, false) => {
                if value <= i16::MAX as u32 {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, true) => {
                if value <= i16::MAX as u32 {
                    HdbValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::INT, false) => {
                if value <= i32::MAX as u32 {
                    HdbValue::INT(value as i32)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::INT, true) => {
                if value <= i32::MAX as u32 {
                    HdbValue::N_INT(Some(value as i32))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::BIGINT, false) => HdbValue::BIGINT(i64::from(value)),
            (BaseTypeId::BIGINT, true) => HdbValue::N_BIGINT(Some(i64::from(value))),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_u32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_u32(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: input_type,
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_u64(&self, value: u64) -> Result<HdbValue, SerializationError> {
        let input_type = "u64";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::TINYINT, false) => {
                if value <= u64::from(u8::MAX) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::TINYINT, true) => {
                if value <= u64::from(u8::MAX) {
                    HdbValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, false) => {
                if value <= i16::MAX as u64 {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::SMALLINT, true) => {
                if value <= i16::MAX as u64 {
                    HdbValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::INT, false) => {
                if value <= i32::MAX as u64 {
                    HdbValue::INT(value as i32)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::INT, true) => {
                if value <= i32::MAX as u64 {
                    HdbValue::N_INT(Some(value as i32))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::BIGINT, false) => {
                if value <= i64::MAX as u64 {
                    HdbValue::BIGINT(value as i64)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::BIGINT, true) => {
                if value <= i64::MAX as u64 {
                    HdbValue::N_BIGINT(Some(value as i64))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_u64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_u64(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: input_type,
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_f32(&self, value: f32) -> Result<HdbValue, SerializationError> {
        let input_type = "f32";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::REAL, false) => HdbValue::REAL(value),
            (BaseTypeId::REAL, true) => HdbValue::N_REAL(Some(value)),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_f32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_f32(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: "f32",
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_f64(&self, value: f64) -> Result<HdbValue, SerializationError> {
        let input_type = "f64";
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::DOUBLE, false) => HdbValue::DOUBLE(value),
            (BaseTypeId::DOUBLE, true) => HdbValue::N_DOUBLE(Some(value)),
            (BaseTypeId::DECIMAL, false) => HdbValue::DECIMAL(
                BigDecimal::from_f64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            (BaseTypeId::DECIMAL, true) => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_f64(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            _ => {
                return Err(SerializationError::Type {
                    value_type: "f64",
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_char(&self, value: char) -> Result<HdbValue, SerializationError> {
        let mut s = String::new();
        s.push(value);
        Ok(match self.type_id().base_type_id() {
            BaseTypeId::CHAR
            | BaseTypeId::VARCHAR
            | BaseTypeId::NCHAR
            | BaseTypeId::NVARCHAR
            | BaseTypeId::STRING
            | BaseTypeId::NSTRING
            | BaseTypeId::TEXT
            | BaseTypeId::SHORTTEXT => HdbValue::STRING(s),
            _ => {
                return Err(SerializationError::Type {
                    value_type: "char",
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_str(&self, value: &str) -> Result<HdbValue, SerializationError> {
        let map_i = |e: ParseIntError| {
            parse_error(value, "some integer type".to_string(), Some(Box::new(e)))
        };
        let map_bd = |e: ParseBigDecimalError| {
            parse_error(value, "BigDecimal".to_string(), Some(Box::new(e)))
        };
        let map_f = |e: ParseFloatError| {
            parse_error(value, "some float type".to_string(), Some(Box::new(e)))
        };
        let map_d =
            |e: HdbError| parse_error(value, "some date type".to_string(), Some(Box::new(e)));
        Ok(match self.type_id().base_type_id() {
            BaseTypeId::TINYINT => HdbValue::TINYINT(u8::from_str(value).map_err(map_i)?),
            BaseTypeId::SMALLINT => HdbValue::SMALLINT(i16::from_str(value).map_err(map_i)?),
            BaseTypeId::INT => HdbValue::INT(i32::from_str(value).map_err(map_i)?),
            BaseTypeId::BIGINT => HdbValue::BIGINT(i64::from_str(value).map_err(map_i)?),
            BaseTypeId::REAL => HdbValue::REAL(f32::from_str(value).map_err(map_f)?),
            BaseTypeId::DOUBLE => HdbValue::DOUBLE(f64::from_str(value).map_err(map_f)?),
            BaseTypeId::CHAR
            | BaseTypeId::VARCHAR
            | BaseTypeId::NCHAR
            | BaseTypeId::NVARCHAR
            | BaseTypeId::STRING
            | BaseTypeId::NSTRING
            | BaseTypeId::TEXT
            | BaseTypeId::SHORTTEXT
            | BaseTypeId::CLOB
            | BaseTypeId::NCLOB => HdbValue::STRING(String::from(value)),
            BaseTypeId::DECIMAL => HdbValue::DECIMAL(BigDecimal::from_str(value).map_err(map_bd)?),
            BaseTypeId::LONGDATE => {
                HdbValue::LONGDATE(LongDate::from_date_string(value).map_err(map_d)?)
            }
            BaseTypeId::SECONDDATE => {
                HdbValue::SECONDDATE(SecondDate::from_date_string(value).map_err(map_d)?)
            }
            BaseTypeId::DAYDATE => {
                HdbValue::DAYDATE(DayDate::from_date_string(value).map_err(map_d)?)
            }
            BaseTypeId::SECONDTIME => {
                HdbValue::SECONDTIME(SecondTime::from_date_string(value).map_err(map_d)?)
            }

            _ => {
                return Err(SerializationError::Type {
                    value_type: "&str",
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_bytes(&self, value: &[u8]) -> Result<HdbValue, SerializationError> {
        Ok(match self.type_id().as_tuple() {
            (BaseTypeId::BLOB, false) => HdbValue::BLOB(new_blob_to_db((*value).to_vec())),
            (BaseTypeId::BLOB, true) => HdbValue::N_BLOB(Some(new_blob_to_db((*value).to_vec()))),
            _ => {
                return Err(SerializationError::Type {
                    value_type: "bytes",
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_none(&self) -> Result<HdbValue, SerializationError> {
        if !self.is_nullable() {
            Err(SerializationError::Type {
                value_type: "none",
                db_type: self.descriptor(),
            })
        } else {
            Ok(match self.type_id().base_type_id() {
                BaseTypeId::NOTHING => panic!("Must not happen"),
                BaseTypeId::TINYINT => HdbValue::N_TINYINT(None),
                BaseTypeId::SMALLINT => HdbValue::N_SMALLINT(None),
                BaseTypeId::INT => HdbValue::N_INT(None),
                BaseTypeId::BIGINT => HdbValue::N_BIGINT(None),
                BaseTypeId::DECIMAL => HdbValue::N_DECIMAL(None),
                BaseTypeId::REAL => HdbValue::N_REAL(None),
                BaseTypeId::DOUBLE => HdbValue::N_DOUBLE(None),
                BaseTypeId::CHAR => HdbValue::N_CHAR(None),
                BaseTypeId::VARCHAR => HdbValue::N_VARCHAR(None),
                BaseTypeId::NCHAR => HdbValue::N_NCHAR(None),
                BaseTypeId::NVARCHAR => HdbValue::N_NVARCHAR(None),
                BaseTypeId::BINARY => HdbValue::N_BINARY(None),
                BaseTypeId::VARBINARY => HdbValue::N_VARBINARY(None),
                BaseTypeId::CLOB => HdbValue::N_CLOB(None),
                BaseTypeId::NCLOB => HdbValue::N_NCLOB(None),
                BaseTypeId::BLOB => HdbValue::N_BLOB(None),
                BaseTypeId::BOOLEAN => HdbValue::N_BOOLEAN(None),
                BaseTypeId::STRING => HdbValue::N_STRING(None),
                BaseTypeId::NSTRING => HdbValue::N_NSTRING(None),
                BaseTypeId::BSTRING => HdbValue::N_BSTRING(None),
                BaseTypeId::SMALLDECIMAL => HdbValue::N_SMALLDECIMAL(None),
                BaseTypeId::TEXT => HdbValue::N_TEXT(None),
                BaseTypeId::SHORTTEXT => HdbValue::N_SHORTTEXT(None),
                BaseTypeId::LONGDATE => HdbValue::N_LONGDATE(None),
                BaseTypeId::SECONDDATE => HdbValue::N_SECONDDATE(None),
                BaseTypeId::DAYDATE => HdbValue::N_DAYDATE(None),
                BaseTypeId::SECONDTIME => HdbValue::N_SECONDTIME(None),
            })
        }
    }

    fn descriptor(&self) -> String {
        self.type_id().to_string()
    }
}

fn decimal_range(ovt: &'static str) -> SerializationError {
    SerializationError::Range(ovt, "Decimal".to_string())
}
