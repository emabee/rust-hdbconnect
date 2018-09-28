use bigdecimal::ParseBigDecimalError;
use hdb_error::HdbError;
use std::num::ParseFloatError;
use std::num::ParseIntError;
use types_impl::lob::new_blob_to_db;

use protocol::parts::hdb_value::HdbValue;
use protocol::parts::parameter_descriptor::ParameterDescriptor;
use protocol::parts::type_id;
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
        Ok(match self.type_id() {
            type_id::BOOLEAN => HdbValue::BOOLEAN(value),
            type_id::N_BOOLEAN => HdbValue::N_BOOLEAN(Some(value)),
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
        Ok(match self.type_id() {
            type_id::TINYINT => if value >= 0 {
                HdbValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if value >= 0 {
                HdbValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::SMALLINT => HdbValue::SMALLINT(i16::from(value)),
            type_id::N_SMALLINT => HdbValue::N_SMALLINT(Some(i16::from(value))),
            type_id::INT => HdbValue::INT(i32::from(value)),
            type_id::N_INT => HdbValue::N_INT(Some(i32::from(value))),
            type_id::BIGINT => HdbValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => HdbValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_i8(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::TINYINT => if (value >= 0) && (value <= i16::from(u8::MAX)) {
                HdbValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if (value >= 0) && (value <= i16::from(u8::MAX)) {
                HdbValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::SMALLINT => HdbValue::SMALLINT(value),
            type_id::N_SMALLINT => HdbValue::N_SMALLINT(Some(value)),
            type_id::INT => HdbValue::INT(i32::from(value)),
            type_id::N_INT => HdbValue::N_INT(Some(i32::from(value))),
            type_id::BIGINT => HdbValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => HdbValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_i16(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::TINYINT => if (value >= 0) && (value <= i32::from(u8::MAX)) {
                HdbValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if (value >= 0) && (value <= i32::from(u8::MAX)) {
                HdbValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::SMALLINT => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            type_id::N_SMALLINT => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    HdbValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            type_id::INT => HdbValue::INT(value),
            type_id::N_INT => HdbValue::N_INT(Some(value)),
            type_id::BIGINT => HdbValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => HdbValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_i32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
                BigDecimal::from_i32(value).ok_or_else(|| decimal_range(input_type))?,
            )),
            type_id::DAYDATE => HdbValue::DAYDATE(DayDate::new(value)),
            type_id::N_DAYDATE => HdbValue::N_DAYDATE(Some(DayDate::new(value))),
            type_id::SECONDTIME => HdbValue::SECONDTIME(SecondTime::new(value)),
            type_id::N_SECONDTIME => HdbValue::N_SECONDTIME(Some(SecondTime::new(value))),
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
        Ok(match self.type_id() {
            type_id::TINYINT => if (value >= 0) && (value <= i64::from(u8::MAX)) {
                HdbValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if (value >= 0) && (value <= i64::from(u8::MAX)) {
                HdbValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::SMALLINT => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            type_id::N_SMALLINT => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    HdbValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            type_id::INT => if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                HdbValue::INT(value as i32)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_INT => if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                HdbValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::BIGINT => HdbValue::BIGINT(value),
            type_id::N_BIGINT => HdbValue::N_BIGINT(Some(value)),
            type_id::LONGDATE => HdbValue::LONGDATE(LongDate::new(value)),
            type_id::N_LONGDATE => HdbValue::N_LONGDATE(Some(LongDate::new(value))),
            type_id::SECONDDATE => HdbValue::SECONDDATE(SecondDate::new(value)),
            type_id::N_SECONDDATE => HdbValue::N_SECONDDATE(Some(SecondDate::new(value))),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_i64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::TINYINT => HdbValue::TINYINT(value),
            type_id::N_TINYINT => HdbValue::N_TINYINT(Some(value)),
            type_id::SMALLINT => HdbValue::SMALLINT(i16::from(value)),
            type_id::N_SMALLINT => HdbValue::N_SMALLINT(Some(i16::from(value))),
            type_id::INT => HdbValue::INT(i32::from(value)),
            type_id::N_INT => HdbValue::N_INT(Some(i32::from(value))),
            type_id::BIGINT => HdbValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => HdbValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_u8(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::TINYINT => if value <= u16::from(u8::MAX) {
                HdbValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if value <= u16::from(u8::MAX) {
                HdbValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::SMALLINT => if value <= i16::MAX as u16 {
                HdbValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_SMALLINT => if value <= i16::MAX as u16 {
                HdbValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::INT => HdbValue::INT(i32::from(value)),
            type_id::N_INT => HdbValue::N_INT(Some(i32::from(value))),
            type_id::BIGINT => HdbValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => HdbValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_u16(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::TINYINT => if value <= u32::from(u8::MAX) {
                HdbValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if value <= u32::from(u8::MAX) {
                HdbValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::SMALLINT => if value <= i16::MAX as u32 {
                HdbValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_SMALLINT => if value <= i16::MAX as u32 {
                HdbValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::INT => if value <= i32::MAX as u32 {
                HdbValue::INT(value as i32)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_INT => if value <= i32::MAX as u32 {
                HdbValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::BIGINT => HdbValue::BIGINT(i64::from(value)),
            type_id::N_BIGINT => HdbValue::N_BIGINT(Some(i64::from(value))),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_u32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::TINYINT => if value <= u64::from(u8::MAX) {
                HdbValue::TINYINT(value as u8)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_TINYINT => if value <= u64::from(u8::MAX) {
                HdbValue::N_TINYINT(Some(value as u8))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::SMALLINT => if value <= i16::MAX as u64 {
                HdbValue::SMALLINT(value as i16)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_SMALLINT => if value <= i16::MAX as u64 {
                HdbValue::N_SMALLINT(Some(value as i16))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::INT => if value <= i32::MAX as u64 {
                HdbValue::INT(value as i32)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_INT => if value <= i32::MAX as u64 {
                HdbValue::N_INT(Some(value as i32))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::BIGINT => if value <= i64::MAX as u64 {
                HdbValue::BIGINT(value as i64)
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::N_BIGINT => if value <= i64::MAX as u64 {
                HdbValue::N_BIGINT(Some(value as i64))
            } else {
                return Err(SerializationError::Range(input_type, self.descriptor()));
            },
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_u64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::REAL => HdbValue::REAL(value),
            type_id::N_REAL => HdbValue::N_REAL(Some(value)),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_f32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::DOUBLE => HdbValue::DOUBLE(value),
            type_id::N_DOUBLE => HdbValue::N_DOUBLE(Some(value)),
            type_id::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_f64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            type_id::N_DECIMAL => HdbValue::N_DECIMAL(Some(
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
        Ok(match self.type_id() {
            type_id::CHAR
            | type_id::VARCHAR
            | type_id::NCHAR
            | type_id::NVARCHAR
            | type_id::STRING
            | type_id::NSTRING
            | type_id::TEXT
            | type_id::SHORTTEXT => HdbValue::STRING(s),
            _ => {
                return Err(SerializationError::Type {
                    value_type: "char",
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_str(&self, value: &str) -> Result<HdbValue, SerializationError> {
        let map_i = |e: ParseIntError| parse_error(value, "some integer type", Some(Box::new(e)));
        let map_bd = |e: ParseBigDecimalError| parse_error(value, "BigDecimal", Some(Box::new(e)));
        let map_f = |e: ParseFloatError| parse_error(value, "some float type", Some(Box::new(e)));
        let map_d = |e: HdbError| parse_error(value, "some date type", Some(Box::new(e)));
        Ok(match self.type_id() {
            type_id::TINYINT => HdbValue::TINYINT(u8::from_str(value).map_err(map_i)?),
            type_id::SMALLINT => HdbValue::SMALLINT(i16::from_str(value).map_err(map_i)?),
            type_id::INT => HdbValue::INT(i32::from_str(value).map_err(map_i)?),
            type_id::BIGINT => HdbValue::BIGINT(i64::from_str(value).map_err(map_i)?),
            type_id::REAL => HdbValue::REAL(f32::from_str(value).map_err(map_f)?),
            type_id::DOUBLE => HdbValue::DOUBLE(f64::from_str(value).map_err(map_f)?),
            type_id::CHAR
            | type_id::VARCHAR
            | type_id::NCHAR
            | type_id::NVARCHAR
            | type_id::STRING
            | type_id::NSTRING
            | type_id::TEXT
            | type_id::SHORTTEXT
            | type_id::N_CLOB
            | type_id::N_NCLOB
            | type_id::NCLOB
            | type_id::CLOB => HdbValue::STRING(String::from(value)),
            type_id::DECIMAL => HdbValue::DECIMAL(BigDecimal::from_str(value).map_err(map_bd)?),
            type_id::LONGDATE => {
                HdbValue::LONGDATE(LongDate::from_date_string(value).map_err(map_d)?)
            }
            type_id::SECONDDATE => {
                HdbValue::SECONDDATE(SecondDate::from_date_string(value).map_err(map_d)?)
            }
            type_id::DAYDATE => HdbValue::DAYDATE(DayDate::from_date_string(value).map_err(map_d)?),
            type_id::SECONDTIME => {
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
        Ok(match self.type_id() {
            type_id::BLOB => HdbValue::BLOB(new_blob_to_db((*value).to_vec())),
            type_id::N_BLOB => HdbValue::N_BLOB(Some(new_blob_to_db((*value).to_vec()))),
            _ => {
                return Err(SerializationError::Type {
                    value_type: "bytes",
                    db_type: self.descriptor(),
                })
            }
        })
    }
    fn from_none(&self) -> Result<HdbValue, SerializationError> {
        Ok(match self.type_id() {
            type_id::N_TINYINT => HdbValue::N_TINYINT(None),
            type_id::N_SMALLINT => HdbValue::N_SMALLINT(None),
            type_id::N_INT => HdbValue::N_INT(None),
            type_id::N_BIGINT => HdbValue::N_BIGINT(None),
            type_id::N_REAL => HdbValue::N_REAL(None),
            type_id::N_DOUBLE => HdbValue::N_DOUBLE(None),
            type_id::N_CHAR => HdbValue::N_CHAR(None),
            type_id::N_VARCHAR => HdbValue::N_VARCHAR(None),
            type_id::N_NCHAR => HdbValue::N_NCHAR(None),
            type_id::N_NVARCHAR => HdbValue::N_NVARCHAR(None),
            type_id::N_BINARY => HdbValue::N_BINARY(None),
            type_id::N_VARBINARY => HdbValue::N_VARBINARY(None),
            type_id::N_CLOB => HdbValue::N_CLOB(None),
            type_id::N_NCLOB => HdbValue::N_NCLOB(None),
            type_id::N_BLOB => HdbValue::N_BLOB(None),
            type_id::N_BOOLEAN => HdbValue::N_BOOLEAN(None),
            type_id::N_STRING => HdbValue::N_STRING(None),
            type_id::N_NSTRING => HdbValue::N_NSTRING(None),
            type_id::N_BSTRING => HdbValue::N_BSTRING(None),
            type_id::N_TEXT => HdbValue::N_TEXT(None),
            type_id::N_SHORTTEXT => HdbValue::N_SHORTTEXT(None),
            type_id::N_LONGDATE => HdbValue::N_LONGDATE(None),
            type_id::N_SECONDDATE => HdbValue::N_SECONDDATE(None),
            type_id::N_DAYDATE => HdbValue::N_DAYDATE(None),
            type_id::N_SECONDTIME => HdbValue::N_SECONDTIME(None),
            _ => {
                return Err(SerializationError::Type {
                    value_type: "none",
                    db_type: self.descriptor(),
                })
            }
        })
    }

    fn descriptor(&self) -> &'static str {
        match self.type_id() {
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
            type_id::N_SECONDDATE => "Nullable SECONDDATE",
            type_id::SECONDDATE => "SECONDDATE",
            type_id::N_DAYDATE => "Nullable DAYDATE",
            type_id::DAYDATE => "DAYDATE",
            type_id::N_SECONDTIME => "Nullable SECONDTIME",
            type_id::SECONDTIME => "SECONDTIME",
            _ => "[no descriptor available]",
        }
    }
}

fn decimal_range(ovt: &'static str) -> SerializationError {
    SerializationError::Range(ovt, "Decimal")
}
