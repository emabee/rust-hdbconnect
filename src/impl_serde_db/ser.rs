use crate::hdb_error::HdbError;
use crate::types_impl::lob::new_blob_to_db;
use bigdecimal::ParseBigDecimalError;
use std::num::ParseFloatError;
use std::num::ParseIntError;

use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptor;
use crate::protocol::parts::type_id::TypeId;
use crate::types::DayDate;
use crate::types::LongDate;
use crate::types::SecondDate;
use crate::types::SecondTime;

use bigdecimal::BigDecimal;
use bigdecimal::FromPrimitive;
use serde_db::ser::{parse_error, DbvFactory, SerializationError};
use std::str::FromStr;
use std::{i16, i32, i64, i8, u16, u32, u8};

impl DbvFactory for ParameterDescriptor {
    type DBV = HdbValue;

    fn from_bool(&self, value: bool) -> Result<HdbValue, SerializationError> {
        Ok(match self.type_id() {
            TypeId::BOOLEAN => HdbValue::BOOLEAN(value),
            TypeId::TINYINT => HdbValue::BOOLEAN(value),
            _ => return Err(type_mismatch("boolean", self.descriptor())),
        })
    }

    fn from_i8(&self, value: i8) -> Result<HdbValue, SerializationError> {
        let input_type = "i8";
        Ok(match self.type_id() {
            TypeId::TINYINT => {
                if value >= 0 {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::SMALLINT => HdbValue::SMALLINT(i16::from(value)),
            TypeId::INT => HdbValue::INT(i32::from(value)),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_i8(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn from_i16(&self, value: i16) -> Result<HdbValue, SerializationError> {
        let input_type = "i16";
        Ok(match self.type_id() {
            TypeId::TINYINT => {
                if (value >= 0) && (value <= i16::from(u8::MAX)) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::SMALLINT => HdbValue::SMALLINT(value),
            TypeId::INT => HdbValue::INT(i32::from(value)),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_i16(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn from_i32(&self, value: i32) -> Result<HdbValue, SerializationError> {
        let input_type = "i32";
        Ok(match self.type_id() {
            TypeId::TINYINT => {
                if (value >= 0) && (value <= i32::from(u8::MAX)) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::SMALLINT => {
                if (value >= i32::from(i16::MIN)) && (value <= i32::from(i16::MAX)) {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::INT => HdbValue::INT(value),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_i32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            TypeId::DAYDATE => HdbValue::DAYDATE(DayDate::new(value)),
            TypeId::SECONDTIME => HdbValue::SECONDTIME(SecondTime::new(value)),
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn from_i64(&self, value: i64) -> Result<HdbValue, SerializationError> {
        let input_type = "i64";
        Ok(match self.type_id() {
            TypeId::TINYINT => {
                if (value >= 0) && (value <= i64::from(u8::MAX)) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::SMALLINT => {
                if (value >= i64::from(i16::MIN)) && (value <= i64::from(i16::MAX)) {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::INT => {
                if (value >= i64::from(i32::MIN)) && (value <= i64::from(i32::MAX)) {
                    HdbValue::INT(value as i32)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::BIGINT => HdbValue::BIGINT(value),
            TypeId::LONGDATE => HdbValue::LONGDATE(LongDate::new(value)),
            TypeId::SECONDDATE => HdbValue::SECONDDATE(SecondDate::new(value)),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_i64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }
    fn from_u8(&self, value: u8) -> Result<HdbValue, SerializationError> {
        let input_type = "u8";
        Ok(match self.type_id() {
            TypeId::TINYINT => HdbValue::TINYINT(value),
            TypeId::SMALLINT => HdbValue::SMALLINT(i16::from(value)),
            TypeId::INT => HdbValue::INT(i32::from(value)),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_u8(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }
    fn from_u16(&self, value: u16) -> Result<HdbValue, SerializationError> {
        let input_type = "u16";
        Ok(match self.type_id() {
            TypeId::TINYINT => {
                if value <= u16::from(u8::MAX) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::SMALLINT => {
                if value <= i16::MAX as u16 {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::INT => HdbValue::INT(i32::from(value)),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_u16(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn from_u32(&self, value: u32) -> Result<HdbValue, SerializationError> {
        let input_type = "u32";
        Ok(match self.type_id() {
            TypeId::TINYINT => {
                if value <= u32::from(u8::MAX) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::SMALLINT => {
                if value <= i16::MAX as u32 {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::INT => {
                if value <= i32::MAX as u32 {
                    HdbValue::INT(value as i32)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_u32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn from_u64(&self, value: u64) -> Result<HdbValue, SerializationError> {
        let input_type = "u64";
        Ok(match self.type_id() {
            TypeId::TINYINT => {
                if value <= u64::from(u8::MAX) {
                    HdbValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::SMALLINT => {
                if value <= i16::MAX as u64 {
                    HdbValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::INT => {
                if value <= i32::MAX as u64 {
                    HdbValue::INT(value as i32)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::BIGINT => {
                if value <= i64::MAX as u64 {
                    HdbValue::BIGINT(value as i64)
                } else {
                    return Err(SerializationError::Range(input_type, self.descriptor()));
                }
            }
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_u64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn from_f32(&self, value: f32) -> Result<HdbValue, SerializationError> {
        let input_type = "f32";
        Ok(match self.type_id() {
            TypeId::REAL => HdbValue::REAL(value),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_f32(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch("f32", self.descriptor())),
        })
    }

    fn from_f64(&self, value: f64) -> Result<HdbValue, SerializationError> {
        let input_type = "f64";
        Ok(match self.type_id() {
            TypeId::DOUBLE => HdbValue::DOUBLE(value),
            TypeId::DECIMAL => HdbValue::DECIMAL(
                BigDecimal::from_f64(value).ok_or_else(|| decimal_range(input_type))?,
            ),
            _ => return Err(type_mismatch("f64", self.descriptor())),
        })
    }

    fn from_char(&self, value: char) -> Result<HdbValue, SerializationError> {
        let mut s = String::new();
        s.push(value);
        Ok(match self.type_id() {
            TypeId::CHAR
            | TypeId::VARCHAR
            | TypeId::NCHAR
            | TypeId::NVARCHAR
            | TypeId::STRING
            | TypeId::NSTRING
            | TypeId::TEXT
            | TypeId::SHORTTEXT => HdbValue::STRING(s),
            _ => return Err(type_mismatch("char", self.descriptor())),
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
        Ok(match self.type_id() {
            TypeId::TINYINT => HdbValue::TINYINT(u8::from_str(value).map_err(map_i)?),
            TypeId::SMALLINT => HdbValue::SMALLINT(i16::from_str(value).map_err(map_i)?),
            TypeId::INT => HdbValue::INT(i32::from_str(value).map_err(map_i)?),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from_str(value).map_err(map_i)?),
            TypeId::REAL => HdbValue::REAL(f32::from_str(value).map_err(map_f)?),
            TypeId::DOUBLE => HdbValue::DOUBLE(f64::from_str(value).map_err(map_f)?),
            TypeId::CHAR
            | TypeId::VARCHAR
            | TypeId::NCHAR
            | TypeId::NVARCHAR
            | TypeId::STRING
            | TypeId::NSTRING
            | TypeId::TEXT
            | TypeId::SHORTTEXT
            | TypeId::CLOB
            | TypeId::NCLOB => HdbValue::STRING(String::from(value)),
            TypeId::DECIMAL => HdbValue::DECIMAL(BigDecimal::from_str(value).map_err(map_bd)?),
            TypeId::LONGDATE => {
                HdbValue::LONGDATE(LongDate::from_date_string(value).map_err(map_d)?)
            }
            TypeId::SECONDDATE => {
                HdbValue::SECONDDATE(SecondDate::from_date_string(value).map_err(map_d)?)
            }
            TypeId::DAYDATE => HdbValue::DAYDATE(DayDate::from_date_string(value).map_err(map_d)?),
            TypeId::SECONDTIME => {
                HdbValue::SECONDTIME(SecondTime::from_date_string(value).map_err(map_d)?)
            }
            _ => return Err(type_mismatch("&str", self.descriptor())),
        })
    }

    fn from_bytes(&self, value: &[u8]) -> Result<HdbValue, SerializationError> {
        Ok(match self.type_id() {
            TypeId::BINARY | TypeId::VARBINARY | TypeId::GEOMETRY | TypeId::POINT => {
                HdbValue::BINARY((*value).to_vec())
            }
            TypeId::BLOB => HdbValue::BLOB(new_blob_to_db((*value).to_vec())),
            TypeId::NCLOB => HdbValue::STRING(
                String::from_utf8(value.to_vec())
                    .map_err(|e| parse_error("bytes", "NCLOB".to_string(), Some(Box::new(e))))?,
            ),
            _ => return Err(type_mismatch("bytes", self.descriptor())),
        })
    }

    fn from_none(&self) -> Result<HdbValue, SerializationError> {
        if !self.is_nullable() {
            Err(type_mismatch("none", self.descriptor()))
        } else {
            // work around a bug in HANA: it doesn't accept NULL SECONDDATE values
            let btid = match self.type_id() {
                TypeId::SECONDTIME => TypeId::SECONDDATE,
                btid => btid,
            };
            Ok(HdbValue::NULL(btid))
        }
    }

    fn descriptor(&self) -> String {
        self.type_id().to_string()
    }
}

fn decimal_range(ovt: &'static str) -> SerializationError {
    SerializationError::Range(ovt, "Decimal".to_string())
}

fn type_mismatch(value_type: &'static str, db_type: String) -> SerializationError {
    SerializationError::Type {
        value_type,
        db_type,
    }
}
