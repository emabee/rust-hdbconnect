use crate::types::{DayDate, LongDate, SecondDate, SecondTime};
use crate::{HdbValue, ParameterDescriptor, TypeId};
use bigdecimal::{BigDecimal, FromPrimitive, ParseBigDecimalError};
use serde_db::ser::{DbvFactory, SerializationError, parse_error};
use std::num::ParseFloatError;
use std::num::ParseIntError;
use std::str::FromStr;

impl DbvFactory for &ParameterDescriptor {
    type DBV = HdbValue<'static>;

    fn serialize_bool(&self, value: bool) -> Result<HdbValue<'static>, SerializationError> {
        Ok(match self.type_id() {
            TypeId::BOOLEAN | TypeId::TINYINT | TypeId::SMALLINT | TypeId::INT | TypeId::BIGINT => {
                HdbValue::BOOLEAN(value)
            }
            _ => return Err(type_mismatch("boolean", self.descriptor())),
        })
    }

    fn serialize_i8(&self, value: i8) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "i8";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::TINYINT => HdbValue::TINYINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::SMALLINT => HdbValue::SMALLINT(i16::from(value)),
            TypeId::INT => HdbValue::INT(i32::from(value)),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_i8(value).ok_or_else(|| decimal_range(input_type))?,
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn serialize_i16(&self, value: i16) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "i16";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::TINYINT => HdbValue::TINYINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::SMALLINT => HdbValue::SMALLINT(value),
            TypeId::INT => HdbValue::INT(i32::from(value)),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_i16(value).ok_or_else(|| decimal_range(input_type))?,
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn serialize_i32(&self, value: i32) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "i32";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::TINYINT => HdbValue::TINYINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::SMALLINT => HdbValue::SMALLINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::INT => HdbValue::INT(value),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),
            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_i32(value).ok_or_else(|| decimal_range(input_type))?,
                )
            }
            TypeId::DAYDATE => HdbValue::DAYDATE(DayDate::new(value)),
            TypeId::SECONDTIME => HdbValue::SECONDTIME(SecondTime::new(value)),
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn serialize_i64(&self, value: i64) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "i64";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::TINYINT => HdbValue::TINYINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::SMALLINT => HdbValue::SMALLINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::INT => HdbValue::INT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::BIGINT => HdbValue::BIGINT(value),
            TypeId::LONGDATE => HdbValue::LONGDATE(LongDate::new(value)),
            TypeId::SECONDDATE => HdbValue::SECONDDATE(SecondDate::new(value)),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_i64(value).ok_or_else(|| decimal_range(input_type))?,
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }
    fn serialize_u8(&self, value: u8) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "u8";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::TINYINT => HdbValue::TINYINT(value),
            TypeId::SMALLINT => HdbValue::SMALLINT(i16::from(value)),
            TypeId::INT => HdbValue::INT(i32::from(value)),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_u8(value).ok_or_else(|| decimal_range(input_type))?,
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }
    fn serialize_u16(&self, value: u16) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "u16";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::TINYINT => HdbValue::TINYINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::SMALLINT => HdbValue::SMALLINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::INT => HdbValue::INT(i32::from(value)),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_u16(value).ok_or_else(|| decimal_range(input_type))?,
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn serialize_u32(&self, value: u32) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "u32";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::TINYINT => HdbValue::TINYINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::SMALLINT => HdbValue::SMALLINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::INT => HdbValue::INT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::BIGINT => HdbValue::BIGINT(i64::from(value)),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_u32(value).ok_or_else(|| decimal_range(input_type))?,
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn serialize_u64(&self, value: u64) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "u64";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::TINYINT => HdbValue::TINYINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::SMALLINT => HdbValue::SMALLINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::INT => HdbValue::INT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::BIGINT => HdbValue::BIGINT(
                num::cast(value)
                    .ok_or_else(|| SerializationError::Range(input_type, self.descriptor()))?,
            ),
            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_u64(value).ok_or_else(|| decimal_range(input_type))?,
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch(input_type, self.descriptor())),
        })
    }

    fn serialize_f32(&self, value: f32) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "f32";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::REAL => HdbValue::REAL(value),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_f32(value)
                        .ok_or_else(|| decimal_range(input_type))?
                        .with_scale(i64::from(f32::DIGITS)),
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch("f32", self.descriptor())),
        })
    }

    fn serialize_f64(&self, value: f64) -> Result<HdbValue<'static>, SerializationError> {
        let input_type = "f64";
        let tid = self.type_id();
        Ok(match tid {
            TypeId::DOUBLE => HdbValue::DOUBLE(value),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(
                    BigDecimal::from_f64(value)
                        .ok_or_else(|| decimal_range(input_type))?
                        .with_scale(i64::from(f64::DIGITS)),
                )
            }
            TypeId::VARCHAR | TypeId::NVARCHAR | TypeId::TEXT | TypeId::SHORTTEXT => {
                HdbValue::STRING(format!("{value}"))
            }
            _ => return Err(type_mismatch("f64", self.descriptor())),
        })
    }

    fn serialize_char(&self, value: char) -> Result<HdbValue<'static>, SerializationError> {
        Ok(match self.type_id() {
            TypeId::CHAR
            | TypeId::VARCHAR
            | TypeId::NCHAR
            | TypeId::NVARCHAR
            | TypeId::STRING
            | TypeId::NSTRING
            | TypeId::TEXT
            | TypeId::SHORTTEXT => HdbValue::STRING(value.to_string()),
            _ => return Err(type_mismatch("char", self.descriptor())),
        })
    }

    fn serialize_str(&self, value: &str) -> Result<HdbValue<'static>, SerializationError> {
        let map_i = |e: ParseIntError| {
            parse_error(value, "some integer type".to_string(), Some(Box::new(e)))
        };
        let map_bd = |e: ParseBigDecimalError| {
            parse_error(value, "BigDecimal".to_string(), Some(Box::new(e)))
        };
        let map_f = |e: ParseFloatError| {
            parse_error(value, "some float type".to_string(), Some(Box::new(e)))
        };

        let tid = self.type_id();
        Ok(match tid {
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
            | TypeId::ALPHANUM
            | TypeId::TEXT
            | TypeId::SHORTTEXT
            | TypeId::CLOB
            | TypeId::NCLOB
            | TypeId::LONGDATE
            | TypeId::SECONDDATE
            | TypeId::DAYDATE
            | TypeId::SECONDTIME => HdbValue::STRING(String::from(value)),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                HdbValue::DECIMAL(BigDecimal::from_str(value).map_err(map_bd)?)
            }

            _ => return Err(type_mismatch("&str", self.descriptor())),
        })
    }

    fn serialize_bytes(&self, value: &[u8]) -> Result<HdbValue<'static>, SerializationError> {
        let tid = self.type_id();
        Ok(match tid {
            TypeId::BLOB | TypeId::BLOCATOR | TypeId::BINARY | TypeId::VARBINARY => {
                HdbValue::BINARY((*value).to_vec())
            }
            TypeId::GEOMETRY => HdbValue::GEOMETRY((*value).to_vec()),
            TypeId::POINT => HdbValue::POINT((*value).to_vec()),
            TypeId::NCLOB => HdbValue::STRING(
                String::from_utf8(value.to_vec())
                    .map_err(|e| parse_error("bytes", "NCLOB".to_string(), Some(Box::new(e))))?,
            ),
            _ => return Err(type_mismatch("bytes", self.descriptor())),
        })
    }

    fn serialize_none(&self) -> Result<HdbValue<'static>, SerializationError> {
        if self.is_nullable() {
            Ok(HdbValue::NULL)
        } else {
            Err(type_mismatch("none", self.descriptor()))
        }
    }

    fn descriptor(&self) -> String {
        format!("{:?}", self.type_id())
    }
}

fn decimal_range(ovt: &'static str) -> SerializationError {
    SerializationError::Range(ovt, "some Decimal".to_string())
}

fn type_mismatch(value_type: &'static str, db_type: String) -> SerializationError {
    SerializationError::Type {
        value_type,
        db_type,
    }
}
