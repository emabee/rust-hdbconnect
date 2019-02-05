use crate::HdbError;
use bigdecimal::ToPrimitive;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde_db::de::{
    ConversionError, DbValue, DbValueInto, DeserializableResultset, DeserializableRow,
    DeserializationError, DeserializationResult,
};
use std::error::Error;
use std::{fmt, i16, i32, i64, i8, u16, u32, u8};

use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::resultset::ResultSet;
use crate::protocol::parts::row::Row;

impl DeserializableResultset for ResultSet {
    type ROW = Row;
    type E = HdbError;

    fn has_multiple_rows(&mut self) -> Result<bool, DeserializationError> {
        Ok(ResultSet::has_multiple_rows(self))
    }

    fn next(&mut self) -> DeserializationResult<Option<Row>> {
        Ok(ResultSet::next_row(self)?)
    }

    fn number_of_fields(&self) -> usize {
        ResultSet::metadata(self).number_of_fields()
    }

    fn fieldname(&self, i: usize) -> Option<&String> {
        ResultSet::metadata(self).displayname(i).ok()
    }
}

impl DeserializableRow for Row {
    type V = HdbValue;
    type E = HdbError;

    fn len(&self) -> usize {
        Row::len(self)
    }

    fn next(&mut self) -> Option<HdbValue> {
        Row::next_value(self)
    }

    fn number_of_fields(&self) -> usize {
        self.number_of_fields()
    }

    fn fieldname(&self, field_idx: usize) -> Option<&String> {
        Row::metadata(self).displayname(field_idx).ok()
    }
}

impl DbValue for HdbValue {
    fn is_null(&self) -> bool {
        match *self {
            HdbValue::NULL(_) => true,
            _ => false,
        }
    }
}

impl DbValueInto<bool> for HdbValue {
    fn try_into(self) -> Result<bool, ConversionError> {
        match self {
            HdbValue::BOOLEAN(b) => Ok(b),
            HdbValue::TINYINT(1) => Ok(true),
            HdbValue::TINYINT(0) => Ok(false),
            value => Err(wrong_type(&value, "bool")),
        }
    }
}

impl DbValueInto<u8> for HdbValue {
    fn try_into(self) -> Result<u8, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(u),
            HdbValue::SMALLINT(i) => {
                if (i >= 0) && (i <= i16::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(i64::from(i), "u8"))
                }
            }
            HdbValue::INT(i) => {
                if (i >= 0) && (i <= i32::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(i64::from(i), "u8"))
                }
            }
            HdbValue::BIGINT(i) => {
                if (i >= 0) && (i <= i64::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(i, "u8"))
                }
            }
            HdbValue::DECIMAL(bigdec) => bigdec.to_u8().ok_or_else(|| decimal_range("u8")),
            value => Err(wrong_type(&value, "u8")),
        }
    }
}

impl DbValueInto<u16> for HdbValue {
    fn try_into(self) -> Result<u16, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(u16::from(u)),

            HdbValue::SMALLINT(i) => {
                if i >= 0 {
                    Ok(i as u16)
                } else {
                    Err(number_range(i64::from(i), "u16"))
                }
            }

            HdbValue::INT(i) => {
                if (i >= 0) && (i <= i32::from(u16::MAX)) {
                    Ok(i as u16)
                } else {
                    Err(number_range(i64::from(i), "u16"))
                }
            }

            HdbValue::BIGINT(i) => {
                if (i >= 0) && (i <= i64::from(u16::MAX)) {
                    Ok(i as u16)
                } else {
                    Err(number_range(i, "u16"))
                }
            }
            HdbValue::DECIMAL(bigdec) => bigdec.to_u16().ok_or_else(|| decimal_range("u16")),
            value => Err(wrong_type(&value, "u16")),
        }
    }
}

impl DbValueInto<u32> for HdbValue {
    fn try_into(self) -> Result<u32, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(u32::from(u)),
            HdbValue::SMALLINT(i) => {
                if i >= 0 {
                    Ok(i as u32)
                } else {
                    Err(number_range(i64::from(i), "u32"))
                }
            }
            HdbValue::INT(i) => {
                if i >= 0 {
                    Ok(i as u32)
                } else {
                    Err(number_range(i64::from(i), "u32"))
                }
            }
            HdbValue::BIGINT(i) => {
                if (i >= 0) && (i <= i64::from(u32::MAX)) {
                    Ok(i as u32)
                } else {
                    Err(number_range(i, "u32"))
                }
            }
            HdbValue::DECIMAL(bigdec) => bigdec.to_u32().ok_or_else(|| decimal_range("u32")),
            value => Err(wrong_type(&value, "u32")),
        }
    }
}

impl DbValueInto<u64> for HdbValue {
    fn try_into(self) -> Result<u64, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(u64::from(u)),
            HdbValue::SMALLINT(i) => {
                if i >= 0 {
                    Ok(i as u64)
                } else {
                    Err(number_range(i64::from(i), "u64"))
                }
            }
            HdbValue::INT(i) => {
                if i >= 0 {
                    Ok(i as u64)
                } else {
                    Err(number_range(i64::from(i), "u64"))
                }
            }
            HdbValue::BIGINT(i) => {
                if i >= 0 {
                    Ok(i as u64)
                } else {
                    Err(number_range(i, "u64"))
                }
            }
            HdbValue::DECIMAL(bigdec) => bigdec.to_u64().ok_or_else(|| decimal_range("u64")),
            value => Err(wrong_type(&value, "u64")),
        }
    }
}

impl DbValueInto<i8> for HdbValue {
    fn try_into(self) -> Result<i8, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => {
                if u <= i8::MAX as u8 {
                    Ok(u as i8)
                } else {
                    Err(number_range(i64::from(u), "i8"))
                }
            }
            HdbValue::SMALLINT(i) => {
                if (i >= i16::from(i8::MIN)) && (i <= i16::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(i64::from(i), "i8"))
                }
            }
            HdbValue::INT(i) => {
                if (i >= i32::from(i8::MIN)) && (i <= i32::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(i64::from(i), "i8"))
                }
            }
            HdbValue::BIGINT(i) => {
                if (i >= i64::from(i8::MIN)) && (i <= i64::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(i, "i8"))
                }
            }
            HdbValue::DECIMAL(bigdec) => bigdec.to_i8().ok_or_else(|| decimal_range("i8")),
            value => Err(wrong_type(&value, "i8")),
        }
    }
}

impl DbValueInto<i16> for HdbValue {
    fn try_into(self) -> Result<i16, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(i16::from(u)),
            HdbValue::SMALLINT(i) => Ok(i),
            HdbValue::INT(i) => {
                if (i >= i32::from(i16::MIN)) && (i <= i32::from(i16::MAX)) {
                    Ok(i as i16)
                } else {
                    Err(number_range(i64::from(i), "i16"))
                }
            }
            HdbValue::BIGINT(i) => {
                if (i >= i64::from(i16::MIN)) && (i <= i64::from(i16::MAX)) {
                    Ok(i as i16)
                } else {
                    Err(number_range(i, "i16"))
                }
            }
            HdbValue::DECIMAL(bigdec) => bigdec.to_i16().ok_or_else(|| decimal_range("i16")),
            value => Err(wrong_type(&value, "i16")),
        }
    }
}

impl DbValueInto<i32> for HdbValue {
    fn try_into(self) -> Result<i32, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(i32::from(u)),
            HdbValue::SMALLINT(i) => Ok(i32::from(i)),
            HdbValue::INT(i) => Ok(i),
            HdbValue::BIGINT(i) => {
                if (i >= i64::from(i32::MIN)) && (i <= i64::from(i32::MAX)) {
                    Ok(i as i32)
                } else {
                    Err(number_range(i, "i32"))
                }
            }
            HdbValue::DECIMAL(bigdec) => bigdec.to_i32().ok_or_else(|| decimal_range("i32")),
            value => Err(wrong_type(&value, "i32")),
        }
    }
}

impl DbValueInto<i64> for HdbValue {
    fn try_into(self) -> Result<i64, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(i64::from(u)),
            HdbValue::SMALLINT(i) => Ok(i64::from(i)),
            HdbValue::INT(i) => Ok(i64::from(i)),
            HdbValue::BIGINT(i) => Ok(i),
            HdbValue::LONGDATE(ld) => Ok(*ld.ref_raw()),
            HdbValue::SECONDDATE(sd) => Ok(*sd.ref_raw()),
            HdbValue::DECIMAL(bigdec) => bigdec.to_i64().ok_or_else(|| decimal_range("i64")),
            value => Err(wrong_type(&value, "i64")),
        }
    }
}

impl DbValueInto<f32> for HdbValue {
    fn try_into(self) -> Result<f32, ConversionError> {
        match self {
            HdbValue::DECIMAL(bigdec) => bigdec.to_f32().ok_or_else(|| decimal_range("f32")),
            HdbValue::REAL(f) => Ok(f),
            value => Err(wrong_type(&value, "f32")),
        }
    }
}

impl DbValueInto<f64> for HdbValue {
    fn try_into(self) -> Result<f64, ConversionError> {
        match self {
            HdbValue::DECIMAL(bigdec) => bigdec.to_f64().ok_or_else(|| decimal_range("f64")),
            HdbValue::DOUBLE(f) => Ok(f),
            value => Err(wrong_type(&value, "f64")),
        }
    }
}

impl DbValueInto<String> for HdbValue {
    fn try_into(self) -> Result<String, ConversionError> {
        trace!("try_into -> String");
        match self {
            HdbValue::TINYINT(i) => Ok(format!("{}", i)),
            HdbValue::SMALLINT(i) => Ok(format!("{}", i)),
            HdbValue::INT(i) => Ok(format!("{}", i)),
            HdbValue::BIGINT(i) => Ok(format!("{}", i)),
            HdbValue::REAL(f) => Ok(format!("{}", f)),
            HdbValue::DOUBLE(f) => Ok(format!("{}", f)),
            HdbValue::TEXT(s) | HdbValue::STRING(s) => Ok(s),

            HdbValue::LONGDATE(ld) => Ok(str_from(&ld)),
            HdbValue::SECONDDATE(sd) => Ok(str_from(&sd)),
            HdbValue::DAYDATE(date) => Ok(str_from(&date)),
            HdbValue::SECONDTIME(time) => Ok(str_from(&time)),
            HdbValue::DECIMAL(bigdec) => Ok(format!("{}", bigdec)),
            HdbValue::CLOB(clob) => Ok(clob
                .into_string()
                .map_err(|e| ConversionError::Incomplete(e.description().to_owned()))?),
            HdbValue::NCLOB(nclob) => Ok(nclob
                .into_string()
                .map_err(|e| ConversionError::Incomplete(e.description().to_owned()))?),
            value => Err(wrong_type(&value, "String")),
        }
    }
}

impl DbValueInto<NaiveDateTime> for HdbValue {
    fn try_into(self) -> Result<NaiveDateTime, ConversionError> {
        trace!("try_into -> NaiveDateTime");
        match self {
            HdbValue::LONGDATE(ld) => {
                let (year, month, day, hour, min, sec, frac) = ld.as_ymd_hms_f();
                Ok(NaiveDateTime::new(
                    NaiveDate::from_ymd(year, month, day),
                    NaiveTime::from_hms_nano(hour, min, sec, frac * 100),
                ))
            }
            HdbValue::SECONDDATE(sd) => {
                let (year, month, day, hour, min, sec) = sd.as_ymd_hms();
                Ok(NaiveDateTime::new(
                    NaiveDate::from_ymd(year, month, day),
                    NaiveTime::from_hms(hour, min, sec),
                ))
            }
            _ => Err(ConversionError::ValueType(
                "Not a LongDate or SecondDate value".to_owned(),
            )),
        }
    }
}

impl DbValueInto<Vec<u8>> for HdbValue {
    fn try_into(self) -> Result<Vec<u8>, ConversionError> {
        match self {
            HdbValue::BLOB(blob) => Ok(blob
                .into_bytes()
                .map_err(|e| ConversionError::Incomplete(e.description().to_owned()))?),

            HdbValue::BINARY(v) | HdbValue::GEOMETRY(v) | HdbValue::POINT(v) => Ok(v),

            HdbValue::TEXT(s) | HdbValue::STRING(s) => Ok(s.into_bytes()),

            value => Err(wrong_type(&value, "Vec<u8>")),
        }
    }
}

fn wrong_type(tv: &HdbValue, ovt: &str) -> ConversionError {
    ConversionError::ValueType(format!(
        "The value {:?} cannot be converted into type {}",
        tv, ovt
    ))
}

fn number_range(value: i64, ovt: &str) -> ConversionError {
    ConversionError::NumberRange(format!(
        "The value {:?} exceeds the number range of type {}",
        value, ovt
    ))
}

fn decimal_range(ovt: &str) -> ConversionError {
    ConversionError::NumberRange(format!(
        "The given decimal value cannot be converted into a number of type {}",
        ovt
    ))
}

/// Deserializes a `LongDate` into a String format.
fn str_from<T: fmt::Display>(t: &T) -> String {
    format!("{}", t)
}

// FIXME: improve this implementation
impl From<HdbError> for DeserializationError {
    fn from(e: HdbError) -> DeserializationError {
        DeserializationError::Usage(e.to_string())
    }
}
