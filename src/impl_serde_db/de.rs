use bigdecimal::ToPrimitive;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde_db::de::{
    ConversionError, DbValue, DbValueInto, DeserializableResultset, DeserializableRow,
    DeserializationError, DeserializationResult,
};
use std::error::Error;
use std::{i16, i32, i64, i8, u16, u32, u8};
use {HdbError, HdbResult};

use protocol::parts::hdb_value::HdbValue;
use protocol::parts::longdate::LongDate;
use protocol::parts::resultset::ResultSet;
use protocol::parts::row::Row;
use protocol::parts::seconddate::SecondDate;

#[doc(hidden)]
impl DeserializableResultset for ResultSet {
    type ROW = Row;
    type E = HdbError;

    /// Returns true if more than 1 row is contained.
    fn has_multiple_rows(&mut self) -> Result<bool, DeserializationError> {
        Ok(ResultSet::has_multiple_rows(self))
    }

    /// Reverses the order of the rows.
    fn reverse_rows(&mut self) {
        ResultSet::reverse_rows(self)
    }

    /// Removes the last row and returns it, or None if it is empty.
    fn pop_row(&mut self) -> DeserializationResult<Option<Row>> {
        Ok(ResultSet::pop_row(self))
    }

    /// Returns the number of fields.
    fn number_of_fields(&self) -> usize {
        ResultSet::metadata(self).number_of_fields()
    }

    /// Returns the name of the i'th column
    fn get_fieldname(&self, i: usize) -> Option<&String> {
        ResultSet::metadata(self).displayname(i).ok()
    }

    /// Fetches all not yet transported result lines from the server.
    ///
    /// Bigger resultsets are typically not transported in one DB roundtrip;
    /// the number of roundtrips depends on the size of the resultset
    /// and the configured fetch_size of the connection.
    fn fetch_all(&mut self) -> HdbResult<()> {
        ResultSet::fetch_all(self)
    }
}

#[doc(hidden)]
impl DeserializableRow for Row {
    type V = HdbValue;
    type E = HdbError;

    // Returns the length of the row.
    fn len(&self) -> usize {
        Row::len(self)
    }

    // Removes and returns the last value.
    fn pop(&mut self) -> Option<HdbValue> {
        Row::pop(self)
    }

    // Returns the name of the column at the specified index.
    fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        Row::get_fieldname(self, field_idx).ok()
    }

    // Reverses the order of the values.
    fn reverse_values(&mut self) {
        Row::reverse_values(self)
    }
}

#[doc(hidden)]
impl DbValue for HdbValue {
    fn is_null(&self) -> bool {
        match *self {
            HdbValue::N_TINYINT(None)
            | HdbValue::N_SMALLINT(None)
            | HdbValue::N_INT(None)
            | HdbValue::N_BIGINT(None)
            | HdbValue::N_DECIMAL(None)
            | HdbValue::N_SMALLDECIMAL(None)
            | HdbValue::N_REAL(None)
            | HdbValue::N_DOUBLE(None)
            | HdbValue::N_CHAR(None)
            | HdbValue::N_VARCHAR(None)
            | HdbValue::N_NCHAR(None)
            | HdbValue::N_NVARCHAR(None)
            | HdbValue::N_BINARY(None)
            | HdbValue::N_VARBINARY(None)
            | HdbValue::N_CLOB(None)
            | HdbValue::N_NCLOB(None)
            | HdbValue::N_BLOB(None)
            | HdbValue::N_BOOLEAN(None)
            | HdbValue::N_STRING(None)
            | HdbValue::N_NSTRING(None)
            | HdbValue::N_BSTRING(None)
            | HdbValue::N_TEXT(None)
            | HdbValue::N_SHORTTEXT(None)
            | HdbValue::N_LONGDATE(None)
            | HdbValue::N_SECONDDATE(None) => true,

            HdbValue::NOTHING
            | HdbValue::N_TINYINT(Some(_))
            | HdbValue::N_SMALLINT(Some(_))
            | HdbValue::N_INT(Some(_))
            | HdbValue::N_BIGINT(Some(_))
            | HdbValue::N_DECIMAL(Some(_))
            | HdbValue::N_SMALLDECIMAL(Some(_))
            | HdbValue::N_REAL(Some(_))
            | HdbValue::N_DOUBLE(Some(_))
            | HdbValue::N_CHAR(Some(_))
            | HdbValue::N_VARCHAR(Some(_))
            | HdbValue::N_NCHAR(Some(_))
            | HdbValue::N_NVARCHAR(Some(_))
            | HdbValue::N_BINARY(Some(_))
            | HdbValue::N_VARBINARY(Some(_))
            | HdbValue::N_CLOB(Some(_))
            | HdbValue::N_NCLOB(Some(_))
            | HdbValue::N_BLOB(Some(_))
            | HdbValue::N_BOOLEAN(Some(_))
            | HdbValue::N_STRING(Some(_))
            | HdbValue::N_NSTRING(Some(_))
            | HdbValue::N_BSTRING(Some(_))
            | HdbValue::N_TEXT(Some(_))
            | HdbValue::N_SHORTTEXT(Some(_))
            | HdbValue::N_LONGDATE(Some(_))
            | HdbValue::N_SECONDDATE(Some(_))
            | HdbValue::TINYINT(_)
            | HdbValue::SMALLINT(_)
            | HdbValue::INT(_)
            | HdbValue::BIGINT(_)
            | HdbValue::DECIMAL(_)
            | HdbValue::SMALLDECIMAL(_)
            | HdbValue::REAL(_)
            | HdbValue::DOUBLE(_)
            | HdbValue::CHAR(_)
            | HdbValue::VARCHAR(_)
            | HdbValue::NCHAR(_)
            | HdbValue::NVARCHAR(_)
            | HdbValue::BINARY(_)
            | HdbValue::VARBINARY(_)
            | HdbValue::CLOB(_)
            | HdbValue::NCLOB(_)
            | HdbValue::BLOB(_)
            | HdbValue::BOOLEAN(_)
            | HdbValue::STRING(_)
            | HdbValue::NSTRING(_)
            | HdbValue::BSTRING(_)
            | HdbValue::TEXT(_)
            | HdbValue::SHORTTEXT(_)
            | HdbValue::LONGDATE(_)
            | HdbValue::SECONDDATE(_) => false,
        }
    }
}

#[doc(hidden)]
impl DbValueInto<bool> for HdbValue {
    fn try_into(self) -> Result<bool, ConversionError> {
        match self {
            HdbValue::BOOLEAN(b) | HdbValue::N_BOOLEAN(Some(b)) => Ok(b),
            value => Err(wrong_type(&value, "bool")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<u8> for HdbValue {
    fn try_into(self) -> Result<u8, ConversionError> {
        match self {
            HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => Ok(u),

            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => {
                if (i >= 0) && (i <= i16::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(i64::from(i), "u8"))
                }
            }

            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => {
                if (i >= 0) && (i <= i32::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(i64::from(i), "u8"))
                }
            }

            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= i64::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(i, "u8"))
                }
            }
            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_u8().ok_or_else(|| decimal_range("u8"))
            }
            value => Err(wrong_type(&value, "u8")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<u16> for HdbValue {
    fn try_into(self) -> Result<u16, ConversionError> {
        match self {
            HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => Ok(u16::from(u)),

            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => if i >= 0 {
                Ok(i as u16)
            } else {
                Err(number_range(i64::from(i), "u16"))
            },

            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => {
                if (i >= 0) && (i <= i32::from(u16::MAX)) {
                    Ok(i as u16)
                } else {
                    Err(number_range(i64::from(i), "u16"))
                }
            }

            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= i64::from(u16::MAX)) {
                    Ok(i as u16)
                } else {
                    Err(number_range(i, "u16"))
                }
            }
            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_u16().ok_or_else(|| decimal_range("u16"))
            }
            value => Err(wrong_type(&value, "u16")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<u32> for HdbValue {
    fn try_into(self) -> Result<u32, ConversionError> {
        match self {
            HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => Ok(u32::from(u)),

            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => if i >= 0 {
                Ok(i as u32)
            } else {
                Err(number_range(i64::from(i), "u32"))
            },

            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => if i >= 0 {
                Ok(i as u32)
            } else {
                Err(number_range(i64::from(i), "u32"))
            },

            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= i64::from(u32::MAX)) {
                    Ok(i as u32)
                } else {
                    Err(number_range(i, "u32"))
                }
            }
            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_u32().ok_or_else(|| decimal_range("u32"))
            }
            value => Err(wrong_type(&value, "u32")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<u64> for HdbValue {
    fn try_into(self) -> Result<u64, ConversionError> {
        match self {
            HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => Ok(u64::from(u)),

            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => if i >= 0 {
                Ok(i as u64)
            } else {
                Err(number_range(i64::from(i), "u64"))
            },

            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => if i >= 0 {
                Ok(i as u64)
            } else {
                Err(number_range(i64::from(i), "u64"))
            },

            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => if i >= 0 {
                Ok(i as u64)
            } else {
                Err(number_range(i, "u64"))
            },

            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_u64().ok_or_else(|| decimal_range("u64"))
            }
            value => Err(wrong_type(&value, "u64")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<i8> for HdbValue {
    fn try_into(self) -> Result<i8, ConversionError> {
        match self {
            HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => if u <= i8::MAX as u8 {
                Ok(u as i8)
            } else {
                Err(number_range(i64::from(u), "i8"))
            },

            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => {
                if (i >= i16::from(i8::MIN)) && (i <= i16::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(i64::from(i), "i8"))
                }
            }

            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => {
                if (i >= i32::from(i8::MIN)) && (i <= i32::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(i64::from(i), "i8"))
                }
            }

            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => {
                if (i >= i64::from(i8::MIN)) && (i <= i64::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(i, "i8"))
                }
            }
            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_i8().ok_or_else(|| decimal_range("i8"))
            }
            value => Err(wrong_type(&value, "i8")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<i16> for HdbValue {
    fn try_into(self) -> Result<i16, ConversionError> {
        match self {
            HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => Ok(i16::from(u)),

            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => Ok(i),

            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => {
                if (i >= i32::from(i16::MIN)) && (i <= i32::from(i16::MAX)) {
                    Ok(i as i16)
                } else {
                    Err(number_range(i64::from(i), "i16"))
                }
            }

            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => {
                if (i >= i64::from(i16::MIN)) && (i <= i64::from(i16::MAX)) {
                    Ok(i as i16)
                } else {
                    Err(number_range(i, "i16"))
                }
            }
            HdbValue::DECIMAL(dec) | HdbValue::N_DECIMAL(Some(dec)) => {
                dec.to_i16().ok_or_else(|| decimal_range("i16"))
            }
            value => Err(wrong_type(&value, "i16")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<i32> for HdbValue {
    fn try_into(self) -> Result<i32, ConversionError> {
        match self {
            HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => Ok(i32::from(u)),

            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => Ok(i32::from(i)),

            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => Ok(i),

            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => {
                if (i >= i64::from(i32::MIN)) && (i <= i64::from(i32::MAX)) {
                    Ok(i as i32)
                } else {
                    Err(number_range(i, "i32"))
                }
            }
            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_i32().ok_or_else(|| decimal_range("i32"))
            }
            value => Err(wrong_type(&value, "i32")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<i64> for HdbValue {
    fn try_into(self) -> Result<i64, ConversionError> {
        match self {
            HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => Ok(i64::from(u)),
            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => Ok(i64::from(i)),
            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => Ok(i64::from(i)),
            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => Ok(i),
            HdbValue::LONGDATE(ld) | HdbValue::N_LONGDATE(Some(ld)) => Ok(*ld.ref_raw()),
            HdbValue::SECONDDATE(sd) | HdbValue::N_SECONDDATE(Some(sd)) => Ok(*sd.ref_raw()),
            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_i64().ok_or_else(|| decimal_range("i64"))
            }
            value => Err(wrong_type(&value, "i64")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<f32> for HdbValue {
    fn try_into(self) -> Result<f32, ConversionError> {
        match self {
            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_f32().ok_or_else(|| decimal_range("f32"))
            }
            HdbValue::REAL(f) | HdbValue::N_REAL(Some(f)) => Ok(f),
            value => Err(wrong_type(&value, "f32")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<f64> for HdbValue {
    fn try_into(self) -> Result<f64, ConversionError> {
        match self {
            HdbValue::DECIMAL(bigdec) | HdbValue::N_DECIMAL(Some(bigdec)) => {
                bigdec.to_f64().ok_or_else(|| decimal_range("f64"))
            }
            HdbValue::DOUBLE(f) | HdbValue::N_DOUBLE(Some(f)) => Ok(f),
            value => Err(wrong_type(&value, "f64")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<String> for HdbValue {
    fn try_into(self) -> Result<String, ConversionError> {
        trace!("try_into -> String");
        match self {
            HdbValue::TINYINT(i) | HdbValue::N_TINYINT(Some(i)) => Ok(format!("{}", i)),
            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => Ok(format!("{}", i)),
            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => Ok(format!("{}", i)),
            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => Ok(format!("{}", i)),
            HdbValue::REAL(f) | HdbValue::N_REAL(Some(f)) => Ok(format!("{}", f)),
            HdbValue::DOUBLE(f) | HdbValue::N_DOUBLE(Some(f)) => Ok(format!("{}", f)),
            HdbValue::CHAR(s)
            | HdbValue::VARCHAR(s)
            | HdbValue::NCHAR(s)
            | HdbValue::NVARCHAR(s)
            | HdbValue::STRING(s)
            | HdbValue::NSTRING(s)
            | HdbValue::TEXT(s)
            | HdbValue::SHORTTEXT(s)
            | HdbValue::N_CHAR(Some(s))
            | HdbValue::N_VARCHAR(Some(s))
            | HdbValue::N_NCHAR(Some(s))
            | HdbValue::N_NVARCHAR(Some(s))
            | HdbValue::N_STRING(Some(s))
            | HdbValue::N_NSTRING(Some(s))
            | HdbValue::N_SHORTTEXT(Some(s))
            | HdbValue::N_TEXT(Some(s)) => Ok(s),

            HdbValue::LONGDATE(ld) | HdbValue::N_LONGDATE(Some(ld)) => Ok(str_from_longdate(&ld)),
            HdbValue::SECONDDATE(sd) | HdbValue::N_SECONDDATE(Some(sd)) => {
                Ok(str_from_seconddate(&sd))
            }

            HdbValue::DECIMAL(hdbdec) | HdbValue::N_DECIMAL(Some(hdbdec)) => {
                Ok(format!("{}", hdbdec))
            }

            HdbValue::CLOB(clob)
            | HdbValue::NCLOB(clob)
            | HdbValue::N_CLOB(Some(clob))
            | HdbValue::N_NCLOB(Some(clob)) => Ok(clob
                .into_string()
                .map_err(|e| ConversionError::Incomplete(e.description().to_owned()))?),

            value => Err(wrong_type(&value, "String")),
        }
    }
}

#[doc(hidden)]
impl DbValueInto<NaiveDateTime> for HdbValue {
    fn try_into(self) -> Result<NaiveDateTime, ConversionError> {
        trace!("try_into -> NaiveDateTime");
        match self {
            HdbValue::LONGDATE(ld) | HdbValue::N_LONGDATE(Some(ld)) => {
                let (year, month, day, hour, min, sec, frac) = ld.as_ymd_hms_f();
                Ok(NaiveDateTime::new(
                    NaiveDate::from_ymd(year, month, day),
                    NaiveTime::from_hms_nano(hour, min, sec, frac * 100),
                ))
            }
            HdbValue::SECONDDATE(sd) | HdbValue::N_SECONDDATE(Some(sd)) => {
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

#[doc(hidden)]
impl DbValueInto<Vec<u8>> for HdbValue {
    fn try_into(self) -> Result<Vec<u8>, ConversionError> {
        match self {
            HdbValue::BLOB(blob) | HdbValue::N_BLOB(Some(blob)) => Ok(blob
                .into_bytes()
                .map_err(|e| ConversionError::Incomplete(e.description().to_owned()))?),

            HdbValue::BINARY(v)
            | HdbValue::VARBINARY(v)
            | HdbValue::BSTRING(v)
            | HdbValue::N_BINARY(Some(v))
            | HdbValue::N_VARBINARY(Some(v))
            | HdbValue::N_BSTRING(Some(v)) => Ok(v),

            HdbValue::CHAR(s)
            | HdbValue::VARCHAR(s)
            | HdbValue::NCHAR(s)
            | HdbValue::NVARCHAR(s)
            | HdbValue::STRING(s)
            | HdbValue::NSTRING(s)
            | HdbValue::TEXT(s)
            | HdbValue::SHORTTEXT(s)
            | HdbValue::N_CHAR(Some(s))
            | HdbValue::N_VARCHAR(Some(s))
            | HdbValue::N_NCHAR(Some(s))
            | HdbValue::N_NVARCHAR(Some(s))
            | HdbValue::N_STRING(Some(s))
            | HdbValue::N_NSTRING(Some(s))
            | HdbValue::N_SHORTTEXT(Some(s))
            | HdbValue::N_TEXT(Some(s)) => Ok(s.into_bytes()),

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
fn str_from_longdate(ld: &LongDate) -> String {
    format!("{}", ld)
}

/// Deserializes a `SecondDate` into a String format.
fn str_from_seconddate(sd: &SecondDate) -> String {
    format!("{}", sd)
}
