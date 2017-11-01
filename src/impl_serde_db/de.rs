use {HdbError, HdbResult};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde_db::de::{ConversionError, DbValue, DbValueInto, DeserializableResultset,
                   DeserializableRow, DeserializationError, DeserializationResult};
use std::{i16, i32, i64, i8, u16, u32, u8};
use std::error::Error;

use protocol::lowlevel::parts::longdate::LongDate;
use protocol::lowlevel::parts::resultset::ResultSet;
use protocol::lowlevel::parts::row::Row;
use protocol::lowlevel::parts::typed_value::TypedValue;

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
        ResultSet::number_of_fields(self)
    }

    /// Returns the name of the column at the specified index
    fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        ResultSet::get_fieldname(self, field_idx)
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

impl DeserializableRow for Row {
    type V = TypedValue;
    type E = HdbError;

    // Returns the length of the row.
    fn len(&self) -> usize {
        Row::len(self)
    }

    // Removes and returns the last value.
    fn pop(&mut self) -> Option<TypedValue> {
        Row::pop(self)
    }

    // Returns the name of the column at the specified index.
    fn get_fieldname(&self, field_idx: usize) -> Option<&String> {
        Row::get_fieldname(self, field_idx)
    }

    // Reverses the order of the values.
    fn reverse_values(&mut self) {
        Row::reverse_values(self)
    }
}

impl DbValue for TypedValue {
    fn is_null(&self) -> bool {
        match *self {
            TypedValue::N_TINYINT(None) |
            TypedValue::N_SMALLINT(None) |
            TypedValue::N_INT(None) |
            TypedValue::N_BIGINT(None) |
            TypedValue::N_REAL(None) |
            TypedValue::N_DOUBLE(None) |
            TypedValue::N_CHAR(None) |
            TypedValue::N_VARCHAR(None) |
            TypedValue::N_NCHAR(None) |
            TypedValue::N_NVARCHAR(None) |
            TypedValue::N_BINARY(None) |
            TypedValue::N_VARBINARY(None) |
            TypedValue::N_CLOB(None) |
            TypedValue::N_NCLOB(None) |
            TypedValue::N_BLOB(None) |
            TypedValue::N_BOOLEAN(None) |
            TypedValue::N_STRING(None) |
            TypedValue::N_NSTRING(None) |
            TypedValue::N_BSTRING(None) |
            TypedValue::N_TEXT(None) |
            TypedValue::N_SHORTTEXT(None) |
            TypedValue::N_LONGDATE(None) => true,

            TypedValue::N_TINYINT(Some(_)) |
            TypedValue::N_SMALLINT(Some(_)) |
            TypedValue::N_INT(Some(_)) |
            TypedValue::N_BIGINT(Some(_)) |
            TypedValue::N_REAL(Some(_)) |
            TypedValue::N_DOUBLE(Some(_)) |
            TypedValue::N_CHAR(Some(_)) |
            TypedValue::N_VARCHAR(Some(_)) |
            TypedValue::N_NCHAR(Some(_)) |
            TypedValue::N_NVARCHAR(Some(_)) |
            TypedValue::N_BINARY(Some(_)) |
            TypedValue::N_VARBINARY(Some(_)) |
            TypedValue::N_CLOB(Some(_)) |
            TypedValue::N_NCLOB(Some(_)) |
            TypedValue::N_BLOB(Some(_)) |
            TypedValue::N_BOOLEAN(Some(_)) |
            TypedValue::N_STRING(Some(_)) |
            TypedValue::N_NSTRING(Some(_)) |
            TypedValue::N_BSTRING(Some(_)) |
            TypedValue::N_TEXT(Some(_)) |
            TypedValue::N_SHORTTEXT(Some(_)) |
            TypedValue::N_LONGDATE(Some(_)) |
            TypedValue::TINYINT(_) |
            TypedValue::SMALLINT(_) |
            TypedValue::INT(_) |
            TypedValue::BIGINT(_) |
            TypedValue::REAL(_) |
            TypedValue::DOUBLE(_) |
            TypedValue::CHAR(_) |
            TypedValue::VARCHAR(_) |
            TypedValue::NCHAR(_) |
            TypedValue::NVARCHAR(_) |
            TypedValue::BINARY(_) |
            TypedValue::VARBINARY(_) |
            TypedValue::CLOB(_) |
            TypedValue::NCLOB(_) |
            TypedValue::BLOB(_) |
            TypedValue::BOOLEAN(_) |
            TypedValue::STRING(_) |
            TypedValue::NSTRING(_) |
            TypedValue::BSTRING(_) |
            TypedValue::TEXT(_) |
            TypedValue::SHORTTEXT(_) |
            TypedValue::LONGDATE(_) => false,
        }
    }
}

impl DbValueInto<bool> for TypedValue {
    fn try_into(self) -> Result<bool, ConversionError> {
        match self {
            TypedValue::BOOLEAN(b) | TypedValue::N_BOOLEAN(Some(b)) => Ok(b),
            value => Err(wrong_type(&value, "bool")),
        }
    }
}

impl DbValueInto<u8> for TypedValue {
    fn try_into(self) -> Result<u8, ConversionError> {
        match self {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => Ok(u),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => {
                if (i >= 0) && (i <= i16::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(&(i64::from(i)), "u8"))
                }
            }

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => {
                if (i >= 0) && (i <= i32::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(&(i64::from(i)), "u8"))
                }
            }

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= i64::from(u8::MAX)) {
                    Ok(i as u8)
                } else {
                    Err(number_range(&i, "u8"))
                }
            }

            value => Err(wrong_type(&value, "u8")),
        }
    }
}

impl DbValueInto<u16> for TypedValue {
    fn try_into(self) -> Result<u16, ConversionError> {
        match self {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => Ok(u16::from(u)),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => if i >= 0 {
                Ok(i as u16)
            } else {
                Err(number_range(&(i64::from(i)), "u16"))
            },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => {
                if (i >= 0) && (i <= i32::from(u16::MAX)) {
                    Ok(i as u16)
                } else {
                    Err(number_range(&(i64::from(i)), "u16"))
                }
            }

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= i64::from(u16::MAX)) {
                    Ok(i as u16)
                } else {
                    Err(number_range(&i, "u16"))
                }
            }

            value => Err(wrong_type(&value, "u16")),
        }
    }
}

impl DbValueInto<u32> for TypedValue {
    fn try_into(self) -> Result<u32, ConversionError> {
        match self {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => Ok(u32::from(u)),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => if i >= 0 {
                Ok(i as u32)
            } else {
                Err(number_range(&(i64::from(i)), "u32"))
            },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => if i >= 0 {
                Ok(i as u32)
            } else {
                Err(number_range(&(i64::from(i)), "u32"))
            },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i)) => {
                if (i >= 0) && (i <= i64::from(u32::MAX)) {
                    Ok(i as u32)
                } else {
                    Err(number_range(&i, "u32"))
                }
            }

            value => Err(wrong_type(&value, "u32")),
        }
    }
}

impl DbValueInto<u64> for TypedValue {
    fn try_into(self) -> Result<u64, ConversionError> {
        match self {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => Ok(u64::from(u)),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => if i >= 0 {
                Ok(i as u64)
            } else {
                Err(number_range(&(i64::from(i)), "u64"))
            },

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => if i >= 0 {
                Ok(i as u64)
            } else {
                Err(number_range(&(i64::from(i)), "u64"))
            },

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i)) => if i >= 0 {
                Ok(i as u64)
            } else {
                Err(number_range(&i, "u64"))
            },

            value => Err(wrong_type(&value, "u64")),
        }
    }
}

impl DbValueInto<i8> for TypedValue {
    fn try_into(self) -> Result<i8, ConversionError> {
        match self {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => if u <= i8::MAX as u8 {
                Ok(u as i8)
            } else {
                Err(number_range(&(i64::from(u)), "i8"))
            },

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => {
                if (i >= i16::from(i8::MIN)) && (i <= i16::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(&(i64::from(i)), "i8"))
                }
            }

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => {
                if (i >= i32::from(i8::MIN)) && (i <= i32::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(&(i64::from(i)), "i8"))
                }
            }

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i64::from(i8::MIN)) && (i <= i64::from(i8::MAX)) {
                    Ok(i as i8)
                } else {
                    Err(number_range(&i, "i8"))
                }
            }

            value => Err(wrong_type(&value, "i8")),
        }
    }
}

impl DbValueInto<i16> for TypedValue {
    fn try_into(self) -> Result<i16, ConversionError> {
        match self {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => Ok(i16::from(u)),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => Ok(i),

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => {
                if (i >= i32::from(i16::MIN)) && (i <= i32::from(i16::MAX)) {
                    Ok(i as i16)
                } else {
                    Err(number_range(&(i64::from(i)), "i16"))
                }
            }

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i64::from(i16::MIN)) && (i <= i64::from(i16::MAX)) {
                    Ok(i as i16)
                } else {
                    Err(number_range(&i, "i16"))
                }
            }

            value => Err(wrong_type(&value, "i16")),
        }
    }
}

impl DbValueInto<i32> for TypedValue {
    fn try_into(self) -> Result<i32, ConversionError> {
        match self {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => Ok(i32::from(u)),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => Ok(i32::from(i)),

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => Ok(i),

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i)) => {
                if (i >= i64::from(i32::MIN)) && (i <= i64::from(i32::MAX)) {
                    Ok(i as i32)
                } else {
                    Err(number_range(&i, "i32"))
                }
            }
            value => Err(wrong_type(&value, "i32")),
        }
    }
}

impl DbValueInto<i64> for TypedValue {
    fn try_into(self) -> Result<i64, ConversionError> {
        match self {
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => Ok(i64::from(u)),

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => Ok(i64::from(i)),

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => Ok(i64::from(i)),

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) |
            TypedValue::LONGDATE(LongDate(i)) |
            TypedValue::N_LONGDATE(Some(LongDate(i))) => Ok(i),

            value => Err(wrong_type(&value, "i64")),
        }
    }
}

impl DbValueInto<f32> for TypedValue {
    fn try_into(self) -> Result<f32, ConversionError> {
        match self {
            TypedValue::REAL(f) | TypedValue::N_REAL(Some(f)) => Ok(f),
            value => Err(wrong_type(&value, "f32")),
        }
    }
}

impl DbValueInto<f64> for TypedValue {
    fn try_into(self) -> Result<f64, ConversionError> {
        match self {
            TypedValue::DOUBLE(f) | TypedValue::N_DOUBLE(Some(f)) => Ok(f),
            value => Err(wrong_type(&value, "f64")),
        }
    }
}

impl DbValueInto<String> for TypedValue {
    fn try_into(self) -> Result<String, ConversionError> {
        trace!("try_into -> String");
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

            TypedValue::LONGDATE(ld) | TypedValue::N_LONGDATE(Some(ld)) => {
                Ok(str_from_longdate(&ld))
            }

            TypedValue::CLOB(clob) |
            TypedValue::NCLOB(clob) |
            TypedValue::N_CLOB(Some(clob)) |
            TypedValue::N_NCLOB(Some(clob)) => Ok(clob.into_string()
                .map_err(|e| ConversionError::Incomplete(e.description().to_owned()))?),

            value => Err(wrong_type(&value, "String")),
        }
    }
}

impl DbValueInto<NaiveDateTime> for TypedValue {
    fn try_into(self) -> Result<NaiveDateTime, ConversionError> {
        trace!("try_into -> NaiveDateTime");
        match self {
            TypedValue::LONGDATE(ld) | TypedValue::N_LONGDATE(Some(ld)) => {
                let (y, m, d, h, min, s, f) = ld.as_ymd_hms_f();
                Ok(NaiveDateTime::new(
                    NaiveDate::from_ymd(y, m, d),
                    NaiveTime::from_hms_nano(h, min, s, f * 100),
                ))
            }
            _ => Err(ConversionError::ValueType("Not a LongDate value".to_owned())),
        }
    }
}


impl DbValueInto<Vec<u8>> for TypedValue {
    fn try_into(self) -> Result<Vec<u8>, ConversionError> {
        match self {
            TypedValue::BLOB(blob) | TypedValue::N_BLOB(Some(blob)) => Ok(blob.into_bytes()
                .map_err(|e| ConversionError::Incomplete(e.description().to_owned()))?),

            TypedValue::BINARY(v) |
            TypedValue::VARBINARY(v) |
            TypedValue::BSTRING(v) |
            TypedValue::N_BINARY(Some(v)) |
            TypedValue::N_VARBINARY(Some(v)) |
            TypedValue::N_BSTRING(Some(v)) => Ok(v),

            value => Err(wrong_type(&value, "seq")),
        }
    }
}



fn wrong_type(tv: &TypedValue, ovt: &str) -> ConversionError {
    ConversionError::ValueType(format!("The value {:?} cannot be converted into type {}", tv, ovt))
}

fn number_range(value: &i64, ovt: &str) -> ConversionError {
    ConversionError::NumberRange(
        format!("The value {:?} exceeds the number range of type {}", value, ovt),
    )
}


/// Deserializes a `LongDate` into a String format.
fn str_from_longdate(ld: &LongDate) -> String {
    format!("{}", ld)
}
