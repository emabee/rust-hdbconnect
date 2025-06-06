use crate::{HdbError, HdbValue, OutputParameters, ParameterDescriptor, Row, Rows};
use bigdecimal::ToPrimitive;
use serde_db::de::{
    ConversionError, DbValue, DbValueInto, DeserializableResultSet, DeserializableRow,
    DeserializationError, DeserializationResult,
};
use std::{
    fmt,
    num::{ParseFloatError, ParseIntError},
};

impl DeserializableResultSet for Rows {
    type Row = Row;
    type Error = DeserializationError;

    fn has_multiple_rows(&mut self) -> Result<bool, DeserializationError> {
        Ok(self.len > 1)
    }

    fn next(&mut self) -> DeserializationResult<Option<Row>> {
        Ok(self.row_iter.next())
    }

    fn number_of_fields(&self) -> usize {
        self.metadata.len()
    }

    fn field_name(&self, i: usize) -> Option<&str> {
        Some(self.metadata[i].displayname())
    }
}

impl DeserializableRow for Row {
    type Value = HdbValue<'static>;
    type Error = DeserializationError;

    fn len(&self) -> usize {
        self.len()
    }

    fn next(&mut self) -> Option<HdbValue<'static>> {
        self.next_value()
    }

    fn number_of_fields(&self) -> usize {
        self.metadata().len()
    }

    fn field_name(&self, field_idx: usize) -> Option<&str> {
        Some(self.metadata()[field_idx].displayname())
    }
}

pub(crate) struct DeserializableOutputParameters {
    descriptors: Vec<ParameterDescriptor>,
    value_iter: <Vec<HdbValue<'static>> as IntoIterator>::IntoIter,
}
impl DeserializableOutputParameters {
    pub(crate) fn new(op: OutputParameters) -> DeserializableOutputParameters {
        let (descriptors, values) = op.into_descriptors_and_values();
        DeserializableOutputParameters {
            descriptors,
            value_iter: values.into_iter(),
        }
    }
}

impl DeserializableRow for DeserializableOutputParameters {
    type Value = HdbValue<'static>;
    type Error = DeserializationError;

    fn len(&self) -> usize {
        self.value_iter.len()
    }

    fn next(&mut self) -> Option<HdbValue<'static>> {
        self.value_iter.next()
    }

    fn number_of_fields(&self) -> usize {
        self.descriptors.len()
    }

    fn field_name(&self, field_idx: usize) -> Option<&str> {
        self.descriptors
            .get(field_idx)
            .and_then(ParameterDescriptor::name)
    }
}

impl DbValue for HdbValue<'static> {
    fn is_null(&self) -> bool {
        matches!(*self, HdbValue::NULL)
    }
}

impl DbValueInto<bool> for HdbValue<'static> {
    fn try_into(self) -> Result<bool, ConversionError> {
        match self {
            HdbValue::BOOLEAN(b) => Ok(b),
            HdbValue::TINYINT(1)
            | HdbValue::SMALLINT(1)
            | HdbValue::INT(1)
            | HdbValue::BIGINT(1) => Ok(true),
            HdbValue::STRING(ref s) => match s.as_ref() {
                "true" | "TRUE" | "True" => Ok(true),
                "false" | "FALSE" | "False" => Ok(false),
                _ => Err(wrong_type(&self, "bool")),
            },
            HdbValue::TINYINT(0)
            | HdbValue::SMALLINT(0)
            | HdbValue::INT(0)
            | HdbValue::BIGINT(0) => Ok(false),
            value => Err(wrong_type(&value, "bool")),
        }
    }
}

impl DbValueInto<u8> for HdbValue<'static> {
    fn try_into(self) -> Result<u8, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(u),
            HdbValue::SMALLINT(i) => {
                Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u8"))?)
            }
            HdbValue::INT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u8"))?),
            HdbValue::BIGINT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i, "u8"))?),
            HdbValue::DECIMAL(bigdec) => bigdec.to_u8().ok_or_else(|| decimal_range("u8")),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseIntError| parse_int_err(&e)),
            value => Err(wrong_type(&value, "u8")),
        }
    }
}

impl DbValueInto<u16> for HdbValue<'static> {
    fn try_into(self) -> Result<u16, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(u16::from(u)),
            HdbValue::SMALLINT(i) => {
                Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u16"))?)
            }
            HdbValue::INT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u16"))?),
            HdbValue::BIGINT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i, "u16"))?),
            HdbValue::DECIMAL(bigdec) => bigdec.to_u16().ok_or_else(|| decimal_range("u16")),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseIntError| parse_int_err(&e)),
            value => Err(wrong_type(&value, "u16")),
        }
    }
}

impl DbValueInto<u32> for HdbValue<'static> {
    fn try_into(self) -> Result<u32, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(u32::from(u)),
            HdbValue::SMALLINT(i) => {
                Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u32"))?)
            }
            HdbValue::INT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u32"))?),
            HdbValue::BIGINT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i, "u32"))?),
            HdbValue::DECIMAL(bigdec) => bigdec.to_u32().ok_or_else(|| decimal_range("u32")),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseIntError| parse_int_err(&e)),
            value => Err(wrong_type(&value, "u32")),
        }
    }
}

impl DbValueInto<u64> for HdbValue<'static> {
    fn try_into(self) -> Result<u64, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(u64::from(u)),
            HdbValue::SMALLINT(i) => {
                Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u64"))?)
            }
            HdbValue::INT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u64"))?),
            HdbValue::BIGINT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i, "u64"))?),
            HdbValue::DECIMAL(bigdec) => bigdec.to_u64().ok_or_else(|| decimal_range("u64")),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseIntError| parse_int_err(&e)),
            value => Err(wrong_type(&value, "u64")),
        }
    }
}

impl DbValueInto<i8> for HdbValue<'static> {
    fn try_into(self) -> Result<i8, ConversionError> {
        match self {
            HdbValue::TINYINT(i) => {
                Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "i8"))?)
            }
            HdbValue::SMALLINT(i) => {
                Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "i8"))?)
            }
            HdbValue::INT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "i8"))?),
            HdbValue::BIGINT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i, "i8"))?),
            HdbValue::DECIMAL(bigdec) => bigdec.to_i8().ok_or_else(|| decimal_range("i8")),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseIntError| parse_int_err(&e)),
            value => Err(wrong_type(&value, "i8")),
        }
    }
}

impl DbValueInto<i16> for HdbValue<'static> {
    fn try_into(self) -> Result<i16, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(i16::from(u)),
            HdbValue::SMALLINT(i) => Ok(i),
            HdbValue::INT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i64::from(i), "u8"))?),
            HdbValue::BIGINT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i, "u8"))?),
            HdbValue::DECIMAL(bigdec) => bigdec.to_i16().ok_or_else(|| decimal_range("i16")),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseIntError| parse_int_err(&e)),
            value => Err(wrong_type(&value, "i16")),
        }
    }
}

impl DbValueInto<i32> for HdbValue<'static> {
    fn try_into(self) -> Result<i32, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(i32::from(u)),
            HdbValue::SMALLINT(i) => Ok(i32::from(i)),
            HdbValue::INT(i) => Ok(i),
            HdbValue::BIGINT(i) => Ok(num::cast(i).ok_or_else(|| number_range(i, "i32"))?),
            HdbValue::DECIMAL(bigdec) => bigdec.to_i32().ok_or_else(|| decimal_range("i32")),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseIntError| parse_int_err(&e)),
            value => Err(wrong_type(&value, "i32")),
        }
    }
}

impl DbValueInto<i64> for HdbValue<'static> {
    fn try_into(self) -> Result<i64, ConversionError> {
        match self {
            HdbValue::TINYINT(u) => Ok(i64::from(u)),
            HdbValue::SMALLINT(i) => Ok(i64::from(i)),
            HdbValue::INT(i) => Ok(i64::from(i)),
            HdbValue::BIGINT(i) => Ok(i),
            HdbValue::LONGDATE(ld) => Ok(*ld.ref_raw()),
            HdbValue::SECONDDATE(sd) => Ok(*sd.ref_raw()),
            HdbValue::DECIMAL(bigdec) => bigdec.to_i64().ok_or_else(|| decimal_range("i64")),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseIntError| parse_int_err(&e)),
            value => Err(wrong_type(&value, "i64")),
        }
    }
}

impl DbValueInto<f32> for HdbValue<'static> {
    fn try_into(self) -> Result<f32, ConversionError> {
        match self {
            HdbValue::DECIMAL(bigdec) => bigdec.to_f32().ok_or_else(|| decimal_range("f32")),
            HdbValue::REAL(f) => Ok(f),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseFloatError| parse_float_err(&e)),
            value => Err(wrong_type(&value, "f32")),
        }
    }
}

impl DbValueInto<f64> for HdbValue<'static> {
    fn try_into(self) -> Result<f64, ConversionError> {
        match self {
            HdbValue::DECIMAL(bigdec) => bigdec.to_f64().ok_or_else(|| decimal_range("f64")),
            HdbValue::DOUBLE(f) => Ok(f),
            HdbValue::STRING(s) => s.parse().map_err(|e: ParseFloatError| parse_float_err(&e)),
            value => Err(wrong_type(&value, "f64")),
        }
    }
}

impl DbValueInto<String> for HdbValue<'static> {
    fn try_into(self) -> Result<String, ConversionError> {
        trace!("try_into -> String");
        match self {
            HdbValue::NULL => Ok(String::from("<NULL>")), // FIXME intentionally NOT implemented!?
            HdbValue::TINYINT(i) => Ok(format!("{i}")),
            HdbValue::SMALLINT(i) => Ok(format!("{i}")),
            HdbValue::INT(i) => Ok(format!("{i}")),
            HdbValue::BIGINT(i) => Ok(format!("{i}")),
            HdbValue::REAL(f) => Ok(format!("{f}")),
            HdbValue::DOUBLE(f) => Ok(format!("{f}")),
            HdbValue::STRING(s) => Ok(s),
            HdbValue::DBSTRING(bytes) => {
                Err(ConversionError::Other(Box::new(HdbError::Cesu8AsBytes {
                    bytes,
                })))
            }
            HdbValue::LONGDATE(ld) => Ok(str_from(&ld)),
            HdbValue::SECONDDATE(sd) => Ok(str_from(&sd)),
            HdbValue::DAYDATE(date) => Ok(str_from(&date)),
            HdbValue::SECONDTIME(time) => Ok(str_from(&time)),
            HdbValue::DECIMAL(bigdec) => Ok(format!("{bigdec}")),

            #[cfg(feature = "sync")]
            HdbValue::SYNC_CLOB(clob) => Ok(clob
                .into_string_if_complete()
                .map_err(|e| ConversionError::Incomplete(e.to_string()))?),
            #[cfg(feature = "async")]
            HdbValue::ASYNC_CLOB(clob) => Ok(clob
                .into_string_if_complete()
                .map_err(|e| ConversionError::Incomplete(e.to_string()))?),

            #[cfg(feature = "sync")]
            HdbValue::SYNC_NCLOB(nclob) => Ok(nclob
                .into_string_if_complete()
                .map_err(|e| ConversionError::Incomplete(e.to_string()))?),
            #[cfg(feature = "async")]
            HdbValue::ASYNC_NCLOB(nclob) => Ok(nclob
                .into_string_if_complete()
                .map_err(|e| ConversionError::Incomplete(e.to_string()))?),

            value => Err(wrong_type(&value, "String")),
        }
    }
}

impl DbValueInto<Vec<u8>> for HdbValue<'static> {
    fn try_into(self) -> Result<Vec<u8>, ConversionError> {
        match self {
            #[cfg(feature = "sync")]
            HdbValue::SYNC_BLOB(blob) => Ok(blob
                .into_bytes_if_complete()
                .map_err(|e| ConversionError::Incomplete(e.to_string()))?),

            #[cfg(feature = "async")]
            HdbValue::ASYNC_BLOB(blob) => Ok(blob
                .into_bytes_if_complete()
                .map_err(|e| ConversionError::Incomplete(e.to_string()))?),

            HdbValue::BINARY(v) | HdbValue::GEOMETRY(v) | HdbValue::POINT(v) => Ok(v),

            HdbValue::STRING(s) => Ok(s.into_bytes()),
            HdbValue::DBSTRING(v) => Ok(v),

            value => Err(wrong_type(&value, "Vec<u8>")),
        }
    }
}

fn wrong_type(tv: &HdbValue, ovt: &str) -> ConversionError {
    ConversionError::ValueType(format!(
        "The value {tv:?} cannot be converted into type {ovt}",
    ))
}

fn number_range(value: i64, ovt: &str) -> ConversionError {
    ConversionError::NumberRange(format!(
        "The value {value:?} exceeds the number range of type {ovt}",
    ))
}

fn decimal_range(ovt: &str) -> ConversionError {
    ConversionError::NumberRange(format!(
        "The given decimal value cannot be converted into a number of type {ovt}",
    ))
}

fn parse_int_err(e: &ParseIntError) -> ConversionError {
    ConversionError::ValueType(e.to_string())
}

fn parse_float_err(e: &ParseFloatError) -> ConversionError {
    ConversionError::ValueType(e.to_string())
}

/// Deserializes a `LongDate` into a String format.
fn str_from<T: fmt::Display>(t: &T) -> String {
    format!("{t}")
}

// TODO improve this implementation
impl From<HdbError> for DeserializationError {
    fn from(e: HdbError) -> Self {
        Self::Usage(e.to_string())
    }
}
