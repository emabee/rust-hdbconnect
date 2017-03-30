use protocol::lowlevel::parts::longdate::LongDate;
use protocol::lowlevel::parts::lob::*;
use protocol::lowlevel::parts::parameter_metadata::ParameterDescriptor;
use protocol::lowlevel::parts::parameters::ParameterRow;
use protocol::lowlevel::parts::type_id::*;
use protocol::lowlevel::parts::typed_value::TypedValue;
use super::{SerializationError, SerializationResult};
use super::super::hdbdate::longdate_from_str;

use serde;
use std::{u8, i16, i32, i64};

/// A structure for serializing Rust values into a parameter row for a prepared statement.
pub struct Serializer {
    output: ParameterRow,
    metadata: Vec<ParameterDescriptor>,
}

impl Serializer {
    pub fn new(metadata: Vec<ParameterDescriptor>) -> Self {
        Serializer {
            output: ParameterRow::new(),
            metadata: metadata,
        }
    }

    /// translate the specified struct into a Row
    pub fn into_row<T>(input: &T, md: Vec<ParameterDescriptor>) -> SerializationResult<ParameterRow>
        where T: serde::ser::Serialize
    {
        trace!("Serializer::into_row()");
        let mut serializer = Serializer::new(md);
        {
            input.serialize(&mut serializer)?;
        }
        Ok(serializer.output)
    }

    /// get the type code of the current field
    fn expected_type_code(&self) -> SerializationResult<u8> {
        match self.metadata.get(self.output.values.len()) {
            Some(pd) => Ok(pd.value_type),
            None => return Err(SerializationError::StructuralMismatch("too many values specified")),
        }
    }
}

impl<'a> serde::ser::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = SerializationError;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn serialize_bool(mut self, value: bool) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_bool()");
        match self.expected_type_code()? {
            TYPEID_BOOLEAN => self.output.push(TypedValue::BOOLEAN(value)),
            TYPEID_N_BOOLEAN => self.output.push(TypedValue::N_BOOLEAN(Some(value))),
            target_tc => return Err(SerializationError::TypeMismatch("boolean", target_tc)),
        }
        Ok(())
    }

    fn serialize_i8(mut self, value: i8) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_i8()");
        let input_type = "i8";
        match self.expected_type_code()? {
            TYPEID_TINYINT => {
                if value >= 0 {
                    self.output.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value >= 0 {
                    self.output.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => self.output.push(TypedValue::SMALLINT(value as i16)),
            TYPEID_N_SMALLINT => self.output.push(TypedValue::N_SMALLINT(Some(value as i16))),
            TYPEID_INT => self.output.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.output.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.output.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.output.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_i16(mut self, value: i16) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_i16()");
        let input_type = "i16";
        match self.expected_type_code()? {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i16) {
                    self.output.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i16) {
                    self.output.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => self.output.push(TypedValue::SMALLINT(value)),
            TYPEID_N_SMALLINT => self.output.push(TypedValue::N_SMALLINT(Some(value))),
            TYPEID_INT => self.output.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.output.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.output.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.output.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_i32(mut self, value: i32) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_i32()");
        let input_type = "i32";
        match self.expected_type_code()? {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i32) {
                    self.output.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i32) {
                    self.output.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if (value >= i16::MIN as i32) && (value <= i16::MAX as i32) {
                    self.output.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= i16::MIN as i32) && (value <= i16::MAX as i32) {
                    self.output.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_INT => self.output.push(TypedValue::INT(value)),
            TYPEID_N_INT => self.output.push(TypedValue::N_INT(Some(value))),
            TYPEID_BIGINT => self.output.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.output.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_i64(mut self, value: i64) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_i64()");
        let input_type = "i64";
        match self.expected_type_code()? {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i64) {
                    self.output.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i64) {
                    self.output.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if (value >= i16::MIN as i64) && (value <= i16::MAX as i64) {
                    self.output.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= i16::MIN as i64) && (value <= i16::MAX as i64) {
                    self.output.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_INT => {
                if (value >= i32::MIN as i64) && (value <= i32::MAX as i64) {
                    self.output.push(TypedValue::INT(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_INT => {
                if (value >= i32::MIN as i64) && (value <= i32::MAX as i64) {
                    self.output.push(TypedValue::N_INT(Some(value as i32)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_BIGINT => self.output.push(TypedValue::BIGINT(value)),
            TYPEID_N_BIGINT => self.output.push(TypedValue::N_BIGINT(Some(value))),
            TYPEID_LONGDATE => self.output.push(TypedValue::LONGDATE(LongDate(value))),
            TYPEID_N_LONGDATE => self.output.push(TypedValue::N_LONGDATE(Some(LongDate(value)))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_u8(mut self, value: u8) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_u8()");
        let input_type = "u8";
        match self.expected_type_code()? {
            TYPEID_TINYINT => self.output.push(TypedValue::TINYINT(value)),
            TYPEID_N_TINYINT => self.output.push(TypedValue::N_TINYINT(Some(value))),
            TYPEID_SMALLINT => self.output.push(TypedValue::SMALLINT(value as i16)),
            TYPEID_N_SMALLINT => self.output.push(TypedValue::N_SMALLINT(Some(value as i16))),
            TYPEID_INT => self.output.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.output.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.output.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.output.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_u16(mut self, value: u16) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_u16()");
        let input_type = "u16";
        match self.expected_type_code()? {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u16 {
                    self.output.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u16 {
                    self.output.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u16 {
                    self.output.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u16 {
                    self.output.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => self.output.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.output.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.output.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.output.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_u32(mut self, value: u32) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_u32()");
        let input_type = "u32";
        match self.expected_type_code()? {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u32 {
                    self.output.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u32 {
                    self.output.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u32 {
                    self.output.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u32 {
                    self.output.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => {
                if value <= i32::MAX as u32 {
                    self.output.push(TypedValue::INT(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_N_INT => {
                if value <= i32::MAX as u32 {
                    self.output.push(TypedValue::N_INT(Some(value as i32)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_BIGINT => self.output.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.output.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_u64(mut self, value: u64) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_u64()");
        let input_type = "u64";
        match self.expected_type_code()? {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u64 {
                    self.output.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u64 {
                    self.output.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u64 {
                    self.output.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u64 {
                    self.output.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => {
                if value <= i32::MAX as u64 {
                    self.output.push(TypedValue::INT(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_N_INT => {
                if value <= i32::MAX as u64 {
                    self.output.push(TypedValue::N_INT(Some(value as i32)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_BIGINT => {
                if value <= i64::MAX as u64 {
                    self.output.push(TypedValue::BIGINT(value as i64))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_BIGINT));
                }
            }
            TYPEID_N_BIGINT => {
                if value <= i64::MAX as u64 {
                    self.output.push(TypedValue::N_BIGINT(Some(value as i64)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_BIGINT));
                }
            }
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_f32(mut self, value: f32) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_f32()");
        match self.expected_type_code()? {
            TYPEID_REAL => self.output.push(TypedValue::REAL(value)),
            TYPEID_N_REAL => self.output.push(TypedValue::N_REAL(Some(value))),
            target_tc => return Err(SerializationError::TypeMismatch("f32", target_tc)),
        }
        Ok(())
    }

    fn serialize_f64(mut self, value: f64) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_f64()");
        match self.expected_type_code()? {
            TYPEID_DOUBLE => self.output.push(TypedValue::DOUBLE(value)),
            TYPEID_N_DOUBLE => self.output.push(TypedValue::N_DOUBLE(Some(value))),
            target_tc => return Err(SerializationError::TypeMismatch("f64", target_tc)),
        }
        Ok(())
    }

    fn serialize_char(mut self, value: char) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_char()");
        let mut s = String::new();
        s.push(value);
        match self.expected_type_code()? {
            TYPEID_CHAR | TYPEID_VARCHAR | TYPEID_NCHAR | TYPEID_NVARCHAR | TYPEID_STRING |
            TYPEID_NSTRING | TYPEID_TEXT | TYPEID_SHORTTEXT => {
                self.output.push(TypedValue::STRING(s))
            }
            target_tc => return Err(SerializationError::TypeMismatch("char", target_tc)),
        }
        Ok(())
    }

    fn serialize_str(mut self, value: &str) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_str() with {}", value);
        let s = String::from(value);
        match self.expected_type_code()? {
            TYPEID_CHAR | TYPEID_VARCHAR | TYPEID_NCHAR | TYPEID_NVARCHAR | TYPEID_STRING |
            TYPEID_NSTRING | TYPEID_TEXT | TYPEID_SHORTTEXT | TYPEID_N_CLOB | TYPEID_N_NCLOB | TYPEID_NCLOB | TYPEID_CLOB => {
                self.output.push(TypedValue::STRING(s))
            }
            TYPEID_LONGDATE => self.output.push(TypedValue::LONGDATE(longdate_from_str(value)?)),

            target_tc => return Err(SerializationError::TypeMismatch("&str", target_tc)),
        }
        Ok(())
    }

    fn serialize_bytes(mut self, value: &[u8]) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_bytes()");
        match self.expected_type_code()? {
            TYPEID_BLOB => self.output.push(TypedValue::BLOB(new_blob_to_db((*value).to_vec()))),
            TYPEID_N_BLOB => {
                self.output.push(TypedValue::N_BLOB(Some(new_blob_to_db((*value).to_vec()))))
            }
            target_tc => return Err(SerializationError::TypeMismatch("bytes", target_tc)),
        }
        Ok(())
    }

    fn serialize_unit(self) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_unit()");
        Err(SerializationError::TypeMismatch("unit", self.expected_type_code()?))
    }

    #[allow(unused_variables)]
    fn serialize_unit_struct(self, name: &'static str) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_unit_struct()");
        Err(SerializationError::TypeMismatch("unit_struct", self.expected_type_code()?))
    }

    #[allow(unused_variables)]
    fn serialize_unit_variant(self, name: &'static str, variant_index: usize,
                              variant: &'static str)
                              -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_unit_variant()");
        Err(SerializationError::TypeMismatch("unit_variant", self.expected_type_code()?))
    }

    #[allow(unused_variables)]
    fn serialize_newtype_struct<T: ?Sized + serde::ser::Serialize>
        (self, name: &'static str, value: &T)
         -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_newtype_struct()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_newtype_variant<T: ?Sized + serde::ser::Serialize>
        (self, name: &'static str, variant_index: usize, variant: &'static str, value: &T)
         -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_newtype_variant()");
        value.serialize(self)
    }

    fn serialize_none(mut self) -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_none()");
        match self.expected_type_code()? {
            TYPEID_N_TINYINT => self.output.push(TypedValue::N_TINYINT(None)),
            TYPEID_N_SMALLINT => self.output.push(TypedValue::N_SMALLINT(None)),
            TYPEID_N_INT => self.output.push(TypedValue::N_INT(None)),
            TYPEID_N_BIGINT => self.output.push(TypedValue::N_BIGINT(None)),
            TYPEID_N_REAL => self.output.push(TypedValue::N_REAL(None)),
            TYPEID_N_DOUBLE => self.output.push(TypedValue::N_DOUBLE(None)),
            TYPEID_N_CHAR => self.output.push(TypedValue::N_CHAR(None)),
            TYPEID_N_VARCHAR => self.output.push(TypedValue::N_VARCHAR(None)),
            TYPEID_N_NCHAR => self.output.push(TypedValue::N_NCHAR(None)),
            TYPEID_N_NVARCHAR => self.output.push(TypedValue::N_NVARCHAR(None)),
            TYPEID_N_BINARY => self.output.push(TypedValue::N_BINARY(None)),
            TYPEID_N_VARBINARY => self.output.push(TypedValue::N_VARBINARY(None)),
            TYPEID_N_CLOB => self.output.push(TypedValue::N_CLOB(None)),
            TYPEID_N_NCLOB => self.output.push(TypedValue::N_NCLOB(None)),
            TYPEID_N_BLOB => self.output.push(TypedValue::N_BLOB(None)),
            TYPEID_N_BOOLEAN => self.output.push(TypedValue::N_BOOLEAN(None)),
            TYPEID_N_STRING => self.output.push(TypedValue::N_STRING(None)),
            TYPEID_N_NSTRING => self.output.push(TypedValue::N_NSTRING(None)),
            TYPEID_N_BSTRING => self.output.push(TypedValue::N_BSTRING(None)),
            TYPEID_N_TEXT => self.output.push(TypedValue::N_TEXT(None)),
            TYPEID_N_SHORTTEXT => self.output.push(TypedValue::N_SHORTTEXT(None)),
            TYPEID_N_LONGDATE => self.output.push(TypedValue::N_LONGDATE(None)),
            target_tc => return Err(SerializationError::TypeMismatch("none", target_tc)),
        }
        Ok(())
    }

    fn serialize_some<T: ?Sized + serde::ser::Serialize>(self, value: &T)
                                                         -> SerializationResult<Self::Ok> {
        trace!("Serializer::serialize_some()");
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> SerializationResult<Self::SerializeSeq> {
        trace!("Serializer::serialize_seq()");
        Ok(Compound { ser: self })
    }

    fn serialize_seq_fixed_size(self, size: usize) -> SerializationResult<Self::SerializeSeq> {
        trace!("Serializer::serialize_seq_fixed_size()");
        self.serialize_seq(Some(size))
    }

    fn serialize_tuple(self, len: usize) -> SerializationResult<Self::SerializeTuple> {
        trace!("Serializer::serialize_tuple()");
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(self, _name: &'static str, len: usize)
                              -> SerializationResult<Self::SerializeTupleStruct> {
        trace!("Serializer::serialize_tuple_struct()");
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(self, _name: &'static str, _variant_index: usize,
                               _variant: &'static str, len: usize)
                               -> SerializationResult<Self::SerializeTupleVariant> {
        trace!("Serializer::serialize_tuple_variant()");
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, _len: Option<usize>) -> SerializationResult<Self::SerializeMap> {
        panic!("FIXME: Serializer::serialize_map()")
    }

    fn serialize_struct(self, _name: &'static str, len: usize)
                        -> SerializationResult<Self::SerializeStruct> {
        trace!("Serializer::serialize_struct()");
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(self, _name: &'static str, _variant_index: usize,
                                _variant: &'static str, _len: usize)
                                -> SerializationResult<Self::SerializeStructVariant> {
        panic!("FIXME: Serializer::serialize_struct_variant()")
    }
}

#[doc(hidden)]
pub struct Compound<'a> {
    ser: &'a mut Serializer,
}

impl<'a> serde::ser::SerializeSeq for Compound<'a> {
    type Ok = ();
    type Error = SerializationError;

    #[inline]
    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeSeq::serialize_element()");
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeSeq::end()");
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTuple for Compound<'a> {
    type Ok = ();
    type Error = SerializationError;

    #[inline]
    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeTuple::serialize_element()");
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    #[inline]
    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeTuple::end()");
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTupleStruct for Compound<'a> {
    type Ok = ();
    type Error = SerializationError;

    #[inline]
    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeTupleStruct::serialize_field()");
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    #[inline]
    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeTupleStruct::end()");
        serde::ser::SerializeSeq::end(self)
    }
}

impl<'a> serde::ser::SerializeTupleVariant for Compound<'a> {
    type Ok = ();
    type Error = SerializationError;

    #[inline]
    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeTupleVariant::serialize_field()");
        serde::ser::SerializeSeq::serialize_element(self, value)
    }

    #[inline]
    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeTupleVariant::end()");
        Ok(())
    }
}


impl<'a> serde::ser::SerializeMap for Compound<'a> {
    type Ok = ();
    type Error = SerializationError;

    #[inline]
    fn serialize_key<T: ?Sized>(&mut self, _key: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeMap::serialize_key()");
        Ok(())
    }

    #[inline]
    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeMap::serialize_value()");
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeMap::end()");
        Ok(())
    }
}


impl<'a> serde::ser::SerializeStruct for Compound<'a> {
    type Ok = ();
    type Error = SerializationError;

    #[inline]
    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T)
                                  -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeStruct::serialize_field()");
        try!(serde::ser::SerializeMap::serialize_key(self, key));
        serde::ser::SerializeMap::serialize_value(self, value)
    }

    #[inline]
    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeStruct::end()");
        serde::ser::SerializeMap::end(self)
    }
}

impl<'a> serde::ser::SerializeStructVariant for Compound<'a> {
    type Ok = ();
    type Error = SerializationError;

    #[inline]
    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T)
                                  -> SerializationResult<()>
        where T: serde::ser::Serialize
    {
        trace!("Compound: SerializeStructVariant::serialize_field()");
        serde::ser::SerializeStruct::serialize_field(self, key, value)
    }

    #[inline]
    fn end(self) -> SerializationResult<Self::Ok> {
        trace!("Compound: SerializeStructVariant::end()");
        serde::ser::SerializeStruct::end(self)
    }
}
