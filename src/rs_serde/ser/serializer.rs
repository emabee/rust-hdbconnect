use protocol::lowlevel::parts::longdate::LongDate;
use protocol::lowlevel::parts::lob::*;
use protocol::lowlevel::parts::parameter_metadata::{ParameterDescriptor, ParameterMetadata,
                                                    ParMode};
use protocol::lowlevel::parts::parameters::ParameterRow;
use protocol::lowlevel::parts::type_id::*;
use protocol::lowlevel::parts::typed_value::TypedValue;
use super::{SerializationError, SerializeResult};
use super::super::hdbdate::longdate_from_str;

use serde;
use std::{u8, i16, i32, i64};

/// A structure for serializing Rust values into a parameter row for a prepared statement.
pub struct Serializer<'a> {
    row: ParameterRow,
    metadata: Vec<&'a ParameterDescriptor>,
}

impl<'a> Serializer<'a> {
    pub fn new(metadata: &'a ParameterMetadata) -> Self {
        let mut serializer = Serializer {
            row: ParameterRow::new(),
            metadata: Vec::<&ParameterDescriptor>::new(),
        };
        for ref pd in &metadata.descriptors {
            match pd.mode {
                ParMode::IN | ParMode::INOUT => serializer.metadata.push(&pd),
                ParMode::OUT => {}
            }
        }
        serializer
    }

    /// translate the specified struct into a Row
    pub fn into_row<T>(value: &T, md: &ParameterMetadata) -> SerializeResult<ParameterRow>
        where T: serde::ser::Serialize
    {
        trace!("Serializer::into_row()");
        let mut serializer = Serializer::new(md);
        try!(value.serialize(&mut serializer));
        Ok(serializer.row)
    }

    /// get the type code of the current field
    fn expected_type_code(&self) -> SerializeResult<u8> {
        match self.metadata.get(self.row.values.len()) {
            Some(pd) => Ok(pd.value_type),
            None => return Err(SerializationError::StructuralMismatch("too many values specified")),
        }
    }
}

impl<'a> serde::ser::Serializer for Serializer<'a> {
    type Error = SerializationError;
    type SeqState = ();
    type TupleState = ();
    type TupleStructState = ();
    type TupleVariantState = ();
    type MapState = ();
    type StructState = ();
    type StructVariantState = ();

    fn serialize_bool(&mut self, value: bool) -> SerializeResult<()> {
        trace!("Serializer::serialize_bool()");
        match try!(self.expected_type_code()) {
            TYPEID_BOOLEAN => self.row.push(TypedValue::BOOLEAN(value)),
            TYPEID_N_BOOLEAN => self.row.push(TypedValue::N_BOOLEAN(Some(value))),
            target_tc => return Err(SerializationError::TypeMismatch("boolean", target_tc)),
        }
        Ok(())
    }

    fn serialize_isize(&mut self, value: isize) -> SerializeResult<()> {
        trace!("Serializer::serialize_isize()");
        let input_type = "isize";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as isize) {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as isize) {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_TINYINT));
                }
            }

            TYPEID_SMALLINT => {
                if (value >= 0) && (value <= i16::MAX as isize) {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= 0) && (value <= i16::MAX as isize) {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }

            TYPEID_INT => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_i8(&mut self, value: i8) -> SerializeResult<()> {
        trace!("Serializer::serialize_i8()");
        let input_type = "i8";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if value >= 0 {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value >= 0 {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => self.row.push(TypedValue::SMALLINT(value as i16)),
            TYPEID_N_SMALLINT => self.row.push(TypedValue::N_SMALLINT(Some(value as i16))),
            TYPEID_INT => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_i16(&mut self, value: i16) -> SerializeResult<()> {
        trace!("Serializer::serialize_i16()");
        let input_type = "i16";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i16) {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i16) {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => self.row.push(TypedValue::SMALLINT(value)),
            TYPEID_N_SMALLINT => self.row.push(TypedValue::N_SMALLINT(Some(value))),
            TYPEID_INT => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_i32(&mut self, value: i32) -> SerializeResult<()> {
        trace!("Serializer::serialize_i32()");
        let input_type = "i32";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i32) {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i32) {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if (value >= i16::MIN as i32) && (value <= i16::MAX as i32) {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= i16::MIN as i32) && (value <= i16::MAX as i32) {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_INT => self.row.push(TypedValue::INT(value)),
            TYPEID_N_INT => self.row.push(TypedValue::N_INT(Some(value))),
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_i64(&mut self, value: i64) -> SerializeResult<()> {
        trace!("Serializer::serialize_i64()");
        let input_type = "i64";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i64) {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i64) {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if (value >= i16::MIN as i64) && (value <= i16::MAX as i64) {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= i16::MIN as i64) && (value <= i16::MAX as i64) {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_INT => {
                if (value >= i32::MIN as i64) && (value <= i32::MAX as i64) {
                    self.row.push(TypedValue::INT(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_INT => {
                if (value >= i32::MIN as i64) && (value <= i32::MAX as i64) {
                    self.row.push(TypedValue::N_INT(Some(value as i32)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value))),
            TYPEID_LONGDATE => self.row.push(TypedValue::LONGDATE(LongDate(value))),
            TYPEID_N_LONGDATE => self.row.push(TypedValue::N_LONGDATE(Some(LongDate(value)))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_usize(&mut self, value: usize) -> SerializeResult<()> {
        trace!("Serializer::serialize_usize()");
        let input_type = "usize";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if value <= u8::MAX as usize {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as usize {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as usize {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as usize {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_INT => {
                if value <= i32::MAX as usize {
                    self.row.push(TypedValue::INT(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_INT => {
                if value <= i32::MAX as usize {
                    self.row.push(TypedValue::N_INT(Some(value as i32)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_u8(&mut self, value: u8) -> SerializeResult<()> {
        trace!("Serializer::serialize_u8()");
        let input_type = "u8";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => self.row.push(TypedValue::TINYINT(value)),
            TYPEID_N_TINYINT => self.row.push(TypedValue::N_TINYINT(Some(value))),
            TYPEID_SMALLINT => self.row.push(TypedValue::SMALLINT(value as i16)),
            TYPEID_N_SMALLINT => self.row.push(TypedValue::N_SMALLINT(Some(value as i16))),
            TYPEID_INT => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_u16(&mut self, value: u16) -> SerializeResult<()> {
        trace!("Serializer::serialize_u16()");
        let input_type = "u16";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u16 {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u16 {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u16 {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u16 {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_u32(&mut self, value: u32) -> SerializeResult<()> {
        trace!("Serializer::serialize_u32()");
        let input_type = "u32";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u32 {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u32 {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u32 {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u32 {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => {
                if value <= i32::MAX as u32 {
                    self.row.push(TypedValue::INT(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_N_INT => {
                if value <= i32::MAX as u32 {
                    self.row.push(TypedValue::N_INT(Some(value as i32)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_BIGINT => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_u64(&mut self, value: u64) -> SerializeResult<()> {
        trace!("Serializer::serialize_u64()");
        let input_type = "u64";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u64 {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u64 {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u64 {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u64 {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => {
                if value <= i32::MAX as u64 {
                    self.row.push(TypedValue::INT(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_N_INT => {
                if value <= i32::MAX as u64 {
                    self.row.push(TypedValue::N_INT(Some(value as i32)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_BIGINT => {
                if value <= i64::MAX as u64 {
                    self.row.push(TypedValue::BIGINT(value as i64))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_BIGINT));
                }
            }
            TYPEID_N_BIGINT => {
                if value <= i64::MAX as u64 {
                    self.row.push(TypedValue::N_BIGINT(Some(value as i64)))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_BIGINT));
                }
            }
            target_tc => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    fn serialize_f32(&mut self, value: f32) -> SerializeResult<()> {
        trace!("Serializer::serialize_f32()");
        match try!(self.expected_type_code()) {
            TYPEID_REAL => self.row.push(TypedValue::REAL(value)),
            TYPEID_N_REAL => self.row.push(TypedValue::N_REAL(Some(value))),
            target_tc => return Err(SerializationError::TypeMismatch("f32", target_tc)),
        }
        Ok(())
    }

    fn serialize_f64(&mut self, value: f64) -> SerializeResult<()> {
        trace!("Serializer::serialize_f64()");
        match try!(self.expected_type_code()) {
            TYPEID_DOUBLE => self.row.push(TypedValue::DOUBLE(value)),
            TYPEID_N_DOUBLE => self.row.push(TypedValue::N_DOUBLE(Some(value))),
            target_tc => return Err(SerializationError::TypeMismatch("f64", target_tc)),
        }
        Ok(())
    }

    fn serialize_char(&mut self, value: char) -> SerializeResult<()> {
        trace!("Serializer::serialize_char()");
        let mut s = String::new();
        s.push(value);
        match try!(self.expected_type_code()) {
            TYPEID_CHAR | TYPEID_VARCHAR | TYPEID_NCHAR | TYPEID_NVARCHAR | TYPEID_STRING |
            TYPEID_NSTRING | TYPEID_TEXT | TYPEID_SHORTTEXT => self.row.push(TypedValue::STRING(s)),
            target_tc => return Err(SerializationError::TypeMismatch("char", target_tc)),
        }
        Ok(())
    }

    fn serialize_str(&mut self, value: &str) -> SerializeResult<()> {
        trace!("Serializer::serialize_str() with {}", value);
        let s = String::from(value);
        match try!(self.expected_type_code()) {
            TYPEID_CHAR | TYPEID_VARCHAR | TYPEID_NCHAR | TYPEID_NVARCHAR | TYPEID_STRING |
            TYPEID_NSTRING | TYPEID_TEXT | TYPEID_SHORTTEXT => self.row.push(TypedValue::STRING(s)),

            TYPEID_LONGDATE => self.row.push(TypedValue::LONGDATE(try!(longdate_from_str(value)))),

            target_tc => return Err(SerializationError::TypeMismatch("&str", target_tc)),
        }
        Ok(())
    }

    fn serialize_bytes(&mut self, value: &[u8]) -> SerializeResult<()> {
        trace!("Serializer::serialize_bytes()");
        match try!(self.expected_type_code()) {
            TYPEID_BLOB => self.row.push(TypedValue::BLOB(new_blob_to_db((*value).to_vec()))),
            TYPEID_N_BLOB => {
                self.row.push(TypedValue::N_BLOB(Some(new_blob_to_db((*value).to_vec()))))
            }
            target_tc => return Err(SerializationError::TypeMismatch("bytes", target_tc)),
        }
        Ok(())
    }

    fn serialize_unit(&mut self) -> SerializeResult<()> {
        trace!("Serializer::serialize_unit()");
        Err(SerializationError::TypeMismatch("unit", try!(self.expected_type_code())))
    }

    #[allow(unused_variables)]
    fn serialize_unit_struct(&mut self, name: &'static str) -> SerializeResult<()> {
        trace!("Serializer::serialize_unit_struct()");
        Err(SerializationError::TypeMismatch("unit_struct", try!(self.expected_type_code())))
    }

    #[allow(unused_variables)]
    fn serialize_unit_variant(&mut self, name: &'static str, variant_index: usize,
                              variant: &'static str)
                              -> SerializeResult<()> {
        trace!("Serializer::serialize_unit_variant()");
        Err(SerializationError::TypeMismatch("unit_variant", try!(self.expected_type_code())))
    }

    #[allow(unused_variables)]
    fn serialize_newtype_struct<T: serde::ser::Serialize>(&mut self, name: &'static str, value: T)
                                                          -> SerializeResult<()> {
        trace!("Serializer::serialize_newtype_struct()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_newtype_variant<T: serde::ser::Serialize>(&mut self, name: &str,
                                                           variant_index: usize,
                                                           variant: &'static str, value: T)
                                                           -> SerializeResult<()> {
        trace!("Serializer::serialize_newtype_variant()");
        value.serialize(self)
    }

    fn serialize_none(&mut self) -> SerializeResult<()> {
        trace!("Serializer::serialize_none()");
        match try!(self.expected_type_code()) {
            TYPEID_N_TINYINT => self.row.push(TypedValue::N_TINYINT(None)),
            TYPEID_N_SMALLINT => self.row.push(TypedValue::N_SMALLINT(None)),
            TYPEID_N_INT => self.row.push(TypedValue::N_INT(None)),
            TYPEID_N_BIGINT => self.row.push(TypedValue::N_BIGINT(None)),
            TYPEID_N_REAL => self.row.push(TypedValue::N_REAL(None)),
            TYPEID_N_DOUBLE => self.row.push(TypedValue::N_DOUBLE(None)),
            TYPEID_N_CHAR => self.row.push(TypedValue::N_CHAR(None)),
            TYPEID_N_VARCHAR => self.row.push(TypedValue::N_VARCHAR(None)),
            TYPEID_N_NCHAR => self.row.push(TypedValue::N_NCHAR(None)),
            TYPEID_N_NVARCHAR => self.row.push(TypedValue::N_NVARCHAR(None)),
            TYPEID_N_BINARY => self.row.push(TypedValue::N_BINARY(None)),
            TYPEID_N_VARBINARY => self.row.push(TypedValue::N_VARBINARY(None)),
            TYPEID_N_CLOB => self.row.push(TypedValue::N_CLOB(None)),
            TYPEID_N_NCLOB => self.row.push(TypedValue::N_NCLOB(None)),
            TYPEID_N_BLOB => self.row.push(TypedValue::N_BLOB(None)),
            TYPEID_N_BOOLEAN => self.row.push(TypedValue::N_BOOLEAN(None)),
            TYPEID_N_STRING => self.row.push(TypedValue::N_STRING(None)),
            TYPEID_N_NSTRING => self.row.push(TypedValue::N_NSTRING(None)),
            TYPEID_N_BSTRING => self.row.push(TypedValue::N_BSTRING(None)),
            TYPEID_N_TEXT => self.row.push(TypedValue::N_TEXT(None)),
            TYPEID_N_SHORTTEXT => self.row.push(TypedValue::N_SHORTTEXT(None)),
            TYPEID_N_LONGDATE => self.row.push(TypedValue::N_LONGDATE(None)),
            target_tc => return Err(SerializationError::TypeMismatch("none", target_tc)),
        }
        Ok(())
    }

    fn serialize_some<T: serde::ser::Serialize>(&mut self, value: T) -> SerializeResult<()> {
        trace!("Serializer::serialize_some()");
        value.serialize(self)
    }


    #[allow(unused_variables)]
    fn serialize_seq(&mut self, len: Option<usize>) -> SerializeResult<Self::SeqState> {
        trace!("Serializer::serialize_seq()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_seq_elt<T: serde::ser::Serialize>(&mut self, state: &mut Self::SeqState,
                                                   value: T)
                                                   -> SerializeResult<()> {
        trace!("Serializer::serialize_seq_elt()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_seq_end(&mut self, state: Self::SeqState) -> SerializeResult<()> {
        trace!("Serializer::serialize_seq_end()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_seq_fixed_size(&mut self, size: usize) -> SerializeResult<Self::SeqState> {
        trace!("Serializer::serialize_seq_fixed_size()");
        Ok(())
    }


    #[allow(unused_variables)]
    fn serialize_tuple(&mut self, len: usize) -> Result<Self::TupleState, Self::Error> {
        trace!("Serializer::serialize_tuple()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_tuple_elt<T: serde::ser::Serialize>(&mut self, state: &mut Self::TupleState,
                                                     value: T)
                                                     -> Result<(), Self::Error> {
        trace!("Serializer::serialize_tuple_elt()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_tuple_end(&mut self, state: Self::TupleState) -> Result<(), Self::Error> {
        trace!("Serializer::serialize_tuple_end()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_tuple_struct(&mut self, name: &'static str, len: usize)
                              -> Result<Self::TupleStructState, Self::Error> {
        trace!("Serializer::serialize_tuple_struct()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_tuple_struct_elt<T: serde::ser::Serialize>(&mut self,
                                                            state: &mut Self::TupleStructState,
                                                            value: T)
                                                            -> Result<(), Self::Error> {
        trace!("Serializer::serialize_tuple_struct_elt()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_tuple_struct_end(&mut self, state: Self::TupleStructState)
                                  -> Result<(), Self::Error> {
        trace!("Serializer::serialize_tuple_struct_end()");
        Ok(())
    }


    #[allow(unused_variables)]
    fn serialize_tuple_variant(&mut self, name: &'static str, variant_index: usize,
                               variant: &'static str, len: usize)
                               -> Result<Self::TupleVariantState, Self::Error> {
        trace!("Serializer::serialize_tuple_variant()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_tuple_variant_elt<T: serde::ser::Serialize>(&mut self,
                                                             state: &mut Self::TupleVariantState,
                                                             value: T)
                                                             -> Result<(), Self::Error> {
        trace!("Serializer::serialize_tuple_variant_elt()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_tuple_variant_end(&mut self, state: Self::TupleVariantState)
                                   -> Result<(), Self::Error> {
        trace!("Serializer::serialize_tuple_variant_end()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_map(&mut self, len: Option<usize>) -> Result<Self::MapState, Self::Error> {
        trace!("Serializer::serialize_map()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_map_key<T: serde::ser::Serialize>(&mut self, state: &mut Self::MapState, key: T)
                                                   -> Result<(), Self::Error> {
        trace!("Serializer::serialize_map_key()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_map_value<T: serde::ser::Serialize>(&mut self, state: &mut Self::MapState,
                                                     value: T)
                                                     -> Result<(), Self::Error> {
        trace!("Serializer::serialize_map_value()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_map_end(&mut self, state: Self::MapState) -> Result<(), Self::Error> {
        trace!("Serializer::serialize_map_end()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_struct(&mut self, name: &'static str, len: usize)
                        -> Result<Self::StructState, Self::Error> {
        trace!("Serializer::serialize_struct()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_struct_elt<V: serde::ser::Serialize>(&mut self, state: &mut Self::StructState,
                                                      key: &'static str, value: V)
                                                      -> Result<(), Self::Error> {
        trace!("Serializer::serialize_struct_elt()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_struct_end(&mut self, state: Self::StructState) -> Result<(), Self::Error> {
        trace!("Serializer::serialize_struct_end()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_struct_variant(&mut self, name: &'static str, variant_index: usize,
                                variant: &'static str, len: usize)
                                -> Result<Self::StructVariantState, Self::Error> {
        trace!("Serializer::serialize_struct_variant()");
        Ok(())
    }

    #[allow(unused_variables)]
    fn serialize_struct_variant_elt<V: serde::ser::Serialize>(&mut self,
                                                  state: &mut Self::StructVariantState,
                                                  key: &'static str, value: V)
                                                  -> Result<(), Self::Error> {
        trace!("Serializer::serialize_struct_variant_elt()");
        value.serialize(self)
    }

    #[allow(unused_variables)]
    fn serialize_struct_variant_end(&mut self, state: Self::StructVariantState)
                                    -> Result<(), Self::Error> {
        trace!("Serializer::serialize_struct_variant_end()");
        Ok(())
    }
}
