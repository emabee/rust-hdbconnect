use protocol::lowlevel::parts::parameter_metadata::{ParameterDescriptor,ParameterMetadata,ParMode};
use protocol::lowlevel::parts::parameters::ParameterRow;
use protocol::lowlevel::parts::type_id::*;
use protocol::lowlevel::parts::typed_value::TypedValue;
use super::error::{SerializationError, SerializeResult};

use serde;
use std::{u8,i16,i32,i64};

/// A structure for serializing Rust values into JSON.
pub struct Serializer<'a> {
    row: ParameterRow,
    metadata: Vec<&'a ParameterDescriptor>,
}

impl<'a> Serializer<'a> {
    #[inline]
    pub fn new(metadata: &'a ParameterMetadata) -> Self {
        let mut serializer = Serializer {
            row: ParameterRow::new(),
            metadata: Vec::<&ParameterDescriptor>::new(),
        };
        for ref pd in &metadata.descriptors {
            match pd.mode {
                ParMode::IN | ParMode::INOUT => serializer.metadata.push(&pd),
                ParMode::OUT => {},
            }
        }
        serializer
    }
    pub fn _into_row(self) -> ParameterRow {
        self.row
    }

    fn expected_type_code(&self) -> SerializeResult<u8> {
        match self.metadata.get( self.row.values.len() ) {
            Some(pd) => Ok(pd.value_type),
            None => return Err(SerializationError::StructuralMismatch("too many values specified")),
        }
    }


    /// translate the specified struct into a Row
    #[inline]
    pub fn into_row<T>(value: &T, md: &ParameterMetadata) -> SerializeResult<ParameterRow>
        where T: serde::ser::Serialize,
    {
        let mut serializer = Serializer::new(md);
        try!(value.serialize(&mut serializer));
        Ok(serializer._into_row())
    }
}

impl<'a> serde::ser::Serializer for Serializer<'a>
{
    type Error = SerializationError;

    #[inline]
    fn visit_bool(&mut self, value: bool) -> SerializeResult<()> {
        match try!(self.expected_type_code()) {
            TYPEID_BOOLEAN   => self.row.push(TypedValue::BOOLEAN(value)),
            TYPEID_N_BOOLEAN => self.row.push(TypedValue::N_BOOLEAN(Some(value))),
            target_tc  => return Err(SerializationError::TypeMismatch("boolean",target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_isize(&mut self, value: isize) -> SerializeResult<()> {
        let input_type = "isize";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if (value >= 0) && (value <= u8::MAX as isize) {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if (value >= 0) && (value <= u8::MAX as isize) {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_N_TINYINT)) }
            },

            TYPEID_SMALLINT   => {
                if (value >= 0) && (value <= i16::MAX as isize) {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_SMALLINT => {
                if (value >= 0) && (value <= i16::MAX as isize) {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_N_SMALLINT)) }
            },

            TYPEID_INT        => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT      => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_i8(&mut self, value: i8) -> SerializeResult<()> {
        let input_type = "i8";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if value >= 0 {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if value >= 0 {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_SMALLINT   => self.row.push(TypedValue::SMALLINT(value as i16)),
            TYPEID_N_SMALLINT => self.row.push(TypedValue::N_SMALLINT(Some(value as i16))),
            TYPEID_INT        => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT      => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_i16(&mut self, value: i16) -> SerializeResult<()> {
        let input_type = "i16";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if (value >= 0) && (value <= u8::MAX as i16) {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if (value >= 0) && (value <= u8::MAX as i16) {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_SMALLINT   => self.row.push(TypedValue::SMALLINT(value)),
            TYPEID_N_SMALLINT => self.row.push(TypedValue::N_SMALLINT(Some(value))),
            TYPEID_INT        => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT      => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_i32(&mut self, value: i32) -> SerializeResult<()> {
        let input_type = "i32";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if (value >= 0) && (value <= u8::MAX as i32) {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if (value >= 0) && (value <= u8::MAX as i32) {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_SMALLINT   => {
                if (value >= i16::MIN as i32) && (value <= i16::MAX as i32) {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_SMALLINT => {
                if (value >= i16::MIN as i32) && (value <= i16::MAX as i32) {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_N_SMALLINT)) }
            },
            TYPEID_INT        => self.row.push(TypedValue::INT(value)),
            TYPEID_N_INT      => self.row.push(TypedValue::N_INT(Some(value))),
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_i64(&mut self, value: i64) -> SerializeResult<()> {
        let input_type = "i64";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if (value >= 0) && (value <= u8::MAX as i64) {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if (value >= 0) && (value <= u8::MAX as i64) {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_SMALLINT   => {
                if (value >= i16::MIN as i64) && (value <= i16::MAX as i64) {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_SMALLINT => {
                if (value >= i16::MIN as i64) && (value <= i16::MAX as i64) {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_N_SMALLINT)) }
            },
            TYPEID_INT   => {
                if (value >= i32::MIN as i64) && (value <= i32::MAX as i64) {
                    self.row.push(TypedValue::INT(value as i32))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_INT => {
                if (value >= i32::MIN as i64) && (value <= i32::MAX as i64) {
                    self.row.push(TypedValue::N_INT(Some(value as i32)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_N_SMALLINT)) }
            },
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_usize(&mut self, value: usize) -> SerializeResult<()> {
        let input_type = "usize";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if value <= u8::MAX as usize {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if value <= u8::MAX as usize {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_SMALLINT   => {
                if value <= i16::MAX as usize {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as usize {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_N_SMALLINT)) }
            },
            TYPEID_INT   => {
                if value <= i32::MAX as usize {
                    self.row.push(TypedValue::INT(value as i32))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_INT => {
                if value <= i32::MAX as usize {
                    self.row.push(TypedValue::N_INT(Some(value as i32)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_N_SMALLINT)) }
            },
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_u8(&mut self, value: u8) -> SerializeResult<()> {
        let input_type = "u8";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => self.row.push(TypedValue::TINYINT(value)),
            TYPEID_N_TINYINT  => self.row.push(TypedValue::N_TINYINT(Some(value))),
            TYPEID_SMALLINT   => self.row.push(TypedValue::SMALLINT(value as i16)),
            TYPEID_N_SMALLINT => self.row.push(TypedValue::N_SMALLINT(Some(value as i16))),
            TYPEID_INT        => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT      => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_u16(&mut self, value: u16) -> SerializeResult<()> {
        let input_type = "u16";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if value <= u8::MAX as u16  {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if value <= u8::MAX as u16  {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_SMALLINT    => {
                if value <= i16::MAX as u16  {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_SMALLINT  => {
                if value <= i16::MAX as u16  {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_INT        => self.row.push(TypedValue::INT(value as i32)),
            TYPEID_N_INT      => self.row.push(TypedValue::N_INT(Some(value as i32))),
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_u32(&mut self, value: u32) -> SerializeResult<()> {
        let input_type = "u32";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if value <= u8::MAX as u32  {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if value <= u8::MAX as u32  {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_SMALLINT    => {
                if value <= i16::MAX as u32  {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_SMALLINT  => {
                if value <= i16::MAX as u32  {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_INT    => {
                if value <= i32::MAX as u32  {
                    self.row.push(TypedValue::INT(value as i32))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_INT)) }
            },
            TYPEID_N_INT  => {
                if value <= i32::MAX as u32  {
                    self.row.push(TypedValue::N_INT(Some(value as i32)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_INT)) }
            },
            TYPEID_BIGINT     => self.row.push(TypedValue::BIGINT(value as i64)),
            TYPEID_N_BIGINT   => self.row.push(TypedValue::N_BIGINT(Some(value as i64))),
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_u64(&mut self, value: u64) -> SerializeResult<()> {
        let input_type = "u64";
        match try!(self.expected_type_code()) {
            TYPEID_TINYINT    => {
                if value <= u8::MAX as u64  {
                    self.row.push(TypedValue::TINYINT(value as u8))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_N_TINYINT  => {
                if value <= u8::MAX as u64  {
                    self.row.push(TypedValue::N_TINYINT(Some(value as u8)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_SMALLINT    => {
                if value <= i16::MAX as u64  {
                    self.row.push(TypedValue::SMALLINT(value as i16))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_SMALLINT)) }
            },
            TYPEID_N_SMALLINT  => {
                if value <= i16::MAX as u64  {
                    self.row.push(TypedValue::N_SMALLINT(Some(value as i16)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_TINYINT)) }
            },
            TYPEID_INT    => {
                if value <= i32::MAX as u64  {
                    self.row.push(TypedValue::INT(value as i32))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_INT)) }
            },
            TYPEID_N_INT  => {
                if value <= i32::MAX as u64  {
                    self.row.push(TypedValue::N_INT(Some(value as i32)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_INT)) }
            },
            TYPEID_BIGINT    => {
                if value <= i64::MAX as u64  {
                    self.row.push(TypedValue::BIGINT(value as i64))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_BIGINT)) }
            },
            TYPEID_N_BIGINT  => {
                if value <= i64::MAX as u64  {
                    self.row.push(TypedValue::N_BIGINT(Some(value as i64)))
                } else { return Err(SerializationError::RangeErr(input_type,TYPEID_BIGINT)) }
            },
            target_tc  => return Err(SerializationError::TypeMismatch(input_type, target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_f32(&mut self, value: f32) -> SerializeResult<()> {
        match try!(self.expected_type_code()) {
            TYPEID_REAL   => self.row.push(TypedValue::REAL(value)),
            TYPEID_N_REAL => self.row.push(TypedValue::N_REAL(Some(value))),
            target_tc  => return Err(SerializationError::TypeMismatch("f32",target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_f64(&mut self, value: f64) -> SerializeResult<()> {
        match try!(self.expected_type_code()) {
            TYPEID_DOUBLE   => self.row.push(TypedValue::DOUBLE(value)),
            TYPEID_N_DOUBLE => self.row.push(TypedValue::N_DOUBLE(Some(value))),
            target_tc  => return Err(SerializationError::TypeMismatch("f64",target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_char(&mut self, value: char) -> SerializeResult<()> {
        let mut s = String::new();
        s.push(value);
        match try!(self.expected_type_code()) {
            TYPEID_CHAR       => self.row.push(TypedValue::STRING(s)),      //CHAR(s)),
            TYPEID_VARCHAR    => self.row.push(TypedValue::STRING(s)),      //VARCHAR(s)),
            TYPEID_NCHAR      => self.row.push(TypedValue::STRING(s)),      //NCHAR(s)),
            TYPEID_NVARCHAR   => self.row.push(TypedValue::STRING(s)),      //NVARCHAR(s)),
            TYPEID_STRING     => self.row.push(TypedValue::STRING(s)),      //STRING(s)),
            TYPEID_NSTRING    => self.row.push(TypedValue::STRING(s)),      //NSTRING(s)),
            TYPEID_TEXT       => self.row.push(TypedValue::STRING(s)),      //TEXT(s)),
            TYPEID_SHORTTEXT  => self.row.push(TypedValue::STRING(s)),      //SHORTTEXT(s)),
            target_tc  => return Err(SerializationError::TypeMismatch("char",target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_str(&mut self, value: &str) -> SerializeResult<()> {
        let s = String::from(value);
        match try!(self.expected_type_code()) {
            TYPEID_CHAR       => self.row.push(TypedValue::STRING(s)),      //CHAR(s)),
            TYPEID_VARCHAR    => self.row.push(TypedValue::STRING(s)),      //VARCHAR(s)),
            TYPEID_NCHAR      => self.row.push(TypedValue::STRING(s)),      //NCHAR(s)),
            TYPEID_NVARCHAR   => self.row.push(TypedValue::STRING(s)),      //NVARCHAR(s)),
            TYPEID_STRING     => self.row.push(TypedValue::STRING(s)),      //STRING(s)),
            TYPEID_NSTRING    => self.row.push(TypedValue::STRING(s)),      //NSTRING(s)),
            TYPEID_TEXT       => self.row.push(TypedValue::STRING(s)),      //TEXT(s)),
            TYPEID_SHORTTEXT  => self.row.push(TypedValue::STRING(s)),      //SHORTTEXT(s)),
            target_tc  => return Err(SerializationError::TypeMismatch("&str",target_tc)),
        }
        Ok(())
    }

    #[inline]
    fn visit_none(&mut self) -> SerializeResult<()> {
        match try!(self.expected_type_code()) {
            TYPEID_N_TINYINT    => self.row.push(TypedValue::N_TINYINT(None)),
            TYPEID_N_SMALLINT   => self.row.push(TypedValue::N_SMALLINT(None)),
            TYPEID_N_INT        => self.row.push(TypedValue::N_INT(None)),
            TYPEID_N_BIGINT     => self.row.push(TypedValue::N_BIGINT(None)),
            TYPEID_N_REAL       => self.row.push(TypedValue::N_REAL(None)),
            TYPEID_N_DOUBLE     => self.row.push(TypedValue::N_DOUBLE(None)),
            TYPEID_N_CHAR       => self.row.push(TypedValue::N_CHAR(None)),
            TYPEID_N_VARCHAR    => self.row.push(TypedValue::N_VARCHAR(None)),
            TYPEID_N_NCHAR      => self.row.push(TypedValue::N_NCHAR(None)),
            TYPEID_N_NVARCHAR   => self.row.push(TypedValue::N_NVARCHAR(None)),
            TYPEID_N_BINARY     => self.row.push(TypedValue::N_BINARY(None)),
            TYPEID_N_VARBINARY  => self.row.push(TypedValue::N_VARBINARY(None)),
            TYPEID_N_CLOB       => self.row.push(TypedValue::N_CLOB(None)),
            TYPEID_N_NCLOB      => self.row.push(TypedValue::N_NCLOB(None)),
            TYPEID_N_BLOB       => self.row.push(TypedValue::N_BLOB(None)),
            TYPEID_N_BOOLEAN    => self.row.push(TypedValue::N_BOOLEAN(None)),
            TYPEID_N_STRING     => self.row.push(TypedValue::N_STRING(None)),
            TYPEID_N_NSTRING    => self.row.push(TypedValue::N_NSTRING(None)),
            TYPEID_N_BSTRING    => self.row.push(TypedValue::N_BSTRING(None)),
            TYPEID_N_TEXT       => self.row.push(TypedValue::N_TEXT(None)),
            TYPEID_N_SHORTTEXT  => self.row.push(TypedValue::N_SHORTTEXT(None)),
            TYPEID_N_LONGDATE   => self.row.push(TypedValue::N_LONGDATE(None)),
            target_tc  => return Err(SerializationError::TypeMismatch("&str",target_tc)),
        }
        Ok(())
    }

    #[inline]
    #[allow(unused_variables)]
    fn visit_some<V>(&mut self, value: V) -> SerializeResult<()>
        where V: serde::ser::Serialize
    {
        value.serialize(self)
    }

    #[inline]
    #[allow(unused_variables)]
    fn visit_unit(&mut self) -> SerializeResult<()> {
        Err(SerializationError::TypeMismatch("unit",try!(self.expected_type_code())))
    }

    /// Override `visit_newtype_struct` to serialize newtypes without an object wrapper.
    #[inline]
    #[allow(unused_variables)]
    fn visit_newtype_struct<T>(&mut self,
                               _name: &'static str,
                               value: T) -> SerializeResult<()>
        where T: serde::ser::Serialize,
    {
        value.serialize(self)
    }

    #[inline]
    #[allow(unused_variables)]
    fn visit_unit_variant(&mut self,
                          _name: &str,
                          _variant_index: usize,
                          variant: &str) -> SerializeResult<()> {
        Err(SerializationError::TypeMismatch("unit_variant",try!(self.expected_type_code())))
    }

    #[inline]
    #[allow(unused_variables)]
    fn visit_newtype_variant<T>(&mut self,
                                _name: &str,
                                _variant_index: usize,
                                variant: &str,
                                value: T) -> SerializeResult<()>
        where T: serde::ser::Serialize,
    {
        Err(SerializationError::TypeMismatch("newtype_variant",try!(self.expected_type_code())))
    }

    #[inline]
    #[allow(unused_mut,unused_variables)]
    fn visit_seq<V>(&mut self, mut visitor: V) -> SerializeResult<()>
        where V: serde::ser::SeqVisitor,
    {
        Err(SerializationError::TypeMismatch("seq",try!(self.expected_type_code())))
    }

    #[inline]
    #[allow(unused_variables)]
    fn visit_tuple_variant<V>(&mut self,
                              _name: &str,
                              _variant_index: usize,
                              variant: &str,
                              visitor: V) -> SerializeResult<()>
        where V: serde::ser::SeqVisitor,
    {
        Err(SerializationError::TypeMismatch("tuple_variant",try!(self.expected_type_code())))
    }

    #[inline]
    #[allow(unused_variables)]
    fn visit_seq_elt<T>(&mut self, value: T) -> SerializeResult<()>
        where T: serde::ser::Serialize,
    {
        Err(SerializationError::TypeMismatch("seq_elt",try!(self.expected_type_code())))
    }

    #[inline]
    #[allow(unused_mut,unused_variables)]
    fn visit_map<V>(&mut self, mut visitor: V) -> SerializeResult<()>
        where V: serde::ser::MapVisitor,
    {
        // match visitor.len() {
        //     Some(len) if len == 0 => {
        //         self.writer.write_all(b"{}").map_err(From::from)
        //     }
        //     _ => {
        //         try!(self.formatter.open(&mut self.writer, b'{'));
        //
        //         self.first = true;
        //
                while let Some(()) = try!(visitor.visit(self)) { }  // FIXME why do we need a loop here?
                Ok(())
        //
        //         self.formatter.close(&mut self.writer, b'}')
        //     }
        // }
    }

    #[inline]
    #[allow(unused_variables)]
    fn visit_struct_variant<V>(&mut self,
                               _name: &str,
                               _variant_index: usize,
                               variant: &str,
                               visitor: V) -> SerializeResult<()>
        where V: serde::ser::MapVisitor,
    {
        Err(SerializationError::TypeMismatch("struct_variant",try!(self.expected_type_code())))
    }

    #[inline]
    #[allow(unused_variables)]
    fn visit_map_elt<K, V>(&mut self, key: K, value: V) -> SerializeResult<()>
        where K: serde::ser::Serialize,
              V: serde::ser::Serialize,
    {
        // try!(key.serialize(&mut MapKeySerializer { ser: self }));
        try!(value.serialize(self));
        Ok(())
    }
}

// struct MapKeySerializer<'a> {
//     ser: &'a mut Serializer,
// }
//
// impl<'a> serde::ser::Serializer for MapKeySerializer<'a> {
//     type Error = SerializationError;
//
//     #[inline]
//     fn visit_str(&mut self, value: &str) -> SerializeResult<()> {
//         self.ser.visit_str(value)
//     }
//
//     fn visit_bool(&mut self, _value: bool) -> SerializeResult<()> {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_i64(&mut self, _value: i64) -> SerializeResult<()> {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_u64(&mut self, _value: u64) -> SerializeResult<()> {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_f64(&mut self, _value: f64) -> SerializeResult<()> {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_unit(&mut self) -> SerializeResult<()> {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_none(&mut self) -> SerializeResult<()> {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_some<V>(&mut self, _value: V) -> SerializeResult<()>
//         where V: serde::ser::Serialize
//     {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_seq<V>(&mut self, _visitor: V) -> SerializeResult<()>
//         where V: serde::ser::SeqVisitor,
//     {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_seq_elt<T>(&mut self, _value: T) -> SerializeResult<()>
//         where T: serde::ser::Serialize,
//     {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_map<V>(&mut self, _visitor: V) -> SerializeResult<()>
//         where V: serde::ser::MapVisitor,
//     {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
//
//     fn visit_map_elt<K, V>(&mut self, _key: K, _value: V) -> SerializeResult<()>
//         where K: serde::ser::Serialize,
//               V: serde::ser::Serialize,
//     {
//         Err(Error::SyntaxError(ErrorCode::KeyMustBeAString, 0, 0))
//     }
// }

// /// Serializes and escapes a `&[u8]` into a JSON string.
// #[inline]
// pub fn escape_bytes<W>(wr: &mut W, bytes: &[u8]) -> SerializeResult<()>
//     where W: io::Write
// {
//     try!(wr.write_all(b"\""));
//
//     let mut start = 0;
//
//     for (i, byte) in bytes.iter().enumerate() {
//         let escaped = match *byte {
//             b'"' => b"\\\"",
//             b'\\' => b"\\\\",
//             b'\x08' => b"\\b",
//             b'\x0c' => b"\\f",
//             b'\n' => b"\\n",
//             b'\r' => b"\\r",
//             b'\t' => b"\\t",
//             _ => { continue; }
//         };
//
//         if start < i {
//             try!(wr.write_all(&bytes[start..i]));
//         }
//
//         try!(wr.write_all(escaped));
//
//         start = i + 1;
//     }
//
//     if start != bytes.len() {
//         try!(wr.write_all(&bytes[start..]));
//     }
//
//     try!(wr.write_all(b"\""));
//     Ok(())
// }
//
// /// Serializes and escapes a `&str` into a JSON string.
// #[inline]
// pub fn escape_str<W>(wr: &mut W, value: &str) -> SerializeResult<()>
//     where W: io::Write
// {
//     escape_bytes(wr, value.as_bytes())
// }
//
// #[inline]
// fn escape_char<W>(wr: &mut W, value: char) -> SerializeResult<()>
//     where W: io::Write
// {
//     // FIXME: this allocation is required in order to be compatible with stable
//     // rust, which doesn't support encoding a `char` into a stack buffer.
//     let mut s = String::new();
//     s.push(value);
//     escape_bytes(wr, s.as_bytes())
// }
//
// fn fmt_f32_or_null<W>(wr: &mut W, value: f32) -> SerializeResult<()>
//     where W: io::Write
// {
//     match value.classify() {
//         FpCategory::Nan | FpCategory::Infinite => {
//             try!(wr.write_all(b"null"))
//         }
//         _ => {
//             try!(write!(wr, "{:?}", value))
//         }
//     }
//
//     Ok(())
// }
//
// fn fmt_f64_or_null<W>(wr: &mut W, value: f64) -> SerializeResult<()>
//     where W: io::Write
// {
//     match value.classify() {
//         FpCategory::Nan | FpCategory::Infinite => {
//             try!(wr.write_all(b"null"))
//         }
//         _ => {
//             try!(write!(wr, "{:?}", value))
//         }
//     }
//
//     Ok(())
// }
