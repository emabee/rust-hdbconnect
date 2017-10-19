use super::{PrtResult, prot_err, util};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io;
use std::{u8, u16, u32, i8, i16, i32, i64};

use serde_db::ser::{DbvFactory, SerializationError};
use protocol::lowlevel::parts::hdbdate::longdate_from_str;
use protocol::lowlevel::parts::longdate::LongDate;
use protocol::lowlevel::parts::lob::new_blob_to_db;
use protocol::lowlevel::parts::typed_value::TypedValue;
use protocol::lowlevel::parts::type_id::*;


#[derive(Clone,Debug)]
pub struct ParameterMetadata {
    pub descriptors: Vec<ParameterDescriptor>,
}
impl ParameterMetadata {
    fn new() -> ParameterMetadata {
        ParameterMetadata { descriptors: Vec::<ParameterDescriptor>::new() }
    }
}

/// Metadata for a parameter.
#[derive(Clone,Debug)]
pub struct ParameterDescriptor {
    /// bit 0: mandatory; 1: optional, 2: has_default
    pub option: ParameterOption,
    /// value type
    pub value_type: u8,
    /// Scale of the parameter
    pub fraction: u16,
    /// length/precision of the parameter
    pub length: u16,
    /// whether the parameter is input or output
    pub mode: ParMode,
    /// Offset of parameter name in part, set to 0xFFFFFFFF to signal no name
    pub name_offset: u32,
    /// Name
    pub name: String,
}
impl ParameterDescriptor {
    fn new(option: ParameterOption, value_type: u8, mode: ParMode, name_offset: u32, length: u16,
           fraction: u16)
           -> ParameterDescriptor {
        ParameterDescriptor {
            option: option,
            value_type: value_type,
            mode: mode,
            name_offset: name_offset,
            length: length,
            fraction: fraction,
            name: String::new(),
        }
    }
}
impl DbvFactory for ParameterDescriptor {
    type DBV = TypedValue;

    fn from_bool(&self, value: bool) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_BOOLEAN => TypedValue::BOOLEAN(value),
            TYPEID_N_BOOLEAN => TypedValue::N_BOOLEAN(Some(value)),
            _ => return Err(SerializationError::TypeMismatch("boolean", self.descriptor())),
        })
    }

    fn from_i8(&self, value: i8) -> Result<TypedValue, SerializationError> {
        let input_type = "i8";
        Ok(match self.value_type {
            TYPEID_TINYINT => {
                if value >= 0 {
                    TypedValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value >= 0 {
                    TypedValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => TypedValue::SMALLINT(value as i16),
            TYPEID_N_SMALLINT => TypedValue::N_SMALLINT(Some(value as i16)),
            TYPEID_INT => TypedValue::INT(value as i32),
            TYPEID_N_INT => TypedValue::N_INT(Some(value as i32)),
            TYPEID_BIGINT => TypedValue::BIGINT(value as i64),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(value as i64)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i16(&self, value: i16) -> Result<TypedValue, SerializationError> {
        let input_type = "i16";
        Ok(match self.value_type {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i16) {
                    TypedValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i16) {
                    TypedValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => TypedValue::SMALLINT(value),
            TYPEID_N_SMALLINT => TypedValue::N_SMALLINT(Some(value)),
            TYPEID_INT => TypedValue::INT(value as i32),
            TYPEID_N_INT => TypedValue::N_INT(Some(value as i32)),
            TYPEID_BIGINT => TypedValue::BIGINT(value as i64),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(value as i64)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i32(&self, value: i32) -> Result<TypedValue, SerializationError> {
        let input_type = "i32";
        Ok(match self.value_type {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i32) {
                    TypedValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i32) {
                    TypedValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if (value >= i16::MIN as i32) && (value <= i16::MAX as i32) {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= i16::MIN as i32) && (value <= i16::MAX as i32) {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_INT => TypedValue::INT(value),
            TYPEID_N_INT => TypedValue::N_INT(Some(value)),
            TYPEID_BIGINT => TypedValue::BIGINT(value as i64),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(value as i64)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_i64(&self, value: i64) -> Result<TypedValue, SerializationError> {
        let input_type = "i64";
        Ok(match self.value_type {
            TYPEID_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i64) {
                    TypedValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if (value >= 0) && (value <= u8::MAX as i64) {
                    TypedValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if (value >= i16::MIN as i64) && (value <= i16::MAX as i64) {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if (value >= i16::MIN as i64) && (value <= i16::MAX as i64) {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_INT => {
                if (value >= i32::MIN as i64) && (value <= i32::MAX as i64) {
                    TypedValue::INT(value as i32)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_INT => {
                if (value >= i32::MIN as i64) && (value <= i32::MAX as i64) {
                    TypedValue::N_INT(Some(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_N_SMALLINT));
                }
            }
            TYPEID_BIGINT => TypedValue::BIGINT(value),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(value)),
            TYPEID_LONGDATE => TypedValue::LONGDATE(LongDate(value)),
            TYPEID_N_LONGDATE => TypedValue::N_LONGDATE(Some(LongDate(value))),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u8(&self, value: u8) -> Result<TypedValue, SerializationError> {
        let input_type = "u8";
        Ok(match self.value_type {
            TYPEID_TINYINT => TypedValue::TINYINT(value),
            TYPEID_N_TINYINT => TypedValue::N_TINYINT(Some(value)),
            TYPEID_SMALLINT => TypedValue::SMALLINT(value as i16),
            TYPEID_N_SMALLINT => TypedValue::N_SMALLINT(Some(value as i16)),
            TYPEID_INT => TypedValue::INT(value as i32),
            TYPEID_N_INT => TypedValue::N_INT(Some(value as i32)),
            TYPEID_BIGINT => TypedValue::BIGINT(value as i64),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(value as i64)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u16(&self, value: u16) -> Result<TypedValue, SerializationError> {
        let input_type = "u16";
        Ok(match self.value_type {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u16 {
                    TypedValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u16 {
                    TypedValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u16 {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u16 {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => TypedValue::INT(value as i32),
            TYPEID_N_INT => TypedValue::N_INT(Some(value as i32)),
            TYPEID_BIGINT => TypedValue::BIGINT(value as i64),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(value as i64)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u32(&self, value: u32) -> Result<TypedValue, SerializationError> {
        let input_type = "u32";
        Ok(match self.value_type {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u32 {
                    TypedValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u32 {
                    TypedValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u32 {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u32 {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => {
                if value <= i32::MAX as u32 {
                    TypedValue::INT(value as i32)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_N_INT => {
                if value <= i32::MAX as u32 {
                    TypedValue::N_INT(Some(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_BIGINT => TypedValue::BIGINT(value as i64),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(Some(value as i64)),
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_u64(&self, value: u64) -> Result<TypedValue, SerializationError> {
        let input_type = "u64";
        Ok(match self.value_type {
            TYPEID_TINYINT => {
                if value <= u8::MAX as u64 {
                    TypedValue::TINYINT(value as u8)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_N_TINYINT => {
                if value <= u8::MAX as u64 {
                    TypedValue::N_TINYINT(Some(value as u8))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_SMALLINT => {
                if value <= i16::MAX as u64 {
                    TypedValue::SMALLINT(value as i16)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_SMALLINT));
                }
            }
            TYPEID_N_SMALLINT => {
                if value <= i16::MAX as u64 {
                    TypedValue::N_SMALLINT(Some(value as i16))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_TINYINT));
                }
            }
            TYPEID_INT => {
                if value <= i32::MAX as u64 {
                    TypedValue::INT(value as i32)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_N_INT => {
                if value <= i32::MAX as u64 {
                    TypedValue::N_INT(Some(value as i32))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_INT));
                }
            }
            TYPEID_BIGINT => {
                if value <= i64::MAX as u64 {
                    TypedValue::BIGINT(value as i64)
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_BIGINT));
                }
            }
            TYPEID_N_BIGINT => {
                if value <= i64::MAX as u64 {
                    TypedValue::N_BIGINT(Some(value as i64))
                } else {
                    return Err(SerializationError::RangeErr(input_type, TYPEID_BIGINT));
                }
            }
            _ => return Err(SerializationError::TypeMismatch(input_type, self.descriptor())),
        })
    }
    fn from_f32(&self, value: f32) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_REAL => TypedValue::REAL(value),
            TYPEID_N_REAL => TypedValue::N_REAL(Some(value)),
            _ => return Err(SerializationError::TypeMismatch("f32", self.descriptor())),
        })
    }
    fn from_f64(&self, value: f64) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_DOUBLE => TypedValue::DOUBLE(value),
            TYPEID_N_DOUBLE => TypedValue::N_DOUBLE(Some(value)),
            _ => return Err(SerializationError::TypeMismatch("f64", self.descriptor())),
        })
    }
    fn from_char(&self, value: char) -> Result<TypedValue, SerializationError> {
        let mut s = String::new();
        s.push(value);
        Ok(match self.value_type {
            TYPEID_CHAR | TYPEID_VARCHAR | TYPEID_NCHAR | TYPEID_NVARCHAR | TYPEID_STRING |
            TYPEID_NSTRING | TYPEID_TEXT | TYPEID_SHORTTEXT => TypedValue::STRING(s),
            _ => return Err(SerializationError::TypeMismatch("char", self.descriptor())),
        })
    }
    fn from_str(&self, value: &str) -> Result<TypedValue, SerializationError> {
        let s = String::from(value);
        Ok(match self.value_type {
            TYPEID_CHAR | TYPEID_VARCHAR | TYPEID_NCHAR | TYPEID_NVARCHAR | TYPEID_STRING |
            TYPEID_NSTRING | TYPEID_TEXT | TYPEID_SHORTTEXT | TYPEID_N_CLOB | TYPEID_N_NCLOB |
            TYPEID_NCLOB | TYPEID_CLOB => TypedValue::STRING(s),
            TYPEID_LONGDATE => TypedValue::LONGDATE(longdate_from_str(value)?),

            _ => return Err(SerializationError::TypeMismatch("&str", self.descriptor())),
        })
    }
    fn from_bytes(&self, value: &[u8]) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_BLOB => TypedValue::BLOB(new_blob_to_db((*value).to_vec())),
            TYPEID_N_BLOB => TypedValue::N_BLOB(Some(new_blob_to_db((*value).to_vec()))),
            _ => return Err(SerializationError::TypeMismatch("bytes", self.descriptor())),
        })
    }
    fn from_none(&self) -> Result<TypedValue, SerializationError> {
        Ok(match self.value_type {
            TYPEID_N_TINYINT => TypedValue::N_TINYINT(None),
            TYPEID_N_SMALLINT => TypedValue::N_SMALLINT(None),
            TYPEID_N_INT => TypedValue::N_INT(None),
            TYPEID_N_BIGINT => TypedValue::N_BIGINT(None),
            TYPEID_N_REAL => TypedValue::N_REAL(None),
            TYPEID_N_DOUBLE => TypedValue::N_DOUBLE(None),
            TYPEID_N_CHAR => TypedValue::N_CHAR(None),
            TYPEID_N_VARCHAR => TypedValue::N_VARCHAR(None),
            TYPEID_N_NCHAR => TypedValue::N_NCHAR(None),
            TYPEID_N_NVARCHAR => TypedValue::N_NVARCHAR(None),
            TYPEID_N_BINARY => TypedValue::N_BINARY(None),
            TYPEID_N_VARBINARY => TypedValue::N_VARBINARY(None),
            TYPEID_N_CLOB => TypedValue::N_CLOB(None),
            TYPEID_N_NCLOB => TypedValue::N_NCLOB(None),
            TYPEID_N_BLOB => TypedValue::N_BLOB(None),
            TYPEID_N_BOOLEAN => TypedValue::N_BOOLEAN(None),
            TYPEID_N_STRING => TypedValue::N_STRING(None),
            TYPEID_N_NSTRING => TypedValue::N_NSTRING(None),
            TYPEID_N_BSTRING => TypedValue::N_BSTRING(None),
            TYPEID_N_TEXT => TypedValue::N_TEXT(None),
            TYPEID_N_SHORTTEXT => TypedValue::N_SHORTTEXT(None),
            TYPEID_N_LONGDATE => TypedValue::N_LONGDATE(None),
            _ => return Err(SerializationError::TypeMismatch("none", self.descriptor())),
        })
    }

    fn descriptor(&self) -> String {
        String::from(match self.value_type {
            TYPEID_N_TINYINT => "Nullable TINYINT",
            TYPEID_TINYINT => "TINYINT",
            TYPEID_N_SMALLINT => "Nullable SMALLINT",
            TYPEID_SMALLINT => "SMALLINT",
            TYPEID_N_INT => "Nullable INT",
            TYPEID_INT => "INT",
            TYPEID_N_BIGINT => "Nullable BIGINT",
            TYPEID_BIGINT => "BIGINT",
            TYPEID_N_REAL => "Nullable REAL",
            TYPEID_REAL => "REAL",
            TYPEID_N_DOUBLE => "Nullable DOUBLE",
            TYPEID_DOUBLE => "DOUBLE",
            TYPEID_N_CHAR => "Nullable CHAR",
            TYPEID_CHAR => "CHAR",
            TYPEID_N_VARCHAR => "Nullable VARCHAR",
            TYPEID_VARCHAR => "VARCHAR",
            TYPEID_N_NCHAR => "Nullable NCHAR",
            TYPEID_NCHAR => "NCHAR",
            TYPEID_N_NVARCHAR => "Nullable NVARCHAR",
            TYPEID_NVARCHAR => "NVARCHAR",
            TYPEID_N_BINARY => "Nullable BINARY",
            TYPEID_BINARY => "BINARY",
            TYPEID_N_VARBINARY => "Nullable VARBINARY",
            TYPEID_VARBINARY => "VARBINARY",
            TYPEID_N_CLOB => "Nullable CLOB",
            TYPEID_CLOB => "CLOB",
            TYPEID_N_NCLOB => "Nullable NCLOB",
            TYPEID_NCLOB => "NCLOB",
            TYPEID_N_BLOB => "Nullable BLOB",
            TYPEID_BLOB => "BLOB",
            TYPEID_N_BOOLEAN => "Nullable BOOLEAN",
            TYPEID_BOOLEAN => "BOOLEAN",
            TYPEID_N_STRING => "Nullable STRING",
            TYPEID_STRING => "STRING",
            TYPEID_N_NSTRING => "Nullable NSTRING",
            TYPEID_NSTRING => "NSTRING",
            TYPEID_N_BSTRING => "Nullable BSTRING",
            TYPEID_BSTRING => "BSTRING",
            TYPEID_N_TEXT => "Nullable TEXT",
            TYPEID_TEXT => "TEXT",
            TYPEID_N_SHORTTEXT => "Nullable SHORTTEXT",
            TYPEID_SHORTTEXT => "SHORTTEXT",
            TYPEID_N_LONGDATE => "Nullable LONGDATE",
            TYPEID_LONGDATE => "LONGDATE",
            i => return format!("no descriptor available for {}", i),
        })
    }
}

impl ParameterMetadata {
    pub fn parse(count: i32, arg_size: u32, rdr: &mut io::BufRead) -> PrtResult<ParameterMetadata> {
        let mut consumed = 0;
        let mut pmd = ParameterMetadata::new();
        for _ in 0..count {
            // 16 byte each
            let option = ParameterOption::from_u8(rdr.read_u8()?)?;
            let value_type = rdr.read_u8()?;
            let mode = ParMode::from_u8(rdr.read_u8()?)?;
            rdr.read_u8()?;
            let name_offset = rdr.read_u32::<LittleEndian>()?;
            let length = rdr.read_u16::<LittleEndian>()?;
            let fraction = rdr.read_u16::<LittleEndian>()?;
            rdr.read_u32::<LittleEndian>()?;
            consumed += 16;
            assert!(arg_size >= consumed);
            pmd.descriptors.push(ParameterDescriptor::new(option,
                                                          value_type,
                                                          mode,
                                                          name_offset,
                                                          length,
                                                          fraction));
        }
        // read the parameter names
        for ref mut descriptor in &mut pmd.descriptors {
            if descriptor.name_offset != u32::MAX {
                let length = rdr.read_u8()?;
                let name = util::cesu8_to_string(&util::parse_bytes(length as usize, rdr)?)?;
                descriptor.name.push_str(&name);
                consumed += 1 + length as u32;
                assert!(arg_size >= consumed);
            }
        }

        Ok(pmd)
    }
}

/// Describes whether a parameter is Nullable or not or if it has even d default value.
#[derive(Clone,Debug)]
pub enum ParameterOption {
    /// Parameter can be Null.
    Nullable,
    /// A value must be specified.
    NotNull,
    /// A value is given if no value is given explicitly
    HasDefault,
}
impl ParameterOption {
    /// check if the parameter is nullable
    pub fn is_nullable(&self) -> bool {
        match *self {
            ParameterOption::Nullable => true,
            _ => false,
        }
    }

    fn from_u8(val: u8) -> PrtResult<ParameterOption> {
        match val {
            1 => Ok(ParameterOption::NotNull),
            2 => Ok(ParameterOption::Nullable),
            4 => Ok(ParameterOption::HasDefault),
            _ => {
                Err(prot_err(&format!("ParameterOption::from_u8() not implemented for value {}",
                                      val)))
            }
        }
    }
}

/// Describes whether a parameter is used for input, output, or both.
#[derive(Clone,Debug)]
pub enum ParMode {
    /// input parameter
    IN,
    /// input and output parameter
    INOUT,
    /// output parameter
    OUT,
}
impl ParMode {
    fn from_u8(v: u8) -> PrtResult<ParMode> {
        match v {
            1 => Ok(ParMode::IN),
            2 => Ok(ParMode::INOUT),
            4 => Ok(ParMode::OUT),
            _ => Err(prot_err(&format!("invalid value for ParMode: {}", v))),
        }
    }
}
