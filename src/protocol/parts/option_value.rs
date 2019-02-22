use crate::protocol::util;
use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use cesu8;
use std::io;

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, PartialEq)]
pub enum OptionValue {
    INT(i32),         // INTEGER
    BIGINT(i64),      // BIGINT
    DOUBLE(f64),      // DOUBLE
    BOOLEAN(bool),    // Boolean
    STRING(String),   // Character string
    BSTRING(Vec<u8>), // Binary string
}

impl OptionValue {
    pub fn emit<T: io::Write>(&self, w: &mut T) -> HdbResult<()> {
        w.write_u8(self.type_id())?; // I1
        match *self {
            // variable
            OptionValue::INT(i) => w.write_i32::<LittleEndian>(i)?,
            OptionValue::BIGINT(i) => w.write_i64::<LittleEndian>(i)?,
            OptionValue::DOUBLE(f) => w.write_f64::<LittleEndian>(f)?,
            OptionValue::BOOLEAN(b) => w.write_u8(if b { 1 } else { 0 })?,
            OptionValue::STRING(ref s) => emit_length_and_string(s, w)?,
            OptionValue::BSTRING(ref v) => emit_length_and_bytes(v, w)?,
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        1 + match *self {
            OptionValue::INT(_) => 4,
            OptionValue::BIGINT(_) | OptionValue::DOUBLE(_) => 8,
            OptionValue::BOOLEAN(_) => 1,
            OptionValue::STRING(ref s) => util::cesu8_length(s) + 2,
            OptionValue::BSTRING(ref v) => v.len() + 2,
        }
    }

    pub fn type_id(&self) -> u8 {
        match *self {
            OptionValue::INT(_) => 3,
            OptionValue::BIGINT(_) => 4,
            OptionValue::DOUBLE(_) => 7,
            OptionValue::BOOLEAN(_) => 28,
            OptionValue::STRING(_) => 29,
            OptionValue::BSTRING(_) => 33,
        }
    }

    pub fn parse<T: io::BufRead>(rdr: &mut T) -> HdbResult<OptionValue> {
        let value_type = rdr.read_u8()?; // U1
        OptionValue::parse_value(value_type, rdr)
    }

    fn parse_value(typecode: u8, rdr: &mut io::BufRead) -> HdbResult<OptionValue> {
        match typecode {
            3 => Ok(OptionValue::INT(rdr.read_i32::<LittleEndian>()?)), // I4
            4 => Ok(OptionValue::BIGINT(rdr.read_i64::<LittleEndian>()?)), // I8
            7 => Ok(OptionValue::DOUBLE(rdr.read_f64::<LittleEndian>()?)), // F8
            28 => Ok(OptionValue::BOOLEAN(rdr.read_u8()? > 0)),         // B1
            29 => Ok(OptionValue::STRING(parse_length_and_string(rdr)?)),
            33 => Ok(OptionValue::BSTRING(parse_length_and_binary(rdr)?)),
            _ => Err(HdbError::Impl(format!(
                "OptionValue::parse_value() not implemented for type code {}",
                typecode
            ))),
        }
    }
}

fn emit_length_and_string(s: &str, w: &mut io::Write) -> HdbResult<()> {
    emit_length_and_bytes(&cesu8::to_cesu8(s), w)
}

fn emit_length_and_bytes(v: &[u8], w: &mut io::Write) -> HdbResult<()> {
    w.write_i16::<LittleEndian>(v.len() as i16)?; // I2: length of value
    w.write_all(v)?; // B (varying)
    Ok(())
}

fn parse_length_and_string(rdr: &mut io::BufRead) -> HdbResult<String> {
    util::string_from_cesu8(parse_length_and_binary(rdr)?)
}

fn parse_length_and_binary(rdr: &mut io::BufRead) -> HdbResult<Vec<u8>> {
    let len = rdr.read_i16::<LittleEndian>()? as usize; // I2: length of value
    util::parse_bytes(len, rdr) // B (varying)
}

impl std::fmt::Display for OptionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            OptionValue::INT(v) => write!(f, "{}", v),
            OptionValue::BIGINT(v) => write!(f, "{}", v),
            OptionValue::DOUBLE(v) => write!(f, "{}", v),
            OptionValue::BOOLEAN(v) => write!(f, "{}", v),
            OptionValue::STRING(v) => write!(f, "{}", v),
            OptionValue::BSTRING(v) => write!(f, "{:?}", v),
        }
    }
}
