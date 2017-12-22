use super::{util, PrtError, PrtResult};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

#[allow(non_camel_case_types)]
#[derive(Clone, Debug)]
pub enum PrtOptionValue {
    INT(i32),         // INTEGER
    BIGINT(i64),      // BIGINT
    DOUBLE(f64),      // DOUBLE
    BOOLEAN(bool),    // Boolean
    STRING(String),   // Character string
    BSTRING(Vec<u8>), // Binary string
}

impl PrtOptionValue {
    pub fn serialize(&self, w: &mut io::Write) -> PrtResult<()> {
        w.write_u8(self.type_id())?; // I1
        match *self {
            // variable
            PrtOptionValue::INT(i) => w.write_i32::<LittleEndian>(i)?,
            PrtOptionValue::BIGINT(i) => w.write_i64::<LittleEndian>(i)?,
            PrtOptionValue::DOUBLE(f) => w.write_f64::<LittleEndian>(f)?,
            PrtOptionValue::BOOLEAN(b) => w.write_u8(if b { 1 } else { 0 })?,
            PrtOptionValue::STRING(ref s) => serialize_length_and_string(s, w)?,
            PrtOptionValue::BSTRING(ref v) => serialize_length_and_bytes(v, w)?,
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        1 + match *self {
            PrtOptionValue::INT(_) => 4,
            PrtOptionValue::BIGINT(_) | PrtOptionValue::DOUBLE(_) => 8,
            PrtOptionValue::BOOLEAN(_) => 1,
            PrtOptionValue::STRING(ref s) => util::cesu8_length(s) + 2,
            PrtOptionValue::BSTRING(ref v) => v.len() + 2,
        }
    }

    fn type_id(&self) -> u8 {
        match *self {
            PrtOptionValue::INT(_) => 3,
            PrtOptionValue::BIGINT(_) => 4,
            PrtOptionValue::DOUBLE(_) => 7,
            PrtOptionValue::BOOLEAN(_) => 28,
            PrtOptionValue::STRING(_) => 29,
            PrtOptionValue::BSTRING(_) => 33,
        }
    }

    pub fn parse(rdr: &mut io::BufRead) -> PrtResult<PrtOptionValue> {
        let value_type = rdr.read_u8()?; // U1
        PrtOptionValue::parse_value(value_type, rdr)
    }

    fn parse_value(typecode: u8, rdr: &mut io::BufRead) -> PrtResult<PrtOptionValue> {
        match typecode {
            3 => Ok(PrtOptionValue::INT(rdr.read_i32::<LittleEndian>()?)), // I4
            4 => Ok(PrtOptionValue::BIGINT(rdr.read_i64::<LittleEndian>()?)), // I8
            7 => Ok(PrtOptionValue::DOUBLE(rdr.read_f64::<LittleEndian>()?)), // F8
            28 => Ok(PrtOptionValue::BOOLEAN(rdr.read_u8()? > 0)),         // B1
            29 => Ok(PrtOptionValue::STRING(parse_length_and_string(rdr)?)),
            33 => Ok(PrtOptionValue::BSTRING(parse_length_and_binary(rdr)?)),
            _ => Err(PrtError::ProtocolError(format!(
                "PrtOptionValue::parse_value() not implemented for type code {}",
                typecode
            ))),
        }
    }
}

fn serialize_length_and_string(s: &str, w: &mut io::Write) -> PrtResult<()> {
    serialize_length_and_bytes(&util::string_to_cesu8(s), w)
}

fn serialize_length_and_bytes(v: &[u8], w: &mut io::Write) -> PrtResult<()> {
    w.write_i16::<LittleEndian>(v.len() as i16)?; // I2: length of value
    util::serialize_bytes(v, w) // B (varying)
}

fn parse_length_and_string(rdr: &mut io::BufRead) -> PrtResult<String> {
    Ok(util::cesu8_to_string(&parse_length_and_binary(rdr)?)?)
}

fn parse_length_and_binary(rdr: &mut io::BufRead) -> PrtResult<Vec<u8>> {
    let len = rdr.read_i16::<LittleEndian>()? as usize; // I2: length of value
    util::parse_bytes(len, rdr) // B (varying)
}
