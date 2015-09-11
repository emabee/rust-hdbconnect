use super::bufread::*;
use super::valtype::*;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{Error,ErrorKind,Read,Result,Write};
use std::net::TcpStream;

#[derive(Debug)]
pub struct HdbOption {
    pub id: HdbOptionId,
    pub value: HdbOptionValue,
}
impl HdbOption {
    pub fn encode (&self, w: &mut Write)  -> IoResult<()> {
        try!(w.write_i8(self.id.to_i8()));                          // I1           OPTION KEY
        try!(w.write_i8(ValType::STRING.to_i8()));                  // I1           TYPE OF OPTION VALUE
        try!(self.value.encode(w));
        Ok(())
    }

    pub fn size(&self) -> usize {
        2 + self.value.size()
    }

    pub fn try_to_parse(rdr: &mut BufReader<&mut TcpStream>) -> IoResult<HdbOption> {
        let option_id = try!(HdbOptionId::from_i8(try!(rdr.read_i8())));    // I1
        let option_type = try!(ValType::from_i8(try!(rdr.read_i8())));      // I1

        let value = try!(HdbOptionValue::try_to_parse(option_type,rdr));

        Ok(HdbOption{id: option_id, value: value})
    }
}


#[derive(Debug)]
pub enum HdbOptionId {
    Version,
    ClientType,
    ClientApplicationProgram,
}
impl HdbOptionId {
    pub fn to_i8(&self) -> i8 {
        match *self {
            HdbOptionId::Version => 1,
            HdbOptionId::ClientType => 2,
            HdbOptionId::ClientApplicationProgram => 3,
        }
    }
    pub fn from_i8(val: i8) -> Result<HdbOptionId> { match val {
        1 => Ok(HdbOptionId::Version),
        2 => Ok(HdbOptionId::ClientType),
        3 => Ok(HdbOptionId::ClientApplicationProgram),
        _ => Err(Error::new(ErrorKind::Other,format!("Invalid value for HdbOptionId detected: {}",val))),
    }}
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum HdbOptionValue {
    BOOLEAN(bool),
    INT(i32),
    BIGINT(i64),
    STRING(String),
    BSTRING(Vec<u8>),
    DOUBLE(f64),
}
impl HdbOptionValue {
    fn encode(&self, w: &mut Write) -> IoResult<()> {
        match *self {
            HdbOptionValue::STRING(ref s) => {
                try!(w.write_i16::<LittleEndian>(s.len() as i16));  // I2           LENGTH OF OPTION VALUE
                for b in s.as_bytes() {try!(w.write_u8(*b));}       // B variable   OPTION VALUE
            },
            HdbOptionValue::BSTRING(ref s) => {
                try!(w.write_i16::<LittleEndian>(s.len() as i16));  // I2           LENGTH OF OPTION VALUE
                for b in s {try!(w.write_u8(*b));}                  // B variable   OPTION VALUE
            },
            _ => return Err(Error::new(ErrorKind::Other,format!("Encoding for ValType not yet implemented: {:?}",self))),
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        match *self {
            HdbOptionValue::BOOLEAN(_) => {1usize},
            HdbOptionValue::INT(_) => {4usize},
            HdbOptionValue::BIGINT(_) => {8usize},
            HdbOptionValue::STRING(ref s) => {s.len() + 2},
            HdbOptionValue::BSTRING(ref v) => {v.len() + 2},
            HdbOptionValue::DOUBLE(_) => {8usize},
        }
    }

    pub fn try_to_parse(option_type: ValType, rdr: &mut BufReader<&mut TcpStream>) -> IoResult<HdbOptionValue> {
        match option_type {
            ValType::STRING => {
                let length = try!(rdr.read_i16::<LittleEndian>());                  // I2
                let mut option_value = Vec::<u8>::with_capacity(length as usize);
                try!(rdr.read(&mut option_value));                                  // variable
                let s = try!(String::from_utf8(option_value)
                             .map_err(|_|{Error::new(ErrorKind::Other, "Invalid UTF-8 received for option")}));
                Ok(HdbOptionValue::STRING(s))
            },
            _ => {return Err(Error::new(ErrorKind::Other, format!("Invalid value for option_type detected: {:?}",option_type))); }
        }
    }
}
