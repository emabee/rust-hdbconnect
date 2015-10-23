use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io;


#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[derive(Clone,Debug)]
pub enum OptionValue {
    INT(i32), 				// INTEGER
    BIGINT(i64), 			// BIGINT
    DOUBLE(f64), 			// DOUBLE
    BOOLEAN(bool), 			// Boolean
    STRING(String), 		// Character string
    BSTRING(Vec<u8>),   	// Binary string
}

#[allow(dead_code)]
impl OptionValue {
    pub fn encode(&self, w: &mut io::Write) -> io::Result<()> {
        try!(w.write_u8(self.type_id()));                   // I1
        match *self {                                       // variable
            OptionValue::INT(i)              => {try!(w.write_i32::<LittleEndian>(i))},
            OptionValue::BIGINT(i)           => {try!(w.write_i64::<LittleEndian>(i))},
            OptionValue::DOUBLE(f)           => {try!(w.write_f64::<LittleEndian>(f))},
            OptionValue::BOOLEAN(b)          => {try!(w.write_u8(match b{true => 1, false => 0}))},
            OptionValue::STRING(ref s)       => {try!(encode_length_and_string(s,w))},
            OptionValue::BSTRING(ref v)      => {try!(encode_length_and_bytes(v,w))},
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        1 + match *self {
            OptionValue::INT(_)              => 4,
            OptionValue::BIGINT(_)           => 8,
            OptionValue::DOUBLE(_)           => 8,
            OptionValue::BOOLEAN(_)          => 1,
            OptionValue::STRING(ref s)       => util::cesu8_length(s) + 2,
            OptionValue::BSTRING(ref v)      => v.len() + 2,
        }
    }

    fn type_id(&self) -> u8 { match *self {
        OptionValue::INT(_)          =>  3,
        OptionValue::BIGINT(_)       =>  4,
        OptionValue::DOUBLE(_)       =>  7,
        OptionValue::BOOLEAN(_)      => 28,
        OptionValue::STRING(_)       => 29,
        OptionValue::BSTRING(_)      => 33,
    }}

    pub fn parse(rdr: &mut io::BufRead) -> io::Result<OptionValue> {
        let value_type = try!(rdr.read_u8());                                       // U1
        OptionValue::parse_value(value_type, rdr)
    }

    pub fn parse_value(typecode: u8, rdr: &mut io::BufRead) -> io::Result<OptionValue> { match typecode {
        3  => Ok(OptionValue::INT(      try!(rdr.read_i32::<LittleEndian>()) )),    // I4
        4  => Ok(OptionValue::BIGINT(   try!(rdr.read_i64::<LittleEndian>()) )),    // I8
        7  => Ok(OptionValue::DOUBLE(   try!(rdr.read_f64::<LittleEndian>()) )),    // F8
        28 => Ok(OptionValue::BOOLEAN(  try!(rdr.read_u8()) > 0 )),                 // B1
        29 => Ok(OptionValue::STRING(   try!(parse_length_and_string(rdr)) )),
        33 => Ok(OptionValue::BSTRING(  try!(parse_length_and_binary(rdr)) )),
        _  => Err(util::io_error(&format!("OptionValue::parse_value() not implemented for type code {}",typecode))),
    }}
}

fn encode_length_and_string(s: &String, w: &mut io::Write) -> io::Result<()> {
    encode_length_and_bytes(&util::string_to_cesu8(s), w)
}

fn encode_length_and_bytes(v: &Vec<u8>, w: &mut io::Write) -> io::Result<()> {
    try!(w.write_i16::<LittleEndian>(v.len() as i16));                              // I2           LENGTH OF VALUE
    util::encode_bytes(v, w)                                                        // B variable   VALUE BYTES
}

fn parse_length_and_string(rdr: &mut io::BufRead) -> io::Result<String> {
    Ok(try!(util::cesu8_to_string(&try!(parse_length_and_binary(rdr)))))
}

fn parse_length_and_binary(rdr: &mut io::BufRead) -> io::Result<Vec<u8>> {
    let len = try!(rdr.read_i16::<LittleEndian>()) as usize;                        // I2           LENGTH OF VALUE
    util::parse_bytes(len,rdr)                                                      // B variable
}
