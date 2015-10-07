use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::i16;
use std::io::Result as IoResult;
use std::io::{BufRead,Error,ErrorKind,Write};


#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[derive(Clone,Debug)]
pub enum TypedValue {               // Description, Support Level
    NULL, 						    // NULL value, -
    TINYINT(Option<u8>),            // TINYINT, 1
    SMALLINT(Option<i16>), 			// SMALLINT, 1
    INT(Option<i32>), 				// INTEGER, 1
    BIGINT(Option<i64>), 			// BIGINT, 1
//  DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    REAL(Option<f32>), 				// REAL, 1
    DOUBLE(Option<f64>), 			// DOUBLE, 1
    CHAR(Option<String>), 			// CHAR, 1
    VARCHAR(Option<String>), 		// VARCHAR, 1
    NCHAR(Option<String>), 			// NCHAR (Unicode character type), 1
    NVARCHAR(Option<String>),		// NVARCHAR (Unicode character type), 1
    BINARY(Option<Vec<u8>>), 		// BINARY, 1
    VARBINARY(Option<Vec<u8>>),		// VARBINARY, 1
//  DATE = 14, 						// DATE (deprecated type), 1 (deprecated with 3)
//  TIME = 15, 						// TIME (deprecated type), 1 (deprecated with 3)
//  TIMESTAMP = 16, 				// TIMESTAMP (millisecond precision), 1 (deprecated with 3)
//  TIME_TZ = 17, 					// Reserved, do not use, -
//  TIME_LTZ = 18, 					// Reserved, do not use, -
//  TIMESTAMP_TZ = 19, 				// Reserved, do not use, -
//  TIMESTAMP_LTZ = 20, 			// Reserved, do not use, -
//  INTERVAL_YM = 21, 				// Reserved, do not use, -
//  INTERVAL_DS = 22, 				// Reserved, do not use, -
//  ROWID = 23, 					// Reserved, do not use, -
//  UROWID = 24, 					// Reserved, do not use, -
// CLOB(Option<String>), 			// Character Large Object, 1
// NCLOB(Option<String>), 			// Unicode Character Large Object, 1
// BLOB(Option<Vec<u8>>), 			// Binary Large Object, 1
    BOOLEAN(Option<bool>), 			// Boolean
    STRING(Option<String>), 		// Character string, 1
    NSTRING(Option<String>),		// Unicode character string, 1
//  BLOCATOR = 31, 					// Binary locator, 1
//  NLOCATOR = 32, 					// Unicode character locator, 1
    BSTRING(Option<Vec<u8>>),   	// Binary string, 1
//  DECIMAL_DIGIT_ARRAY = 34, 		// Reserved, do not use, -
//  VARCHAR2 = 35, 					// VARCHAR, -
//  VARCHAR3 = 36, 					// VARCHAR, -
//  NVARCHAR3 = 37, 				// NVARCHAR, -
//  VARBINARY3 = 38, 				// VARBINARY, -
//  VARGROUP = 39, 					// Reserved, do not use, -
//  TINYINT_NOTNULL = 40, 			// Reserved, do not use, -
//  SMALLINT_NOTNULL = 41, 			// Reserved, do not use, -
//  INT_NOTNULL = 42, 				// Reserved, do not use, -
//  BIGINT_NOTNULL = 43, 			// Reserved, do not use, -
//  ARGUMENT = 44, 					// Reserved, do not use, -
//  TABLE = 45, 					// Reserved, do not use, -
//  CURSOR = 46, 					// Reserved, do not use, -
//  SMALLDECIMAL = 47, 				// SMALLDECIMAL data type, -
//  ABAPITAB = 48, 					// ABAPSTREAM procedure parameter, 1
//  ABAPSTRUCT = 49, 				// ABAP structure procedure parameter, 1
//  ARRAY = 50, 					// Reserved, do not use, -
    TEXT(Option<String>), 			// TEXT data type, 3
    SHORTTEXT(Option<String>), 		// SHORTTEXT data type, 3
//  FIXEDSTRING = 53, 				// Reserved, do not use, -
//  FIXEDPOINTDECIMAL = 54, 		// Reserved, do not use, -
//  ALPHANUM = 55, 					// ALPHANUM data type, 3
//  TLOCATOR = 56, 					// Reserved, do not use, -
//  LONGDATE = 61, 					// TIMESTAMP data type, 3
//  SECONDDATE = 62, 				// TIMESTAMP type with second precision, 3
//  DAYDATE = 63, 					// DATE data type, 3
//  SECONDTIME = 64, 				// TIME data type, 3
//  CSDATE = 65, 					// Reserved, do not use, -
//  CSTIME = 66, 					// Reserved, do not use, -
//  BLOB_DISK = 71, 				// Reserved, do not use, -
//  CLOB_DISK = 72, 				// Reserved, do not use, -
//  NCLOB_DISK = 73, 				// Reserved, do not use, -
//  GEOMETRY = 74, 					// Reserved, do not use, -
//  POINT = 75, 					// Reserved, do not use, -
//  FIXED16 = 76, 					// Reserved, do not use, -
//  BLOB_HYBRID = 77, 				// Reserved, do not use, -
//  CLOB_HYBRID = 78, 				// Reserved, do not use, -
//  NCLOB_HYBRID = 79, 				// Reserved, do not use, -
//  POINTZ = 80, 					// Reserved, do not use, -
}

#[allow(dead_code)]
impl TypedValue {
    pub fn encode(&self, w: &mut Write) -> IoResult<()> {
        try!(w.write_u8(self.type_id()));                   // I1
        match *self {                                       // variable
            TypedValue::NULL                => {},
            TypedValue::TINYINT(o)          => if let Some(u) = o {try!(w.write_u8(u))},
            TypedValue::SMALLINT(o)         => if let Some(i) = o {try!(w.write_i16::<LittleEndian>(i))},
            TypedValue::INT(o)              => if let Some(i) = o {try!(w.write_i32::<LittleEndian>(i))},
            TypedValue::BIGINT(o)           => if let Some(i) = o {try!(w.write_i64::<LittleEndian>(i))},
            TypedValue::REAL(o)             => if let Some(f) = o {try!(w.write_f32::<LittleEndian>(f))},
            TypedValue::DOUBLE(o)           => if let Some(f) = o {try!(w.write_f64::<LittleEndian>(f))},
            TypedValue::CHAR(ref o)         => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
            TypedValue::VARCHAR(ref o)      => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
            TypedValue::NCHAR(ref o)        => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
            TypedValue::NVARCHAR(ref o)     => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
            TypedValue::BINARY(ref o)       => if let &Some(ref v) = o {try!(encode_length_and_bytes(v,w))},
            TypedValue::VARBINARY(ref o)    => if let &Some(ref v) = o {try!(encode_length_and_bytes(v,w))},
            TypedValue::BOOLEAN(o)          => if let Some(b) = o {try!(w.write_u8(match b{true => 1, false => 0}))},
            TypedValue::STRING(ref o)       => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
            TypedValue::NSTRING(ref o)      => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
            TypedValue::BSTRING(ref o)      => if let &Some(ref v) = o {try!(encode_length_and_bytes(v,w))},
            TypedValue::TEXT(ref o)         => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
            TypedValue::SHORTTEXT(ref o)    => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        fn l_hdblen(s: &String) -> usize {
            match util::cesu8_length(s) {
                clen if clen <= MAX_1_BYTE_LENGTH as usize    => 1 + 1 + clen,
                clen if clen <= MAX_2_BYTE_LENGTH as usize    => 1 + 3 + clen,
                clen                                          => 1 + 5 + clen,
            }
        }

        1 + match *self {
            TypedValue::NULL => 0,
            TypedValue::TINYINT(o)          => match o {Some(_) => 1, None => 0},
            TypedValue::SMALLINT(o)         => match o {Some(_) => 2, None => 0},
            TypedValue::INT(o)              => match o {Some(_) => 4, None => 0},
            TypedValue::BIGINT(o)           => match o {Some(_) => 8, None => 0},
            TypedValue::REAL(o)             => match o {Some(_) => 4, None => 0},
            TypedValue::DOUBLE(o)           => match o {Some(_) => 8, None => 0},
            TypedValue::CHAR(ref o)         => match o {&Some(ref s) => l_hdblen(s), &None => 0},
            TypedValue::VARCHAR(ref o)      => match o {&Some(ref s) => l_hdblen(s), &None => 0},
            TypedValue::NCHAR(ref o)        => match o {&Some(ref s) => l_hdblen(s), &None => 0},
            TypedValue::NVARCHAR(ref o)     => match o {&Some(ref s) => l_hdblen(s), &None => 0},
            TypedValue::BINARY(ref o)       => match o {&Some(ref v) => v.len() + 2, &None => 0},
            TypedValue::VARBINARY(ref o)    => match o {&Some(ref v) => v.len() + 2, &None => 0},
            TypedValue::BOOLEAN(o)          => match o {Some(_) => 1, None => 0},
            TypedValue::STRING(ref o)       => match o {&Some(ref s) => l_hdblen(s), &None => 0},
            TypedValue::NSTRING(ref o)      => match o {&Some(ref s) => l_hdblen(s), &None => 0},
            TypedValue::BSTRING(ref o)      => match o {&Some(ref v) => v.len() + 2, &None => 0},
            TypedValue::TEXT(ref o)         => match o {&Some(ref s) => l_hdblen(s), &None => 0},
            TypedValue::SHORTTEXT(ref o)    => match o {&Some(ref s) => l_hdblen(s), &None => 0},
        }
    }


    fn type_id(&self) -> u8 { match *self {
        TypedValue::NULL => 0,
        TypedValue::TINYINT(o)          => match o {Some(_)  =>  1, None =>   1 + 128},
        TypedValue::SMALLINT(o)         => match o {Some(_)  =>  2, None =>   2 + 128},
        TypedValue::INT(o)              => match o {Some(_)  =>  3, None =>   3 + 128},
        TypedValue::BIGINT(o)           => match o {Some(_)  =>  4, None =>   4 + 128},
    //  TypedValue::DECIMAL(o)          => match o {Some(_)  =>  5, None =>   5 + 128},
        TypedValue::REAL(o)             => match o {Some(_)  =>  6, None =>   6 + 128},
        TypedValue::DOUBLE(ref o)       => match o {&Some(_) =>  7, &None =>  7 + 128},
        TypedValue::CHAR(ref o)         => match o {&Some(_) =>  8, &None =>  8 + 128},
        TypedValue::VARCHAR(ref o)      => match o {&Some(_) =>  9, &None =>  9 + 128},
        TypedValue::NCHAR(ref o)        => match o {&Some(_) => 10, &None => 10 + 128},
        TypedValue::NVARCHAR(ref o)     => match o {&Some(_) => 11, &None => 11 + 128},
        TypedValue::BINARY(ref o)       => match o {&Some(_) => 12, &None => 12 + 128},
        TypedValue::VARBINARY(ref o)    => match o {&Some(_) => 13, &None => 13 + 128},
     // TypedValue::TIMESTAMP(o)        => match o {&Some(_) => 16, None =>  16 + 128},
     // TypedValue::CLOB(ref o)         => match o {&Some(_) => 25, &None => 25 + 128},
     // TypedValue::NCLOB(ref o)        => match o {&Some(_) => 26, &None => 26 + 128},
     // TypedValue::BLOB(ref o)         => match o {&Some(_) => 27, &None => 27 + 128},
        TypedValue::BOOLEAN(o)          => match o {Some(_)  => 28, None  => 28 + 128},
        TypedValue::STRING(ref o)       => match o {&Some(_) => 29, &None => 29 + 128},
        TypedValue::NSTRING(ref o)      => match o {&Some(_) => 30, &None => 30 + 128},
     // TypedValue::BLOCATOR(o)         => match o {Some(_)  => 31, None =>  31 + 128},
     // TypedValue::NLOCATOR(o)         => match o {Some(_)  => 32, None =>  32 + 128},
        TypedValue::BSTRING(ref o)      => match o {&Some(_) => 33, &None => 33 + 128},
     // TypedValue::VARCHAR2(o)         => match o {Some(_)  => 35, None =>  35 + 128},
     // TypedValue::VARCHAR3(o)         => match o {Some(_)  => 36, None =>  36 + 128},
     // TypedValue::NVARCHAR3(o)        => match o {Some(_)  => 37, None =>  37 + 128},
     // TypedValue::VARBINARY3(o)       => match o {Some(_)  => 38, None =>  38 + 128},
     // TypedValue::SMALLDECIMAL(o)     => match o {Some(_)  => 47, None =>  47 + 128},
     // TypedValue::ABAPITAB(o)         => match o {Some(_)  => 48, None =>  48 + 128},
     // TypedValue::ABAPSTRUCT(o)       => match o {Some(_)  => 49, None =>  49 + 128},
        TypedValue::TEXT(ref o)         => match o {&Some(_) => 51, &None => 51 + 128},
        TypedValue::SHORTTEXT(ref o)    => match o {&Some(_) => 52, &None => 52 + 128},
     // TypedValue::ALPHANUM(o)         => match o {Some(_)  => 55, None =>  55 + 128},
     // TypedValue::LONGDATE(o)         => match o {Some(_)  => 61, None =>  61 + 128},
     // TypedValue::SECONDDATE(o)       => match o {Some(_)  => 62, None =>  62 + 128},
     // TypedValue::DAYDATE(o)          => match o {Some(_)  => 63, None =>  63 + 128},
     // TypedValue::SECONDTIME(o)       => match o {Some(_)  => 64, None =>  64 + 128},
    }}

    pub fn parse(rdr: &mut BufRead) -> IoResult<TypedValue> {
        let value_type = try!(rdr.read_u8());           // U1
        TypedValue::parse_value(value_type, rdr)
    }

    pub fn parse_value(typecode: u8, rdr: &mut BufRead) -> IoResult<TypedValue> { match typecode {
        0 => Ok(TypedValue::NULL) ,
        1 => Ok(TypedValue::TINYINT(    Some(try!(rdr.read_u8()) ))),
        2 => Ok(TypedValue::SMALLINT(   Some(try!(rdr.read_i16::<LittleEndian>()) ))),
        3 => Ok(TypedValue::INT(        Some(try!(rdr.read_i32::<LittleEndian>()) ))),
        4 => Ok(TypedValue::BIGINT(     Some(try!(rdr.read_i64::<LittleEndian>()) ))),
     // 5 => Ok(TypedValue::DECIMAL) ,
        6 => Ok(TypedValue::REAL(       Some(try!(rdr.read_f32::<LittleEndian>()) ))),
        7 => Ok(TypedValue::DOUBLE(     Some(try!(rdr.read_f64::<LittleEndian>()) ))),
        8 => Ok(TypedValue::CHAR(       Some(try!(parse_length_and_string(rdr)) ))),// _length_and_string
        9 => Ok(TypedValue::VARCHAR(    Some(try!(parse_length_and_string(rdr)) ))),
        10 => Ok(TypedValue::NCHAR(     Some(try!(parse_length_and_string(rdr)) ))),
        11 => Ok(TypedValue::NVARCHAR(  Some(try!(parse_length_and_string(rdr)) ))),
     // 12 => Ok(TypedValue::BINARY) ,
     // 13 => Ok(TypedValue::VARBINARY) ,
     // 16 => Ok(TypedValue::TIMESTAMP) ,
     // 25 => Ok(TypedValue::CLOB(      Some(try!(parse_length_and_string(rdr)) ))),
     // 26 => Ok(TypedValue::NCLOB(     Some(try!(parse_length_and_string(rdr)) ))),
     // 27 => Ok(TypedValue::BLOB(      Some(try!(parse_length_and_binary(rdr)) ))),
        28 => Ok(TypedValue::BOOLEAN(   Some(try!(rdr.read_u8()) > 0 ))),
        29 => Ok(TypedValue::STRING(    Some(try!(parse_length_and_string(rdr)) ))),
        30 => Ok(TypedValue::NSTRING(   Some(try!(parse_length_and_string(rdr)) ))),
     // 31 => Ok(TypedValue::BLOCATOR) ,
     // 32 => Ok(TypedValue::NLOCATOR) ,
        33 => Ok(TypedValue::BSTRING(   Some(try!(parse_length_and_binary(rdr)) ))),
     // 35 => Ok(TypedValue::VARCHAR2) ,
     // 36 => Ok(TypedValue::VARCHAR3) ,
     // 37 => Ok(TypedValue::NVARCHAR3) ,
     // 38 => Ok(TypedValue::VARBINARY3) ,
     // 47 => Ok(TypedValue::SMALLDECIMAL) ,
     // 48 => Ok(TypedValue::ABAPITAB) ,
     // 49 => Ok(TypedValue::ABAPSTRUCT) ,
        51 => Ok(TypedValue::TEXT(      Some(try!(parse_length_and_string(rdr)) ))),
        52 => Ok(TypedValue::SHORTTEXT( Some(try!(parse_length_and_string(rdr)) ))),
     // 55 => Ok(TypedValue::ALPHANUM) ,
     // 61 => Ok(TypedValue::LONGDATE) ,
     // 62 => Ok(TypedValue::SECONDDATE) ,
     // 63 => Ok(TypedValue::DAYDATE) ,
     // 64 => Ok(TypedValue::SECONDTIME) ,
        128 ... 255 => {
            match typecode - 128 {
                0 => Ok(TypedValue::NULL) ,
                1 => Ok(TypedValue::TINYINT(None)),
                2 => Ok(TypedValue::SMALLINT(None)),
                3 => Ok(TypedValue::INT(None)),
                4 => Ok(TypedValue::BIGINT(None)),
             // 5 => Ok(TypedValue::DECIMAL(None)),
                6 => Ok(TypedValue::REAL(None)),
                7 => Ok(TypedValue::DOUBLE(None)),
                8 => Ok(TypedValue::CHAR(None)),
                9 => Ok(TypedValue::VARCHAR(None)),
                10 => Ok(TypedValue::NCHAR(None)),
                11 => Ok(TypedValue::NVARCHAR(None)),
             // 12 => Ok(TypedValue::BINARY(None)),
             // 13 => Ok(TypedValue::VARBINARY(None)),
             // 16 => Ok(TypedValue::TIMESTAMP(None)),
             // 25 => Ok(TypedValue::CLOB(None)),
             // 26 => Ok(TypedValue::NCLOB(None)),
             // 27 => Ok(TypedValue::BLOB(None)),
                28 => Ok(TypedValue::BOOLEAN(None)),
                29 => Ok(TypedValue::STRING(None)),
                30 => Ok(TypedValue::NSTRING(None)),
             // 31 => Ok(TypedValue::BLOCATOR(None)),
             // 32 => Ok(TypedValue::NLOCATOR(None)),
                33 => Ok(TypedValue::BSTRING(None)),
             // 35 => Ok(TypedValue::VARCHAR2(None)),
             // 36 => Ok(TypedValue::VARCHAR3(None)),
             // 37 => Ok(TypedValue::NVARCHAR3(None)),
             // 38 => Ok(TypedValue::VARBINARY3(None)),
             // 47 => Ok(TypedValue::SMALLDECIMAL(None)),
             // 48 => Ok(TypedValue::ABAPITAB(None)),
             // 49 => Ok(TypedValue::ABAPSTRUCT(None)),
                51 => Ok(TypedValue::TEXT(None)),
                52 => Ok(TypedValue::SHORTTEXT(None)),
             // 55 => Ok(TypedValue::ALPHANUM(None)),
             // 61 => Ok(TypedValue::LONGDATE(None)),
             // 62 => Ok(TypedValue::SECONDDATE(None)),
             // 63 => Ok(TypedValue::DAYDATE(None)),
             // 64 => Ok(TypedValue::SECONDTIME(None)),
                _ => Err(Error::new(ErrorKind::Other,format!("parse_value() not implemented for type code {}",typecode))),
            }
        }
        _ => Err(Error::new(ErrorKind::Other,format!("parse_value() not implemented for type code {}",typecode))),
    }}
}


const MAX_1_BYTE_LENGTH:u8 = 245;
const MAX_2_BYTE_LENGTH:i16 = i16::MAX;
const LENGTH_INDICATOR_2BYTE:u8 = 246;
const LENGTH_INDICATOR_4BYTE:u8 = 247;

#[allow(dead_code)]
fn encode_length_and_string(s: &String, w: &mut Write) -> IoResult<()> {
    encode_length_and_bytes(&util::string_to_cesu8(s), w)
}

#[allow(dead_code)]
fn encode_length_and_bytes(v: &Vec<u8>, w: &mut Write) -> IoResult<()> {
    match v.len() {
        l if l <= MAX_1_BYTE_LENGTH as usize => {
            try!(w.write_u8(l as u8));                      // B1           LENGTH OF VALUE
        },
        l if l <= MAX_2_BYTE_LENGTH as usize => {
            try!(w.write_u8(LENGTH_INDICATOR_2BYTE));       // B1           246
            try!(w.write_i16::<LittleEndian>(l as i16));    // I2           LENGTH OF VALUE
        },
        l => {
            try!(w.write_u8(LENGTH_INDICATOR_4BYTE));       // B1           247
            try!(w.write_i32::<LittleEndian>(l as i32));    // I4           LENGTH OF VALUE
        }
    }
    util::encode_bytes(v, w)                                // B variable   VALUE BYTES
}

fn parse_length_and_string(rdr: &mut BufRead) -> IoResult<String> {
    Ok(try!(util::cesu8_to_string(&try!(parse_length_and_binary(rdr)))))
}

fn parse_length_and_binary(rdr: &mut BufRead) -> IoResult<Vec<u8>> {
    let l8 = try!(rdr.read_u8());                           // B1
    let len = match l8 {
        l if l <= MAX_1_BYTE_LENGTH => {
            l8 as usize
        },
        LENGTH_INDICATOR_2BYTE  => {
            try!(rdr.read_i16::<LittleEndian>()) as usize   // I2           LENGTH OF VALUE
        },
        LENGTH_INDICATOR_4BYTE => {
            try!(rdr.read_i32::<LittleEndian>()) as usize   // I4           LENGTH OF VALUE
        },
        l => {panic!("Invalid value in first byte of length: {}",l)},
    };
    util::parse_bytes(len,rdr)                              // B variable
}
