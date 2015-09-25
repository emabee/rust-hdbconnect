use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::io::Result as IoResult;
use std::io::{BufRead,Error,ErrorKind,Read,Write};
use std::iter::repeat;


#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[derive(Clone,Debug)]
pub enum TypedValue {               // Description, Support Level
    NULL, 						    // NULL value, -
    TINYINT(u8), 					// TINYINT, 1
    SMALLINT(i16), 					// SMALLINT, 1
    INT(i32), 						// INTEGER, 1
    BIGINT(i64), 					// BIGINT, 1
//  DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    REAL(f32), 						// REAL, 1
    DOUBLE(f64), 					// DOUBLE, 1
    CHAR(String), 					// CHAR, 1
    VARCHAR(String), 				// VARCHAR, 1
    NCHAR(String), 				    // NCHAR (Unicode character type), 1
    NVARCHAR(String),				// NVARCHAR (Unicode character type), 1
    BINARY(Vec<u8>), 				// BINARY, 1
    VARBINARY(Vec<u8>),				// VARBINARY, 1
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
    CLOB(String), 					// Character Large Object, 1
    NCLOB(String), 					// Unicode Character Large Object, 1
    BLOB(Vec<u8>), 					// Binary Large Object, 1
    BOOLEAN(bool), 					// Boolean
    STRING(String), 				// Character string, 1
    NSTRING(String),				// Unicode character string, 1
//  BLOCATOR = 31, 					// Binary locator, 1
//  NLOCATOR = 32, 					// Unicode character locator, 1
//  BSTRING = 33, 					// Binary string, 1
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
    TEXT(String), 					// TEXT data type, 3
    SHORTTEXT(String), 				// SHORTTEXT data type, 3
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
        try!(w.write_i8(self.type_id()));                                       // I1
        match *self {
            TypedValue::NULL        => try!(w.write_i8(-1)),                    // FIXME is this correct??
            TypedValue::TINYINT(u)  => try!(w.write_u8(u)),
            TypedValue::SMALLINT(i) => try!(w.write_i16::<LittleEndian>(i)),
            TypedValue::INT(i)      => try!(w.write_i32::<LittleEndian>(i)),
            TypedValue::BIGINT(i)   => try!(w.write_i64::<LittleEndian>(i)),
            TypedValue::REAL(f)     => try!(w.write_f32::<LittleEndian>(f)),
            TypedValue::DOUBLE(f)   => try!(w.write_f64::<LittleEndian>(f)),
            TypedValue::CHAR(ref s)         => try!(encode_string(s,w)),
            TypedValue::VARCHAR(ref s)      => try!(encode_string(s,w)),
            TypedValue::NCHAR(ref s)        => try!(encode_string(s,w)),
            TypedValue::NVARCHAR(ref s)     => try!(encode_string(s,w)),
            TypedValue::BINARY(ref v)       => try!(encode_bytes(v,w)),
            TypedValue::VARBINARY(ref v)    => try!(encode_bytes(v,w)),
            TypedValue::CLOB(ref s)         => try!(encode_string(s,w)),
            TypedValue::NCLOB(ref s)        => try!(encode_string(s,w)),
            TypedValue::BLOB(ref v)         => try!(encode_bytes(v,w)),
            TypedValue::BOOLEAN(b)  => try!(w.write_u8(match b{true => 1, false => 0})),
            TypedValue::STRING(ref s)       => try!(encode_string(s,w)),
            TypedValue::NSTRING(ref s)      => try!(encode_string(s,w)),
            TypedValue::TEXT(ref s)         => try!(encode_string(s,w)),
            TypedValue::SHORTTEXT(ref s)    => try!(encode_string(s,w)),
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        1 + match *self {
            TypedValue::NULL => 1,
            TypedValue::TINYINT(_) => 1,
            TypedValue::SMALLINT(_) => 2,
            TypedValue::INT(_) => 4,
            TypedValue::BIGINT(_) => 8,
            TypedValue::REAL(_) => 4,
            TypedValue::DOUBLE(_) => 8,
            TypedValue::CHAR(ref s) => s.len() + 2,
            TypedValue::VARCHAR(ref s) => s.len() + 2,
            TypedValue::NCHAR(ref s) => s.len() + 2,
            TypedValue::NVARCHAR(ref s) => s.len() + 2,
            TypedValue::BINARY(ref v) => v.len() + 2,
            TypedValue::VARBINARY(ref v) => v.len() + 2,
            TypedValue::CLOB(ref s) => s.len() + 2,
            TypedValue::NCLOB(ref s) => s.len() + 2,
            TypedValue::BLOB(ref v) => v.len() + 2,
            TypedValue::BOOLEAN(_) => 1,
            TypedValue::STRING(ref s) => s.len() + 2,
            TypedValue::NSTRING(ref s) => s.len() + 2,
            TypedValue::TEXT(ref s) => s.len() + 2,
            TypedValue::SHORTTEXT(ref s) => s.len() + 2,
        }
    }


    fn type_id(&self) -> i8 {match *self {
        TypedValue::NULL => 0,
        TypedValue::TINYINT(_) => 1,
        TypedValue::SMALLINT(_) => 2,
        TypedValue::INT(_) => 3,
        TypedValue::BIGINT(_) => 4,
    //  TypedValue::DECIMAL => 5,
        TypedValue::REAL(_) => 6,
        TypedValue::DOUBLE(_) => 7,
        TypedValue::CHAR(_) => 8,
        TypedValue::VARCHAR(_) => 9,
        TypedValue::NCHAR(_) => 10,
        TypedValue::NVARCHAR(_) => 11,
        TypedValue::BINARY(_) => 12,
        TypedValue::VARBINARY(_) => 13,
     // TypedValue::TIMESTAMP => 16,
        TypedValue::CLOB(_) => 25,
        TypedValue::NCLOB(_) => 26,
        TypedValue::BLOB(_) => 27,
        TypedValue::BOOLEAN(_) => 28,
        TypedValue::STRING(_) => 29,
        TypedValue::NSTRING(_) => 30,
     // TypedValue::BLOCATOR => 31,
     // TypedValue::NLOCATOR => 32,
     // TypedValue::BSTRING => 33,
     // TypedValue::VARCHAR2 => 35,
     // TypedValue::VARCHAR3 => 36,
     // TypedValue::NVARCHAR3 => 37,
     // TypedValue::VARBINARY3 => 38,
     // TypedValue::SMALLDECIMAL => 47,
     // TypedValue::ABAPITAB => 48,
     // TypedValue::ABAPSTRUCT => 49,
        TypedValue::TEXT(_) => 51,
        TypedValue::SHORTTEXT(_) => 52,
     // TypedValue::ALPHANUM => 55,
     // TypedValue::LONGDATE => 61,
     // TypedValue::SECONDDATE => 62,
     // TypedValue::DAYDATE => 63,
     // TypedValue::SECONDTIME => 64,
    }}

    pub fn parse(rdr: &mut BufRead) -> IoResult<TypedValue> {
        let value_type = try!(rdr.read_i8());           // I1
        parse_value(value_type, rdr)
    }
}

fn encode_string(s: &String, w: &mut Write) -> IoResult<()> {
    try!(w.write_i16::<LittleEndian>(s.len() as i16));  // I2           LENGTH OF OPTION VALUE
    for b in s.as_bytes() {try!(w.write_u8(*b));}       // B variable   OPTION VALUE
    Ok(())
}

fn encode_bytes(v: &Vec<u8>, w: &mut Write) -> IoResult<()> {
    try!(w.write_i16::<LittleEndian>(v.len() as i16));  // I2           LENGTH OF OPTION VALUE
    for b in v {try!(w.write_u8(*b));}                   // B variable   OPTION VALUE
    Ok(())
}


fn parse_value(val: i8, rdr: &mut BufRead) -> IoResult<TypedValue> { match val {
    0 => Ok(TypedValue::NULL) ,
    1 => Ok(TypedValue::TINYINT(   try!(rdr.read_u8()) )),
    2 => Ok(TypedValue::SMALLINT(  try!(rdr.read_i16::<LittleEndian>()) )),
    3 => Ok(TypedValue::INT(       try!(rdr.read_i32::<LittleEndian>()) )),
    4 => Ok(TypedValue::BIGINT(    try!(rdr.read_i64::<LittleEndian>()) )),
 // 5 => Ok(TypedValue::DECIMAL) ,
    6 => Ok(TypedValue::REAL(      try!(rdr.read_f32::<LittleEndian>()) )),
    7 => Ok(TypedValue::DOUBLE(    try!(rdr.read_f64::<LittleEndian>()) )),
    8 => Ok(TypedValue::CHAR(      try!(parse_string(rdr)) )),
    9 => Ok(TypedValue::VARCHAR(   try!(parse_string(rdr)) )),
    10 => Ok(TypedValue::NCHAR(    try!(parse_string(rdr)) )),
    11 => Ok(TypedValue::NVARCHAR( try!(parse_string(rdr)) )),
 // 12 => Ok(TypedValue::BINARY) ,
 // 13 => Ok(TypedValue::VARBINARY) ,
 // 16 => Ok(TypedValue::TIMESTAMP) ,
    25 => Ok(TypedValue::CLOB(     try!(parse_string(rdr)) )),
    26 => Ok(TypedValue::NCLOB(    try!(parse_string(rdr)) )),
    27 => Ok(TypedValue::BLOB(     try!(parse_binary(rdr)) )),
    28 => Ok(TypedValue::BOOLEAN(  try!(rdr.read_u8()) > 0 )),
    29 => Ok(TypedValue::STRING(   try!(parse_string(rdr)) )),
    30 => Ok(TypedValue::NSTRING(  try!(parse_string(rdr)) )),
 // 31 => Ok(TypedValue::BLOCATOR) ,
 // 32 => Ok(TypedValue::NLOCATOR) ,
 // 33 => Ok(TypedValue::BSTRING) ,
 // 35 => Ok(TypedValue::VARCHAR2) ,
 // 36 => Ok(TypedValue::VARCHAR3) ,
 // 37 => Ok(TypedValue::NVARCHAR3) ,
 // 38 => Ok(TypedValue::VARBINARY3) ,
 // 47 => Ok(TypedValue::SMALLDECIMAL) ,
 // 48 => Ok(TypedValue::ABAPITAB) ,
 // 49 => Ok(TypedValue::ABAPSTRUCT) ,
    51 => Ok(TypedValue::TEXT(     try!(parse_string(rdr)) )),
    52 => Ok(TypedValue::SHORTTEXT(try!(parse_string(rdr)) )),
 // 55 => Ok(TypedValue::ALPHANUM) ,
 // 61 => Ok(TypedValue::LONGDATE) ,
 // 62 => Ok(TypedValue::SECONDDATE) ,
 // 63 => Ok(TypedValue::DAYDATE) ,
 // 64 => Ok(TypedValue::SECONDTIME) ,
    _ => Err(Error::new(ErrorKind::Other,format!("parse_value() not implemented for type code {}",val))),
}}

fn parse_string(rdr: &mut BufRead) -> IoResult<String> {
    let length = try!(rdr.read_i16::<LittleEndian>());                  // I2 (always)
    let mut buffer: Vec<u8> = repeat(0u8).take(length as usize).collect();
    try!(rdr.read(&mut buffer[..]));                                    // variable
    Ok(try!(String::from_utf8(buffer)
                 .map_err(|_|{Error::new(ErrorKind::Other, "Invalid UTF-8 received for String-typed value")})))
}

fn parse_binary(rdr: &mut BufRead) -> IoResult<Vec<u8>> {
    let length = try!(rdr.read_i16::<LittleEndian>());                  // I2 (always)
    let mut vec: Vec<u8> = repeat(0u8).take(length as usize).collect();
    try!(rdr.read(&mut vec[..]));                                       // variable
    Ok(vec)
}
