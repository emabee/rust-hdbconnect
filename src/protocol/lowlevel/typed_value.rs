use super::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use std::i16;
use std::u32;
use std::u64;
use std::io::{self,Cursor,Read};
use std::iter::repeat;

#[allow(non_camel_case_types)]
#[derive(Clone,Debug)]
pub enum TypedValue {               // Description, Support Level
    NULL, 						    // NULL value, -
    TINYINT(u8),                    // TINYINT, 1
    SMALLINT(i16), 			        // SMALLINT, 1
    INT(i32), 				        // INTEGER, 1
    BIGINT(i64), 			        // BIGINT, 1
//  DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    REAL(f32), 				        // REAL, 1
    DOUBLE(f64), 			        // DOUBLE, 1
    CHAR(String), 			        // CHAR, 1
    VARCHAR(String), 		        // VARCHAR, 1
    NCHAR(String), 			        // NCHAR (Unicode character type), 1
    NVARCHAR(String),		        // NVARCHAR (Unicode character type), 1
    BINARY(Vec<u8>), 		        // BINARY, 1
    VARBINARY(Vec<u8>),		        // VARBINARY, 1
    CLOB(String), 			        // Character Large Object, 1
    NCLOB(String), 			        // Unicode Character Large Object, 1
    BLOB(Vec<u8>), 			        // Binary Large Object, 1
    BOOLEAN(bool), 			        // Boolean
    STRING(String), 		        // Character string, 1
    NSTRING(String),		        // Unicode character string, 1
//  BLOCATOR = 31, 					// Binary locator, 1
//  NLOCATOR = 32, 					// Unicode character locator, 1
    BSTRING(Vec<u8>),   	        // Binary string, 1
//  SMALLDECIMAL = 47, 				// SMALLDECIMAL data type, -
//  ABAPITAB = 48, 					// ABAPSTREAM procedure parameter, 1
//  ABAPSTRUCT = 49, 				// ABAP structure procedure parameter, 1
    TEXT(String), 			        // TEXT data type, 3
    SHORTTEXT(String), 		        // SHORTTEXT data type, 3
    LONGDATE(i64), 					// TIMESTAMP data type, 3
//  SECONDDATE = 62, 				// TIMESTAMP type with second precision, 3
//  DAYDATE = 63, 					// DATE data type, 3
//  SECONDTIME = 64, 				// TIME data type, 3

    N_TINYINT(Option<u8>),          // TINYINT, 1
    N_SMALLINT(Option<i16>), 		// SMALLINT, 1
    N_INT(Option<i32>), 			// INTEGER, 1
    N_BIGINT(Option<i64>), 			// BIGINT, 1
//  N_DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    N_REAL(Option<f32>), 			// REAL, 1
    N_DOUBLE(Option<f64>), 			// DOUBLE, 1
    N_CHAR(Option<String>), 		// CHAR, 1
    N_VARCHAR(Option<String>), 		// VARCHAR, 1
    N_NCHAR(Option<String>), 		// NCHAR (Unicode character type), 1
    N_NVARCHAR(Option<String>),		// NVARCHAR (Unicode character type), 1
    N_BINARY(Option<Vec<u8>>), 		// BINARY, 1
    N_VARBINARY(Option<Vec<u8>>),	// VARBINARY, 1
    N_CLOB(Option<String>), 		// Character Large Object, 1
    N_NCLOB(Option<String>), 		// Unicode Character Large Object, 1
    N_BLOB(Option<Vec<u8>>), 		// Binary Large Object, 1
    N_BOOLEAN(Option<bool>), 		// Boolean
    N_STRING(Option<String>), 		// Character string, 1
    N_NSTRING(Option<String>),		// Unicode character string, 1
//  N_BLOCATOR = 31, 				// Binary locator, 1
//  N_NLOCATOR = 32, 				// Unicode character locator, 1
    N_BSTRING(Option<Vec<u8>>),   	// Binary string, 1
//  N_SMALLDECIMAL = 47, 			// SMALLDECIMAL data type, -
//  N_ABAPITAB = 48, 				// ABAPSTREAM procedure parameter, 1
//  N_ABAPSTRUCT = 49, 				// ABAP structure procedure parameter, 1
    N_TEXT(Option<String>), 		// TEXT data type, 3
    N_SHORTTEXT(Option<String>), 	// SHORTTEXT data type, 3
    N_LONGDATE(Option<i64>),    	// TIMESTAMP data type, 3
//  N_SECONDDATE = 62, 				// TIMESTAMP type with second precision, 3
//  N_DAYDATE = 63, 				// DATE data type, 3
//  N_SECONDTIME = 64, 				// TIME data type, 3
}

impl TypedValue {
    pub fn encode(&self, w: &mut io::Write) -> io::Result<()> {
        try!(w.write_u8(self.type_id()));                   // I1
        match *self {                                       // variable
            TypedValue::NULL                => {},
            TypedValue::TINYINT(u)          => try!(w.write_u8(u)),
            TypedValue::SMALLINT(i)         => try!(w.write_i16::<LittleEndian>(i)),
            TypedValue::INT(i)              => try!(w.write_i32::<LittleEndian>(i)),
            TypedValue::BIGINT(i)           => try!(w.write_i64::<LittleEndian>(i)),
            TypedValue::REAL(f)             => try!(w.write_f32::<LittleEndian>(f)),
            TypedValue::DOUBLE(f)           => try!(w.write_f64::<LittleEndian>(f)),
            TypedValue::BOOLEAN(b)          => try!(w.write_u8(match b{true => 1, false => 0})),
            TypedValue::LONGDATE(i)         => try!(w.write_i64::<LittleEndian>(i)),
            TypedValue::CHAR(ref s) |
            TypedValue::VARCHAR(ref s) |
            TypedValue::NCHAR(ref s) |
            TypedValue::NVARCHAR(ref s) |
            TypedValue::STRING(ref s) |
            TypedValue::NSTRING(ref s) |
            TypedValue::TEXT(ref s) |
            TypedValue::CLOB(ref s) |
            TypedValue::NCLOB(ref s) |
            TypedValue::SHORTTEXT(ref s)    => try!(encode_length_and_string(s,w)),
            TypedValue::BINARY(ref v) |
            TypedValue::VARBINARY(ref v) |
            TypedValue::BLOB(ref v) |
            TypedValue::BSTRING(ref v)      => try!(encode_length_and_bytes(v,w)),

            TypedValue::N_TINYINT(o)        => if let Some(u) = o {try!(w.write_u8(u))},
            TypedValue::N_SMALLINT(o)       => if let Some(i) = o {try!(w.write_i16::<LittleEndian>(i))},
            TypedValue::N_INT(o)            => if let Some(i) = o {try!(w.write_i32::<LittleEndian>(i))},
            TypedValue::N_BIGINT(o)         => if let Some(i) = o {try!(w.write_i64::<LittleEndian>(i))},
            TypedValue::N_REAL(o)           => if let Some(f) = o {try!(w.write_f32::<LittleEndian>(f))},
            TypedValue::N_DOUBLE(o)         => if let Some(f) = o {try!(w.write_f64::<LittleEndian>(f))},
            TypedValue::N_BOOLEAN(o)        => if let Some(b) = o {try!(w.write_u8(match b{true => 1, false => 0}))},
            TypedValue::N_LONGDATE(o)       => if let Some(i) = o {try!(w.write_i64::<LittleEndian>(i))},
            TypedValue::N_CHAR(ref o) |
            TypedValue::N_VARCHAR(ref o) |
            TypedValue::N_NCHAR(ref o) |
            TypedValue::N_NVARCHAR(ref o) |
            TypedValue::N_STRING(ref o) |
            TypedValue::N_NSTRING(ref o) |
            TypedValue::N_TEXT(ref o) |
            TypedValue::N_CLOB(ref o) |
            TypedValue::N_NCLOB(ref o) |
            TypedValue::N_SHORTTEXT(ref o)  => if let &Some(ref s) = o {try!(encode_length_and_string(s,w))},
            TypedValue::N_BINARY(ref o) |
            TypedValue::N_VARBINARY(ref o) |
            TypedValue::N_BLOB(ref o) |
            TypedValue::N_BSTRING(ref o)    => if let &Some(ref v) = o {try!(encode_length_and_bytes(v,w))},
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
            TypedValue::TINYINT(_)          => 1,
            TypedValue::SMALLINT(_)         => 2,
            TypedValue::INT(_)              => 4,
            TypedValue::BIGINT(_)           => 8,
            TypedValue::REAL(_)             => 4,
            TypedValue::DOUBLE(_)           => 8,
            TypedValue::BOOLEAN(_)          => 1,
            TypedValue::LONGDATE(_)         => 8,
            TypedValue::CHAR(ref s) |
            TypedValue::VARCHAR(ref s) |
            TypedValue::NCHAR(ref s) |
            TypedValue::NVARCHAR(ref s) |
            TypedValue::STRING(ref s) |
            TypedValue::NSTRING(ref s) |
            TypedValue::TEXT(ref s) |
            TypedValue::CLOB(ref s) |
            TypedValue::NCLOB(ref s) |
            TypedValue::SHORTTEXT(ref s)    => l_hdblen(s),
            TypedValue::BINARY(ref v) |
            TypedValue::VARBINARY(ref v) |
            TypedValue::BLOB(ref v) |
            TypedValue::BSTRING(ref v)      => v.len() + 2,

            TypedValue::N_TINYINT(o)        => match o {Some(_) => 1, None => 0},
            TypedValue::N_SMALLINT(o)       => match o {Some(_) => 2, None => 0},
            TypedValue::N_INT(o)            => match o {Some(_) => 4, None => 0},
            TypedValue::N_BIGINT(o)         => match o {Some(_) => 8, None => 0},
            TypedValue::N_REAL(o)           => match o {Some(_) => 4, None => 0},
            TypedValue::N_DOUBLE(o)         => match o {Some(_) => 8, None => 0},
            TypedValue::N_BOOLEAN(o)        => match o {Some(_) => 1, None => 0},
            TypedValue::N_LONGDATE(o)       => match o {Some(_) => 8, None => 0},
            TypedValue::N_CHAR(ref o) |
            TypedValue::N_VARCHAR(ref o) |
            TypedValue::N_NCHAR(ref o) |
            TypedValue::N_NVARCHAR(ref o) |
            TypedValue::N_STRING(ref o) |
            TypedValue::N_NSTRING(ref o) |
            TypedValue::N_TEXT(ref o) |
            TypedValue::N_CLOB(ref o) |
            TypedValue::N_NCLOB(ref o) |
            TypedValue::N_SHORTTEXT(ref o)  => match o {&Some(ref s) => l_hdblen(s), &None => 0},
            TypedValue::N_BINARY(ref o) |
            TypedValue::N_VARBINARY(ref o) |
            TypedValue::N_BLOB(ref o) |
            TypedValue::N_BSTRING(ref o)    => match o {&Some(ref v) => v.len() + 2, &None => 0},
        }
    }

    /// hdb protocol uses ids < 128 for non-null values, and ids > 128 for null values
    pub fn type_id(&self) -> u8 { match *self {
        TypedValue::NULL                =>   0,
        TypedValue::TINYINT(_)          =>   1,
        TypedValue::INT(_)              =>   3,
        TypedValue::SMALLINT(_)         =>   2,
        TypedValue::BIGINT(_)           =>   4,
    //  TypedValue::DECIMAL(_)          =>   5,
        TypedValue::REAL(_)             =>   6,
        TypedValue::DOUBLE(_)           =>   7,
        TypedValue::CHAR(_)             =>   8,
        TypedValue::VARCHAR(_)          =>   9,
        TypedValue::NCHAR(_)            =>  10,
        TypedValue::NVARCHAR(_)         =>  11,
        TypedValue::BINARY(_)           =>  12,
        TypedValue::VARBINARY(_)        =>  13,
     // TypedValue::TIMESTAMP(_)        =>  16,
        TypedValue::CLOB(_)             =>  25,
        TypedValue::NCLOB(_)            =>  26,
        TypedValue::BLOB(_)             =>  27,
        TypedValue::BOOLEAN(_)          =>  28,
        TypedValue::STRING(_)           =>  29,
        TypedValue::NSTRING(_)          =>  30,
     // TypedValue::BLOCATOR(_)         =>  31,
     // TypedValue::NLOCATOR(_)         =>  32,
        TypedValue::BSTRING(_)          =>  33,
     // TypedValue::SMALLDECIMAL(_)     =>  47,
     // TypedValue::ABAPITAB(_)         =>  48,
     // TypedValue::ABAPSTRUCT(_)       =>  49,
        TypedValue::TEXT(_)             =>  51,
        TypedValue::SHORTTEXT(_)        =>  52,
        TypedValue::LONGDATE(_)         =>  61,
     // TypedValue::SECONDDATE(_)       =>  62,
     // TypedValue::DAYDATE(_)          =>  63,
     // TypedValue::SECONDTIME(_)       =>  64,

        TypedValue::N_TINYINT(_)         =>  1 + 128,
        TypedValue::N_SMALLINT(_)        =>  2 + 128,
        TypedValue::N_INT(_)             =>  3 + 128,
        TypedValue::N_BIGINT(_)          =>  4 + 128,
    //  TypedValue::N_DECIMALo_)         =>  5 + 128,
        TypedValue::N_REAL(_)            =>  6 + 128,
        TypedValue::N_DOUBLE(_)          =>  7 + 128,
        TypedValue::N_CHAR(_)            =>  8 + 128,
        TypedValue::N_VARCHAR(_)         =>  9 + 128,
        TypedValue::N_NCHAR(_)           => 10 + 128,
        TypedValue::N_NVARCHAR(_)        => 11 + 128,
        TypedValue::N_BINARY(_)          => 12 + 128,
        TypedValue::N_VARBINARY(_)       => 13 + 128,
     // TypedValue::N_TIMESTAMP(_)       => 16 + 128,
        TypedValue::N_CLOB(_)            => 25 + 128,
        TypedValue::N_NCLOB(_)           => 26 + 128,
        TypedValue::N_BLOB(_)            => 27 + 128,
        TypedValue::N_BOOLEAN(_)         => 28 + 128,
        TypedValue::N_STRING(_)          => 29 + 128,
        TypedValue::N_NSTRING(_)         => 30 + 128,
     // TypedValue::N_BLOCATOR(_)        => 31 + 128,
     // TypedValue::N_NLOCATOR(_)        => 32 + 128,
        TypedValue::N_BSTRING(_)         => 33 + 128,
     // TypedValue::N_SMALLDECIMAL(_)    => 47 + 128,
     // TypedValue::N_ABAPITAB(_)        => 48 + 128,
     // TypedValue::N_ABAPSTRUCT(_)      => 49 + 128,
        TypedValue::N_TEXT(_)            => 51 + 128,
        TypedValue::N_SHORTTEXT(_)       => 52 + 128,
        TypedValue::N_LONGDATE(_)        => 61 + 128,
     // TypedValue::N_SECONDDATE(_)      => 62 + 128,
     // TypedValue::N_DAYDATE(_)         => 63 + 128,
     // TypedValue::N_SECONDTIME(_)      => 64 + 128,
    }}

    pub fn parse(typecode: u8, nullable: bool, rdr: &mut io::BufRead) -> io::Result<TypedValue> {
        // let null_indicator = if typecode > 128 {
        //     try!(rdr.read_u8())
        // } else {
        //     1
        // };
        TypedValue::parse_value(typecode, nullable, rdr)
    }

    /// here typecode is always < 127
    // the flag nullable from the metadata governs our behavior:
    // if it is true, we return types with typecode above 128, which use Option<type>,
    // if it is false, we return types with the original typecode, which use plain values
    fn parse_value(p_typecode: u8, nullable: bool, rdr: &mut io::BufRead)
      -> io::Result<TypedValue>
    {
        let typecode = p_typecode + match nullable {true => 128, false => 0};
        match typecode {
            1  => Ok(TypedValue::TINYINT(    { try!(ind_not_null(rdr)); try!(rdr.read_u8()) }) ),
            2  => Ok(TypedValue::SMALLINT(   { try!(ind_not_null(rdr)); try!(rdr.read_i16::<LittleEndian>()) }) ),
            3  => Ok(TypedValue::INT(        { try!(ind_not_null(rdr)); try!(rdr.read_i32::<LittleEndian>()) }) ),
            4  => Ok(TypedValue::BIGINT(     { try!(ind_not_null(rdr)); try!(rdr.read_i64::<LittleEndian>()) }) ),
         // 5  => Ok(TypedValue::DECIMAL(
            6  => Ok(TypedValue::REAL(       try!(parse_real(rdr)) )),
            7  => Ok(TypedValue::DOUBLE(     try!(parse_double(rdr)) )),
            8  => Ok(TypedValue::CHAR(       try!(parse_length_and_string(rdr)) )),
            9  => Ok(TypedValue::VARCHAR(    try!(parse_length_and_string(rdr)) )),
            10 => Ok(TypedValue::NCHAR(      try!(parse_length_and_string(rdr)) )),
            11 => Ok(TypedValue::NVARCHAR(   try!(parse_length_and_string(rdr)) )),
            12 => Ok(TypedValue::BINARY(     try!(parse_length_and_binary(rdr)) )),
            13 => Ok(TypedValue::VARBINARY(  try!(parse_length_and_binary(rdr)) )),
         // 16 => Ok(TypedValue::TIMESTAMP(
            25 => Ok(TypedValue::CLOB(       try!(parse_length_and_string(rdr)) )),
            26 => Ok(TypedValue::NCLOB(      try!(parse_length_and_string(rdr)) )),
            27 => Ok(TypedValue::BLOB(       try!(parse_length_and_binary(rdr)) )),
            28 => Ok(TypedValue::BOOLEAN(    try!(rdr.read_u8()) > 0 )),
            29 => Ok(TypedValue::STRING(     try!(parse_length_and_string(rdr)) )),
            30 => Ok(TypedValue::NSTRING(    try!(parse_length_and_string(rdr)) )),
         // 31 => Ok(TypedValue::BLOCATOR(
         // 32 => Ok(TypedValue::NLOCATOR(
            33 => Ok(TypedValue::BSTRING(    try!(parse_length_and_binary(rdr)) )),
         // 47 => Ok(TypedValue::SMALLDECIMAL(
         // 48 => Ok(TypedValue::ABAPITAB(
         // 49 => Ok(TypedValue::ABAPSTRUCT(
            51 => Ok(TypedValue::TEXT(       try!(parse_length_and_string(rdr)) )),
            52 => Ok(TypedValue::SHORTTEXT(  try!(parse_length_and_string(rdr)) )),
            61 => Ok(TypedValue::LONGDATE(   { try!(ind_not_null(rdr)); try!(rdr.read_i64::<LittleEndian>()) }) ),
         // 62 => Ok(TypedValue::SECONDDATE(
         // 63 => Ok(TypedValue::DAYDATE(
         // 64 => Ok(TypedValue::SECONDTIME(

            128 => Ok(TypedValue::NULL) ,
            129 => Ok(TypedValue::N_TINYINT(match try!(ind_null(rdr)) {
                                true  => None,
                                false => Some(try!(rdr.read_u8()))
                    })),
            130 => Ok(TypedValue::N_SMALLINT(match try!(ind_null(rdr)) {
                                true  => None,
                                false => Some(try!(rdr.read_i16::<LittleEndian>()))
                    })),
            131 => Ok(TypedValue::N_INT(match try!(ind_null(rdr)) {
                                true  => None,
                                false => Some(try!(rdr.read_i32::<LittleEndian>()))
                    })),
            132 => Ok(TypedValue::N_BIGINT(match try!(ind_null(rdr)) {
                                true  => None,
                                false => Some(try!(rdr.read_i64::<LittleEndian>()))
                    })),
         // 133 => Ok(TypedValue::N_DECIMAL(
            134 => Ok(TypedValue::N_REAL(       try!(parse_nullable_real(rdr)) )),
            135 => Ok(TypedValue::N_DOUBLE(     try!(parse_nullable_double(rdr)) )),
            136 => Ok(TypedValue::N_CHAR(       try!(parse_nullable_length_and_string(rdr)) )),
            137 => Ok(TypedValue::N_VARCHAR(    try!(parse_nullable_length_and_string(rdr)) )),
            138 => Ok(TypedValue::N_NCHAR(      try!(parse_nullable_length_and_string(rdr)) )),
            139 => Ok(TypedValue::N_NVARCHAR(   try!(parse_nullable_length_and_string(rdr)) )),
            140 => Ok(TypedValue::N_BINARY(     try!(parse_nullable_length_and_binary(rdr)) )),
            141 => Ok(TypedValue::N_VARBINARY(  try!(parse_nullable_length_and_binary(rdr)) )),
         // 144 => Ok(TypedValue::N_TIMESTAMP(
            153 => Ok(TypedValue::N_CLOB(       try!(parse_nullable_length_and_string(rdr)) )),
            154 => Ok(TypedValue::N_NCLOB(      try!(parse_nullable_length_and_string(rdr)) )),
            155 => Ok(TypedValue::N_BLOB(       try!(parse_nullable_length_and_binary(rdr)) )),
            156 => Ok(TypedValue::N_BOOLEAN(match try!(ind_null(rdr)) {
                                true  => None,
                                false => Some(try!(rdr.read_u8()) > 0),
                    })),
            157 => Ok(TypedValue::N_STRING(     try!(parse_nullable_length_and_string(rdr)) )),
            158 => Ok(TypedValue::N_NSTRING(    try!(parse_nullable_length_and_string(rdr)) )),
         // 159 => Ok(TypedValue::N_BLOCATOR(
         // 160 => Ok(TypedValue::N_NLOCATOR(
            161 => Ok(TypedValue::N_BSTRING(    try!(parse_nullable_length_and_binary(rdr)) )),
         // 175 => Ok(TypedValue::N_SMALLDECIMAL(
         // 176 => Ok(TypedValue::N_ABAPITAB(
         // 177 => Ok(TypedValue::N_ABAPSTRUCT(
            179 => Ok(TypedValue::N_TEXT(       try!(parse_nullable_length_and_string(rdr)) )),
            180 => Ok(TypedValue::N_SHORTTEXT(  try!(parse_nullable_length_and_string(rdr)) )),
            189 => Ok(TypedValue::N_LONGDATE(match try!(ind_null(rdr)) {
                                true  => None,
                                false => Some(try!(rdr.read_i64::<LittleEndian>()))
                    })),
         // 190 => Ok(TypedValue::N_SECONDDATE(
         // 191 => Ok(TypedValue::N_DAYDATE(
         // 192 => Ok(TypedValue::N_SECONDTIME(

            _   => Err(util::io_error(&format!("TypedValue::parse_value() not implemented for type code {}",typecode))),
        }
    }
}

// reads the nullindicator and returns Ok(true) if it has value 0 or Ok(false) otherwise
fn ind_null(rdr: &mut io::BufRead) -> io::Result<bool> {
    Ok( try!(rdr.read_u8()) == 0 )
}

// reads the nullindicator and throws an error if it has value 0
fn ind_not_null(rdr: &mut io::BufRead) -> io::Result<()> {
    match try!(ind_null(rdr)) {
        true => Err(util::io_error("null value returned for not-null column")),
        false => Ok(())
    }
}


fn parse_real(rdr: &mut io::BufRead) -> io::Result<f32> {
    let mut vec: Vec<u8> = repeat(0u8).take(4).collect();
    try!(rdr.read(&mut vec[..]));

    let mut r = Cursor::new(&vec);
    let tmp = try!(r.read_u32::<LittleEndian>());
    match tmp {
        u32::MAX => Err(util::io_error("Unexpected NULL Value in parse_real()")),
        _ => {r.set_position(0); Ok(try!(r.read_f32::<LittleEndian>()))},
    }
}

fn parse_nullable_real(rdr: &mut io::BufRead) -> io::Result<Option<f32>> {
    let mut vec: Vec<u8> = repeat(0u8).take(4).collect();
    try!(rdr.read(&mut vec[..]));
    let mut r = Cursor::new(&vec);
    let tmp = try!(r.read_u32::<LittleEndian>());
    match tmp {
        u32::MAX => Ok(None),
        _ => {r.set_position(0); Ok(Some(try!(r.read_f32::<LittleEndian>())))},
    }
}

fn parse_double(rdr: &mut io::BufRead) -> io::Result<f64> {
    let mut vec: Vec<u8> = repeat(0u8).take(8).collect();
    try!(rdr.read(&mut vec[..]));
    let mut r = Cursor::new(&vec);
    let tmp = try!(r.read_u64::<LittleEndian>());
    match tmp {
        u64::MAX => Err(util::io_error("Unexpected NULL Value in parse_double()")),
        _ => {r.set_position(0); Ok(try!(r.read_f64::<LittleEndian>()))},
    }
}

fn parse_nullable_double(rdr: &mut io::BufRead) -> io::Result<Option<f64>> {
    let mut vec: Vec<u8> = repeat(0u8).take(8).collect();
    try!(rdr.read(&mut vec[..]));
    let mut r = Cursor::new(&vec);
    let tmp = try!(r.read_u64::<LittleEndian>());
    match tmp {
        u64::MAX => Ok(None),
        _ => {r.set_position(0); Ok(Some(try!(r.read_f64::<LittleEndian>())))},
    }
}

const MAX_1_BYTE_LENGTH:u8      = 245;
const MAX_2_BYTE_LENGTH:i16     = i16::MAX;
const LENGTH_INDICATOR_2BYTE:u8 = 246;
const LENGTH_INDICATOR_4BYTE:u8 = 247;
const LENGTH_INDICATOR_NULL:u8  = 255;

fn encode_length_and_string(s: &String, w: &mut io::Write) -> io::Result<()> {
    encode_length_and_bytes(&util::string_to_cesu8(s), w)
}

fn encode_length_and_bytes(v: &Vec<u8>, w: &mut io::Write) -> io::Result<()> {
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

fn parse_length_and_string(rdr: &mut io::BufRead) -> io::Result<String> {
    match util::cesu8_to_string(&try!(parse_length_and_binary(rdr))) {
        Ok(s) => Ok(s),
        Err(e) => {error!("cesu-8 problem occured in typed_value:parse_length_and_string()");Err(e)}
    }
}

fn parse_length_and_binary(rdr: &mut io::BufRead) -> io::Result<Vec<u8>> {
    let l8 = try!(rdr.read_u8());                                                   // B1
    let len = match l8 {
        l if l <= MAX_1_BYTE_LENGTH => l8 as usize,
        LENGTH_INDICATOR_2BYTE  =>  try!(rdr.read_i16::<LittleEndian>()) as usize,  // I2
        LENGTH_INDICATOR_4BYTE  =>  try!(rdr.read_i32::<LittleEndian>()) as usize,  // I4
        l => {panic!("Invalid value in length indicator: {}",l)},
    };
    util::parse_bytes(len,rdr)                                                      // B variable
}

fn parse_nullable_length_and_string(rdr: &mut io::BufRead) -> io::Result<Option<String>> {
    match try!(parse_nullable_length_and_binary(rdr)) {
        Some(vec) => match util::cesu8_to_string(&vec) {
            Ok(s) => Ok(Some(s)),
            Err(_) => Err(util::io_error("cesu-8 problem occured in typed_value:parse_length_and_string()")),
        },
        None => Ok(None),
    }
}

fn parse_nullable_length_and_binary(rdr: &mut io::BufRead) -> io::Result<Option<Vec<u8>>> {
    let l8 = try!(rdr.read_u8());                                                   // B1
    let len = match l8 {
        l if l <= MAX_1_BYTE_LENGTH => l8 as usize,
        LENGTH_INDICATOR_2BYTE  => try!(rdr.read_i16::<LittleEndian>()) as usize,   // I2
        LENGTH_INDICATOR_4BYTE  => try!(rdr.read_i32::<LittleEndian>()) as usize,   // I4
        LENGTH_INDICATOR_NULL   => return Ok(None),
        l => {panic!("Invalid value in length indicator: {}",l)},
    };
    Ok(Some(try!(util::parse_bytes(len,rdr))))                                            // B variable
}
