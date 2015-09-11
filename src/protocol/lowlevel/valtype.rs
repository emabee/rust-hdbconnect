use std::fmt;
use std::io::{Error,ErrorKind};
use std::io::Result as IoResult;

#[allow(dead_code)]
#[allow(non_camel_case_types)]
pub enum ValType {                  // Description, Support Level
    NULL = 0, 						// NULL value, -
    TINYINT = 1, 					// TINYINT, 1
    SMALLINT = 2, 					// SMALLINT, 1
    INT = 3, 						// INTEGER, 1
    BIGINT = 4, 					// BIGINT, 1
    DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    REAL = 6, 						// REAL, 1
    DOUBLE = 7, 					// DOUBLE, 1
    CHAR = 8, 						// CHAR, 1
    VARCHAR = 9, 					// VARCHAR, 1
    NCHAR = 10, 					// NCHAR (Unicode character type), 1
    NVARCHAR = 11, 					// NVARCHAR (Unicode character type), 1
    BINARY = 12, 					// BINARY, 1
    VARBINARY = 13, 				// VARBINARY, 1
//  DATE = 14, 						// DATE (deprecated type), 1 (deprecated with 3)
//  TIME = 15, 						// TIME (deprecated type), 1 (deprecated with 3)
    TIMESTAMP = 16, 				// TIMESTAMP (millisecond precision), 1 (deprecated with 3)
//  TIME_TZ = 17, 					// Reserved, do not use, -
//  TIME_LTZ = 18, 					// Reserved, do not use, -
//  TIMESTAMP_TZ = 19, 				// Reserved, do not use, -
//  TIMESTAMP_LTZ = 20, 			// Reserved, do not use, -
//  INTERVAL_YM = 21, 				// Reserved, do not use, -
//  INTERVAL_DS = 22, 				// Reserved, do not use, -
//  ROWID = 23, 					// Reserved, do not use, -
//  UROWID = 24, 					// Reserved, do not use, -
    CLOB = 25, 						// Character Large Object, 1
    NCLOB = 26, 					// Unicode Character Large Object, 1
    BLOB = 27, 						// Binary Large Object, 1
//  BOOLEAN = 28, 					// Reserved, do not use, -
    STRING = 29, 					// Character string, 1
    NSTRING = 30, 					// Unicode character string, 1
    BLOCATOR = 31, 					// Binary locator, 1
    NLOCATOR = 32, 					// Unicode character locator, 1
    BSTRING = 33, 					// Binary string, 1
//  DECIMAL_DIGIT_ARRAY = 34, 		// Reserved, do not use, -
    VARCHAR2 = 35, 					// VARCHAR, -
    VARCHAR3 = 36, 					// VARCHAR, -
    NVARCHAR3 = 37, 				// NVARCHAR, -
    VARBINARY3 = 38, 				// VARBINARY, -
//  VARGROUP = 39, 					// Reserved, do not use, -
//  TINYINT_NOTNULL = 40, 			// Reserved, do not use, -
//  SMALLINT_NOTNULL = 41, 			// Reserved, do not use, -
//  INT_NOTNULL = 42, 				// Reserved, do not use, -
//  BIGINT_NOTNULL = 43, 			// Reserved, do not use, -
//  ARGUMENT = 44, 					// Reserved, do not use, -
//  TABLE = 45, 					// Reserved, do not use, -
//  CURSOR = 46, 					// Reserved, do not use, -
    SMALLDECIMAL = 47, 				// SMALLDECIMAL data type, -
    ABAPITAB = 48, 					// ABAPSTREAM procedure parameter, 1
    ABAPSTRUCT = 49, 				// ABAP structure procedure parameter, 1
//  ARRAY = 50, 					// Reserved, do not use, -
    TEXT = 51, 						// TEXT data type, 3
    SHORTTEXT = 52, 				// SHORTTEXT data type, 3
//  FIXEDSTRING = 53, 				// Reserved, do not use, -
//  FIXEDPOINTDECIMAL = 54, 		// Reserved, do not use, -
    ALPHANUM = 55, 					// ALPHANUM data type, 3
//  TLOCATOR = 56, 					// Reserved, do not use, -
    LONGDATE = 61, 					// TIMESTAMP data type, 3
    SECONDDATE = 62, 				// TIMESTAMP type with second precision, 3
    DAYDATE = 63, 					// DATE data type, 3
    SECONDTIME = 64, 				// TIME data type, 3
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
impl ValType {
    pub fn to_i8(&self) -> i8 {match *self {
        ValType::NULL => 0,
        ValType::TINYINT => 1,
        ValType::SMALLINT => 2,
        ValType::INT => 3,
        ValType::BIGINT => 4,
        ValType::DECIMAL => 5,
        ValType::REAL => 6,
        ValType::DOUBLE => 7,
        ValType::CHAR => 8,
        ValType::VARCHAR => 9,
        ValType::NCHAR => 10,
        ValType::NVARCHAR => 11,
        ValType::BINARY => 12,
        ValType::VARBINARY => 13,
        ValType::TIMESTAMP => 16,
        ValType::CLOB => 25,
        ValType::NCLOB => 26,
        ValType::BLOB => 27,
        ValType::STRING => 29,
        ValType::NSTRING => 30,
        ValType::BLOCATOR => 31,
        ValType::NLOCATOR => 32,
        ValType::BSTRING => 33,
        ValType::VARCHAR2 => 35,
        ValType::VARCHAR3 => 36,
        ValType::NVARCHAR3 => 37,
        ValType::VARBINARY3 => 38,
        ValType::SMALLDECIMAL => 47,
        ValType::ABAPITAB => 48,
        ValType::ABAPSTRUCT => 49,
        ValType::TEXT => 51,
        ValType::SHORTTEXT => 52,
        ValType::ALPHANUM => 55,
        ValType::LONGDATE => 61,
        ValType::SECONDDATE => 62,
        ValType::DAYDATE => 63,
        ValType::SECONDTIME => 64,
    }}

    pub fn from_i8(val: i8) -> IoResult<ValType> { match val {
        0 => Ok(ValType::NULL) ,
        1 => Ok(ValType::TINYINT) ,
        2 => Ok(ValType::SMALLINT) ,
        3 => Ok(ValType::INT) ,
        4 => Ok(ValType::BIGINT) ,
        5 => Ok(ValType::DECIMAL) ,
        6 => Ok(ValType::REAL) ,
        7 => Ok(ValType::DOUBLE) ,
        8 => Ok(ValType::CHAR) ,
        9 => Ok(ValType::VARCHAR) ,
        10 => Ok(ValType::NCHAR) ,
        11 => Ok(ValType::NVARCHAR) ,
        12 => Ok(ValType::BINARY) ,
        13 => Ok(ValType::VARBINARY) ,
        16 => Ok(ValType::TIMESTAMP) ,
        25 => Ok(ValType::CLOB) ,
        26 => Ok(ValType::NCLOB) ,
        27 => Ok(ValType::BLOB) ,
        29 => Ok(ValType::STRING) ,
        30 => Ok(ValType::NSTRING) ,
        31 => Ok(ValType::BLOCATOR) ,
        32 => Ok(ValType::NLOCATOR) ,
        33 => Ok(ValType::BSTRING) ,
        35 => Ok(ValType::VARCHAR2) ,
        36 => Ok(ValType::VARCHAR3) ,
        37 => Ok(ValType::NVARCHAR3) ,
        38 => Ok(ValType::VARBINARY3) ,
        47 => Ok(ValType::SMALLDECIMAL) ,
        48 => Ok(ValType::ABAPITAB) ,
        49 => Ok(ValType::ABAPSTRUCT) ,
        51 => Ok(ValType::TEXT) ,
        52 => Ok(ValType::SHORTTEXT) ,
        55 => Ok(ValType::ALPHANUM) ,
        61 => Ok(ValType::LONGDATE) ,
        62 => Ok(ValType::SECONDDATE) ,
        63 => Ok(ValType::DAYDATE) ,
        64 => Ok(ValType::SECONDTIME) ,
        _ => Err(Error::new(ErrorKind::Other,format!("Invalid value for ValType detected: {}",val))),
    }}
}


impl fmt::Debug for ValType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(),fmt::Error> {
        try!(write!(f, "{}", self.to_i8()));
        Ok(())
    }
}
