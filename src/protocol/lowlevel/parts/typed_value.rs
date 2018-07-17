use protocol::lowlevel::parts::hdb_decimal::{serialize_decimal, HdbDecimal};
use protocol::lowlevel::parts::lob::*;
use protocol::lowlevel::parts::longdate::LongDate;
use protocol::lowlevel::parts::type_id;
use protocol::lowlevel::{cesu8, util};
use {HdbError, HdbResult};

use byteorder::{LittleEndian, WriteBytesExt};
use serde;
use serde_db::de::DbValue;
use std::fmt;
use std::i16;
use std::io;

const MAX_1_BYTE_LENGTH: u8 = 245;
const MAX_2_BYTE_LENGTH: i16 = i16::MAX;
const LENGTH_INDICATOR_2BYTE: u8 = 246;
const LENGTH_INDICATOR_4BYTE: u8 = 247;
const LENGTH_INDICATOR_NULL: u8 = 255;

/// Enum for all supported database value types.
#[allow(non_camel_case_types)]
#[derive(Clone, Debug)]
pub enum TypedValue {
    /// Internally used only. Is swapped in where a real value (any of the
    /// others) is swapped out.
    NOTHING,
    /// Stores an 8-bit unsigned integer.
    /// The minimum value is 0. The maximum value is 255.
    TINYINT(u8),
    /// Stores a 16-bit signed integer.
    /// The minimum value is -32,768. The maximum value is 32,767.
    SMALLINT(i16),
    /// Stores a 32-bit signed integer.
    /// The minimum value is -2,147,483,648. The maximum value is 2,147,483,647.
    INT(i32),
    /// Stores a 64-bit signed integer.
    /// The minimum value is -9,223,372,036,854,775,808.
    /// The maximum value is 9,223,372,036,854,775,807.
    BIGINT(i64),
    /// DECIMAL(p, s) is the SQL standard notation for fixed-point decimal.
    /// "p" specifies precision or the number of total digits
    /// (the sum of whole digits and fractional digits).
    /// "s" denotes scale or the number of fractional digits.
    /// If a column is defined as DECIMAL(5, 4) for example,
    /// the numbers 3.14, 3.1415, 3.141592 are stored in the column as 3.1400,
    /// 3.1415, 3.1415, retaining the specified precision(5) and scale(4).
    ///
    /// Precision p, can range from 1 to 38.
    /// The scale can range from 0 to p.
    /// If the scale is not specified, it defaults to 0.
    /// If precision and scale are not specified, DECIMAL becomes a
    /// floating-point decimal number. In this case, precision and scale
    /// can vary within the range 1 to 34 for precision and -6,111 to 6,176
    /// for scale, depending on the stored value.
    ///
    /// Examples:
    /// 0.0000001234 (1234E-10) has precision 4 and scale 10.
    /// 1.0000001234 (10000001234E-10) has precision 11 and scale 10.
    /// The value 1234000000 (1234E6) has precision 4 and scale -6.
    DECIMAL(HdbDecimal),
    /// Stores a single-precision 32-bit floating-point number.
    REAL(f32),
    /// Stores a double-precision 64-bit floating-point number.
    /// The minimum value is -1.7976931348623157E308, the maximum value is
    /// 1.7976931348623157E308 . The smallest positive DOUBLE value is
    /// 2.2250738585072014E-308 and the largest negative DOUBLE value is
    /// -2.2250738585072014E-308.
    DOUBLE(f64),
    /// Fixed-length character String, only ASCII-7 allowed.
    CHAR(String),
    /// The VARCHAR(n) data type specifies a variable-length character string,
    /// where n indicates the maximum length in bytes and is an integer between
    /// 1 and 5000. If the VARCHAR(n) data type is used in a DML query, for
    /// example CAST (A as VARCHAR(n)), <n> indicates the maximum length of
    /// the string in characters. SAP recommends using VARCHAR with ASCII
    /// characters based strings only. For data containing other
    /// characters, SAP recommends using the NVARCHAR data type instead.
    VARCHAR(String),
    /// Fixed-length character string.
    NCHAR(String),
    /// The NVARCHAR(n) data type specifies a variable-length Unicode character
    /// set string, where <n> indicates the maximum length in characters
    /// and is an integer between 1 and 5000.
    NVARCHAR(String),
    /// The BINARY(n) data type is used to store binary data of a specified
    /// length in bytes, where n indicates the fixed length and is an
    /// integer between 1 and 5000.
    BINARY(Vec<u8>),
    /// The VARBINARY(n) data type is used to store binary data of a specified
    /// maximum length in bytes,
    /// where n indicates the maximum length and is an integer between 1 and
    /// 5000.
    VARBINARY(Vec<u8>),
    /// The CLOB data type is used to store a large ASCII character string.
    CLOB(CLOB),
    /// The NCLOB data type is used to store a large Unicode string.
    NCLOB(CLOB),
    /// The BLOB data type is used to store a large binary string.
    BLOB(BLOB),
    /// BOOLEAN stores boolean values, which are TRUE or FALSE.
    BOOLEAN(bool),
    /// The DB returns all Strings as type STRING, independent of the concrete
    /// column type.
    STRING(String),
    /// Likely not used?
    NSTRING(String),
    /// The DB returns all binary values as type BSTRING.
    BSTRING(Vec<u8>),
    // / The SMALLDECIMAL is a floating-point decimal number.
    // / The precision and scale can vary within the range 1~16 for precision
    // / and -369~368 for scale,
    // / depending on the stored value. SMALLDECIMAL is only supported on column store.
    // / DECIMAL and SMALLDECIMAL are floating-point types.
    // / For instance, a decimal column can store any of 3.14, 3.1415, 3.141592
    // / whilst maintaining their precision.
    // / DECIMAL(p, s) is the SQL standard notation for fixed-point decimal.
    // / 3.14, 3.1415, 3.141592 are stored in a decimal(5, 4) column as 3.1400,
    // / 3.1415, 3.1415 for example,
    // / retaining the specified precision(5) and scale(4).
    // SMALLDECIMAL = 47, 				// SMALLDECIMAL data type, -
    /// The TEXT data type enables text search features.
    /// This data type can be defined for column tables, but not for row tables.
    /// This is not a standalone SQL-Type. Selecting a TEXT column yields a
    /// column of type NCLOB.
    TEXT(String),
    /// Similar to TEXT.
    SHORTTEXT(String),
    /// Timestamp, uses eight bytes.
    LONGDATE(LongDate),
    //  SECONDDATE(SecondDate),			// TIMESTAMP type with second precision, 3
    //  DAYDATE = 63, 					// DATE data type, 3
    //  SECONDTIME = 64, 				// TIME data type, 3
    /// Nullable variant of TINYINT.
    N_TINYINT(Option<u8>),
    /// Nullable variant of SMALLINT.
    N_SMALLINT(Option<i16>),
    /// Nullable variant of INT.
    N_INT(Option<i32>),
    /// Nullable variant of BIGINT.
    N_BIGINT(Option<i64>),
    /// Nullable variant of DECIMAL
    N_DECIMAL(Option<HdbDecimal>), // = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    /// Nullable variant of REAL.
    N_REAL(Option<f32>),
    /// Nullable variant of DOUBLE.
    N_DOUBLE(Option<f64>),
    /// Nullable variant of CHAR.
    N_CHAR(Option<String>),
    /// Nullable variant of VARCHAR.
    N_VARCHAR(Option<String>),
    /// Nullable variant of NCHAR.
    N_NCHAR(Option<String>),
    /// Nullable variant of NVARCHAR.
    N_NVARCHAR(Option<String>),
    /// Nullable variant of BINARY.
    N_BINARY(Option<Vec<u8>>),
    /// Nullable variant of VARBINARY.
    N_VARBINARY(Option<Vec<u8>>),
    /// Nullable variant of CLOB.
    N_CLOB(Option<CLOB>),
    /// Nullable variant of NCLOB.
    N_NCLOB(Option<CLOB>),
    /// Nullable variant of BLOB.
    N_BLOB(Option<BLOB>),
    /// Nullable variant of BOOLEAN.
    N_BOOLEAN(Option<bool>),
    /// Nullable variant of STRING.
    N_STRING(Option<String>),
    /// Nullable variant of NSTRING.
    N_NSTRING(Option<String>),
    /// Nullable variant of BSTRING.
    N_BSTRING(Option<Vec<u8>>),
    // N_SMALLDECIMAL = 47, 			// SMALLDECIMAL data type, -
    /// Nullable variant of TEXT.
    N_TEXT(Option<String>),
    /// Nullable variant of SHORTTEXT.
    N_SHORTTEXT(Option<String>),

    // N_SECONDDATE(Option<SecondDate>),// TIMESTAMP type with second precision, 3
    // N_DAYDATE = 63, 				    // DATE data type, 3
    // N_SECONDTIME = 64, 				// TIME data type, 3
    /// Nullable variant of LONGDATE.
    N_LONGDATE(Option<LongDate>),
}

impl TypedValue {
    fn serialize_type_id(&self, w: &mut io::Write) -> HdbResult<bool> {
        let is_null = match *self {
            TypedValue::N_TINYINT(None)
            | TypedValue::N_SMALLINT(None)
            | TypedValue::N_INT(None)
            | TypedValue::N_BIGINT(None)
            | TypedValue::N_REAL(None)
            | TypedValue::N_BOOLEAN(None)
            | TypedValue::N_LONGDATE(None)
            | TypedValue::N_CLOB(None)
            | TypedValue::N_NCLOB(None)
            | TypedValue::N_BLOB(None)
            | TypedValue::N_CHAR(None)
            | TypedValue::N_VARCHAR(None)
            | TypedValue::N_NCHAR(None)
            | TypedValue::N_NVARCHAR(None)
            | TypedValue::N_STRING(None)
            | TypedValue::N_NSTRING(None)
            | TypedValue::N_TEXT(None)
            | TypedValue::N_SHORTTEXT(None)
            | TypedValue::N_BINARY(None)
            | TypedValue::N_VARBINARY(None)
            | TypedValue::N_BSTRING(None) => true,
            _ => false,
        };

        if is_null {
            w.write_u8(self.type_id())?;
        } else {
            w.write_u8(self.type_id() % 128)?;
        }
        Ok(is_null)
    }

    /// hdb protocol uses ids < 128 for non-null values, and ids > 128 for null
    /// values
    fn type_id(&self) -> u8 {
        match *self {
            TypedValue::NOTHING => type_id::NOTHING,
            TypedValue::TINYINT(_) => type_id::TINYINT,
            TypedValue::SMALLINT(_) => type_id::SMALLINT,
            TypedValue::INT(_) => type_id::INT,
            TypedValue::BIGINT(_) => type_id::BIGINT,
            TypedValue::DECIMAL(_) => type_id::DECIMAL,
            TypedValue::REAL(_) => type_id::REAL,
            TypedValue::DOUBLE(_) => type_id::DOUBLE,
            TypedValue::CHAR(_) => type_id::CHAR,
            TypedValue::VARCHAR(_) => type_id::VARCHAR,
            TypedValue::NCHAR(_) => type_id::NCHAR,
            TypedValue::NVARCHAR(_) => type_id::NVARCHAR,
            TypedValue::BINARY(_) => type_id::BINARY,
            TypedValue::VARBINARY(_) => type_id::VARBINARY,
            // TypedValue::TIMESTAMP(_)        => type_id::TIMESTAMP,
            TypedValue::CLOB(_) => type_id::CLOB,
            TypedValue::NCLOB(_) => type_id::NCLOB,
            TypedValue::BLOB(_) => type_id::BLOB,
            TypedValue::BOOLEAN(_) => type_id::BOOLEAN,
            TypedValue::STRING(_) => type_id::STRING,
            TypedValue::NSTRING(_) => type_id::NSTRING,
            TypedValue::BSTRING(_) => type_id::BSTRING,
            // TypedValue::SMALLDECIMAL(_)     => type_id::SMALLDECIMAL,
            TypedValue::TEXT(_) => type_id::TEXT,
            TypedValue::SHORTTEXT(_) => type_id::SHORTTEXT,
            TypedValue::LONGDATE(_) => type_id::LONGDATE,
            // TypedValue::SECONDDATE(_)       => type_id::SECONDDATE,
            // TypedValue::DAYDATE(_)          => type_id::DAYDATE,
            // TypedValue::SECONDTIME(_)       => type_id::SECONDTIME,
            TypedValue::N_TINYINT(_) => type_id::N_TINYINT,
            TypedValue::N_SMALLINT(_) => type_id::N_SMALLINT,
            TypedValue::N_INT(_) => type_id::N_INT,
            TypedValue::N_BIGINT(_) => type_id::N_BIGINT,
            TypedValue::N_DECIMAL(_) => type_id::N_DECIMAL,
            TypedValue::N_REAL(_) => type_id::N_REAL,
            TypedValue::N_DOUBLE(_) => type_id::N_DOUBLE,
            TypedValue::N_CHAR(_) => type_id::N_CHAR,
            TypedValue::N_VARCHAR(_) => type_id::N_VARCHAR,
            TypedValue::N_NCHAR(_) => type_id::N_NCHAR,
            TypedValue::N_NVARCHAR(_) => type_id::N_NVARCHAR,
            TypedValue::N_BINARY(_) => type_id::N_BINARY,
            TypedValue::N_VARBINARY(_) => type_id::N_VARBINARY,
            // TypedValue::N_TIMESTAMP(_)       => type_id::N_TIMESTAMP,
            TypedValue::N_CLOB(_) => type_id::N_CLOB,
            TypedValue::N_NCLOB(_) => type_id::N_NCLOB,
            TypedValue::N_BLOB(_) => type_id::N_BLOB,
            TypedValue::N_BOOLEAN(_) => type_id::N_BOOLEAN,
            TypedValue::N_STRING(_) => type_id::N_STRING,
            TypedValue::N_NSTRING(_) => type_id::N_NSTRING,
            TypedValue::N_BSTRING(_) => type_id::N_BSTRING,
            // TypedValue::N_SMALLDECIMAL(_)    => type_id::N_SMALLDECIMAL,
            TypedValue::N_TEXT(_) => type_id::N_TEXT,
            TypedValue::N_SHORTTEXT(_) => type_id::N_SHORTTEXT,
            TypedValue::N_LONGDATE(_) => type_id::N_LONGDATE, /* TypedValue::N_SECONDDATE(_)
                                                               * => type_id::N_SECONDDATE,
                                                               * TypedValue::N_DAYDATE(_)
                                                               * => type_id::N_DAYDATE,
                                                               * TypedValue::N_SECONDTIME(_)
                                                               * => type_id::N_SECONDTIME, */
        }
    }

    /// Deserialize into a rust type
    pub fn try_into<'x, T: serde::Deserialize<'x>>(self) -> Result<T, HdbError> {
        Ok(DbValue::into_typed(self)?)
    }
}

pub fn serialize(tv: &TypedValue, data_pos: &mut i32, w: &mut io::Write) -> HdbResult<()> {
    if !tv.serialize_type_id(w)? {
        match *tv {
            TypedValue::NOTHING => {
                return Err(HdbError::Impl(
                    "Can't send TypedValue::NOTHING to Database".to_string(),
                ))
            }
            TypedValue::TINYINT(u) | TypedValue::N_TINYINT(Some(u)) => w.write_u8(u)?,

            TypedValue::SMALLINT(i) | TypedValue::N_SMALLINT(Some(i)) => {
                w.write_i16::<LittleEndian>(i)?
            }

            TypedValue::INT(i) | TypedValue::N_INT(Some(i)) => w.write_i32::<LittleEndian>(i)?,

            TypedValue::BIGINT(i) | TypedValue::N_BIGINT(Some(i)) => {
                w.write_i64::<LittleEndian>(i)?
            }

            TypedValue::DECIMAL(ref dec) | TypedValue::N_DECIMAL(Some(ref dec)) => {
                serialize_decimal(dec, w)?
            }

            TypedValue::REAL(f) | TypedValue::N_REAL(Some(f)) => w.write_f32::<LittleEndian>(f)?,

            TypedValue::DOUBLE(f) | TypedValue::N_DOUBLE(Some(f)) => {
                w.write_f64::<LittleEndian>(f)?
            }

            TypedValue::BOOLEAN(true) | TypedValue::N_BOOLEAN(Some(true)) => w.write_u8(1)?,
            TypedValue::BOOLEAN(false) | TypedValue::N_BOOLEAN(Some(false)) => w.write_u8(0)?,

            TypedValue::LONGDATE(ref ld) | TypedValue::N_LONGDATE(Some(ref ld)) => {
                w.write_i64::<LittleEndian>(*ld.ref_raw())?
            }

            TypedValue::CLOB(ref clob)
            | TypedValue::N_CLOB(Some(ref clob))
            | TypedValue::NCLOB(ref clob)
            | TypedValue::N_NCLOB(Some(ref clob)) => {
                serialize_clob_header(clob.len()?, data_pos, w)?
            }

            TypedValue::BLOB(ref blob) | TypedValue::N_BLOB(Some(ref blob)) => {
                serialize_blob_header(blob.len_alldata(), data_pos, w)?
            }

            TypedValue::STRING(ref s)
            | TypedValue::NSTRING(ref s)
            | TypedValue::TEXT(ref s)
            | TypedValue::SHORTTEXT(ref s)
            | TypedValue::N_STRING(Some(ref s))
            | TypedValue::N_NSTRING(Some(ref s))
            | TypedValue::N_TEXT(Some(ref s))
            | TypedValue::N_SHORTTEXT(Some(ref s)) => serialize_length_and_string(s, w)?,

            TypedValue::BINARY(ref v)
            | TypedValue::VARBINARY(ref v)
            | TypedValue::BSTRING(ref v)
            | TypedValue::N_BINARY(Some(ref v))
            | TypedValue::N_VARBINARY(Some(ref v))
            | TypedValue::N_BSTRING(Some(ref v)) => serialize_length_and_bytes(v, w)?,

            TypedValue::N_TINYINT(None)
            | TypedValue::N_SMALLINT(None)
            | TypedValue::N_INT(None)
            | TypedValue::N_BIGINT(None)
            | TypedValue::N_DECIMAL(None)
            | TypedValue::N_REAL(None)
            | TypedValue::N_DOUBLE(None)
            | TypedValue::N_BOOLEAN(None)
            | TypedValue::N_LONGDATE(None)
            | TypedValue::N_STRING(None)
            | TypedValue::N_NSTRING(None)
            | TypedValue::N_TEXT(None)
            | TypedValue::N_SHORTTEXT(None)
            | TypedValue::N_CLOB(None)
            | TypedValue::N_NCLOB(None)
            | TypedValue::N_BLOB(None)
            | TypedValue::N_BINARY(None)
            | TypedValue::N_VARBINARY(None)
            | TypedValue::N_BSTRING(None) => {}

            TypedValue::CHAR(_)
            | TypedValue::N_CHAR(_)
            | TypedValue::NCHAR(_)
            | TypedValue::N_NCHAR(_)
            | TypedValue::VARCHAR(_)
            | TypedValue::N_VARCHAR(_)
            | TypedValue::NVARCHAR(_)
            | TypedValue::N_NVARCHAR(_) => {
                return Err(HdbError::Impl(format!(
                    "TypedValue::serialize() not implemented for type code {}",
                    tv.type_id()
                )))
            }
        }
    }
    Ok(())
}

// is used to calculate the argument size (in serialize)
pub fn size(tv: &TypedValue) -> HdbResult<usize> {
    Ok(1 + match *tv {
        TypedValue::NOTHING => {
            return Err(HdbError::Impl(
                "Can't send TypedValue::NOTHING to Database".to_string(),
            ))
        }
        TypedValue::BOOLEAN(_)
        | TypedValue::N_BOOLEAN(Some(_))
        | TypedValue::TINYINT(_)
        | TypedValue::N_TINYINT(Some(_)) => 1,

        TypedValue::SMALLINT(_) | TypedValue::N_SMALLINT(Some(_)) => 2,

        TypedValue::DECIMAL(_) | TypedValue::N_DECIMAL(Some(_)) => 16,

        TypedValue::INT(_)
        | TypedValue::N_INT(Some(_))
        | TypedValue::REAL(_)
        | TypedValue::N_REAL(Some(_)) => 4,

        TypedValue::BIGINT(_)
        | TypedValue::N_BIGINT(Some(_))
        | TypedValue::DOUBLE(_)
        | TypedValue::N_DOUBLE(Some(_))
        | TypedValue::LONGDATE(_)
        | TypedValue::N_LONGDATE(Some(_)) => 8,

        TypedValue::CLOB(ref clob)
        | TypedValue::N_CLOB(Some(ref clob))
        | TypedValue::NCLOB(ref clob)
        | TypedValue::N_NCLOB(Some(ref clob)) => 9 + clob.len()?,

        TypedValue::BLOB(ref blob) | TypedValue::N_BLOB(Some(ref blob)) => 9 + blob.len_alldata(),

        TypedValue::STRING(ref s)
        | TypedValue::N_STRING(Some(ref s))
        | TypedValue::NSTRING(ref s)
        | TypedValue::N_NSTRING(Some(ref s))
        | TypedValue::TEXT(ref s)
        | TypedValue::N_TEXT(Some(ref s))
        | TypedValue::SHORTTEXT(ref s)
        | TypedValue::N_SHORTTEXT(Some(ref s)) => string_length(s),

        TypedValue::BINARY(ref v)
        | TypedValue::N_BINARY(Some(ref v))
        | TypedValue::VARBINARY(ref v)
        | TypedValue::N_VARBINARY(Some(ref v))
        | TypedValue::BSTRING(ref v)
        | TypedValue::N_BSTRING(Some(ref v)) => v.len() + 2,

        TypedValue::N_TINYINT(None)
        | TypedValue::N_SMALLINT(None)
        | TypedValue::N_INT(None)
        | TypedValue::N_BIGINT(None)
        | TypedValue::N_DECIMAL(None)
        | TypedValue::N_REAL(None)
        | TypedValue::N_DOUBLE(None)
        | TypedValue::N_BOOLEAN(None)
        | TypedValue::N_LONGDATE(None)
        | TypedValue::N_CLOB(None)
        | TypedValue::N_NCLOB(None)
        | TypedValue::N_BLOB(None)
        | TypedValue::N_BINARY(None)
        | TypedValue::N_VARBINARY(None)
        | TypedValue::N_BSTRING(None)
        | TypedValue::N_STRING(None)
        | TypedValue::N_NSTRING(None)
        | TypedValue::N_TEXT(None)
        | TypedValue::N_SHORTTEXT(None) => 0,

        TypedValue::CHAR(_)
        | TypedValue::VARCHAR(_)
        | TypedValue::NCHAR(_)
        | TypedValue::NVARCHAR(_)
        | TypedValue::N_CHAR(_)
        | TypedValue::N_VARCHAR(_)
        | TypedValue::N_NCHAR(_)
        | TypedValue::N_NVARCHAR(_) => {
            return Err(HdbError::Impl(format!(
                "TypedValue::size() not implemented for type code {}",
                tv.type_id()
            )))
        }
    })
}

pub fn string_length(s: &str) -> usize {
    match cesu8::cesu8_length(s) {
        clen if clen <= MAX_1_BYTE_LENGTH as usize => 1 + clen,
        clen if clen <= MAX_2_BYTE_LENGTH as usize => 3 + clen,
        clen => 5 + clen,
    }
}

pub fn serialize_length_and_string(s: &str, w: &mut io::Write) -> HdbResult<()> {
    serialize_length_and_bytes(&cesu8::string_to_cesu8(s), w)
}

fn serialize_length_and_bytes(v: &[u8], w: &mut io::Write) -> HdbResult<()> {
    match v.len() {
        l if l <= MAX_1_BYTE_LENGTH as usize => {
            w.write_u8(l as u8)?; // B1           LENGTH OF VALUE
        }
        l if l <= MAX_2_BYTE_LENGTH as usize => {
            w.write_u8(LENGTH_INDICATOR_2BYTE)?; // B1           246
            w.write_i16::<LittleEndian>(l as i16)?; // I2           LENGTH OF VALUE
        }
        l => {
            w.write_u8(LENGTH_INDICATOR_4BYTE)?; // B1           247
            w.write_i32::<LittleEndian>(l as i32)?; // I4           LENGTH OF VALUE
        }
    }
    util::serialize_bytes(v, w) // B variable   VALUE BYTES
}

fn serialize_blob_header(v_len: usize, data_pos: &mut i32, w: &mut io::Write) -> HdbResult<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_110_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(v_len as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*data_pos as i32)?; // I4           position
    *data_pos += v_len as i32;
    Ok(())
}

fn serialize_clob_header(s_len: usize, data_pos: &mut i32, w: &mut io::Write) -> HdbResult<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_110_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(s_len as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*data_pos as i32)?; // I4           position
    *data_pos += s_len as i32;
    Ok(())
}

impl fmt::Display for TypedValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TypedValue::NOTHING => write!(fmt, "Value already swapped out"),
            TypedValue::TINYINT(value) | TypedValue::N_TINYINT(Some(value)) => {
                write!(fmt, "{}", value)
            }
            TypedValue::SMALLINT(value) | TypedValue::N_SMALLINT(Some(value)) => {
                write!(fmt, "{}", value)
            }
            TypedValue::INT(value) | TypedValue::N_INT(Some(value)) => write!(fmt, "{}", value),
            TypedValue::BIGINT(value) | TypedValue::N_BIGINT(Some(value)) => {
                write!(fmt, "{}", value)
            }
            TypedValue::DECIMAL(ref value) | TypedValue::N_DECIMAL(Some(ref value)) => {
                write!(fmt, "{}", value)
            }
            TypedValue::REAL(value) | TypedValue::N_REAL(Some(value)) => write!(fmt, "{}", value),
            TypedValue::DOUBLE(value) | TypedValue::N_DOUBLE(Some(value)) => {
                write!(fmt, "{}", value)
            }
            TypedValue::CHAR(ref value)
            | TypedValue::N_CHAR(Some(ref value))
            | TypedValue::VARCHAR(ref value)
            | TypedValue::N_VARCHAR(Some(ref value))
            | TypedValue::NCHAR(ref value)
            | TypedValue::N_NCHAR(Some(ref value))
            | TypedValue::NVARCHAR(ref value)
            | TypedValue::N_NVARCHAR(Some(ref value))
            | TypedValue::STRING(ref value)
            | TypedValue::N_STRING(Some(ref value))
            | TypedValue::NSTRING(ref value)
            | TypedValue::N_NSTRING(Some(ref value))
            | TypedValue::TEXT(ref value)
            | TypedValue::N_TEXT(Some(ref value))
            | TypedValue::SHORTTEXT(ref value)
            | TypedValue::N_SHORTTEXT(Some(ref value)) => write!(fmt, "\"{}\"", value),
            TypedValue::BINARY(_) | TypedValue::N_BINARY(Some(_)) => write!(fmt, "<BINARY>"),
            TypedValue::VARBINARY(ref vec) | TypedValue::N_VARBINARY(Some(ref vec)) => {
                write!(fmt, "<VARBINARY length = {}>", vec.len())
            }
            TypedValue::CLOB(_) | TypedValue::N_CLOB(Some(_)) => write!(fmt, "<CLOB>"),
            TypedValue::NCLOB(_) | TypedValue::N_NCLOB(Some(_)) => write!(fmt, "<NCLOB>"),
            TypedValue::BLOB(ref blob) | TypedValue::N_BLOB(Some(ref blob)) => write!(
                fmt,
                "<BLOB length = {}, read = {}>",
                blob.len_alldata(),
                blob.len_readdata()
            ),
            TypedValue::BOOLEAN(value) | TypedValue::N_BOOLEAN(Some(value)) => {
                write!(fmt, "{}", value)
            }
            TypedValue::BSTRING(_) | TypedValue::N_BSTRING(Some(_)) => write!(fmt, "<BSTRING>"),
            TypedValue::LONGDATE(ref value) | TypedValue::N_LONGDATE(Some(ref value)) => {
                write!(fmt, "{}", value)
            }

            TypedValue::N_TINYINT(None)
            | TypedValue::N_SMALLINT(None)
            | TypedValue::N_INT(None)
            | TypedValue::N_BIGINT(None)
            | TypedValue::N_DECIMAL(None)
            | TypedValue::N_REAL(None)
            | TypedValue::N_DOUBLE(None)
            | TypedValue::N_CHAR(None)
            | TypedValue::N_VARCHAR(None)
            | TypedValue::N_NCHAR(None)
            | TypedValue::N_NVARCHAR(None)
            | TypedValue::N_BINARY(None)
            | TypedValue::N_VARBINARY(None)
            | TypedValue::N_CLOB(None)
            | TypedValue::N_NCLOB(None)
            | TypedValue::N_BLOB(None)
            | TypedValue::N_BOOLEAN(None)
            | TypedValue::N_STRING(None)
            | TypedValue::N_NSTRING(None)
            | TypedValue::N_BSTRING(None)
            | TypedValue::N_TEXT(None)
            | TypedValue::N_SHORTTEXT(None)
            | TypedValue::N_LONGDATE(None) => write!(fmt, "<NULL>"),
        }
    }
}

pub mod factory {
    use super::super::lob::*;
    use super::super::longdate::LongDate;
    use super::TypedValue;
    use byteorder::{LittleEndian, ReadBytesExt};
    use protocol::lowlevel::conn_core::AmConnCore;
    use protocol::lowlevel::parts::hdb_decimal::{parse_decimal, parse_nullable_decimal};
    use protocol::lowlevel::{cesu8, util};
    use std::io;
    use std::iter::repeat;
    use std::{u32, u64};
    use {HdbError, HdbResult};

    pub fn parse_from_reply(
        p_typecode: u8,
        nullable: bool,
        am_conn_core: &AmConnCore,
        rdr: &mut io::BufRead,
    ) -> HdbResult<TypedValue> {
        // here p_typecode is always < 127
        // the flag nullable from the metadata governs our behavior:
        // if it is true, we return types with typecode above 128, which use
        // Option<type>, if it is false, we return types with the original
        // typecode, which use plain values
        let typecode = p_typecode + if nullable { 128 } else { 0 };
        match typecode {
            1 => Ok(TypedValue::TINYINT({
                ind_not_null(rdr)?;
                rdr.read_u8()?
            })),
            2 => Ok(TypedValue::SMALLINT({
                ind_not_null(rdr)?;
                rdr.read_i16::<LittleEndian>()?
            })),
            3 => Ok(TypedValue::INT({
                ind_not_null(rdr)?;
                rdr.read_i32::<LittleEndian>()?
            })),
            4 => Ok(TypedValue::BIGINT({
                ind_not_null(rdr)?;
                rdr.read_i64::<LittleEndian>()?
            })),
            5 => Ok(TypedValue::DECIMAL(parse_decimal(rdr)?)),
            6 => Ok(TypedValue::REAL(parse_real(rdr)?)),
            7 => Ok(TypedValue::DOUBLE(parse_double(rdr)?)),
            8 => Ok(TypedValue::CHAR(parse_string(rdr)?)),
            9 => Ok(TypedValue::VARCHAR(parse_string(rdr)?)),
            10 => Ok(TypedValue::NCHAR(parse_string(rdr)?)),
            11 => Ok(TypedValue::NVARCHAR(parse_string(rdr)?)),
            12 => Ok(TypedValue::BINARY(parse_binary(rdr)?)),
            13 => Ok(TypedValue::VARBINARY(parse_binary(rdr)?)),
            // 16 => Ok(TypedValue::TIMESTAMP(
            // FIXME improve error handling:
            25 => Ok(TypedValue::CLOB(parse_clob(am_conn_core, rdr)?)),
            26 => Ok(TypedValue::NCLOB(parse_clob(am_conn_core, rdr)?)),
            27 => Ok(TypedValue::BLOB(parse_blob(am_conn_core, rdr)?)),
            28 => Ok(TypedValue::BOOLEAN(rdr.read_u8()? > 0)),
            29 => Ok(TypedValue::STRING(parse_string(rdr)?)),
            30 => Ok(TypedValue::NSTRING(parse_string(rdr)?)),
            33 => Ok(TypedValue::BSTRING(parse_binary(rdr)?)),
            // 47 => Ok(TypedValue::SMALLDECIMAL(
            51 => Ok(TypedValue::TEXT(parse_string(rdr)?)),
            52 => Ok(TypedValue::SHORTTEXT(parse_string(rdr)?)),
            61 => Ok(TypedValue::LONGDATE(parse_longdate(rdr)?)),
            // 62 => Ok(TypedValue::SECONDDATE(
            // 63 => Ok(TypedValue::DAYDATE(
            // 64 => Ok(TypedValue::SECONDTIME(
            129 => Ok(TypedValue::N_TINYINT(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_u8()?)
            })),

            130 => Ok(TypedValue::N_SMALLINT(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_i16::<LittleEndian>()?)
            })),
            131 => Ok(TypedValue::N_INT(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_i32::<LittleEndian>()?)
            })),
            132 => Ok(TypedValue::N_BIGINT(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_i64::<LittleEndian>()?)
            })),
            133 => Ok(TypedValue::N_DECIMAL(parse_nullable_decimal(rdr)?)),
            134 => Ok(TypedValue::N_REAL(parse_nullable_real(rdr)?)),
            135 => Ok(TypedValue::N_DOUBLE(parse_nullable_double(rdr)?)),
            136 => Ok(TypedValue::N_CHAR(parse_nullable_string(rdr)?)),
            137 => Ok(TypedValue::N_VARCHAR(parse_nullable_string(rdr)?)),
            138 => Ok(TypedValue::N_NCHAR(parse_nullable_string(rdr)?)),
            139 => Ok(TypedValue::N_NVARCHAR(parse_nullable_string(rdr)?)),
            140 => Ok(TypedValue::N_BINARY(parse_nullable_binary(rdr)?)),
            141 => Ok(TypedValue::N_VARBINARY(parse_nullable_binary(rdr)?)),
            // 144 => Ok(TypedValue::N_TIMESTAMP(
            153 => Ok(TypedValue::N_CLOB(parse_nullable_clob(am_conn_core, rdr)?)),
            154 => Ok(TypedValue::N_NCLOB(parse_nullable_clob(am_conn_core, rdr)?)),
            155 => Ok(TypedValue::N_BLOB(parse_nullable_blob_from_reply(
                am_conn_core,
                rdr,
            )?)),
            156 => Ok(TypedValue::N_BOOLEAN(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_u8()? > 0)
            })),
            157 => Ok(TypedValue::N_STRING(parse_nullable_string(rdr)?)),
            158 => Ok(TypedValue::N_NSTRING(parse_nullable_string(rdr)?)),
            161 => Ok(TypedValue::N_BSTRING(parse_nullable_binary(rdr)?)),
            // 175 => Ok(TypedValue::N_SMALLDECIMAL(
            179 => Ok(TypedValue::N_TEXT(parse_nullable_string(rdr)?)),
            180 => Ok(TypedValue::N_SHORTTEXT(parse_nullable_string(rdr)?)),
            189 => Ok(TypedValue::N_LONGDATE(parse_nullable_longdate(rdr)?)),
            // 190 => Ok(TypedValue::N_SECONDDATE(
            // 191 => Ok(TypedValue::N_DAYDATE(
            // 192 => Ok(TypedValue::N_SECONDTIME(
            _ => Err(HdbError::Impl(format!(
                "TypedValue::parse_from_reply() not implemented for type code {}",
                typecode
            ))),
        }
    }

    // reads the nullindicator and returns Ok(true) if it has value 0 or Ok(false)
    // otherwise
    fn ind_null(rdr: &mut io::BufRead) -> HdbResult<bool> {
        Ok(rdr.read_u8()? == 0)
    }

    // reads the nullindicator and throws an error if it has value 0
    fn ind_not_null(rdr: &mut io::BufRead) -> HdbResult<()> {
        if ind_null(rdr)? {
            Err(HdbError::Impl(
                "null value returned for not-null column".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn parse_real(rdr: &mut io::BufRead) -> HdbResult<f32> {
        let mut vec: Vec<u8> = repeat(0u8).take(4).collect();
        rdr.read_exact(&mut vec[..])?;
        let mut r = io::Cursor::new(&vec);
        let tmp = r.read_u32::<LittleEndian>()?;
        match tmp {
            u32::MAX => Err(HdbError::Impl(
                "Unexpected NULL Value in parse_real()".to_owned(),
            )),
            _ => {
                r.set_position(0);
                Ok(r.read_f32::<LittleEndian>()?)
            }
        }
    }

    fn parse_nullable_real(rdr: &mut io::BufRead) -> HdbResult<Option<f32>> {
        let mut vec: Vec<u8> = repeat(0u8).take(4).collect();
        rdr.read_exact(&mut vec[..])?;
        let mut r = io::Cursor::new(&vec);
        let tmp = r.read_u32::<LittleEndian>()?;
        match tmp {
            u32::MAX => Ok(None),
            _ => {
                r.set_position(0);
                Ok(Some(r.read_f32::<LittleEndian>()?))
            }
        }
    }

    fn parse_double(rdr: &mut io::BufRead) -> HdbResult<f64> {
        let mut vec: Vec<u8> = repeat(0u8).take(8).collect();
        rdr.read_exact(&mut vec[..])?;
        let mut r = io::Cursor::new(&vec);
        let tmp = r.read_u64::<LittleEndian>()?;
        match tmp {
            u64::MAX => Err(HdbError::Impl(
                "Unexpected NULL Value in parse_double()".to_owned(),
            )),
            _ => {
                r.set_position(0);
                Ok(r.read_f64::<LittleEndian>()?)
            }
        }
    }

    fn parse_nullable_double(rdr: &mut io::BufRead) -> HdbResult<Option<f64>> {
        let mut vec: Vec<u8> = repeat(0u8).take(8).collect();
        rdr.read_exact(&mut vec[..])?;
        let mut r = io::Cursor::new(&vec);
        let tmp = r.read_u64::<LittleEndian>()?;
        match tmp {
            u64::MAX => Ok(None),
            _ => {
                r.set_position(0);
                Ok(Some(r.read_f64::<LittleEndian>()?))
            }
        }
    }

    // ----- STRINGS and BINARIES
    // ----------------------------------------------------------------
    pub fn parse_string(rdr: &mut io::BufRead) -> HdbResult<String> {
        match cesu8::cesu8_to_string(&parse_binary(rdr)?) {
            Ok(s) => Ok(s),
            Err(e) => {
                error!("cesu-8 problem occured in typed_value:parse_string()");
                Err(e)
            }
        }
    }

    fn parse_binary(rdr: &mut io::BufRead) -> HdbResult<Vec<u8>> {
        let l8 = rdr.read_u8()?; // B1
        let len = match l8 {
            l if l <= super::MAX_1_BYTE_LENGTH => l8 as usize,
            super::LENGTH_INDICATOR_2BYTE => rdr.read_i16::<LittleEndian>()? as usize, // I2
            super::LENGTH_INDICATOR_4BYTE => rdr.read_i32::<LittleEndian>()? as usize, // I4
            l => {
                return Err(HdbError::Impl(format!(
                    "Invalid value in length indicator: {}",
                    l
                )));
            }
        };
        let result = util::parse_bytes(len, rdr)?; // B (varying)
        trace!("parse_binary(): read_bytes = {:?}", result);
        Ok(result)
    }

    fn parse_nullable_string(rdr: &mut io::BufRead) -> HdbResult<Option<String>> {
        match parse_nullable_binary(rdr)? {
            Some(vec) => match cesu8::cesu8_to_string(&vec) {
                Ok(s) => Ok(Some(s)),
                Err(_) => Err(HdbError::Impl(
                    "cesu-8 problem occured in typed_value:parse_string()".to_owned(),
                )),
            },
            None => Ok(None),
        }
    }

    fn parse_nullable_binary(rdr: &mut io::BufRead) -> HdbResult<Option<Vec<u8>>> {
        let l8 = rdr.read_u8()?; // B1
        let len = match l8 {
            l if l <= super::MAX_1_BYTE_LENGTH => l8 as usize,
            super::LENGTH_INDICATOR_2BYTE => rdr.read_i16::<LittleEndian>()? as usize, // I2
            super::LENGTH_INDICATOR_4BYTE => rdr.read_i32::<LittleEndian>()? as usize, // I4
            super::LENGTH_INDICATOR_NULL => return Ok(None),
            l => {
                return Err(HdbError::Impl(format!(
                    "Invalid value in length indicator: {}",
                    l
                )))
            }
        };
        let result = util::parse_bytes(len, rdr)?;
        trace!("parse_nullable_binary(): read_bytes = {:?}", result);
        Ok(Some(result)) // B (varying)
    }

    // ----- BLOBS and CLOBS
    // ===
    // regular parse
    pub fn parse_blob(am_conn_core: &AmConnCore, rdr: &mut io::BufRead) -> HdbResult<BLOB> {
        match parse_nullable_blob_from_reply(am_conn_core, rdr)? {
            Some(blob) => Ok(blob),
            None => Err(HdbError::Impl(
                "Null value found for non-null blob column".to_owned(),
            )),
        }
    }

    pub fn parse_nullable_blob_from_reply(
        am_conn_core: &AmConnCore,
        rdr: &mut io::BufRead,
    ) -> HdbResult<Option<BLOB>> {
        let (is_null, is_last_data) = parse_lob_1(rdr)?;
        if is_null {
            Ok(None)
        } else {
            let (_, length_b, locator_id, data) = parse_lob_2(rdr)?;
            Ok(Some(new_blob_from_db(
                am_conn_core,
                is_last_data,
                length_b,
                locator_id,
                data,
            )))
        }
    }

    pub fn parse_clob(am_conn_core: &AmConnCore, rdr: &mut io::BufRead) -> HdbResult<CLOB> {
        match parse_nullable_clob(am_conn_core, rdr)? {
            Some(clob) => Ok(clob),
            None => Err(HdbError::Impl(
                "Null value found for non-null clob column".to_owned(),
            )),
        }
    }

    pub fn parse_nullable_clob(
        am_conn_core: &AmConnCore,
        rdr: &mut io::BufRead,
    ) -> HdbResult<Option<CLOB>> {
        let (is_null, is_last_data) = parse_lob_1(rdr)?;
        if is_null {
            Ok(None)
        } else {
            let (length_c, length_b, locator_id, data) = parse_lob_2(rdr)?;
            Ok(Some(new_clob_from_db(
                am_conn_core,
                is_last_data,
                length_c,
                length_b,
                locator_id,
                &data,
            )))
        }
    }

    fn parse_lob_1(rdr: &mut io::BufRead) -> HdbResult<(bool, bool)> {
        rdr.consume(1); // let data_type = rdr.read_u8()?; // I1  "type of data": unclear
        let options = rdr.read_u8()?; // I1
        let is_null = (options & 0b_1_u8) != 0;
        let is_last_data = (options & 0b_100_u8) != 0;
        Ok((is_null, is_last_data))
    }

    fn parse_lob_2(rdr: &mut io::BufRead) -> HdbResult<(u64, u64, u64, Vec<u8>)> {
        rdr.consume(2); // U2 (filler)
        let length_c = rdr.read_u64::<LittleEndian>()?; // I8
        let length_b = rdr.read_u64::<LittleEndian>()?; // I8
        let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
        let chunk_length = rdr.read_i32::<LittleEndian>()?; // I4
        let data = util::parse_bytes(chunk_length as usize, rdr)?; // B[chunk_length]
        trace!("Got LOB locator {}", locator_id);
        Ok((length_c, length_b, locator_id, data))
    }

    // -----  LongDates
    // --------------------------------------------------------------------------
    // SECONDDATE_NULL_REPRESENTATION:
    const LONGDATE_NULL_REPRESENTATION: i64 = 3_155_380_704_000_000_001_i64;
    fn parse_longdate(rdr: &mut io::BufRead) -> HdbResult<LongDate> {
        let i = rdr.read_i64::<LittleEndian>()?;
        match i {
            LONGDATE_NULL_REPRESENTATION => Err(HdbError::Impl(
                "Null value found for non-null longdate column".to_owned(),
            )),
            _ => Ok(LongDate::new(i)),
        }
    }

    fn parse_nullable_longdate(rdr: &mut io::BufRead) -> HdbResult<Option<LongDate>> {
        let i = rdr.read_i64::<LittleEndian>()?;
        match i {
            LONGDATE_NULL_REPRESENTATION => Ok(None),
            _ => Ok(Some(LongDate::new(i))),
        }
    }
}
