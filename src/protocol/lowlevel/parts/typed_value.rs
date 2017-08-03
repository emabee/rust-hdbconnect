use protocol::lowlevel::{PrtError, PrtResult, util};
use super::type_id::*;
use super::lob::*;
use super::longdate::LongDate;
use {HdbError, HdbResult};

use byteorder::{LittleEndian, WriteBytesExt};
use std::i16;
use std::io;

const MAX_1_BYTE_LENGTH: u8 = 245;
const MAX_2_BYTE_LENGTH: i16 = i16::MAX;
const LENGTH_INDICATOR_2BYTE: u8 = 246;
const LENGTH_INDICATOR_4BYTE: u8 = 247;
const LENGTH_INDICATOR_NULL: u8 = 255;

/// Enum for all supported database value types.
#[allow(non_camel_case_types)]
#[derive(Clone,Debug)]
pub enum TypedValue {
    /// TINYINT stores an 8-bit unsigned integer.
    /// The minimum value is 0. The maximum value is 255.
    TINYINT(u8),
    /// SMALLINT stores a 16-bit signed integer.
    /// The minimum value is -32,768. The maximum value is 32,767.
    SMALLINT(i16),
    /// INT stores a 32-bit signed integer.
    /// The minimum value is -2,147,483,648. The maximum value is 2,147,483,647.
    INT(i32),
    /// BIGINT stores a 64-bit signed integer.
    /// The minimum value is -9,223,372,036,854,775,808.
    /// The maximum value is 9,223,372,036,854,775,807.
    BIGINT(i64),
    // / DECIMAL(p, s) is the SQL standard notation for fixed-point decimal.
    // / "p" specifies precision or the number of total digits
    // / (the sum of whole digits and fractional digits).
    // / "s" denotes scale or the number of fractional digits.
    // / If a column is defined as DECIMAL(5, 4) for example,
    // / the numbers 3.14, 3.1415, 3.141592 are stored in the column as 3.1400, 3.1415, 3.1415,
    // / retaining the specified precision(5) and scale(4).
    // /
    // / Precision p, can range from 1 to 38.
    // / The scale can range from 0 to p.
    // / If the scale is not specified, it defaults to 0.
    // / If precision and scale are not specified, DECIMAL becomes a floating-point decimal number.
    // / In this case, precision and scale can vary within the range 1 to 34 for precision
    // / and -6,111 to 6,176 for scale, depending on the stored value.
    // /
    // / Examples:
    // / 0.0000001234 (1234E-10) has precision 4 and scale 10.
    // / 1.0000001234 (10000001234E-10) has precision 11 and scale 10.
    // / The value 1234000000 (1234E6) has precision 4 and scale -6.
    // DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
    /// REAL stores a single-precision 32-bit floating-point number.
    REAL(f32),
    /// DOUBLE stores a double-precision 64-bit floating-point number.
    /// The minimum value is -1.7976931348623157E308, the maximum value is 1.7976931348623157E308 .
    /// The smallest positive DOUBLE value is 2.2250738585072014E-308 and the
    /// largest negative DOUBLE value is -2.2250738585072014E-308.
    DOUBLE(f64),
    /// Fixed-length character String, only ASCII-7 allowed.
    CHAR(String),
    /// The VARCHAR(n) data type specifies a variable-length character string,
    /// where n indicates the maximum length in bytes and is an integer between 1 and 5000.
    /// If the VARCHAR(n) data type is used in a DML query, for example CAST (A as VARCHAR(n)),
    /// <n> indicates the maximum length of the string in characters.
    /// SAP recommends using VARCHAR with ASCII characters based strings only.
    /// For data containing other characters, SAP recommends using the NVARCHAR
    /// data type instead.
    VARCHAR(String),
    /// Fixed-length character string.
    NCHAR(String),
    /// The NVARCHAR(n) data type specifies a variable-length Unicode character set string,
    /// where <n> indicates the maximum length in characters and is an integer between 1 and 5000.
    NVARCHAR(String),
    /// The BINARY(n) data type is used to store binary data of a specified length in bytes,
    /// where n indicates the fixed length and is an integer between 1 and 5000.
    BINARY(Vec<u8>),
    /// The VARBINARY(n) data type is used to store binary data of a specified
    /// maximum length in bytes,
    /// where n indicates the maximum length and is an integer between 1 and 5000.
    VARBINARY(Vec<u8>),
    /// The CLOB data type is used to store a large ASCII character string.
    CLOB(CLOB),
    /// The NCLOB data type is used to store a large Unicode string.
    NCLOB(CLOB),
    /// The BLOB data type is used to store a large binary string.
    BLOB(BLOB),
    /// BOOLEAN stores boolean values, which are TRUE or FALSE.
    BOOLEAN(bool),
    /// The DB returns all Strings as type STRING, independent of the concrete column type.
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
    /// This is not a standalone SQL-Type. Selecting a TEXT column yields a column of type NCLOB.
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
    // N_DECIMAL = 5, 					// DECIMAL, and DECIMAL(p,s), 1
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
    // FIXME Rework the following very ncomplete set of functions
    ///
    pub fn get_i32(&self) -> HdbResult<i32> {
        match *self {
            TypedValue::INT(i) => Ok(i),
            TypedValue::N_INT(Some(i)) => Ok(i),
            _ => Err(HdbError::UsageError("Not a i32 value")),
        }
    }

    ///
    pub fn get_string(&self) -> HdbResult<String> {
        match *self {
            TypedValue::VARCHAR(ref s) |
            TypedValue::NVARCHAR(ref s) |
            TypedValue::STRING(ref s) => Ok(s.clone()),
            TypedValue::N_VARCHAR(Some(ref s)) |
            TypedValue::N_NVARCHAR(Some(ref s)) |
            TypedValue::N_STRING(Some(ref s)) => Ok(s.clone()),
            TypedValue::CLOB(_) |
            TypedValue::NCLOB(_) |
            TypedValue::N_CLOB(_) |
            TypedValue::N_NCLOB(_) => Err(HdbError::UsageError("Is a CLOB, not a String value")),
            _ => Err(HdbError::UsageError("Not a String value")),
        }
    }

    fn serialize_type_id(&self, w: &mut io::Write) -> PrtResult<bool> {
        let is_null = match *self {
            TypedValue::N_TINYINT(None) |
            TypedValue::N_SMALLINT(None) |
            TypedValue::N_INT(None) |
            TypedValue::N_BIGINT(None) |
            TypedValue::N_REAL(None) |
            TypedValue::N_BOOLEAN(None) |
            TypedValue::N_LONGDATE(None) |
            TypedValue::N_CLOB(None) |
            TypedValue::N_NCLOB(None) |
            TypedValue::N_BLOB(None) |
            TypedValue::N_CHAR(None) |
            TypedValue::N_VARCHAR(None) |
            TypedValue::N_NCHAR(None) |
            TypedValue::N_NVARCHAR(None) |
            TypedValue::N_STRING(None) |
            TypedValue::N_NSTRING(None) |
            TypedValue::N_TEXT(None) |
            TypedValue::N_SHORTTEXT(None) |
            TypedValue::N_BINARY(None) |
            TypedValue::N_VARBINARY(None) |
            TypedValue::N_BSTRING(None) => true,
            _ => false,
        };

        if is_null {
            w.write_u8(self.type_id())?;
        } else {
            w.write_u8(self.type_id() % 128)?;
        }
        Ok(is_null)
    }


    /// hdb protocol uses ids < 128 for non-null values, and ids > 128 for null values
    fn type_id(&self) -> u8 {
        match *self {
            TypedValue::TINYINT(_) => TYPEID_TINYINT,
            TypedValue::SMALLINT(_) => TYPEID_SMALLINT,
            TypedValue::INT(_) => TYPEID_INT,
            TypedValue::BIGINT(_) => TYPEID_BIGINT,
            // TypedValue::DECIMAL(_)          => TYPEID_DECIMAL,
            TypedValue::REAL(_) => TYPEID_REAL,
            TypedValue::DOUBLE(_) => TYPEID_DOUBLE,
            TypedValue::CHAR(_) => TYPEID_CHAR,
            TypedValue::VARCHAR(_) => TYPEID_VARCHAR,
            TypedValue::NCHAR(_) => TYPEID_NCHAR,
            TypedValue::NVARCHAR(_) => TYPEID_NVARCHAR,
            TypedValue::BINARY(_) => TYPEID_BINARY,
            TypedValue::VARBINARY(_) => TYPEID_VARBINARY,
            // TypedValue::TIMESTAMP(_)        => TYPEID_TIMESTAMP,
            TypedValue::CLOB(_) => TYPEID_CLOB,
            TypedValue::NCLOB(_) => TYPEID_NCLOB,
            TypedValue::BLOB(_) => TYPEID_BLOB,
            TypedValue::BOOLEAN(_) => TYPEID_BOOLEAN,
            TypedValue::STRING(_) => TYPEID_STRING,
            TypedValue::NSTRING(_) => TYPEID_NSTRING,
            TypedValue::BSTRING(_) => TYPEID_BSTRING,
            // TypedValue::SMALLDECIMAL(_)     => TYPEID_SMALLDECIMAL,
            TypedValue::TEXT(_) => TYPEID_TEXT,
            TypedValue::SHORTTEXT(_) => TYPEID_SHORTTEXT,
            TypedValue::LONGDATE(_) => TYPEID_LONGDATE,
            // TypedValue::SECONDDATE(_)       => TYPEID_SECONDDATE,
            // TypedValue::DAYDATE(_)          => TYPEID_DAYDATE,
            // TypedValue::SECONDTIME(_)       => TYPEID_SECONDTIME,
            TypedValue::N_TINYINT(_) => TYPEID_N_TINYINT,
            TypedValue::N_SMALLINT(_) => TYPEID_N_SMALLINT,
            TypedValue::N_INT(_) => TYPEID_N_INT,
            TypedValue::N_BIGINT(_) => TYPEID_N_BIGINT,
            // TypedValue::N_DECIMAL(_)         => TYPEID_N_DECIMAL,
            TypedValue::N_REAL(_) => TYPEID_N_REAL,
            TypedValue::N_DOUBLE(_) => TYPEID_N_DOUBLE,
            TypedValue::N_CHAR(_) => TYPEID_N_CHAR,
            TypedValue::N_VARCHAR(_) => TYPEID_N_VARCHAR,
            TypedValue::N_NCHAR(_) => TYPEID_N_NCHAR,
            TypedValue::N_NVARCHAR(_) => TYPEID_N_NVARCHAR,
            TypedValue::N_BINARY(_) => TYPEID_N_BINARY,
            TypedValue::N_VARBINARY(_) => TYPEID_N_VARBINARY,
            // TypedValue::N_TIMESTAMP(_)       => TYPEID_N_TIMESTAMP,
            TypedValue::N_CLOB(_) => TYPEID_N_CLOB,
            TypedValue::N_NCLOB(_) => TYPEID_N_NCLOB,
            TypedValue::N_BLOB(_) => TYPEID_N_BLOB,
            TypedValue::N_BOOLEAN(_) => TYPEID_N_BOOLEAN,
            TypedValue::N_STRING(_) => TYPEID_N_STRING,
            TypedValue::N_NSTRING(_) => TYPEID_N_NSTRING,
            TypedValue::N_BSTRING(_) => TYPEID_N_BSTRING,
            // TypedValue::N_SMALLDECIMAL(_)    => TYPEID_N_SMALLDECIMAL,
            TypedValue::N_TEXT(_) => TYPEID_N_TEXT,
            TypedValue::N_SHORTTEXT(_) => TYPEID_N_SHORTTEXT,
            TypedValue::N_LONGDATE(_) => TYPEID_N_LONGDATE,
            // TypedValue::N_SECONDDATE(_)      => TYPEID_N_SECONDDATE,
            // TypedValue::N_DAYDATE(_)         => TYPEID_N_DAYDATE,
            // TypedValue::N_SECONDTIME(_)      => TYPEID_N_SECONDTIME,
        }
    }
}

pub fn serialize(tv: &TypedValue, data_pos: &mut i32, w: &mut io::Write) -> PrtResult<()> {
    fn _serialize_not_implemented(type_id: u8) -> PrtError {
        return PrtError::ProtocolError(format!("TypedValue::serialize() not implemented for \
                                                type code {}",
                                               type_id));
    }

    if !tv.serialize_type_id(w)? {
        match *tv {
            TypedValue::TINYINT(u) |
            TypedValue::N_TINYINT(Some(u)) => w.write_u8(u)?,

            TypedValue::SMALLINT(i) |
            TypedValue::N_SMALLINT(Some(i)) => w.write_i16::<LittleEndian>(i)?,

            TypedValue::INT(i) |
            TypedValue::N_INT(Some(i)) => w.write_i32::<LittleEndian>(i)?,

            TypedValue::BIGINT(i) |
            TypedValue::N_BIGINT(Some(i)) => w.write_i64::<LittleEndian>(i)?,

            TypedValue::REAL(f) |
            TypedValue::N_REAL(Some(f)) => w.write_f32::<LittleEndian>(f)?,

            TypedValue::DOUBLE(f) |
            TypedValue::N_DOUBLE(Some(f)) => w.write_f64::<LittleEndian>(f)?,

            TypedValue::BOOLEAN(true) |
            TypedValue::N_BOOLEAN(Some(true)) => w.write_u8(1)?,
            TypedValue::BOOLEAN(false) |
            TypedValue::N_BOOLEAN(Some(false)) => w.write_u8(0)?,

            TypedValue::LONGDATE(LongDate(i)) |
            TypedValue::N_LONGDATE(Some(LongDate(i))) => w.write_i64::<LittleEndian>(i)?,

            TypedValue::CLOB(ref clob) |
            TypedValue::N_CLOB(Some(ref clob)) |
            TypedValue::NCLOB(ref clob) |
            TypedValue::N_NCLOB(Some(ref clob)) => serialize_clob_header(clob.len()?, data_pos, w)?,

            TypedValue::BLOB(ref blob) |
            TypedValue::N_BLOB(Some(ref blob)) => serialize_blob_header(blob.len()?, data_pos, w)?,

            TypedValue::STRING(ref s) |
            TypedValue::NSTRING(ref s) |
            TypedValue::TEXT(ref s) |
            TypedValue::SHORTTEXT(ref s) |
            TypedValue::N_STRING(Some(ref s)) |
            TypedValue::N_NSTRING(Some(ref s)) |
            TypedValue::N_TEXT(Some(ref s)) |
            TypedValue::N_SHORTTEXT(Some(ref s)) => serialize_length_and_string(s, w)?,

            TypedValue::BINARY(ref v) |
            TypedValue::VARBINARY(ref v) |
            TypedValue::BSTRING(ref v) |
            TypedValue::N_BINARY(Some(ref v)) |
            TypedValue::N_VARBINARY(Some(ref v)) |
            TypedValue::N_BSTRING(Some(ref v)) => serialize_length_and_bytes(v, w)?,

            TypedValue::N_TINYINT(None) |
            TypedValue::N_SMALLINT(None) |
            TypedValue::N_INT(None) |
            TypedValue::N_BIGINT(None) |
            TypedValue::N_REAL(None) |
            TypedValue::N_DOUBLE(None) |
            TypedValue::N_BOOLEAN(None) |
            TypedValue::N_LONGDATE(None) |
            TypedValue::N_STRING(None) |
            TypedValue::N_NSTRING(None) |
            TypedValue::N_TEXT(None) |
            TypedValue::N_SHORTTEXT(None) |
            TypedValue::N_CLOB(None) |
            TypedValue::N_NCLOB(None) |
            TypedValue::N_BLOB(None) |
            TypedValue::N_BINARY(None) |
            TypedValue::N_VARBINARY(None) |
            TypedValue::N_BSTRING(None) => {}

            TypedValue::CHAR(_) |
            TypedValue::N_CHAR(_) |
            TypedValue::NCHAR(_) |
            TypedValue::N_NCHAR(_) |
            TypedValue::VARCHAR(_) |
            TypedValue::N_VARCHAR(_) |
            TypedValue::NVARCHAR(_) |
            TypedValue::N_NVARCHAR(_) => return Err(_serialize_not_implemented(tv.type_id())),
        }
    }
    Ok(())
}

// is used to calculate the argument size (in serialize)
pub fn size(tv: &TypedValue) -> PrtResult<usize> {
    fn _size_not_implemented(type_id: u8) -> PrtError {
        return PrtError::ProtocolError(format!("TypedValue::size() not implemented for type \
                                                code {}",
                                               type_id));
    }

    Ok(1 +
       match *tv {
        TypedValue::TINYINT(_) |
        TypedValue::N_TINYINT(Some(_)) => 1,

        TypedValue::SMALLINT(_) |
        TypedValue::N_SMALLINT(Some(_)) => 2,

        TypedValue::INT(_) |
        TypedValue::N_INT(Some(_)) => 4,

        TypedValue::BIGINT(_) |
        TypedValue::N_BIGINT(Some(_)) => 8,

        TypedValue::REAL(_) |
        TypedValue::N_REAL(Some(_)) => 4,

        TypedValue::DOUBLE(_) |
        TypedValue::N_DOUBLE(Some(_)) => 8,

        TypedValue::BOOLEAN(_) |
        TypedValue::N_BOOLEAN(Some(_)) => 1,

        TypedValue::LONGDATE(_) |
        TypedValue::N_LONGDATE(Some(_)) => 8,

        TypedValue::CLOB(ref clob) |
        TypedValue::N_CLOB(Some(ref clob)) |
        TypedValue::NCLOB(ref clob) |
        TypedValue::N_NCLOB(Some(ref clob)) => 9 + clob.len()?,

        TypedValue::BLOB(ref blob) |
        TypedValue::N_BLOB(Some(ref blob)) => 9 + blob.len()?,

        TypedValue::STRING(ref s) |
        TypedValue::N_STRING(Some(ref s)) |
        TypedValue::NSTRING(ref s) |
        TypedValue::N_NSTRING(Some(ref s)) |
        TypedValue::TEXT(ref s) |
        TypedValue::N_TEXT(Some(ref s)) |
        TypedValue::SHORTTEXT(ref s) |
        TypedValue::N_SHORTTEXT(Some(ref s)) => string_length(s),

        TypedValue::BINARY(ref v) |
        TypedValue::N_BINARY(Some(ref v)) |
        TypedValue::VARBINARY(ref v) |
        TypedValue::N_VARBINARY(Some(ref v)) |
        TypedValue::BSTRING(ref v) |
        TypedValue::N_BSTRING(Some(ref v)) => v.len() + 2,

        TypedValue::N_TINYINT(None) |
        TypedValue::N_SMALLINT(None) |
        TypedValue::N_INT(None) |
        TypedValue::N_BIGINT(None) |
        TypedValue::N_REAL(None) |
        TypedValue::N_DOUBLE(None) |
        TypedValue::N_BOOLEAN(None) |
        TypedValue::N_LONGDATE(None) |
        TypedValue::N_CLOB(None) |
        TypedValue::N_NCLOB(None) |
        TypedValue::N_BLOB(None) |
        TypedValue::N_BINARY(None) |
        TypedValue::N_VARBINARY(None) |
        TypedValue::N_BSTRING(None) |
        TypedValue::N_STRING(None) |
        TypedValue::N_NSTRING(None) |
        TypedValue::N_TEXT(None) |
        TypedValue::N_SHORTTEXT(None) => 0,

        TypedValue::CHAR(_) |
        TypedValue::VARCHAR(_) |
        TypedValue::NCHAR(_) |
        TypedValue::NVARCHAR(_) |
        TypedValue::N_CHAR(_) |
        TypedValue::N_VARCHAR(_) |
        TypedValue::N_NCHAR(_) |
        TypedValue::N_NVARCHAR(_) => return Err(_size_not_implemented(tv.type_id())),
    })
}


pub fn string_length(s: &String) -> usize {
    match util::cesu8_length(s) {
        clen if clen <= MAX_1_BYTE_LENGTH as usize => 1 + clen,
        clen if clen <= MAX_2_BYTE_LENGTH as usize => 3 + clen,
        clen => 5 + clen,
    }
}

pub fn serialize_length_and_string(s: &String, w: &mut io::Write) -> PrtResult<()> {
    serialize_length_and_bytes(&util::string_to_cesu8(s), w)
}

fn serialize_length_and_bytes(v: &Vec<u8>, w: &mut io::Write) -> PrtResult<()> {
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


fn serialize_blob_header(v_len: usize, data_pos: &mut i32, w: &mut io::Write) -> PrtResult<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_110_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(v_len as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*data_pos as i32)?; // I4           position
    *data_pos += v_len as i32;
    Ok(())
}

fn serialize_clob_header(s_len: usize, data_pos: &mut i32, w: &mut io::Write) -> PrtResult<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_110_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(s_len as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*data_pos as i32)?; // I4           position
    *data_pos += s_len as i32;
    Ok(())
}


pub mod factory {
    use super::TypedValue;
    use super::super::{PrtError, PrtResult, prot_err, util};
    use super::super::lob::*;
    use super::super::longdate::LongDate;
    use protocol::lowlevel::conn_core::ConnCoreRef;
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::borrow::Cow;
    use std::fmt;
    use std::io;
    use std::iter::repeat;
    use std::{u32, u64};

    pub fn parse_from_reply(p_typecode: u8, nullable: bool, conn_ref: &ConnCoreRef,
                            rdr: &mut io::BufRead)
                            -> PrtResult<TypedValue> {
        // here p_typecode is always < 127
        // the flag nullable from the metadata governs our behavior:
        // if it is true, we return types with typecode above 128, which use Option<type>,
        // if it is false, we return types with the original typecode, which use plain values
        let typecode = p_typecode +
                       match nullable {
            true => 128,
            false => 0,
        };
        match typecode {
            1 => {
                Ok(TypedValue::TINYINT({
                    ind_not_null(rdr)?;
                    rdr.read_u8()?
                }))
            }
            2 => {
                Ok(TypedValue::SMALLINT({
                    ind_not_null(rdr)?;
                    rdr.read_i16::<LittleEndian>()?
                }))
            }
            3 => {
                Ok(TypedValue::INT({
                    ind_not_null(rdr)?;
                    rdr.read_i32::<LittleEndian>()?
                }))
            }
            4 => {
                Ok(TypedValue::BIGINT({
                    ind_not_null(rdr)?;
                    rdr.read_i64::<LittleEndian>()?
                }))
            }
            // 5  => Ok(TypedValue::DECIMAL(
            6 => Ok(TypedValue::REAL(parse_real(rdr)?)),
            7 => Ok(TypedValue::DOUBLE(parse_double(rdr)?)),
            8 => Ok(TypedValue::CHAR(parse_length_and_string(rdr)?)),
            9 => Ok(TypedValue::VARCHAR(parse_length_and_string(rdr)?)),
            10 => Ok(TypedValue::NCHAR(parse_length_and_string(rdr)?)),
            11 => Ok(TypedValue::NVARCHAR(parse_length_and_string(rdr)?)),
            12 => Ok(TypedValue::BINARY(parse_length_and_binary(rdr)?)),
            13 => Ok(TypedValue::VARBINARY(parse_length_and_binary(rdr)?)),
            // 16 => Ok(TypedValue::TIMESTAMP(
            // FIXME improve error handling:
            25 => Ok(TypedValue::CLOB(parse_clob_from_reply(conn_ref, rdr)?)),
            26 => Ok(TypedValue::NCLOB(parse_clob_from_reply(conn_ref, rdr)?)),
            27 => Ok(TypedValue::BLOB(parse_blob_from_reply(conn_ref, rdr)?)),
            28 => Ok(TypedValue::BOOLEAN(rdr.read_u8()? > 0)),
            29 => Ok(TypedValue::STRING(parse_length_and_string(rdr)?)),
            30 => Ok(TypedValue::NSTRING(parse_length_and_string(rdr)?)),
            33 => Ok(TypedValue::BSTRING(parse_length_and_binary(rdr)?)),
            // 47 => Ok(TypedValue::SMALLDECIMAL(
            51 => Ok(TypedValue::TEXT(parse_length_and_string(rdr)?)),
            52 => Ok(TypedValue::SHORTTEXT(parse_length_and_string(rdr)?)),
            61 => Ok(TypedValue::LONGDATE(parse_longdate(rdr)?)),
            // 62 => Ok(TypedValue::SECONDDATE(
            // 63 => Ok(TypedValue::DAYDATE(
            // 64 => Ok(TypedValue::SECONDTIME(
            129 => {
                Ok(TypedValue::N_TINYINT(match ind_null(rdr)? {
                    true => None,
                    false => Some(rdr.read_u8()?),
                }))
            }
            130 => {
                Ok(TypedValue::N_SMALLINT(match ind_null(rdr)? {
                    true => None,
                    false => Some(rdr.read_i16::<LittleEndian>()?),
                }))
            }
            131 => {
                Ok(TypedValue::N_INT(match ind_null(rdr)? {
                    true => None,
                    false => Some(rdr.read_i32::<LittleEndian>()?),
                }))
            }
            132 => {
                Ok(TypedValue::N_BIGINT(match ind_null(rdr)? {
                    true => None,
                    false => Some(rdr.read_i64::<LittleEndian>()?),
                }))
            }
            // 133 => Ok(TypedValue::N_DECIMAL(
            134 => Ok(TypedValue::N_REAL(parse_nullable_real(rdr)?)),
            135 => Ok(TypedValue::N_DOUBLE(parse_nullable_double(rdr)?)),
            136 => Ok(TypedValue::N_CHAR(parse_nullable_length_and_string(rdr)?)),
            137 => Ok(TypedValue::N_VARCHAR(parse_nullable_length_and_string(rdr)?)),
            138 => Ok(TypedValue::N_NCHAR(parse_nullable_length_and_string(rdr)?)),
            139 => Ok(TypedValue::N_NVARCHAR(parse_nullable_length_and_string(rdr)?)),
            140 => Ok(TypedValue::N_BINARY(parse_nullable_length_and_binary(rdr)?)),
            141 => Ok(TypedValue::N_VARBINARY(parse_nullable_length_and_binary(rdr)?)),
            // 144 => Ok(TypedValue::N_TIMESTAMP(
            153 => Ok(TypedValue::N_CLOB(parse_nullable_clob_from_reply(conn_ref, rdr)?)),
            154 => Ok(TypedValue::N_NCLOB(parse_nullable_clob_from_reply(conn_ref, rdr)?)),
            155 => Ok(TypedValue::N_BLOB(parse_nullable_blob_from_reply(conn_ref, rdr)?)),
            156 => {
                Ok(TypedValue::N_BOOLEAN(match ind_null(rdr)? {
                    true => None,
                    false => Some(rdr.read_u8()? > 0),
                }))
            }
            157 => Ok(TypedValue::N_STRING(parse_nullable_length_and_string(rdr)?)),
            158 => Ok(TypedValue::N_NSTRING(parse_nullable_length_and_string(rdr)?)),
            161 => Ok(TypedValue::N_BSTRING(parse_nullable_length_and_binary(rdr)?)),
            // 175 => Ok(TypedValue::N_SMALLDECIMAL(
            179 => Ok(TypedValue::N_TEXT(parse_nullable_length_and_string(rdr)?)),
            180 => Ok(TypedValue::N_SHORTTEXT(parse_nullable_length_and_string(rdr)?)),
            189 => Ok(TypedValue::N_LONGDATE(parse_nullable_longdate(rdr)?)),
            // 190 => Ok(TypedValue::N_SECONDDATE(
            // 191 => Ok(TypedValue::N_DAYDATE(
            // 192 => Ok(TypedValue::N_SECONDTIME(
            _ => {
                Err(PrtError::ProtocolError(format!("TypedValue::parse_from_reply() not \
                                                     implemented for type code {}",
                                                    typecode)))
            }
        }
    }


    // reads the nullindicator and returns Ok(true) if it has value 0 or Ok(false) otherwise
    fn ind_null(rdr: &mut io::BufRead) -> PrtResult<bool> {
        Ok(rdr.read_u8()? == 0)
    }

    // reads the nullindicator and throws an error if it has value 0
    fn ind_not_null(rdr: &mut io::BufRead) -> PrtResult<()> {
        match ind_null(rdr)? {
            true => Err(prot_err("null value returned for not-null column")),
            false => Ok(()),
        }
    }


    fn parse_real(rdr: &mut io::BufRead) -> PrtResult<f32> {
        let mut vec: Vec<u8> = repeat(0u8).take(4).collect();
        rdr.read(&mut vec[..])?;

        let mut r = io::Cursor::new(&vec);
        let tmp = r.read_u32::<LittleEndian>()?;
        match tmp {
            u32::MAX => Err(prot_err("Unexpected NULL Value in parse_real()")),
            _ => {
                r.set_position(0);
                Ok(r.read_f32::<LittleEndian>()?)
            }
        }
    }

    fn parse_nullable_real(rdr: &mut io::BufRead) -> PrtResult<Option<f32>> {
        let mut vec: Vec<u8> = repeat(0u8).take(4).collect();
        rdr.read(&mut vec[..])?;
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

    fn parse_double(rdr: &mut io::BufRead) -> PrtResult<f64> {
        let mut vec: Vec<u8> = repeat(0u8).take(8).collect();
        rdr.read(&mut vec[..])?;
        let mut r = io::Cursor::new(&vec);
        let tmp = r.read_u64::<LittleEndian>()?;
        match tmp {
            u64::MAX => Err(prot_err("Unexpected NULL Value in parse_double()")),
            _ => {
                r.set_position(0);
                Ok(r.read_f64::<LittleEndian>()?)
            }
        }
    }

    fn parse_nullable_double(rdr: &mut io::BufRead) -> PrtResult<Option<f64>> {
        let mut vec: Vec<u8> = repeat(0u8).take(8).collect();
        rdr.read(&mut vec[..])?;
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


    // ----- STRINGS and BINARIES ----------------------------------------------------------------
    pub fn parse_length_and_string(rdr: &mut io::BufRead) -> PrtResult<String> {
        match util::cesu8_to_string(&parse_length_and_binary(rdr)?) {
            Ok(s) => Ok(s),
            Err(e) => {
                error!("cesu-8 problem occured in typed_value:parse_length_and_string()");
                Err(e)
            }
        }
    }

    fn parse_length_and_binary(rdr: &mut io::BufRead) -> PrtResult<Vec<u8>> {
        let l8 = rdr.read_u8()?; // B1
        let len = match l8 {
            l if l <= super::MAX_1_BYTE_LENGTH => l8 as usize,
            super::LENGTH_INDICATOR_2BYTE => rdr.read_i16::<LittleEndian>()? as usize, // I2
            super::LENGTH_INDICATOR_4BYTE => rdr.read_i32::<LittleEndian>()? as usize, // I4
            l => {
                return Err(PrtError::ProtocolError(format!("Invalid value in length indicator: \
                                                            {}",
                                                           l)));
            }
        };
        util::parse_bytes(len, rdr) // B (varying)
    }

    fn parse_nullable_length_and_string(rdr: &mut io::BufRead) -> PrtResult<Option<String>> {
        match parse_nullable_length_and_binary(rdr)? {
            Some(vec) => {
                match util::cesu8_to_string(&vec) {
                    Ok(s) => Ok(Some(s)),
                    Err(_) => {
                        Err(prot_err("cesu-8 problem occured in \
                                      typed_value:parse_length_and_string()"))
                    }
                }
            }
            None => Ok(None),
        }
    }

    fn parse_nullable_length_and_binary(rdr: &mut io::BufRead) -> PrtResult<Option<Vec<u8>>> {
        let l8 = rdr.read_u8()?; // B1
        let len = match l8 {
            l if l <= super::MAX_1_BYTE_LENGTH => l8 as usize,
            super::LENGTH_INDICATOR_2BYTE => rdr.read_i16::<LittleEndian>()? as usize, // I2
            super::LENGTH_INDICATOR_4BYTE => rdr.read_i32::<LittleEndian>()? as usize, // I4
            super::LENGTH_INDICATOR_NULL => return Ok(None),
            l => {
                return Err(PrtError::ProtocolError(format!("Invalid value in length indicator: \
                                                            {}",
                                                           l)))
            }
        };
        Ok(Some(util::parse_bytes(len, rdr)?)) // B (varying)
    }

    // ----- BLOBS and CLOBS
    // ===
    // regular parse
    pub fn parse_blob_from_reply(conn_ref: &ConnCoreRef, rdr: &mut io::BufRead) -> PrtResult<BLOB> {
        match parse_nullable_blob_from_reply(conn_ref, rdr)? {
            Some(blob) => Ok(blob),
            None => Err(prot_err("Null value found for non-null blob column")),
        }
    }
    pub fn parse_nullable_blob_from_reply(conn_ref: &ConnCoreRef, rdr: &mut io::BufRead)
                                          -> PrtResult<Option<BLOB>> {
        let (is_null, is_last_data) = parse_lob_1(rdr)?;
        match is_null {
            true => {
                return Ok(None);
            }
            false => {
                let (_, length_b, locator_id, data) = parse_lob_2(rdr)?;
                Ok(Some(new_blob_from_db(conn_ref, is_last_data, length_b, locator_id, data)))
            }
        }
    }

    pub fn parse_clob_from_reply(conn_ref: &ConnCoreRef, rdr: &mut io::BufRead) -> PrtResult<CLOB> {
        match parse_nullable_clob_from_reply(conn_ref, rdr)? {
            Some(clob) => Ok(clob),
            None => Err(prot_err("Null value found for non-null clob column")),
        }
    }
    pub fn parse_nullable_clob_from_reply(conn_ref: &ConnCoreRef, rdr: &mut io::BufRead)
                                          -> PrtResult<Option<CLOB>> {
        let (is_null, is_last_data) = parse_lob_1(rdr)?;
        match is_null {
            true => {
                return Ok(None);
            }
            false => {
                let (length_c, length_b, locator_id, data) = parse_lob_2(rdr)?;
                let (s, char_count) = util::from_cesu8_with_count(&data)?;
                let s = match s {
                    Cow::Owned(s) => s,
                    Cow::Borrowed(s) => String::from(s),
                };
                assert_eq!(data.len(), s.len());
                Ok(Some(new_clob_from_db(conn_ref,
                                         is_last_data,
                                         length_c,
                                         length_b,
                                         char_count,
                                         locator_id,
                                         s)))
            }
        }
    }

    fn parse_lob_1(rdr: &mut io::BufRead) -> PrtResult<(bool, bool)> {
        rdr.consume(1); //let data_type = rdr.read_u8()?; // I1  "type of data": unclear
        let options = rdr.read_u8()?; // I1
        let is_null = (options & 0b_1_u8) != 0;
        let is_last_data = (options & 0b_100_u8) != 0;
        Ok((is_null, is_last_data))
    }
    fn parse_lob_2(rdr: &mut io::BufRead) -> PrtResult<(u64, u64, u64, Vec<u8>)> {
        rdr.consume(2); // U2 (filler)
        let length_c = rdr.read_u64::<LittleEndian>()?; // I8
        let length_b = rdr.read_u64::<LittleEndian>()?; // I8
        let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
        let chunk_length = rdr.read_i32::<LittleEndian>()?; // I4
        let data = util::parse_bytes(chunk_length as usize, rdr)?; // B[chunk_length]
        trace!("Got LOB locator {}", locator_id);
        Ok((length_c, length_b, locator_id, data))
    }


    // -----  LongDates --------------------------------------------------------------------------
    // SECONDDATE_NULL_REPRESENTATION:
    const LONGDATE_NULL_REPRESENTATION: i64 = 3_155_380_704_000_000_001_i64;
    fn parse_longdate(rdr: &mut io::BufRead) -> PrtResult<LongDate> {
        let i = rdr.read_i64::<LittleEndian>()?;
        match i {
            LONGDATE_NULL_REPRESENTATION => {
                Err(prot_err("Null value found for non-null longdate column"))
            }
            _ => Ok(LongDate(i)),
        }
    }

    fn parse_nullable_longdate(rdr: &mut io::BufRead) -> PrtResult<Option<LongDate>> {
        let i = rdr.read_i64::<LittleEndian>()?;
        match i {
            LONGDATE_NULL_REPRESENTATION => Ok(None),
            _ => Ok(Some(LongDate(i))),
        }
    }


    impl fmt::Display for TypedValue {
        fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            match *self {
                TypedValue::TINYINT(value) |
                TypedValue::N_TINYINT(Some(value)) => write!(fmt, "{}", value),
                TypedValue::SMALLINT(value) |
                TypedValue::N_SMALLINT(Some(value)) => write!(fmt, "{}", value),
                TypedValue::INT(value) |
                TypedValue::N_INT(Some(value)) => write!(fmt, "{}", value),
                TypedValue::BIGINT(value) |
                TypedValue::N_BIGINT(Some(value)) => write!(fmt, "{}", value),
                TypedValue::REAL(value) |
                TypedValue::N_REAL(Some(value)) => write!(fmt, "{}", value),
                TypedValue::DOUBLE(value) |
                TypedValue::N_DOUBLE(Some(value)) => write!(fmt, "{}", value),
                TypedValue::CHAR(ref value) |
                TypedValue::N_CHAR(Some(ref value)) |
                TypedValue::VARCHAR(ref value) |
                TypedValue::N_VARCHAR(Some(ref value)) |
                TypedValue::NCHAR(ref value) |
                TypedValue::N_NCHAR(Some(ref value)) |
                TypedValue::NVARCHAR(ref value) |
                TypedValue::N_NVARCHAR(Some(ref value)) |
                TypedValue::STRING(ref value) |
                TypedValue::N_STRING(Some(ref value)) |
                TypedValue::NSTRING(ref value) |
                TypedValue::N_NSTRING(Some(ref value)) |
                TypedValue::TEXT(ref value) |
                TypedValue::N_TEXT(Some(ref value)) |
                TypedValue::SHORTTEXT(ref value) |
                TypedValue::N_SHORTTEXT(Some(ref value)) => write!(fmt, "\"{}\"", value),
                TypedValue::BINARY(_) |
                TypedValue::N_BINARY(Some(_)) => write!(fmt, "<BINARY>"),
                TypedValue::VARBINARY(_) |
                TypedValue::N_VARBINARY(Some(_)) => write!(fmt, "<VARBINARY>"),
                TypedValue::CLOB(_) |
                TypedValue::N_CLOB(Some(_)) => write!(fmt, "<CLOB>"),
                TypedValue::NCLOB(_) |
                TypedValue::N_NCLOB(Some(_)) => write!(fmt, "<NCLOB>"),
                TypedValue::BLOB(_) |
                TypedValue::N_BLOB(Some(_)) => write!(fmt, "<BLOB>"),
                TypedValue::BOOLEAN(value) |
                TypedValue::N_BOOLEAN(Some(value)) => write!(fmt, "{}", value),
                TypedValue::BSTRING(_) |
                TypedValue::N_BSTRING(Some(_)) => write!(fmt, "<BSTRING>"),
                TypedValue::LONGDATE(ref value) |
                TypedValue::N_LONGDATE(Some(ref value)) => write!(fmt, "{}", value),

                TypedValue::N_TINYINT(None) |
                TypedValue::N_SMALLINT(None) |
                TypedValue::N_INT(None) |
                TypedValue::N_BIGINT(None) |
                TypedValue::N_REAL(None) |
                TypedValue::N_DOUBLE(None) |
                TypedValue::N_CHAR(None) |
                TypedValue::N_VARCHAR(None) |
                TypedValue::N_NCHAR(None) |
                TypedValue::N_NVARCHAR(None) |
                TypedValue::N_BINARY(None) |
                TypedValue::N_VARBINARY(None) |
                TypedValue::N_CLOB(None) |
                TypedValue::N_NCLOB(None) |
                TypedValue::N_BLOB(None) |
                TypedValue::N_BOOLEAN(None) |
                TypedValue::N_STRING(None) |
                TypedValue::N_NSTRING(None) |
                TypedValue::N_BSTRING(None) |
                TypedValue::N_TEXT(None) |
                TypedValue::N_SHORTTEXT(None) |
                TypedValue::N_LONGDATE(None) => write!(fmt, "<NULL>"),
            }
        }
    }
}
