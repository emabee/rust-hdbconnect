use crate::conn_core::AmConnCore;
use crate::protocol::parts::type_id::{BaseTypeId, TypeId};
use crate::protocol::util;
use crate::types::{BLob, CLob, DayDate, LongDate, NCLob, SecondDate, SecondTime};
use crate::types_impl::daydate::{parse_daydate, parse_nullable_daydate};
use crate::types_impl::hdb_decimal::{emit_decimal, parse_decimal, parse_nullable_decimal};
use crate::types_impl::lob::{
    emit_blob_header, emit_clob_header, emit_nclob_header, parse_blob, parse_clob, parse_nclob,
    parse_nullable_blob, parse_nullable_clob, parse_nullable_nclob,
};
use crate::types_impl::longdate::{parse_longdate, parse_nullable_longdate};
use crate::types_impl::seconddate::{parse_nullable_seconddate, parse_seconddate};
use crate::types_impl::secondtime::{parse_nullable_secondtime, parse_secondtime};
use crate::{HdbError, HdbResult};
use bigdecimal::BigDecimal;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use cesu8;
use serde;
use serde_db::de::{ConversionError, DbValue};
use serde_derive::Serialize;
use std::fmt;

const MAX_1_BYTE_LENGTH: u8 = 245;
const MAX_2_BYTE_LENGTH: i16 = std::i16::MAX;
const LENGTH_INDICATOR_2BYTE: u8 = 246;
const LENGTH_INDICATOR_4BYTE: u8 = 247;
const LENGTH_INDICATOR_NULL: u8 = 255;

/// Enum for all supported database value types.
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Serialize)]
pub enum HdbValue {
    /// Internally used only.
    /// Is swapped in where a real value (any of the others) is swapped out.
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
    /// Precision p can range from 1 to 38, scale s can range from 0 to p.
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
    DECIMAL(BigDecimal),
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
    /// Specifies a variable-length Unicode character
    /// set string, where <n> indicates the maximum length in characters
    /// and is an integer between 1 and 5000.
    NVARCHAR(String),
    /// Stores binary data of a specified length in bytes,
    /// where n indicates the fixed length and is an integer between 1 and 5000.
    BINARY(Vec<u8>),
    /// Stores binary data of a specified maximum length in bytes,
    /// where n indicates the maximum length and is an integer between 1 and 5000.
    VARBINARY(Vec<u8>),
    /// Stores a large ASCII character string.
    CLOB(CLob),
    /// Stores a large Unicode string.
    NCLOB(NCLob),
    /// Stores a large binary string.
    BLOB(BLob),
    /// BOOLEAN stores boolean values, which are TRUE or FALSE.
    BOOLEAN(bool),
    /// The DB returns all Strings as type STRING, independent of the concrete column type.
    STRING(String),
    /// Likely not used?
    NSTRING(String),
    /// The DB returns all binary values as type BSTRING.
    BSTRING(Vec<u8>),

    /// Floating-point decimal number.
    ///
    /// The precision and scale can vary within the range 1~16 for precision
    /// and -369~368 for scale, depending on the stored value.  
    /// SMALLDECIMAL is only supported on the HANA column store.
    /// DECIMAL and SMALLDECIMAL are floating-point types.
    /// For instance, a decimal column can store any of 3.14, 3.1415, 3.141592
    /// whilst maintaining their precision.
    /// DECIMAL(p, s) is the SQL standard notation for fixed-point decimal.
    /// 3.14, 3.1415, 3.141592 are stored in a decimal(5, 4) column as 3.1400,
    /// 3.1415, 3.1415 for example,
    /// retaining the specified precision(5) and scale(4).
    SMALLDECIMAL(BigDecimal),

    /// Enables text search features.
    ///
    /// This data type can be defined for column tables, but not for row tables.
    /// This is not a standalone SQL-Type. Selecting a TEXT column yields a
    /// column of type NCLOB.
    TEXT(String),

    /// Similar to TEXT.
    SHORTTEXT(String),
    /// Timestamp with 10^-7 seconds precision, uses eight bytes.
    LONGDATE(LongDate),
    /// TIMESTAMP with second precision.
    SECONDDATE(SecondDate),

    /// DATE with day precision.
    DAYDATE(DayDate),

    /// TIME with second precision.
    SECONDTIME(SecondTime),

    /// Nullable variant of TINYINT.
    N_TINYINT(Option<u8>),
    /// Nullable variant of SMALLINT.
    N_SMALLINT(Option<i16>),
    /// Nullable variant of INT.
    N_INT(Option<i32>),
    /// Nullable variant of BIGINT.
    N_BIGINT(Option<i64>),
    /// Nullable variant of DECIMAL and DECIMAL(p,s).
    N_DECIMAL(Option<BigDecimal>),
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
    N_CLOB(Option<CLob>),
    /// Nullable variant of NCLOB.
    N_NCLOB(Option<NCLob>),
    /// Nullable variant of BLOB.
    N_BLOB(Option<BLob>),
    /// Nullable variant of BOOLEAN.
    N_BOOLEAN(Option<bool>),
    /// Nullable variant of STRING.
    N_STRING(Option<String>),
    /// Nullable variant of NSTRING.
    N_NSTRING(Option<String>),
    /// Nullable variant of BSTRING.
    N_BSTRING(Option<Vec<u8>>),
    /// Nullable variant of SMALLDECIMAL.
    N_SMALLDECIMAL(Option<BigDecimal>),
    /// Nullable variant of TEXT.
    N_TEXT(Option<String>),
    /// Nullable variant of SHORTTEXT.
    N_SHORTTEXT(Option<String>),
    /// Nullable variant of SECONDDATE.
    N_SECONDDATE(Option<SecondDate>),
    /// Nullable variant of DAYDATE.
    N_DAYDATE(Option<DayDate>),
    /// Nullable variant of SECONDTIME.
    N_SECONDTIME(Option<SecondTime>),
    /// Nullable variant of LONGDATE.
    N_LONGDATE(Option<LongDate>),
}

impl HdbValue {
    /// hdb protocol uses ids < 128 for non-null values, and ids > 128 for nullable values
    fn type_id(&self) -> HdbResult<TypeId> {
        Ok(match *self {
            HdbValue::NOTHING => {
                return Err(HdbError::Impl(
                    "Can't send HdbValue::NOTHING to Database".to_string(),
                ))
            }
            HdbValue::TINYINT(_) => type_id_not_null(BaseTypeId::TINYINT),
            HdbValue::SMALLINT(_) => type_id_not_null(BaseTypeId::SMALLINT),
            HdbValue::INT(_) => type_id_not_null(BaseTypeId::INT),
            HdbValue::BIGINT(_) => type_id_not_null(BaseTypeId::BIGINT),
            HdbValue::DECIMAL(_) => type_id_not_null(BaseTypeId::DECIMAL),
            HdbValue::REAL(_) => type_id_not_null(BaseTypeId::REAL),
            HdbValue::DOUBLE(_) => type_id_not_null(BaseTypeId::DOUBLE),
            HdbValue::CHAR(_) => type_id_not_null(BaseTypeId::CHAR),
            HdbValue::VARCHAR(_) => type_id_not_null(BaseTypeId::VARCHAR),
            HdbValue::NCHAR(_) => type_id_not_null(BaseTypeId::NCHAR),
            HdbValue::NVARCHAR(_) => type_id_not_null(BaseTypeId::NVARCHAR),
            HdbValue::BINARY(_) => type_id_not_null(BaseTypeId::BINARY),
            HdbValue::VARBINARY(_) => type_id_not_null(BaseTypeId::VARBINARY),
            HdbValue::CLOB(_) => type_id_not_null(BaseTypeId::CLOB),
            HdbValue::NCLOB(_) => type_id_not_null(BaseTypeId::NCLOB),
            HdbValue::BLOB(_) => type_id_not_null(BaseTypeId::BLOB),
            HdbValue::BOOLEAN(_) => type_id_not_null(BaseTypeId::BOOLEAN),
            HdbValue::STRING(_) => type_id_not_null(BaseTypeId::STRING),
            HdbValue::NSTRING(_) => type_id_not_null(BaseTypeId::NSTRING),
            HdbValue::BSTRING(_) => type_id_not_null(BaseTypeId::BSTRING),
            HdbValue::SMALLDECIMAL(_) => type_id_not_null(BaseTypeId::SMALLDECIMAL),
            HdbValue::TEXT(_) => type_id_not_null(BaseTypeId::TEXT),
            HdbValue::SHORTTEXT(_) => type_id_not_null(BaseTypeId::SHORTTEXT),
            HdbValue::LONGDATE(_) => type_id_not_null(BaseTypeId::LONGDATE),
            HdbValue::SECONDDATE(_) => type_id_not_null(BaseTypeId::SECONDDATE),
            HdbValue::DAYDATE(_) => type_id_not_null(BaseTypeId::DAYDATE),
            HdbValue::SECONDTIME(_) => type_id_not_null(BaseTypeId::SECONDTIME),

            HdbValue::N_TINYINT(_) => type_id_nullable(BaseTypeId::TINYINT),
            HdbValue::N_SMALLINT(_) => type_id_nullable(BaseTypeId::SMALLINT),
            HdbValue::N_INT(_) => type_id_nullable(BaseTypeId::INT),
            HdbValue::N_BIGINT(_) => type_id_nullable(BaseTypeId::BIGINT),
            HdbValue::N_DECIMAL(_) => type_id_nullable(BaseTypeId::DECIMAL),
            HdbValue::N_REAL(_) => type_id_nullable(BaseTypeId::REAL),
            HdbValue::N_DOUBLE(_) => type_id_nullable(BaseTypeId::DOUBLE),
            HdbValue::N_CHAR(_) => type_id_nullable(BaseTypeId::CHAR),
            HdbValue::N_VARCHAR(_) => type_id_nullable(BaseTypeId::VARCHAR),
            HdbValue::N_NCHAR(_) => type_id_nullable(BaseTypeId::NCHAR),
            HdbValue::N_NVARCHAR(_) => type_id_nullable(BaseTypeId::NVARCHAR),
            HdbValue::N_BINARY(_) => type_id_nullable(BaseTypeId::BINARY),
            HdbValue::N_VARBINARY(_) => type_id_nullable(BaseTypeId::VARBINARY),
            HdbValue::N_CLOB(_) => type_id_nullable(BaseTypeId::CLOB),
            HdbValue::N_NCLOB(_) => type_id_nullable(BaseTypeId::NCLOB),
            HdbValue::N_BLOB(_) => type_id_nullable(BaseTypeId::BLOB),
            HdbValue::N_BOOLEAN(_) => type_id_nullable(BaseTypeId::BOOLEAN),
            HdbValue::N_STRING(_) => type_id_nullable(BaseTypeId::STRING),
            HdbValue::N_NSTRING(_) => type_id_nullable(BaseTypeId::NSTRING),
            HdbValue::N_BSTRING(_) => type_id_nullable(BaseTypeId::BSTRING),
            HdbValue::N_SMALLDECIMAL(_) => type_id_nullable(BaseTypeId::SMALLDECIMAL),
            HdbValue::N_TEXT(_) => type_id_nullable(BaseTypeId::TEXT),
            HdbValue::N_SHORTTEXT(_) => type_id_nullable(BaseTypeId::SHORTTEXT),
            HdbValue::N_LONGDATE(_) => type_id_nullable(BaseTypeId::LONGDATE),
            HdbValue::N_SECONDDATE(_) => type_id_nullable(BaseTypeId::SECONDDATE),
            HdbValue::N_DAYDATE(_) => type_id_nullable(BaseTypeId::DAYDATE),
            HdbValue::N_SECONDTIME(_) => type_id_nullable(BaseTypeId::SECONDTIME),
        })
    }

    /// Deserialize into a rust type
    pub fn try_into<'x, T: serde::Deserialize<'x>>(self) -> HdbResult<T> {
        Ok(DbValue::into_typed(self)?)
    }

    /// Convert into hdbconnect::BLob
    pub fn try_into_blob(self) -> HdbResult<BLob> {
        match self {
            HdbValue::BLOB(blob) | HdbValue::N_BLOB(Some(blob)) => Ok(blob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "The value {:?} cannot be converted into a BLOB",
                tv
            )))),
        }
    }

    /// Convert into hdbconnect::CLob
    pub fn try_into_clob(self) -> HdbResult<CLob> {
        match self {
            HdbValue::CLOB(clob) | HdbValue::N_CLOB(Some(clob)) => Ok(clob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "The value {:?} cannot be converted into a CLOB",
                tv
            )))),
        }
    }

    /// Convert into hdbconnect::NCLob
    pub fn try_into_nclob(self) -> HdbResult<NCLob> {
        match self {
            HdbValue::NCLOB(nclob) | HdbValue::N_NCLOB(Some(nclob)) => Ok(nclob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "The value {:?} cannot be converted into a NCLOB",
                tv
            )))),
        }
    }

    /// Returns true if the value is a NULL value.
    pub fn is_null(&self) -> bool {
        match *self {
            HdbValue::N_TINYINT(None)
            | HdbValue::N_SMALLINT(None)
            | HdbValue::N_INT(None)
            | HdbValue::N_BIGINT(None)
            | HdbValue::N_DECIMAL(None)
            | HdbValue::N_REAL(None)
            | HdbValue::N_DOUBLE(None)
            | HdbValue::N_CHAR(None)
            | HdbValue::N_VARCHAR(None)
            | HdbValue::N_NCHAR(None)
            | HdbValue::N_NVARCHAR(None)
            | HdbValue::N_BINARY(None)
            | HdbValue::N_VARBINARY(None)
            | HdbValue::N_CLOB(None)
            | HdbValue::N_NCLOB(None)
            | HdbValue::N_BLOB(None)
            | HdbValue::N_BOOLEAN(None)
            | HdbValue::N_STRING(None)
            | HdbValue::N_NSTRING(None)
            | HdbValue::N_BSTRING(None)
            | HdbValue::N_SMALLDECIMAL(None)
            | HdbValue::N_TEXT(None)
            | HdbValue::N_SHORTTEXT(None)
            | HdbValue::N_SECONDDATE(None)
            | HdbValue::N_DAYDATE(None)
            | HdbValue::N_SECONDTIME(None)
            | HdbValue::N_LONGDATE(None) => true,
            _ => false,
        }
    }

    pub(crate) fn emit<T: std::io::Write>(&self, data_pos: &mut i32, w: &mut T) -> HdbResult<()> {
        if !self.emit_type_id(w)? {
            match *self {
                HdbValue::TINYINT(u) | HdbValue::N_TINYINT(Some(u)) => w.write_u8(u)?,

                HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => {
                    w.write_i16::<LittleEndian>(i)?
                }

                HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => {
                    // trace!("HdbValue::emit INT: {}", i);
                    w.write_i32::<LittleEndian>(i)?
                }

                HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => {
                    w.write_i64::<LittleEndian>(i)?
                }

                HdbValue::DECIMAL(ref bigdec)
                | HdbValue::N_DECIMAL(Some(ref bigdec))
                | HdbValue::SMALLDECIMAL(ref bigdec)
                | HdbValue::N_SMALLDECIMAL(Some(ref bigdec)) => emit_decimal(bigdec, w)?,

                HdbValue::REAL(f) | HdbValue::N_REAL(Some(f)) => w.write_f32::<LittleEndian>(f)?,

                HdbValue::DOUBLE(f) | HdbValue::N_DOUBLE(Some(f)) => {
                    w.write_f64::<LittleEndian>(f)?
                }

                HdbValue::BOOLEAN(true) | HdbValue::N_BOOLEAN(Some(true)) => w.write_u8(1)?,
                HdbValue::BOOLEAN(false) | HdbValue::N_BOOLEAN(Some(false)) => w.write_u8(0)?,

                HdbValue::LONGDATE(ref ld) | HdbValue::N_LONGDATE(Some(ref ld)) => {
                    w.write_i64::<LittleEndian>(*ld.ref_raw())?
                }

                HdbValue::SECONDDATE(ref sd) | HdbValue::N_SECONDDATE(Some(ref sd)) => {
                    w.write_i64::<LittleEndian>(*sd.ref_raw())?
                }

                HdbValue::DAYDATE(ref dd) | HdbValue::N_DAYDATE(Some(ref dd)) => {
                    w.write_i32::<LittleEndian>(*dd.ref_raw())?
                }
                HdbValue::SECONDTIME(ref st) | HdbValue::N_SECONDTIME(Some(ref st)) => {
                    w.write_u32::<LittleEndian>(*st.ref_raw())?
                }

                HdbValue::CLOB(ref clob) | HdbValue::N_CLOB(Some(ref clob)) => {
                    emit_clob_header(clob.len()?, data_pos, w)?
                }

                HdbValue::NCLOB(ref nclob) | HdbValue::N_NCLOB(Some(ref nclob)) => {
                    emit_nclob_header(nclob.len()?, data_pos, w)?
                }

                HdbValue::BLOB(ref blob) | HdbValue::N_BLOB(Some(ref blob)) => {
                    emit_blob_header(blob.len_alldata(), data_pos, w)?
                }

                HdbValue::STRING(ref s) => emit_length_and_string(s, w)?,

                HdbValue::BINARY(ref v) | HdbValue::VARBINARY(ref v) => {
                    emit_length_and_bytes(v, w)?;
                }

                HdbValue::N_TINYINT(None)
                | HdbValue::N_SMALLINT(None)
                | HdbValue::N_INT(None)
                | HdbValue::N_BIGINT(None)
                | HdbValue::N_DECIMAL(None)
                | HdbValue::N_SMALLDECIMAL(None)
                | HdbValue::N_REAL(None)
                | HdbValue::N_DOUBLE(None)
                | HdbValue::N_BOOLEAN(None)
                | HdbValue::N_LONGDATE(None)
                | HdbValue::N_SECONDDATE(None)
                | HdbValue::N_DAYDATE(None)
                | HdbValue::N_SECONDTIME(None)
                | HdbValue::N_STRING(None)
                | HdbValue::N_NSTRING(None)
                | HdbValue::N_TEXT(None)
                | HdbValue::N_SHORTTEXT(None)
                | HdbValue::N_CLOB(None)
                | HdbValue::N_NCLOB(None)
                | HdbValue::N_BLOB(None)
                | HdbValue::N_BINARY(None)
                | HdbValue::N_VARBINARY(None)
                | HdbValue::N_BSTRING(None) => {}

                HdbValue::NOTHING
                | HdbValue::CHAR(_)
                | HdbValue::N_CHAR(_)
                | HdbValue::NCHAR(_)
                | HdbValue::N_NCHAR(_)
                | HdbValue::VARCHAR(_)
                | HdbValue::N_VARCHAR(_)
                | HdbValue::NVARCHAR(_)
                | HdbValue::N_NVARCHAR(_)
                | HdbValue::NSTRING(_)
                | HdbValue::TEXT(_)
                | HdbValue::SHORTTEXT(_)
                | HdbValue::N_STRING(Some(_))
                | HdbValue::N_NSTRING(Some(_))
                | HdbValue::N_TEXT(Some(_))
                | HdbValue::N_SHORTTEXT(Some(_))
                | HdbValue::BSTRING(_)
                | HdbValue::N_BINARY(Some(_))
                | HdbValue::N_VARBINARY(Some(_))
                | HdbValue::N_BSTRING(Some(_)) => {
                    return Err(HdbError::Impl(format!(
                        "HdbValue::emit() not implemented for type {}",
                        self
                    )))
                }
            }
        }
        Ok(())
    }

    // returns true if the value is a null value, false otherwise
    fn emit_type_id(&self, w: &mut std::io::Write) -> HdbResult<bool> {
        let is_null = self.is_null();

        if is_null {
            w.write_u8(self.type_id()?.type_code())?;
        } else {
            w.write_u8(self.type_id()?.base_type_id().type_code())?;
        }
        Ok(is_null)
    }

    // is used to calculate the argument size (in emit)
    pub(crate) fn size(&self) -> HdbResult<usize> {
        Ok(1 + match self {
            HdbValue::BOOLEAN(_)
            | HdbValue::N_BOOLEAN(Some(_))
            | HdbValue::TINYINT(_)
            | HdbValue::N_TINYINT(Some(_)) => 1,

            HdbValue::SMALLINT(_) | HdbValue::N_SMALLINT(Some(_)) => 2,

            HdbValue::DECIMAL(_)
            | HdbValue::N_DECIMAL(Some(_))
            | HdbValue::SMALLDECIMAL(_)
            | HdbValue::N_SMALLDECIMAL(Some(_)) => 16,

            HdbValue::INT(_)
            | HdbValue::N_INT(Some(_))
            | HdbValue::REAL(_)
            | HdbValue::N_REAL(Some(_))
            | HdbValue::DAYDATE(_)
            | HdbValue::N_DAYDATE(Some(_))
            | HdbValue::SECONDTIME(_)
            | HdbValue::N_SECONDTIME(Some(_)) => 4,

            HdbValue::BIGINT(_)
            | HdbValue::N_BIGINT(Some(_))
            | HdbValue::DOUBLE(_)
            | HdbValue::N_DOUBLE(Some(_))
            | HdbValue::LONGDATE(_)
            | HdbValue::N_LONGDATE(Some(_))
            | HdbValue::SECONDDATE(_)
            | HdbValue::N_SECONDDATE(Some(_)) => 8,

            HdbValue::CLOB(ref clob) | HdbValue::N_CLOB(Some(ref clob)) => 9 + clob.len()?,
            HdbValue::NCLOB(ref nclob) | HdbValue::N_NCLOB(Some(ref nclob)) => 9 + nclob.len()?,
            HdbValue::BLOB(ref blob) | HdbValue::N_BLOB(Some(ref blob)) => 9 + blob.len_alldata(),

            HdbValue::STRING(ref s)
            | HdbValue::NSTRING(ref s)
            | HdbValue::TEXT(ref s)
            | HdbValue::SHORTTEXT(ref s)
            | HdbValue::CHAR(ref s)
            | HdbValue::VARCHAR(ref s)
            | HdbValue::NCHAR(ref s)
            | HdbValue::NVARCHAR(ref s)
            | HdbValue::N_STRING(Some(ref s))
            | HdbValue::N_NSTRING(Some(ref s))
            | HdbValue::N_TEXT(Some(ref s))
            | HdbValue::N_SHORTTEXT(Some(ref s))
            | HdbValue::N_CHAR(Some(ref s))
            | HdbValue::N_VARCHAR(Some(ref s))
            | HdbValue::N_NCHAR(Some(ref s))
            | HdbValue::N_NVARCHAR(Some(ref s)) => binary_length(util::cesu8_length(s)),

            HdbValue::BINARY(ref v)
            | HdbValue::N_BINARY(Some(ref v))
            | HdbValue::VARBINARY(ref v)
            | HdbValue::N_VARBINARY(Some(ref v))
            | HdbValue::BSTRING(ref v)
            | HdbValue::N_BSTRING(Some(ref v)) => binary_length(v.len()),

            HdbValue::N_TINYINT(None)
            | HdbValue::N_SMALLINT(None)
            | HdbValue::N_INT(None)
            | HdbValue::N_BIGINT(None)
            | HdbValue::N_DECIMAL(None)
            | HdbValue::N_SMALLDECIMAL(None)
            | HdbValue::N_REAL(None)
            | HdbValue::N_DOUBLE(None)
            | HdbValue::N_BOOLEAN(None)
            | HdbValue::N_LONGDATE(None)
            | HdbValue::N_SECONDDATE(None)
            | HdbValue::N_DAYDATE(None)
            | HdbValue::N_SECONDTIME(None)
            | HdbValue::N_CLOB(None)
            | HdbValue::N_NCLOB(None)
            | HdbValue::N_BLOB(None)
            | HdbValue::N_BINARY(None)
            | HdbValue::N_VARBINARY(None)
            | HdbValue::N_BSTRING(None)
            | HdbValue::N_STRING(None)
            | HdbValue::N_NSTRING(None)
            | HdbValue::N_CHAR(None)
            | HdbValue::N_NCHAR(None)
            | HdbValue::N_VARCHAR(None)
            | HdbValue::N_NVARCHAR(None)
            | HdbValue::N_TEXT(None)
            | HdbValue::N_SHORTTEXT(None) => 0,

            HdbValue::NOTHING => {
                return Err(HdbError::Impl(format!(
                    "HdbValue::size() not implemented for type {}",
                    self
                )))
            }
        })
    }

    pub(crate) fn parse_from_reply(
        type_id: &TypeId,
        am_conn_core: &AmConnCore,
        rdr: &mut std::io::BufRead,
    ) -> HdbResult<HdbValue> {
        match (type_id.base_type_id(), type_id.is_nullable()) {
            (BaseTypeId::TINYINT, false) => Ok(HdbValue::TINYINT({
                ind_not_null(rdr)?;
                rdr.read_u8()?
            })),
            (BaseTypeId::SMALLINT, false) => Ok(HdbValue::SMALLINT({
                ind_not_null(rdr)?;
                rdr.read_i16::<LittleEndian>()?
            })),
            (BaseTypeId::INT, false) => Ok(HdbValue::INT({
                ind_not_null(rdr)?;
                rdr.read_i32::<LittleEndian>()?
            })),
            (BaseTypeId::BIGINT, false) => Ok(HdbValue::BIGINT({
                ind_not_null(rdr)?;
                rdr.read_i64::<LittleEndian>()?
            })),
            (BaseTypeId::DECIMAL, false) => Ok(HdbValue::DECIMAL(parse_decimal(rdr)?)),
            (BaseTypeId::REAL, false) => Ok(HdbValue::REAL(parse_real(rdr)?)),
            (BaseTypeId::DOUBLE, false) => Ok(HdbValue::DOUBLE(parse_double(rdr)?)),
            (BaseTypeId::CHAR, false) => Ok(HdbValue::CHAR(parse_string(rdr)?)),
            (BaseTypeId::VARCHAR, false) => Ok(HdbValue::VARCHAR(parse_string(rdr)?)),
            (BaseTypeId::NCHAR, false) => Ok(HdbValue::NCHAR(parse_string(rdr)?)),
            (BaseTypeId::NVARCHAR, false) => Ok(HdbValue::NVARCHAR(parse_string(rdr)?)),
            (BaseTypeId::BINARY, false) => Ok(HdbValue::BINARY(parse_binary(rdr)?)),
            (BaseTypeId::VARBINARY, false) => Ok(HdbValue::VARBINARY(parse_binary(rdr)?)),
            (BaseTypeId::CLOB, false) => Ok(HdbValue::CLOB(parse_clob(am_conn_core, rdr)?)),
            (BaseTypeId::NCLOB, false) => Ok(HdbValue::NCLOB(parse_nclob(am_conn_core, rdr)?)),
            (BaseTypeId::BLOB, false) => Ok(HdbValue::BLOB(parse_blob(am_conn_core, rdr)?)),
            (BaseTypeId::BOOLEAN, false) => Ok(HdbValue::BOOLEAN(rdr.read_u8()? > 0)),
            (BaseTypeId::STRING, false) => Ok(HdbValue::STRING(parse_string(rdr)?)),
            (BaseTypeId::NSTRING, false) => Ok(HdbValue::NSTRING(parse_string(rdr)?)),
            (BaseTypeId::BSTRING, false) => Ok(HdbValue::BSTRING(parse_binary(rdr)?)),
            (BaseTypeId::SMALLDECIMAL, false) => Ok(HdbValue::SMALLDECIMAL(parse_decimal(rdr)?)),
            (BaseTypeId::TEXT, false) => Ok(HdbValue::TEXT(
                parse_nclob(am_conn_core, rdr)?.into_string()?,
            )),
            (BaseTypeId::SHORTTEXT, false) => Ok(HdbValue::SHORTTEXT(parse_string(rdr)?)),
            (BaseTypeId::LONGDATE, false) => Ok(HdbValue::LONGDATE(parse_longdate(rdr)?)),
            (BaseTypeId::SECONDDATE, false) => Ok(HdbValue::SECONDDATE(parse_seconddate(rdr)?)),
            (BaseTypeId::DAYDATE, false) => Ok(HdbValue::DAYDATE(parse_daydate(rdr)?)),
            (BaseTypeId::SECONDTIME, false) => Ok(HdbValue::SECONDTIME(parse_secondtime(rdr)?)),
            (BaseTypeId::TINYINT, true) => Ok(HdbValue::N_TINYINT(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_u8()?)
            })),

            (BaseTypeId::SMALLINT, true) => Ok(HdbValue::N_SMALLINT(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_i16::<LittleEndian>()?)
            })),
            (BaseTypeId::INT, true) => Ok(HdbValue::N_INT(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_i32::<LittleEndian>()?)
            })),
            (BaseTypeId::BIGINT, true) => Ok(HdbValue::N_BIGINT(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_i64::<LittleEndian>()?)
            })),
            (BaseTypeId::DECIMAL, true) => Ok(HdbValue::N_DECIMAL(parse_nullable_decimal(rdr)?)),
            (BaseTypeId::REAL, true) => Ok(HdbValue::N_REAL(parse_nullable_real(rdr)?)),
            (BaseTypeId::DOUBLE, true) => Ok(HdbValue::N_DOUBLE(parse_nullable_double(rdr)?)),
            (BaseTypeId::CHAR, true) => Ok(HdbValue::N_CHAR(parse_nullable_string(rdr)?)),
            (BaseTypeId::VARCHAR, true) => Ok(HdbValue::N_VARCHAR(parse_nullable_string(rdr)?)),
            (BaseTypeId::NCHAR, true) => Ok(HdbValue::N_NCHAR(parse_nullable_string(rdr)?)),
            (BaseTypeId::NVARCHAR, true) => Ok(HdbValue::N_NVARCHAR(parse_nullable_string(rdr)?)),
            (BaseTypeId::BINARY, true) => Ok(HdbValue::N_BINARY(parse_nullable_binary(rdr)?)),
            (BaseTypeId::VARBINARY, true) => Ok(HdbValue::N_VARBINARY(parse_nullable_binary(rdr)?)),
            (BaseTypeId::CLOB, true) => {
                Ok(HdbValue::N_CLOB(parse_nullable_clob(am_conn_core, rdr)?))
            }
            (BaseTypeId::NCLOB, true) => {
                Ok(HdbValue::N_NCLOB(parse_nullable_nclob(am_conn_core, rdr)?))
            }
            (BaseTypeId::BLOB, true) => {
                Ok(HdbValue::N_BLOB(parse_nullable_blob(am_conn_core, rdr)?))
            }
            (BaseTypeId::BOOLEAN, true) => Ok(HdbValue::N_BOOLEAN(if ind_null(rdr)? {
                None
            } else {
                Some(rdr.read_u8()? > 0)
            })),
            (BaseTypeId::STRING, true) => Ok(HdbValue::N_STRING(parse_nullable_string(rdr)?)),
            (BaseTypeId::NSTRING, true) => Ok(HdbValue::N_NSTRING(parse_nullable_string(rdr)?)),
            (BaseTypeId::BSTRING, true) => Ok(HdbValue::N_BSTRING(parse_nullable_binary(rdr)?)),
            (BaseTypeId::SMALLDECIMAL, true) => {
                Ok(HdbValue::N_SMALLDECIMAL(parse_nullable_decimal(rdr)?))
            }
            (BaseTypeId::TEXT, true) => {
                if let Some(el) = parse_nullable_nclob(am_conn_core, rdr)? {
                    Ok(HdbValue::N_TEXT(Some(el.into_string()?)))
                } else {
                    Ok(HdbValue::N_TEXT(None))
                }
            }
            (BaseTypeId::SHORTTEXT, true) => Ok(HdbValue::N_SHORTTEXT(parse_nullable_string(rdr)?)),
            (BaseTypeId::LONGDATE, true) => Ok(HdbValue::N_LONGDATE(parse_nullable_longdate(rdr)?)),
            (BaseTypeId::SECONDDATE, true) => {
                Ok(HdbValue::N_SECONDDATE(parse_nullable_seconddate(rdr)?))
            }
            (BaseTypeId::DAYDATE, true) => Ok(HdbValue::N_DAYDATE(parse_nullable_daydate(rdr)?)),
            (BaseTypeId::SECONDTIME, true) => {
                Ok(HdbValue::N_SECONDTIME(parse_nullable_secondtime(rdr)?))
            }
        }
    }
}

// reads the nullindicator and returns Ok(true) if it has value 0 or Ok(false)
// otherwise
fn ind_null(rdr: &mut std::io::BufRead) -> HdbResult<bool> {
    Ok(rdr.read_u8()? == 0)
}

// reads the nullindicator and throws an error if it has value 0
fn ind_not_null(rdr: &mut std::io::BufRead) -> HdbResult<()> {
    if ind_null(rdr)? {
        Err(HdbError::Impl(
            "null value returned for not-null column".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn parse_real(rdr: &mut std::io::BufRead) -> HdbResult<f32> {
    let mut vec: Vec<u8> = std::iter::repeat(0u8).take(4).collect();
    rdr.read_exact(&mut vec[..])?;
    let mut r = std::io::Cursor::new(&vec);
    let tmp = r.read_u32::<LittleEndian>()?;
    match tmp {
        std::u32::MAX => Err(HdbError::Impl(
            "Unexpected NULL Value in parse_real()".to_owned(),
        )),
        _ => {
            r.set_position(0);
            Ok(r.read_f32::<LittleEndian>()?)
        }
    }
}

fn parse_nullable_real(rdr: &mut std::io::BufRead) -> HdbResult<Option<f32>> {
    let mut vec: Vec<u8> = std::iter::repeat(0u8).take(4).collect();
    rdr.read_exact(&mut vec[..])?;
    let mut r = std::io::Cursor::new(&vec);
    let tmp = r.read_u32::<LittleEndian>()?;
    match tmp {
        std::u32::MAX => Ok(None),
        _ => {
            r.set_position(0);
            Ok(Some(r.read_f32::<LittleEndian>()?))
        }
    }
}

fn parse_double(rdr: &mut std::io::BufRead) -> HdbResult<f64> {
    let mut vec: Vec<u8> = std::iter::repeat(0u8).take(8).collect();
    rdr.read_exact(&mut vec[..])?;
    let mut r = std::io::Cursor::new(&vec);
    let tmp = r.read_u64::<LittleEndian>()?;
    match tmp {
        std::u64::MAX => Err(HdbError::Impl(
            "Unexpected NULL Value in parse_double()".to_owned(),
        )),
        _ => {
            r.set_position(0);
            Ok(r.read_f64::<LittleEndian>()?)
        }
    }
}

fn parse_nullable_double(rdr: &mut std::io::BufRead) -> HdbResult<Option<f64>> {
    let mut vec: Vec<u8> = std::iter::repeat(0u8).take(8).collect();
    rdr.read_exact(&mut vec[..])?;
    let mut r = std::io::Cursor::new(&vec);
    let tmp = r.read_u64::<LittleEndian>()?;
    match tmp {
        std::u64::MAX => Ok(None),
        _ => {
            r.set_position(0);
            Ok(Some(r.read_f64::<LittleEndian>()?))
        }
    }
}

// ----- STRINGS and BINARIES
// ----------------------------------------------------------------
fn parse_string(rdr: &mut std::io::BufRead) -> HdbResult<String> {
    util::string_from_cesu8(parse_binary(rdr)?)
}

fn parse_binary(rdr: &mut std::io::BufRead) -> HdbResult<Vec<u8>> {
    let l8 = rdr.read_u8()?; // B1
    let len = match l8 {
        l if l <= MAX_1_BYTE_LENGTH => l8 as usize,
        LENGTH_INDICATOR_2BYTE => rdr.read_i16::<LittleEndian>()? as usize, // I2
        LENGTH_INDICATOR_4BYTE => rdr.read_i32::<LittleEndian>()? as usize, // I4
        l => {
            return Err(HdbError::Impl(format!(
                "Invalid value in length indicator: {}",
                l
            )));
        }
    };
    let result = util::parse_bytes(len, rdr)?; // B (varying)
    trace!("parse_binary(): read_bytes = {:?}, len = {}", result, len);
    Ok(result)
}

fn parse_nullable_string(rdr: &mut std::io::BufRead) -> HdbResult<Option<String>> {
    match parse_nullable_binary(rdr)? {
        Some(bytes) => Ok(Some({ util::string_from_cesu8(bytes)? })),
        None => Ok(None),
    }
}

fn parse_nullable_binary(rdr: &mut std::io::BufRead) -> HdbResult<Option<Vec<u8>>> {
    let l8 = rdr.read_u8()?; // B1
    let len = match l8 {
        l if l <= MAX_1_BYTE_LENGTH => l8 as usize,
        LENGTH_INDICATOR_2BYTE => rdr.read_i16::<LittleEndian>()? as usize, // I2
        LENGTH_INDICATOR_4BYTE => rdr.read_i32::<LittleEndian>()? as usize, // I4
        LENGTH_INDICATOR_NULL => return Ok(None),
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

//
fn type_id_not_null(base_type_id: BaseTypeId) -> TypeId {
    TypeId::new(base_type_id, false)
}
fn type_id_nullable(base_type_id: BaseTypeId) -> TypeId {
    TypeId::new(base_type_id, true)
}

pub(crate) fn string_length(s: &str) -> usize {
    binary_length(util::cesu8_length(s))
}

pub(crate) fn binary_length(l: usize) -> usize {
    match l {
        l if l <= MAX_1_BYTE_LENGTH as usize => 1 + l,
        l if l <= MAX_2_BYTE_LENGTH as usize => 3 + l,
        l => 5 + l,
    }
}

pub(crate) fn emit_length_and_string(s: &str, w: &mut std::io::Write) -> HdbResult<()> {
    emit_length_and_bytes(&cesu8::to_cesu8(s), w)
}

fn emit_length_and_bytes(v: &[u8], w: &mut std::io::Write) -> HdbResult<()> {
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
    w.write_all(v)?; // B variable   VALUE BYTES
    Ok(())
}

impl fmt::Display for HdbValue {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            HdbValue::NOTHING => write!(fmt, "Value already swapped out"),
            HdbValue::TINYINT(value) | HdbValue::N_TINYINT(Some(value)) => write!(fmt, "{}", value),
            HdbValue::SMALLINT(value) | HdbValue::N_SMALLINT(Some(value)) => {
                write!(fmt, "{}", value)
            }
            HdbValue::INT(value) | HdbValue::N_INT(Some(value)) => write!(fmt, "{}", value),
            HdbValue::BIGINT(value) | HdbValue::N_BIGINT(Some(value)) => write!(fmt, "{}", value),
            HdbValue::DECIMAL(ref value)
            | HdbValue::N_DECIMAL(Some(ref value))
            | HdbValue::SMALLDECIMAL(ref value)
            | HdbValue::N_SMALLDECIMAL(Some(ref value)) => write!(fmt, "{}", value),
            HdbValue::REAL(value) | HdbValue::N_REAL(Some(value)) => write!(fmt, "{}", value),
            HdbValue::DOUBLE(value) | HdbValue::N_DOUBLE(Some(value)) => write!(fmt, "{}", value),
            HdbValue::CHAR(ref value)
            | HdbValue::N_CHAR(Some(ref value))
            | HdbValue::VARCHAR(ref value)
            | HdbValue::N_VARCHAR(Some(ref value))
            | HdbValue::NCHAR(ref value)
            | HdbValue::N_NCHAR(Some(ref value))
            | HdbValue::NVARCHAR(ref value)
            | HdbValue::N_NVARCHAR(Some(ref value))
            | HdbValue::STRING(ref value)
            | HdbValue::N_STRING(Some(ref value))
            | HdbValue::NSTRING(ref value)
            | HdbValue::N_NSTRING(Some(ref value))
            | HdbValue::TEXT(ref value)
            | HdbValue::N_TEXT(Some(ref value))
            | HdbValue::SHORTTEXT(ref value)
            | HdbValue::N_SHORTTEXT(Some(ref value)) => write!(fmt, "{}", value),
            HdbValue::BINARY(_) | HdbValue::N_BINARY(Some(_)) => write!(fmt, "<BINARY>"),
            HdbValue::VARBINARY(ref vec) | HdbValue::N_VARBINARY(Some(ref vec)) => {
                write!(fmt, "<VARBINARY length = {}>", vec.len())
            }
            HdbValue::CLOB(_) | HdbValue::N_CLOB(Some(_)) => write!(fmt, "<CLOB>"),
            HdbValue::NCLOB(_) | HdbValue::N_NCLOB(Some(_)) => write!(fmt, "<NCLOB>"),
            HdbValue::BLOB(ref blob) | HdbValue::N_BLOB(Some(ref blob)) => write!(
                fmt,
                "<BLOB length = {}, read = {}>",
                blob.len_alldata(),
                blob.len_readdata()
            ),
            HdbValue::BOOLEAN(value) | HdbValue::N_BOOLEAN(Some(value)) => write!(fmt, "{}", value),
            HdbValue::BSTRING(_) | HdbValue::N_BSTRING(Some(_)) => write!(fmt, "<BSTRING>"),
            HdbValue::LONGDATE(ref value) | HdbValue::N_LONGDATE(Some(ref value)) => {
                write!(fmt, "{}", value)
            }
            HdbValue::SECONDDATE(ref value) | HdbValue::N_SECONDDATE(Some(ref value)) => {
                write!(fmt, "{}", value)
            }
            HdbValue::DAYDATE(ref value) | HdbValue::N_DAYDATE(Some(ref value)) => {
                write!(fmt, "{}", value)
            }
            HdbValue::SECONDTIME(ref value) | HdbValue::N_SECONDTIME(Some(ref value)) => {
                write!(fmt, "{}", value)
            }

            HdbValue::N_TINYINT(None)
            | HdbValue::N_SMALLINT(None)
            | HdbValue::N_INT(None)
            | HdbValue::N_BIGINT(None)
            | HdbValue::N_DECIMAL(None)
            | HdbValue::N_SMALLDECIMAL(None)
            | HdbValue::N_REAL(None)
            | HdbValue::N_DOUBLE(None)
            | HdbValue::N_CHAR(None)
            | HdbValue::N_VARCHAR(None)
            | HdbValue::N_NCHAR(None)
            | HdbValue::N_NVARCHAR(None)
            | HdbValue::N_BINARY(None)
            | HdbValue::N_VARBINARY(None)
            | HdbValue::N_CLOB(None)
            | HdbValue::N_NCLOB(None)
            | HdbValue::N_BLOB(None)
            | HdbValue::N_BOOLEAN(None)
            | HdbValue::N_STRING(None)
            | HdbValue::N_NSTRING(None)
            | HdbValue::N_BSTRING(None)
            | HdbValue::N_TEXT(None)
            | HdbValue::N_SHORTTEXT(None)
            | HdbValue::N_LONGDATE(None)
            | HdbValue::N_SECONDDATE(None)
            | HdbValue::N_DAYDATE(None)
            | HdbValue::N_SECONDTIME(None) => write!(fmt, "<NULL>"),
        }
    }
}

// FIXME implement more of these...
impl std::cmp::PartialEq<i32> for HdbValue {
    fn eq(&self, rhs: &i32) -> bool {
        match self {
            HdbValue::TINYINT(i) | HdbValue::N_TINYINT(Some(i)) => i32::from(*i) == *rhs,
            HdbValue::SMALLINT(i) | HdbValue::N_SMALLINT(Some(i)) => i32::from(*i) == *rhs,
            HdbValue::INT(i) | HdbValue::N_INT(Some(i)) => *i == *rhs,
            HdbValue::BIGINT(i) | HdbValue::N_BIGINT(Some(i)) => *i == i64::from(*rhs),
            _ => false,
        }
    }
}
impl std::cmp::PartialEq<&str> for HdbValue {
    fn eq(&self, rhs: &&str) -> bool {
        match self {
            HdbValue::STRING(ref s)
            | HdbValue::CHAR(ref s)
            | HdbValue::VARCHAR(ref s)
            | HdbValue::NCHAR(ref s)
            | HdbValue::NVARCHAR(ref s)
            | HdbValue::N_STRING(Some(ref s))
            | HdbValue::N_CHAR(Some(ref s))
            | HdbValue::N_VARCHAR(Some(ref s))
            | HdbValue::N_NCHAR(Some(ref s))
            | HdbValue::N_NVARCHAR(Some(ref s)) => s == rhs,
            _ => false,
        }
    }
}
