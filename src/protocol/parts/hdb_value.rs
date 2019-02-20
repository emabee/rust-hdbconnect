use crate::conn_core::AmConnCore;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptor;
use crate::protocol::parts::resultset::AmRsCore;
use crate::protocol::parts::type_id::TypeId;
use crate::protocol::util;
use crate::types::{BLob, CLob, DayDate, LongDate, NCLob, SecondDate, SecondTime};
use crate::types_impl::daydate::parse_daydate;
use crate::types_impl::decimal::{emit_decimal, parse_decimal};
use crate::types_impl::lob::{emit_lob_header, parse_blob, parse_clob, parse_nclob};
use crate::types_impl::longdate::parse_longdate;
use crate::types_impl::seconddate::parse_seconddate;
use crate::types_impl::secondtime::parse_secondtime;
use crate::{HdbError, HdbResult};
use bigdecimal::BigDecimal;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use cesu8;
use serde;
use serde_db::de::{ConversionError, DbValue};

const MAX_1_BYTE_LENGTH: u8 = 245;
const MAX_2_BYTE_LENGTH: i16 = std::i16::MAX;
const LENGTH_INDICATOR_2BYTE: u8 = 246;
const LENGTH_INDICATOR_4BYTE: u8 = 247;
const LENGTH_INDICATOR_NULL: u8 = 255;

/// Enum for all supported database value types.
#[allow(non_camel_case_types)]
pub enum HdbValue<'a> {
    /// Is swapped in where a real value (any of the others) is swapped out.
    NOTHING,
    /// Representation of a database NULL value.
    NULL,
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

    /// Representation for fixed-point decimal values.
    DECIMAL(BigDecimal),

    /// Stores a single-precision 32-bit floating-point number.
    REAL(f32),
    /// Stores a double-precision 64-bit floating-point number.
    /// The minimum value is -1.7976931348623157E308, the maximum value is
    /// 1.7976931348623157E308 . The smallest positive DOUBLE value is
    /// 2.2250738585072014E-308 and the largest negative DOUBLE value is
    /// -2.2250738585072014E-308.
    DOUBLE(f64),
    /// Stores binary data.
    BINARY(Vec<u8>),
    /// Stores a large ASCII character string.
    CLOB(CLob),
    /// Stores a large Unicode string.
    NCLOB(NCLob),
    /// Stores a large binary string.
    BLOB(BLob),

    /// Used for streaming LOBs to the database (see
    /// [`PreparedStatement::execute_row()`](struct.PreparedStatement.html#method.execute_row)).
    LOBSTREAM(Option<&'a mut std::io::Read>),

    /// BOOLEAN stores boolean values, which are TRUE or FALSE.
    BOOLEAN(bool),
    /// The DB returns all Strings as type STRING, independent of the concrete column type.
    STRING(String),

    /// Timestamp with 10^-7 seconds precision, uses eight bytes.
    LONGDATE(LongDate),
    /// TIMESTAMP with second precision.
    SECONDDATE(SecondDate),
    /// DATE with day precision.
    DAYDATE(DayDate),
    /// TIME with second precision.
    SECONDTIME(SecondTime),

    /// Spatial type GEOMETRY.
    GEOMETRY(Vec<u8>),
    /// Spatial type POINT.
    POINT(Vec<u8>),
}

impl<'a> HdbValue<'a> {
    pub(crate) fn type_id_for_emit(&self, requested_type_id: TypeId) -> HdbResult<TypeId> {
        Ok(match *self {
            HdbValue::NOTHING => {
                return Err(HdbError::Impl(
                    "Can't send HdbValue::NOTHING to Database".to_string(),
                ));
            }
            HdbValue::NULL => match requested_type_id {
                // work around a bug in HANA: it doesn't accept NULL SECONDTIME values
                TypeId::SECONDTIME => TypeId::SECONDDATE,
                tid => tid,
            },

            HdbValue::TINYINT(_) => TypeId::TINYINT,
            HdbValue::SMALLINT(_) => TypeId::SMALLINT,
            HdbValue::INT(_) => TypeId::INT,
            HdbValue::BIGINT(_) => TypeId::BIGINT,
            HdbValue::DECIMAL(_) => match requested_type_id {
                TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 | TypeId::DECIMAL => {
                    requested_type_id
                }
                _ => {
                    return Err(HdbError::Impl(format!(
                        "Can't send {} type for requested {:?} type",
                        "DECIMAL", requested_type_id
                    )));
                }
            },
            HdbValue::REAL(_) => TypeId::REAL,
            HdbValue::DOUBLE(_) => TypeId::DOUBLE,
            HdbValue::BINARY(_) => TypeId::BINARY,

            HdbValue::CLOB(_) | HdbValue::NCLOB(_) | HdbValue::BLOB(_) | HdbValue::LOBSTREAM(_) => {
                requested_type_id
            }
            HdbValue::BOOLEAN(_) => TypeId::BOOLEAN,
            HdbValue::STRING(_) => TypeId::STRING,
            HdbValue::LONGDATE(_) => TypeId::LONGDATE,
            HdbValue::SECONDDATE(_) => TypeId::SECONDDATE,
            HdbValue::DAYDATE(_) => TypeId::DAYDATE,
            HdbValue::SECONDTIME(_) => TypeId::SECONDTIME,
            HdbValue::GEOMETRY(_) => TypeId::BINARY, // TypeId::GEOMETRY,
            HdbValue::POINT(_) => TypeId::BINARY,    // TypeId::POINT,
        })
    }

    /// Returns true if the value is a NULL value.
    pub fn is_null(&self) -> bool {
        match *self {
            HdbValue::NULL => true,
            _ => false,
        }
    }

    pub(crate) fn emit<T: std::io::Write>(
        &self,
        _data_pos: &mut i32,
        descriptor: &ParameterDescriptor,
        w: &mut T,
    ) -> HdbResult<()> {
        if !self.emit_type_id(descriptor.type_id(), w)? {
            match *self {
                HdbValue::NULL => {}
                HdbValue::TINYINT(u) => w.write_u8(u)?,
                HdbValue::SMALLINT(i) => w.write_i16::<LittleEndian>(i)?,
                HdbValue::INT(i) => w.write_i32::<LittleEndian>(i)?,
                HdbValue::BIGINT(i) => w.write_i64::<LittleEndian>(i)?,
                HdbValue::DECIMAL(ref bigdec) => {
                    emit_decimal(bigdec, descriptor.type_id(), descriptor.scale(), w)?
                }
                HdbValue::REAL(f) => w.write_f32::<LittleEndian>(f)?,
                HdbValue::DOUBLE(f) => w.write_f64::<LittleEndian>(f)?,
                HdbValue::BOOLEAN(b) => emit_bool(b, w)?,
                HdbValue::LONGDATE(ref ld) => w.write_i64::<LittleEndian>(*ld.ref_raw())?,
                HdbValue::SECONDDATE(ref sd) => w.write_i64::<LittleEndian>(*sd.ref_raw())?,
                HdbValue::DAYDATE(ref dd) => w.write_i32::<LittleEndian>(*dd.ref_raw())?,
                HdbValue::SECONDTIME(ref st) => w.write_u32::<LittleEndian>(*st.ref_raw())?,

                HdbValue::LOBSTREAM(None) => emit_lob_header(0, _data_pos, w)?,
                HdbValue::STRING(ref s) => emit_length_and_string(s, w)?,
                HdbValue::BINARY(ref v) | HdbValue::GEOMETRY(ref v) | HdbValue::POINT(ref v) => {
                    emit_length_and_bytes(v, w)?
                }
                _ => {
                    return Err(HdbError::Usage(format!(
                        "HdbValue::{} cannot be sent to the database",
                        self
                    )));
                }
            }
        }
        Ok(())
    }

    // returns true if the value is a null value, false otherwise
    fn emit_type_id(&self, requested_type_id: TypeId, w: &mut std::io::Write) -> HdbResult<bool> {
        let is_null = self.is_null();
        let type_code = self.type_id_for_emit(requested_type_id)?.type_code(is_null);
        w.write_u8(type_code)?;
        Ok(is_null)
    }

    // is used to calculate the argument size (in emit)
    pub(crate) fn size(&self, type_id: TypeId) -> HdbResult<usize> {
        Ok(1 + match self {
            HdbValue::NOTHING | HdbValue::NULL => 0,
            HdbValue::BOOLEAN(_) | HdbValue::TINYINT(_) => 1,
            HdbValue::SMALLINT(_) => 2,
            HdbValue::DECIMAL(_) => match type_id {
                TypeId::DECIMAL => 16,
                TypeId::FIXED8 => 8,
                TypeId::FIXED12 => 12,
                TypeId::FIXED16 => 16,
                tid => {
                    return Err(HdbError::Impl(format!(
                        "invalid TypeId {:?} for DECIMAL",
                        tid
                    )));
                }
            },

            HdbValue::INT(_)
            | HdbValue::REAL(_)
            | HdbValue::DAYDATE(_)
            | HdbValue::SECONDTIME(_) => 4,

            HdbValue::BIGINT(_)
            | HdbValue::DOUBLE(_)
            | HdbValue::LONGDATE(_)
            | HdbValue::SECONDDATE(_) => 8,

            HdbValue::LOBSTREAM(None) => 9,
            HdbValue::STRING(ref s) => binary_length(util::cesu8_length(s)),

            HdbValue::BINARY(ref v) | HdbValue::GEOMETRY(ref v) | HdbValue::POINT(ref v) => {
                binary_length(v.len())
            }

            HdbValue::CLOB(_)
            | HdbValue::NCLOB(_)
            | HdbValue::BLOB(_)
            | HdbValue::LOBSTREAM(Some(_)) => {
                return Err(HdbError::Impl(format!(
                    "size(): can't send {:?} directly to the database",
                    self
                )));
            }
        })
    }
}

impl HdbValue<'static> {
    /// Deserialize into a rust type
    pub fn try_into<'x, T: serde::Deserialize<'x>>(self) -> HdbResult<T> {
        Ok(DbValue::into_typed(self)?)
    }

    /// Convert into hdbconnect::BLob
    pub fn try_into_blob(self) -> HdbResult<BLob> {
        match self {
            HdbValue::BLOB(blob) => Ok(blob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "The value {:?} cannot be converted into a BLOB",
                tv
            )))),
        }
    }

    /// Convert into hdbconnect::CLob
    pub fn try_into_clob(self) -> HdbResult<CLob> {
        match self {
            HdbValue::CLOB(clob) => Ok(clob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "The value {:?} cannot be converted into a CLOB",
                tv
            )))),
        }
    }

    /// Convert into hdbconnect::NCLob
    pub fn try_into_nclob(self) -> HdbResult<NCLob> {
        match self {
            HdbValue::NCLOB(nclob) => Ok(nclob),
            tv => Err(HdbError::Conversion(ConversionError::ValueType(format!(
                "HdbValue::try_into_nclob(): the database value {:?} cannot be converted into a NCLob",
                tv
            )))),
        }
    }

    pub(crate) fn parse_from_reply(
        type_id: TypeId,
        scale: i16,
        nullable: bool,
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        rdr: &mut std::io::BufRead,
    ) -> HdbResult<HdbValue<'static>> {
        let t = type_id;
        match t {
            TypeId::TINYINT => Ok(parse_tinyint(nullable, rdr)?),
            TypeId::SMALLINT => Ok(parse_smallint(nullable, rdr)?),
            TypeId::INT => Ok(parse_int(nullable, rdr)?),
            TypeId::BIGINT => Ok(parse_bigint(nullable, rdr)?),
            TypeId::REAL => Ok(parse_real(nullable, rdr)?),
            TypeId::DOUBLE => Ok(parse_double(nullable, rdr)?),

            TypeId::BOOLEAN => Ok(parse_bool(nullable, rdr)?),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                Ok(parse_decimal(nullable, t, scale, rdr)?)
            }

            TypeId::CHAR
            | TypeId::VARCHAR
            | TypeId::NCHAR
            | TypeId::NVARCHAR
            | TypeId::STRING
            | TypeId::NSTRING
            | TypeId::SHORTTEXT => Ok(parse_string(nullable, t, rdr)?),

            TypeId::BINARY
            | TypeId::VARBINARY
            | TypeId::BSTRING
            | TypeId::GEOMETRY
            | TypeId::POINT => Ok(parse_binary(nullable, t, rdr)?),

            TypeId::BLOB => Ok(parse_blob(am_conn_core, o_am_rscore, nullable, rdr)?),
            TypeId::CLOB => Ok(parse_clob(am_conn_core, o_am_rscore, nullable, rdr)?),
            TypeId::NCLOB | TypeId::TEXT => {
                Ok(parse_nclob(am_conn_core, o_am_rscore, nullable, t, rdr)?)
            }

            TypeId::LONGDATE => Ok(parse_longdate(nullable, rdr)?),
            TypeId::SECONDDATE => Ok(parse_seconddate(nullable, rdr)?),
            TypeId::DAYDATE => Ok(parse_daydate(nullable, rdr)?),
            TypeId::SECONDTIME => Ok(parse_secondtime(nullable, rdr)?),
        }
    }
}

fn emit_bool(b: bool, w: &mut std::io::Write) -> HdbResult<()> {
    // this is the version that works with dataformat_version2 = 4
    // w.write_u8(b as u8)?;

    // as of dataformat_version2 = 8
    w.write_u8(2 * (b as u8))?;
    Ok(())
}

// Reads the NULL indicator and
// - returns Ok(true) if the value is NULL
// - returns Ok(false) if a normal value is to be expected
// - throws an error if NULL is found but nullable is false
fn parse_null(nullable: bool, rdr: &mut std::io::BufRead) -> HdbResult<bool> {
    let is_null = rdr.read_u8()? == 0;
    if is_null && !nullable {
        Err(HdbError::Impl(
            "found null value for not-null column".to_owned(),
        ))
    } else {
        Ok(is_null)
    }
}

fn parse_tinyint(nullable: bool, rdr: &mut std::io::BufRead) -> HdbResult<HdbValue<'static>> {
    Ok(if parse_null(nullable, rdr)? {
        HdbValue::NULL
    } else {
        HdbValue::TINYINT(rdr.read_u8()?)
    })
}

fn parse_smallint(nullable: bool, rdr: &mut std::io::BufRead) -> HdbResult<HdbValue<'static>> {
    Ok(if parse_null(nullable, rdr)? {
        HdbValue::NULL
    } else {
        HdbValue::SMALLINT(rdr.read_i16::<LittleEndian>()?)
    })
}
fn parse_int(nullable: bool, rdr: &mut std::io::BufRead) -> HdbResult<HdbValue<'static>> {
    Ok(if parse_null(nullable, rdr)? {
        HdbValue::NULL
    } else {
        HdbValue::INT(rdr.read_i32::<LittleEndian>()?)
    })
}
fn parse_bigint(nullable: bool, rdr: &mut std::io::BufRead) -> HdbResult<HdbValue<'static>> {
    Ok(if parse_null(nullable, rdr)? {
        HdbValue::NULL
    } else {
        HdbValue::BIGINT(rdr.read_i64::<LittleEndian>()?)
    })
}

fn parse_real(nullable: bool, rdr: &mut std::io::BufRead) -> HdbResult<HdbValue<'static>> {
    let mut vec: Vec<u8> = std::iter::repeat(0u8).take(4).collect();
    rdr.read_exact(&mut vec[..])?;
    let mut cursor = std::io::Cursor::new(&vec);
    let tmp = cursor.read_u32::<LittleEndian>()?;
    let is_null = tmp == std::u32::MAX;

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found NULL value for NOT NULL column".to_owned(),
            ))
        }
    } else {
        cursor.set_position(0);
        Ok(HdbValue::REAL(cursor.read_f32::<LittleEndian>()?))
    }
}

fn parse_double(nullable: bool, rdr: &mut std::io::BufRead) -> HdbResult<HdbValue<'static>> {
    let mut vec: Vec<u8> = std::iter::repeat(0u8).take(8).collect();
    rdr.read_exact(&mut vec[..])?;
    let mut cursor = std::io::Cursor::new(&vec);
    let tmp = cursor.read_u64::<LittleEndian>()?;
    let is_null = tmp == std::u64::MAX;

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found NULL value for NOT NULL column".to_owned(),
            ))
        }
    } else {
        cursor.set_position(0);
        Ok(HdbValue::DOUBLE(cursor.read_f64::<LittleEndian>()?))
    }
}

fn parse_bool(nullable: bool, rdr: &mut std::io::BufRead) -> HdbResult<HdbValue<'static>> {
    //(0x00 = FALSE, 0x01 = NULL, 0x02 = TRUE)
    match rdr.read_u8()? {
        0 => Ok(HdbValue::BOOLEAN(false)),
        2 => Ok(HdbValue::BOOLEAN(true)),
        1 => {
            if nullable {
                Ok(HdbValue::NULL)
            } else {
                Err(HdbError::Impl("parse_bool: got null value".to_string()))
            }
        }
        i => Err(HdbError::Impl(format!("parse_bool: got bad value {}", i))),
    }
}

fn parse_string(
    nullable: bool,
    type_id: TypeId,
    rdr: &mut std::io::BufRead,
) -> HdbResult<HdbValue<'static>> {
    let l8 = rdr.read_u8()?; // B1
    let is_null = l8 == LENGTH_INDICATOR_NULL;

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found NULL value for NOT NULL string column".to_owned(),
            ))
        }
    } else {
        let s = util::string_from_cesu8(_read_bytes(l8, rdr)?)?;
        Ok(match type_id {
            TypeId::CHAR
            | TypeId::VARCHAR
            | TypeId::NCHAR
            | TypeId::NVARCHAR
            | TypeId::NSTRING
            | TypeId::SHORTTEXT
            | TypeId::STRING => HdbValue::STRING(s),
            _ => return Err(HdbError::Impl("unexpected type id for string".to_owned())),
        })
    }
}

fn parse_binary(
    nullable: bool,
    type_id: TypeId,
    rdr: &mut std::io::BufRead,
) -> HdbResult<HdbValue<'static>> {
    let l8 = rdr.read_u8()?; // B1
    let is_null = l8 == LENGTH_INDICATOR_NULL;

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found NULL value for NOT NULL binary column".to_owned(),
            ))
        }
    } else {
        let bytes = _read_bytes(l8, rdr)?;
        Ok(match type_id {
            TypeId::BSTRING | TypeId::VARBINARY | TypeId::BINARY => HdbValue::BINARY(bytes),
            TypeId::GEOMETRY => HdbValue::GEOMETRY(bytes),
            TypeId::POINT => HdbValue::POINT(bytes),
            _ => return Err(HdbError::Impl("unexpected type id for binary".to_owned())),
        })
    }
}

fn _read_bytes(l8: u8, rdr: &mut std::io::BufRead) -> HdbResult<Vec<u8>> {
    let len = match l8 {
        l if l <= MAX_1_BYTE_LENGTH => l8 as usize,
        LENGTH_INDICATOR_2BYTE => rdr.read_i16::<LittleEndian>()? as usize, // I2
        LENGTH_INDICATOR_4BYTE => rdr.read_i32::<LittleEndian>()? as usize, // I4
        l => {
            return Err(HdbError::Impl(format!(
                "Unexpected value in length indicator: {}",
                l
            )));
        }
    };
    util::parse_bytes(len, rdr)
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

impl<'a> std::fmt::Display for HdbValue<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            HdbValue::NOTHING => write!(fmt, "<NOTHING>"),
            HdbValue::NULL => write!(fmt, "<NULL>"),
            HdbValue::TINYINT(value) => write!(fmt, "{}", value),
            HdbValue::SMALLINT(value) => write!(fmt, "{}", value),
            HdbValue::INT(value) => write!(fmt, "{}", value),
            HdbValue::BIGINT(value) => write!(fmt, "{}", value),

            HdbValue::DECIMAL(ref value) => write!(fmt, "{}", value),

            HdbValue::REAL(value) => write!(fmt, "{}", value),
            HdbValue::DOUBLE(value) => write!(fmt, "{}", value),
            HdbValue::STRING(ref value) => {
                if value.len() < 10_000 {
                    write!(fmt, "{}", value)
                } else {
                    write!(fmt, "<STRING length = {}>", value.len())
                }
            }
            HdbValue::BINARY(ref vec) => write!(fmt, "<BINARY length = {}>", vec.len()),

            HdbValue::CLOB(_) => write!(fmt, "<CLOB>"),
            HdbValue::NCLOB(_) => write!(fmt, "<NCLOB>"),
            HdbValue::BLOB(ref blob) => write!(fmt, "<BLOB length = {}>", blob.total_byte_length()),
            HdbValue::LOBSTREAM(_) => write!(fmt, "<LOBSTREAM>"),
            HdbValue::BOOLEAN(value) => write!(fmt, "{}", value),
            HdbValue::LONGDATE(ref value) => write!(fmt, "{}", value),
            HdbValue::SECONDDATE(ref value) => write!(fmt, "{}", value),
            HdbValue::DAYDATE(ref value) => write!(fmt, "{}", value),
            HdbValue::SECONDTIME(ref value) => write!(fmt, "{}", value),
            HdbValue::GEOMETRY(ref vec) => write!(fmt, "<GEOMETRY length = {}>", vec.len()),
            HdbValue::POINT(ref vec) => write!(fmt, "<POINT length = {}>", vec.len()),
        }
    }
}

impl<'a> std::fmt::Debug for HdbValue<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, fmt)
    }
}

// FIXME implement more of these...
impl<'a> std::cmp::PartialEq<i32> for HdbValue<'a> {
    fn eq(&self, rhs: &i32) -> bool {
        match self {
            HdbValue::TINYINT(i) => i32::from(*i) == *rhs,
            HdbValue::SMALLINT(i) => i32::from(*i) == *rhs,
            HdbValue::INT(i) => *i == *rhs,
            HdbValue::BIGINT(i) => *i == i64::from(*rhs),
            _ => false,
        }
    }
}
impl<'a> std::cmp::PartialEq<&str> for HdbValue<'a> {
    fn eq(&self, rhs: &&str) -> bool {
        match self {
            HdbValue::STRING(ref s) => s == rhs,
            _ => false,
        }
    }
}
