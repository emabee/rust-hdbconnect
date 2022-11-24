use crate::conn::AmConnCore;
use crate::protocol::parts::{length_indicator, AmRsCore, ParameterDescriptor, TypeId};
use crate::protocol::{util, util_async, util_sync};
use crate::types::{BLob, CLob, DayDate, LongDate, NCLob, SecondDate, SecondTime};
use crate::types_impl::daydate::{parse_daydate_async, parse_daydate_sync};
use crate::types_impl::decimal;
use crate::types_impl::lob;
use crate::types_impl::longdate::{parse_longdate_async, parse_longdate_sync};
use crate::types_impl::seconddate::{parse_seconddate_async, parse_seconddate_sync};
use crate::types_impl::secondtime::{parse_secondtime_async, parse_secondtime_sync};
use crate::{HdbError, HdbResult};
use bigdecimal::BigDecimal;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

const ALPHANUM_PURELY_NUMERIC: u8 = 0b_1000_0000_u8;
const ALPHANUM_LENGTH_MASK: u8 = 0b_0111_1111_u8;

/// Enum for all supported database value types.
#[allow(non_camel_case_types)]
pub enum HdbValue<'a> {
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
    /// [`PreparedStatement::execute_row()`](crate::PreparedStatement::execute_row)).
    LOBSTREAM(Option<std::sync::Arc<std::sync::Mutex<dyn std::io::Read + Send>>>),

    /// BOOLEAN stores boolean values, which are TRUE or FALSE.
    BOOLEAN(bool),
    /// The DB returns all Strings as type STRING, independent of the concrete column type.
    STRING(String),

    /// Can be used for avoiding cloning when sending large Strings to the database  (see
    /// [`PreparedStatement::execute_row()`](crate::PreparedStatement::execute_row)).
    STR(&'a str),

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

    /// HANA's array type
    ARRAY(Vec<HdbValue<'a>>),
}

impl<'a> HdbValue<'a> {
    pub(crate) fn type_id_for_emit(&self, requested_type_id: TypeId) -> HdbResult<TypeId> {
        Ok(match *self {
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
                    return Err( HdbError::ImplDetailed(format!(
                        "Can't send DECIMAL for requested type {requested_type_id:?}"
                    )));
                }
            },
            HdbValue::REAL(_) => TypeId::REAL,
            HdbValue::DOUBLE(_) => TypeId::DOUBLE,

            HdbValue::CLOB(_) | HdbValue::NCLOB(_) | HdbValue::BLOB(_) | HdbValue::LOBSTREAM(_) =>
                requested_type_id,

            HdbValue::BOOLEAN(_) => TypeId::BOOLEAN,
            HdbValue::STR(_) | HdbValue::STRING(_) => TypeId::STRING,
            HdbValue::LONGDATE(_) => TypeId::LONGDATE,
            HdbValue::SECONDDATE(_) => TypeId::SECONDDATE,
            HdbValue::DAYDATE(_) => TypeId::DAYDATE,
            HdbValue::SECONDTIME(_) => TypeId::SECONDTIME,
            HdbValue::GEOMETRY(_) | // TypeId::GEOMETRY,
            HdbValue::POINT(_) |    // TypeId::POINT,
            HdbValue::BINARY(_) => TypeId::BINARY,
            HdbValue::ARRAY(_) => unimplemented!("Can't send array type to DB; not yet supported"),
        })
    }

    /// Returns true if the value is a NULL value.
    pub fn is_null(&self) -> bool {
        matches!(*self, HdbValue::NULL)
    }

    pub(crate) fn emit_sync(
        &self,
        data_pos: &mut i32,
        descriptor: &ParameterDescriptor,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        if !self.emit_type_id_sync(descriptor.type_id(), w)? {
            match *self {
                HdbValue::NULL => {}
                HdbValue::TINYINT(u) => w.write_u8(u)?,
                HdbValue::SMALLINT(i) => w.write_i16::<LittleEndian>(i)?,
                HdbValue::INT(i) => w.write_i32::<LittleEndian>(i)?,
                HdbValue::BIGINT(i) => w.write_i64::<LittleEndian>(i)?,
                HdbValue::DECIMAL(ref bigdec) => {
                    decimal::emit_sync(bigdec, descriptor.type_id(), descriptor.scale(), w)?
                }
                HdbValue::REAL(f) => w.write_f32::<LittleEndian>(f)?,
                HdbValue::DOUBLE(f) => w.write_f64::<LittleEndian>(f)?,
                HdbValue::BOOLEAN(b) => emit_bool_sync(b, w)?,
                HdbValue::LONGDATE(ref ld) => w.write_i64::<LittleEndian>(*ld.ref_raw())?,
                HdbValue::SECONDDATE(ref sd) => w.write_i64::<LittleEndian>(*sd.ref_raw())?,
                HdbValue::DAYDATE(ref dd) => w.write_i32::<LittleEndian>(*dd.ref_raw())?,
                HdbValue::SECONDTIME(ref st) => w.write_u32::<LittleEndian>(*st.ref_raw())?,

                HdbValue::LOBSTREAM(None) => lob::emit_lob_header_sync(0, data_pos, w)?,
                HdbValue::STR(s) => emit_length_and_string_sync(s, w)?,
                HdbValue::STRING(ref s) => emit_length_and_string_sync(s, w)?,
                HdbValue::BINARY(ref v) | HdbValue::GEOMETRY(ref v) | HdbValue::POINT(ref v) => {
                    emit_length_and_bytes_sync(v, w)?
                }
                _ => {
                    return Err(util::io_error(format!(
                        "HdbValue::{} cannot be sent to the database",
                        self
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        data_pos: &mut i32,
        descriptor: &ParameterDescriptor,
        w: &mut W,
    ) -> std::io::Result<()> {
        if !self.emit_type_id_async(descriptor.type_id(), w).await? {
            match *self {
                HdbValue::NULL => {}
                HdbValue::TINYINT(u) => w.write_u8(u).await?,
                HdbValue::SMALLINT(i) => w.write_all(&i.to_le_bytes()).await?,
                HdbValue::INT(i) => w.write_all(&i.to_le_bytes()).await?,
                HdbValue::BIGINT(i) => w.write_all(&i.to_le_bytes()).await?,
                HdbValue::DECIMAL(ref bigdec) => {
                    decimal::emit_async(bigdec, descriptor.type_id(), descriptor.scale(), w).await?
                }
                HdbValue::REAL(f) => w.write_all(&f.to_le_bytes()).await?,
                HdbValue::DOUBLE(f) => w.write_all(&f.to_le_bytes()).await?,
                HdbValue::BOOLEAN(b) => emit_bool_async(b, w).await?,
                HdbValue::LONGDATE(ref ld) => w.write_all(&ld.ref_raw().to_le_bytes()).await?,
                HdbValue::SECONDDATE(ref sd) => w.write_all(&sd.ref_raw().to_le_bytes()).await?,
                HdbValue::DAYDATE(ref dd) => w.write_all(&dd.ref_raw().to_le_bytes()).await?,
                HdbValue::SECONDTIME(ref st) => w.write_all(&st.ref_raw().to_le_bytes()).await?,

                HdbValue::LOBSTREAM(None) => lob::emit_lob_header_async(0, data_pos, w).await?,
                HdbValue::STR(s) => emit_length_and_string_async(s, w).await?,
                HdbValue::STRING(ref s) => emit_length_and_string_async(s, w).await?,
                HdbValue::BINARY(ref v) | HdbValue::GEOMETRY(ref v) | HdbValue::POINT(ref v) => {
                    emit_length_and_bytes_async(v, w).await?
                }
                _ => {
                    return Err(util::io_error(format!(
                        "HdbValue::{} cannot be sent to the database",
                        self
                    )));
                }
            }
        }
        Ok(())
    }

    // sync_emits the type-id; returns true if the value is a null value, false otherwise
    fn emit_type_id_sync(
        &self,
        requested_type_id: TypeId,
        w: &mut dyn std::io::Write,
    ) -> std::io::Result<bool> {
        let is_null = self.is_null();
        let type_code = self
            .type_id_for_emit(requested_type_id)
            .map_err(|e| util::io_error(e.to_string()))?
            .type_code(is_null);
        w.write_u8(type_code)?;
        Ok(is_null)
    }

    async fn emit_type_id_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        requested_type_id: TypeId,
        w: &mut W,
    ) -> std::io::Result<bool> {
        let is_null = self.is_null();
        let type_code = self
            .type_id_for_emit(requested_type_id)
            .map_err(|e| util::io_error(e.to_string()))?
            .type_code(is_null);
        w.write_u8(type_code).await?;
        Ok(is_null)
    }

    // is used to calculate the part size (in sync_emit)
    pub(crate) fn size(&self, type_id: TypeId) -> std::io::Result<usize> {
        Ok(1 + match self {
            HdbValue::NULL => 0,
            HdbValue::BOOLEAN(_) | HdbValue::TINYINT(_) => 1,
            HdbValue::SMALLINT(_) => 2,
            HdbValue::DECIMAL(_) => match type_id {
                TypeId::FIXED8 => 8,
                TypeId::FIXED12 => 12,
                TypeId::FIXED16 | TypeId::DECIMAL => 16,
                tid => {
                    return Err(util::io_error(format!(
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
            HdbValue::STR(s) => binary_length(util::cesu8_length(s)),
            HdbValue::STRING(ref s) => binary_length(util::cesu8_length(s)),

            HdbValue::BINARY(ref v) | HdbValue::GEOMETRY(ref v) | HdbValue::POINT(ref v) => {
                binary_length(v.len())
            }

            HdbValue::CLOB(_)
            | HdbValue::NCLOB(_)
            | HdbValue::BLOB(_)
            | HdbValue::LOBSTREAM(Some(_)) => {
                return Err(util::io_error(format!(
                    "size(): can't send {:?} directly to the database",
                    self
                )));
            }

            HdbValue::ARRAY(_) => {
                unimplemented!(" size(): can't handle ARRAY; not yet implemented")
            }
        })
    }
}

impl HdbValue<'static> {
    /// Deserialize into a rust type.
    ///
    /// # Errors
    ///
    /// `HdbError::DeserializationError` if the target type does not fit.
    pub fn try_into<'x, T: serde::Deserialize<'x>>(self) -> HdbResult<T> {
        Ok(serde_db::de::DbValue::try_into(self)?)
    }

    /// Convert into `BLob`.
    ///
    /// # Errors
    ///
    /// `HdbError::UsageDetailed` if this is not a `HdbValue::BLOB`.
    pub fn try_into_blob(self) -> HdbResult<BLob> {
        match self {
            HdbValue::BLOB(blob) => Ok(blob),
            v => Err(HdbError::UsageDetailed(format!(
                "The value {:?} cannot be converted into a BLOB",
                v
            ))),
        }
    }

    /// Convert into `CLob`.
    ///
    /// # Errors
    ///
    /// `HdbError::UsageDetailed` if this is not a `HdbValue::CLOB`.
    pub fn try_into_clob(self) -> HdbResult<CLob> {
        match self {
            HdbValue::CLOB(clob) => Ok(clob),
            v => Err(HdbError::UsageDetailed(format!(
                "The value {:?} cannot be converted into a CLOB",
                v
            ))),
        }
    }

    /// Convert into `NCLob`.
    ///
    /// # Errors
    ///
    /// `HdbError::UsageDetailed` if this is not a `HdbValue::NCLOB`.
    pub fn try_into_nclob(self) -> HdbResult<NCLob> {
        match self {
            HdbValue::NCLOB(nclob) => Ok(nclob),
            v => Err(HdbError::UsageDetailed(format!(
                "The database value {:?} cannot be converted into a NCLob",
                v
            ))),
        }
    }

    pub(crate) fn parse_sync(
        type_id: TypeId,
        array_type: bool,
        scale: i16,
        nullable: bool,
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        rdr: &mut dyn std::io::Read,
    ) -> std::io::Result<HdbValue<'static>> {
        let t = type_id;
        if array_type {
            let l8 = rdr.read_u8()?;
            let _bytelen = length_indicator::parse_sync(l8, rdr)?;
            let mut values = vec![];
            for _i in 0..rdr.read_i32::<LittleEndian>()? {
                let value = HdbValue::parse_sync(
                    type_id,
                    false, // nested array_types are not allowed
                    scale,
                    true, // nullable,
                    am_conn_core,
                    o_am_rscore,
                    rdr,
                )?;
                values.push(value);
            }
            Ok(HdbValue::ARRAY(values))
        } else {
            match t {
                TypeId::TINYINT => Ok(parse_tinyint_sync(nullable, rdr)?),
                TypeId::SMALLINT => Ok(parse_smallint_sync(nullable, rdr)?),
                TypeId::INT => Ok(parse_int_sync(nullable, rdr)?),
                TypeId::BIGINT => Ok(parse_bigint_sync(nullable, rdr)?),
                TypeId::REAL => Ok(parse_real_sync(nullable, rdr)?),
                TypeId::DOUBLE => Ok(parse_double_sync(nullable, rdr)?),

                TypeId::BOOLEAN => Ok(parse_bool_sync(nullable, rdr)?),

                TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                    Ok(decimal::parse_sync(nullable, t, scale, rdr)?)
                }

                TypeId::CHAR
                | TypeId::VARCHAR
                | TypeId::NCHAR
                | TypeId::NVARCHAR
                | TypeId::STRING
                | TypeId::NSTRING
                | TypeId::SHORTTEXT => Ok(parse_string_sync(nullable, t, rdr)?),

                TypeId::ALPHANUM => {
                    let res = parse_alphanum_sync(nullable, rdr)?;
                    Ok(res)
                }

                TypeId::BINARY
                | TypeId::VARBINARY
                | TypeId::BSTRING
                | TypeId::GEOMETRY
                | TypeId::POINT => Ok(parse_binary_sync(nullable, t, rdr)?),

                TypeId::BLOCATOR => Err(util::io_error("parsing BLOCATOR not implemented")),
                TypeId::BLOB | TypeId::BINTEXT => Ok(lob::parse_blob_sync(
                    am_conn_core,
                    o_am_rscore,
                    nullable,
                    rdr,
                )?),
                TypeId::CLOB => Ok(lob::parse_clob_sync(
                    am_conn_core,
                    o_am_rscore,
                    nullable,
                    rdr,
                )?),
                TypeId::NCLOB | TypeId::TEXT => Ok(lob::parse_nclob_sync(
                    am_conn_core,
                    o_am_rscore,
                    nullable,
                    t,
                    rdr,
                )?),

                TypeId::LONGDATE => Ok(parse_longdate_sync(nullable, rdr)?),
                TypeId::SECONDDATE => Ok(parse_seconddate_sync(nullable, rdr)?),
                TypeId::DAYDATE => Ok(parse_daydate_sync(nullable, rdr)?),
                TypeId::SECONDTIME => Ok(parse_secondtime_sync(nullable, rdr)?),
            }
        }
    }

    pub(crate) async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        type_id: TypeId,
        array_type: bool, // FIXME use it !! as in the sync variant!!
        scale: i16,
        nullable: bool,
        am_conn_core: &AmConnCore,
        o_am_rscore: &Option<AmRsCore>,
        rdr: &mut R,
    ) -> std::io::Result<HdbValue<'static>> {
        let t = type_id;
        match t {
            TypeId::TINYINT => Ok(parse_tinyint_async(nullable, rdr).await?),
            TypeId::SMALLINT => Ok(parse_smallint_async(nullable, rdr).await?),
            TypeId::INT => Ok(parse_int_async(nullable, rdr).await?),
            TypeId::BIGINT => Ok(parse_bigint_async(nullable, rdr).await?),
            TypeId::REAL => Ok(parse_real_async(nullable, rdr).await?),
            TypeId::DOUBLE => Ok(parse_double_async(nullable, rdr).await?),

            TypeId::BOOLEAN => Ok(parse_bool_async(nullable, rdr).await?),

            TypeId::DECIMAL | TypeId::FIXED8 | TypeId::FIXED12 | TypeId::FIXED16 => {
                Ok(decimal::parse_async(nullable, t, scale, rdr).await?)
            }

            TypeId::CHAR
            | TypeId::VARCHAR
            | TypeId::NCHAR
            | TypeId::NVARCHAR
            | TypeId::STRING
            | TypeId::NSTRING
            | TypeId::SHORTTEXT => Ok(parse_string_async(nullable, t, rdr).await?),

            TypeId::ALPHANUM => {
                let res = parse_alphanum_async(nullable, rdr).await?;
                Ok(res)
            }

            TypeId::BINARY
            | TypeId::VARBINARY
            | TypeId::BSTRING
            | TypeId::GEOMETRY
            | TypeId::POINT => Ok(parse_binary_async(nullable, t, rdr).await?),

            TypeId::BLOCATOR => Err(util::io_error("parsing BLOCATOR not implemented")),
            TypeId::BLOB | TypeId::BINTEXT => {
                Ok(lob::parse_blob_async(am_conn_core, o_am_rscore, nullable, rdr).await?)
            }
            TypeId::CLOB => {
                Ok(lob::parse_clob_async(am_conn_core, o_am_rscore, nullable, rdr).await?)
            }
            TypeId::NCLOB | TypeId::TEXT => {
                Ok(lob::parse_nclob_async(am_conn_core, o_am_rscore, nullable, t, rdr).await?)
            }

            TypeId::LONGDATE => Ok(parse_longdate_async(nullable, rdr).await?),
            TypeId::SECONDDATE => Ok(parse_seconddate_async(nullable, rdr).await?),
            TypeId::DAYDATE => Ok(parse_daydate_async(nullable, rdr).await?),
            TypeId::SECONDTIME => Ok(parse_secondtime_async(nullable, rdr).await?),
        }
    }
}

fn emit_bool_sync(b: bool, w: &mut dyn std::io::Write) -> std::io::Result<()> {
    // this is the version that works with dataformat_version2 = 4
    // w.write_u8(b as u8)?;

    // as of dataformat_version2 = 8
    w.write_u8(2 * (u8::from(b)))?;
    Ok(())
}

async fn emit_bool_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    b: bool,
    w: &mut W,
) -> std::io::Result<()> {
    // this is the version that works with dataformat_version2 = 4
    // w.write_u8(b as u8).await?;

    // as of dataformat_version2 = 8
    w.write_u8(2 * (b as u8)).await?;
    Ok(())
}

// Reads the NULL indicator and
// - returns Ok(true) if the value is NULL
// - returns Ok(false) if a normal value is to be expected
// - throws an error if NULL is found but nullable is false
fn parse_null_sync(nullable: bool, rdr: &mut dyn std::io::Read) -> std::io::Result<bool> {
    let is_null = rdr.read_u8()? == 0;
    if is_null && !nullable {
        Err(util::io_error("found null value for not-null column"))
    } else {
        Ok(is_null)
    }
}

// Reads the NULL indicator and
// - returns Ok(true) if the value is NULL
// - returns Ok(false) if a normal value is to be expected
// - throws an error if NULL is found but nullable is false
async fn parse_null_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<bool> {
    let is_null = rdr.read_u8().await? == 0;
    if is_null && !nullable {
        Err(util::io_error("found null value for not-null column"))
    } else {
        Ok(is_null)
    }
}

fn parse_tinyint_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    Ok(if parse_null_sync(nullable, rdr)? {
        HdbValue::NULL
    } else {
        HdbValue::TINYINT(rdr.read_u8()?)
    })
}

async fn parse_tinyint_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    Ok(if parse_null_async(nullable, rdr).await? {
        HdbValue::NULL
    } else {
        HdbValue::TINYINT(rdr.read_u8().await?)
    })
}

fn parse_smallint_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    Ok(if parse_null_sync(nullable, rdr)? {
        HdbValue::NULL
    } else {
        HdbValue::SMALLINT(rdr.read_i16::<LittleEndian>()?)
    })
}

async fn parse_smallint_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    Ok(if parse_null_async(nullable, rdr).await? {
        HdbValue::NULL
    } else {
        HdbValue::SMALLINT(util_async::read_i16(rdr).await?)
    })
}

fn parse_int_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    Ok(if parse_null_sync(nullable, rdr)? {
        HdbValue::NULL
    } else {
        HdbValue::INT(rdr.read_i32::<LittleEndian>()?)
    })
}

async fn parse_int_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    Ok(if parse_null_async(nullable, rdr).await? {
        HdbValue::NULL
    } else {
        HdbValue::INT(util_async::read_i32(rdr).await?)
    })
}

fn parse_bigint_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    Ok(if parse_null_sync(nullable, rdr)? {
        HdbValue::NULL
    } else {
        HdbValue::BIGINT(rdr.read_i64::<LittleEndian>()?)
    })
}

async fn parse_bigint_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    Ok(if parse_null_async(nullable, rdr).await? {
        HdbValue::NULL
    } else {
        HdbValue::BIGINT(util_async::read_i64(rdr).await?)
    })
}

fn parse_real_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let mut vec: Vec<u8> = std::iter::repeat(0_u8).take(4).collect();
    rdr.read_exact(&mut vec[..])?;
    let mut cursor = std::io::Cursor::new(&vec);
    let tmp = cursor.read_u32::<LittleEndian>()?;
    let is_null = tmp == u32::max_value();

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found NULL value for NOT NULL column"))
        }
    } else {
        cursor.set_position(0);
        Ok(HdbValue::REAL(cursor.read_f32::<LittleEndian>()?))
    }
}

async fn parse_real_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let mut vec: Vec<u8> = std::iter::repeat(0_u8).take(4).collect();
    rdr.read_exact(&mut vec[..]).await?;
    let mut cursor = std::io::Cursor::new(&vec);
    let tmp = cursor.read_u32::<LittleEndian>()?;
    let is_null = tmp == u32::max_value();

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found NULL value for NOT NULL column"))
        }
    } else {
        cursor.set_position(0);
        Ok(HdbValue::REAL(cursor.read_f32::<LittleEndian>()?))
    }
}

fn parse_double_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let mut vec: Vec<u8> = std::iter::repeat(0_u8).take(8).collect();
    rdr.read_exact(&mut vec[..])?;
    let mut cursor = std::io::Cursor::new(&vec);
    let tmp = cursor.read_u64::<LittleEndian>()?;
    let is_null = tmp == u64::max_value();

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found NULL value for NOT NULL column"))
        }
    } else {
        cursor.set_position(0);
        Ok(HdbValue::DOUBLE(cursor.read_f64::<LittleEndian>()?))
    }
}

async fn parse_double_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let mut vec: Vec<u8> = std::iter::repeat(0_u8).take(8).collect();
    rdr.read_exact(&mut vec[..]).await?;
    let mut cursor = std::io::Cursor::new(&vec);
    let tmp = cursor.read_u64::<LittleEndian>()?;
    let is_null = tmp == u64::max_value();

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found NULL value for NOT NULL column"))
        }
    } else {
        cursor.set_position(0);
        Ok(HdbValue::DOUBLE(cursor.read_f64::<LittleEndian>()?))
    }
}

fn parse_bool_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    //(0x00 = FALSE, 0x01 = NULL, 0x02 = TRUE)
    match rdr.read_u8()? {
        0 => Ok(HdbValue::BOOLEAN(false)),
        2 => Ok(HdbValue::BOOLEAN(true)),
        1 => {
            if nullable {
                Ok(HdbValue::NULL)
            } else {
                Err(util::io_error(
                    "parse_bool_sync: got null value".to_string(),
                ))
            }
        }
        i => Err(util::io_error(format!("parse_bool: got bad value {i}"))),
    }
}

async fn parse_bool_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    //(0x00 = FALSE, 0x01 = NULL, 0x02 = TRUE)
    match rdr.read_u8().await? {
        0 => Ok(HdbValue::BOOLEAN(false)),
        2 => Ok(HdbValue::BOOLEAN(true)),
        1 => {
            if nullable {
                Ok(HdbValue::NULL)
            } else {
                Err(util::io_error(
                    "parse_bool_sync: got null value".to_string(),
                ))
            }
        }
        i => Err(util::io_error(format!(
            "parse_bool_sync: got bad value {}",
            i
        ))),
    }
}

fn parse_alphanum_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let indicator1 = rdr.read_u8()?;
    if indicator1 == length_indicator::LENGTH_INDICATOR_NULL {
        // value is null
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error(
                "found NULL value for NOT NULL ALPHANUM column",
            ))
        }
    } else {
        let data_length = indicator1 - 1; // why?!?

        let indicator2 = rdr.read_u8()?;
        let mut value = util_sync::parse_bytes(data_length as usize, rdr)?;

        let s = util::string_from_cesu8(if indicator2 & ALPHANUM_PURELY_NUMERIC == 0 {
            // no prefix
            value
        } else {
            // purely numeric -> prefix with leading zeros
            let field_length = indicator2 & ALPHANUM_LENGTH_MASK;
            let mut prefix: Vec<u8> = std::iter::repeat(48) // '0'
                .take((field_length - data_length) as usize)
                .collect();
            prefix.append(&mut value);
            prefix
        })
        .map_err(util::io_error)?;
        Ok(HdbValue::STRING(s))
    }
}

async fn parse_alphanum_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let indicator1 = rdr.read_u8().await?;
    if indicator1 == length_indicator::LENGTH_INDICATOR_NULL {
        // value is null
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error(
                "found NULL value for NOT NULL ALPHANUM column",
            ))
        }
    } else {
        let data_length = indicator1 - 1; // why?!?

        let indicator2 = rdr.read_u8().await?;
        let mut value = util_async::parse_bytes(data_length as usize, rdr).await?;

        let s = util::string_from_cesu8(if indicator2 & ALPHANUM_PURELY_NUMERIC == 0 {
            // no prefix
            value
        } else {
            // purely numeric -> prefix with leading zeros
            let field_length = indicator2 & ALPHANUM_LENGTH_MASK;
            let mut prefix: Vec<u8> = std::iter::repeat(48) // '0'
                .take((field_length - data_length) as usize)
                .collect();
            prefix.append(&mut value);
            prefix
        })
        .map_err(util::io_error)?;
        Ok(HdbValue::STRING(s))
    }
}

fn parse_string_sync(
    nullable: bool,
    type_id: TypeId,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let l8 = rdr.read_u8()?; // B1
    let is_null = l8 == length_indicator::LENGTH_INDICATOR_NULL;

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error(
                "found NULL value for NOT NULL string column",
            ))
        }
    } else {
        let s = util::string_from_cesu8(parse_length_and_bytes_sync(l8, rdr)?).map_err(
            |cesu8_error| {
                trace!("parse_string: {:?}", cesu8_error);
                util::io_error(cesu8_error)
            },
        )?;
        Ok(match type_id {
            TypeId::CHAR
            | TypeId::VARCHAR
            | TypeId::NCHAR
            | TypeId::NVARCHAR
            | TypeId::NSTRING
            | TypeId::SHORTTEXT
            | TypeId::STRING => HdbValue::STRING(s),
            _ => return Err(util::io_error("unexpected type id for string")),
        })
    }
}

async fn parse_string_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    type_id: TypeId,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let l8 = rdr.read_u8().await?; // B1
    let is_null = l8 == length_indicator::LENGTH_INDICATOR_NULL;

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error(
                "found NULL value for NOT NULL string column",
            ))
        }
    } else {
        let s = util::string_from_cesu8(parse_length_and_bytes_async(l8, rdr).await?)
            .map_err(util::io_error)?;
        Ok(match type_id {
            TypeId::CHAR
            | TypeId::VARCHAR
            | TypeId::NCHAR
            | TypeId::NVARCHAR
            | TypeId::NSTRING
            | TypeId::SHORTTEXT
            | TypeId::STRING => HdbValue::STRING(s),
            _ => return Err(util::io_error("unexpected type id for string")),
        })
    }
}

fn parse_binary_sync(
    nullable: bool,
    type_id: TypeId,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let l8 = rdr.read_u8()?; // B1
    let is_null = l8 == length_indicator::LENGTH_INDICATOR_NULL;

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error(
                "found NULL value for NOT NULL binary column",
            ))
        }
    } else {
        let bytes = parse_length_and_bytes_sync(l8, rdr)?;
        Ok(match type_id {
            TypeId::BSTRING | TypeId::VARBINARY | TypeId::BINARY => HdbValue::BINARY(bytes),
            TypeId::GEOMETRY => HdbValue::GEOMETRY(bytes),
            TypeId::POINT => HdbValue::POINT(bytes),
            _ => return Err(util::io_error("unexpected type id for binary")),
        })
    }
}

async fn parse_binary_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    type_id: TypeId,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let l8 = rdr.read_u8().await?; // B1
    let is_null = l8 == length_indicator::LENGTH_INDICATOR_NULL;

    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error(
                "found NULL value for NOT NULL binary column",
            ))
        }
    } else {
        let bytes = parse_length_and_bytes_async(l8, rdr).await?;
        Ok(match type_id {
            TypeId::BSTRING | TypeId::VARBINARY | TypeId::BINARY => HdbValue::BINARY(bytes),
            TypeId::GEOMETRY => HdbValue::GEOMETRY(bytes),
            TypeId::POINT => HdbValue::POINT(bytes),
            _ => return Err(util::io_error("unexpected type id for binary")),
        })
    }
}

fn parse_length_and_bytes_sync(l8: u8, rdr: &mut dyn std::io::Read) -> std::io::Result<Vec<u8>> {
    let len = length_indicator::parse_sync(l8, rdr)?;
    util_sync::parse_bytes(len, rdr)
}

// FIXME Adapt
#[allow(clippy::cast_sign_loss)]
async fn parse_length_and_bytes_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    l8: u8,
    rdr: &mut R,
) -> std::io::Result<Vec<u8>> {
    let len = length_indicator::parse_async(l8, rdr).await?;
    util_async::parse_bytes(len, rdr).await
}

pub(crate) fn string_length<S: AsRef<str>>(s: S) -> usize {
    binary_length(util::cesu8_length(s.as_ref()))
}

pub(crate) fn binary_length(l: usize) -> usize {
    match l {
        l if l <= length_indicator::MAX_1_BYTE_LENGTH as usize => 1 + l,
        l if l <= length_indicator::MAX_2_BYTE_LENGTH as usize => 3 + l,
        l => 5 + l,
    }
}

pub(crate) fn emit_length_and_string_sync<S: AsRef<str>>(
    s: S,
    w: &mut dyn std::io::Write,
) -> std::io::Result<()> {
    emit_length_and_bytes_sync(&cesu8::to_cesu8(s.as_ref()), w)
}

pub(crate) async fn emit_length_and_string_async<
    S: AsRef<str>,
    W: std::marker::Unpin + tokio::io::AsyncWriteExt,
>(
    s: S,
    w: &mut W,
) -> std::io::Result<()> {
    emit_length_and_bytes_async(&cesu8::to_cesu8(s.as_ref()), w).await
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
fn emit_length_and_bytes_sync(v: &[u8], w: &mut dyn std::io::Write) -> std::io::Result<()> {
    length_indicator::emit_sync(v.len(), w)?;
    w.write_all(v)?;
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
async fn emit_length_and_bytes_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    v: &[u8],
    w: &mut W,
) -> std::io::Result<()> {
    length_indicator::emit_async(v.len(), w).await?;
    w.write_all(v).await?;
    Ok(())
}

impl<'a> std::fmt::Display for HdbValue<'a> {
    #[cfg_attr(tarpaulin, skip)]
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            HdbValue::NULL => write!(fmt, "<NULL>"),
            HdbValue::TINYINT(value) => write!(fmt, "{value}"),
            HdbValue::SMALLINT(value) => write!(fmt, "{value}"),
            HdbValue::INT(value) => write!(fmt, "{value}"),
            HdbValue::BIGINT(value) => write!(fmt, "{value}"),

            HdbValue::DECIMAL(ref value) => write!(fmt, "{value}"),

            HdbValue::REAL(value) => write!(fmt, "{value}"),
            HdbValue::DOUBLE(value) => write!(fmt, "{value}"),
            HdbValue::STR(value) => {
                if value.len() < 10_000 {
                    write!(fmt, "{value}")
                } else {
                    write!(fmt, "<STRING length = {}>", value.len())
                }
            }
            HdbValue::STRING(ref value) => {
                if value.len() < 10_000 {
                    write!(fmt, "{value}")
                } else {
                    write!(fmt, "<STRING length = {}>", value.len())
                }
            }
            HdbValue::BINARY(ref vec) => write!(fmt, "<BINARY length = {}>", vec.len()),

            HdbValue::CLOB(_) => write!(fmt, "<CLOB>"),
            HdbValue::NCLOB(_) => write!(fmt, "<NCLOB>"),
            HdbValue::BLOB(ref blob) => write!(fmt, "<BLOB length = {}>", blob.total_byte_length()),
            HdbValue::LOBSTREAM(_) => write!(fmt, "<LOBSTREAM>"),
            HdbValue::BOOLEAN(value) => write!(fmt, "{value}"),
            HdbValue::LONGDATE(ref value) => write!(fmt, "{value}"),
            HdbValue::SECONDDATE(ref value) => write!(fmt, "{value}"),
            HdbValue::DAYDATE(ref value) => write!(fmt, "{value}"),
            HdbValue::SECONDTIME(ref value) => write!(fmt, "{value}"),
            HdbValue::GEOMETRY(ref vec) => write!(fmt, "<GEOMETRY length = {}>", vec.len()),
            HdbValue::POINT(ref vec) => write!(fmt, "<POINT length = {}>", vec.len()),
            HdbValue::ARRAY(ref vec) => {
                write!(fmt, "[")?;
                for (val, i) in vec.iter().zip((0..vec.len()).rev()) {
                    std::fmt::Display::fmt(val, fmt)?;
                    if i > 0 {
                        write!(fmt, ", ")?;
                    }
                }
                write!(fmt, "]")
            }
        }
    }
}

impl<'a> std::fmt::Debug for HdbValue<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, fmt)
    }
}

// TODO implement more of these...
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

#[cfg(test)]
mod test {
    use crate::types::{DayDate, LongDate, SecondDate, SecondTime};
    use crate::HdbValue;
    use bigdecimal::BigDecimal;
    use num::bigint::BigInt;
    use num::FromPrimitive;

    #[test]
    fn test_display() {
        for value in vec![
            HdbValue::STRING("foo".to_string()),
            HdbValue::INT(42),
            HdbValue::NULL,
            HdbValue::TINYINT(42),
            HdbValue::SMALLINT(42),
            HdbValue::INT(42),
            HdbValue::BIGINT(42),
            HdbValue::DECIMAL(BigDecimal::new(BigInt::from_i64(42_i64).unwrap(), 42_i64)),
            HdbValue::REAL(42_f32),
            HdbValue::DOUBLE(42_f64),
            HdbValue::STR("foo bar"),
            HdbValue::STRING("foo bar".to_string()),
            HdbValue::BINARY(vec![42, 42, 42]),
            // HdbValue::CLOB(_),
            // HdbValue::NCLOB(_),
            // HdbValue::BLOB(_),
            // HdbValue::LOBSTREAM(_),
            HdbValue::BOOLEAN(true),
            HdbValue::LONGDATE(LongDate::new(100_i64)),
            HdbValue::SECONDDATE(SecondDate::new(100_i64)),
            HdbValue::DAYDATE(DayDate::new(100_i32)),
            HdbValue::SECONDTIME(SecondTime::new(100_i32)),
            // HdbValue::GEOMETRY(ref vec),
            // HdbValue::POINT(ref vec),
        ] {
            let _s = value.to_string();
        }
    }
}
