use crate::protocol::{util, util_async, util_sync};
use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[allow(clippy::upper_case_acronyms, non_camel_case_types)]
#[derive(Clone, Debug, PartialEq)]
pub enum OptionValue {
    INT(i32),         // INTEGER
    BIGINT(i64),      // BIGINT
    DOUBLE(f64),      // DOUBLE
    BOOLEAN(bool),    // Boolean
    STRING(String),   // Character string
    BSTRING(Vec<u8>), // Binary string
}

impl OptionValue {
    pub fn get_int(&self) -> HdbResult<i32> {
        if let Self::INT(i) = self {
            Ok(*i)
        } else {
            Err(HdbError::Impl("Not a INT-typed OptionValue"))
        }
    }

    // pub fn get_bigint(&self) -> HdbResult<i64> {
    //     if let Self::BIGINT(i) = self {
    //         Ok(*i)
    //     } else {
    //         Err(HdbError::Impl("Not a BIGINT-typed OptionValue"))
    //     }
    // }

    // pub fn get_double(&self) -> HdbResult<f64> {
    //     if let Self::DOUBLE(d) = self {
    //         Ok(*d)
    //     } else {
    //         Err(HdbError::Impl("Not a DOUBLE-typed OptionValue"))
    //     }
    // }

    pub fn get_bool(&self) -> HdbResult<bool> {
        if let Self::BOOLEAN(b) = self {
            Ok(*b)
        } else {
            Err(HdbError::Impl("Not a BOOLEAN-typed OptionValue"))
        }
    }

    pub fn get_string(&self) -> HdbResult<&String> {
        if let Self::STRING(ref s) = self {
            Ok(s)
        } else {
            Err(HdbError::Impl("Not a STRING-typed OptionValue"))
        }
    }

    // pub fn get_bstring(&self) -> HdbResult<&Vec<u8>> {
    //     if let Self::BSTRING(ref s) = self {
    //         Ok(s)
    //     } else {
    //         Err(HdbError::Impl("Not a BSTRING-typed OptionValue"))
    //     }
    // }

    pub fn emit_sync(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        w.write_u8(self.type_id())?; // I1
        match *self {
            // variable
            Self::INT(i) => w.write_i32::<LittleEndian>(i)?,
            Self::BIGINT(i) => w.write_i64::<LittleEndian>(i)?,
            Self::DOUBLE(f) => w.write_f64::<LittleEndian>(f)?,
            Self::BOOLEAN(b) => w.write_u8(b.into())?,
            Self::STRING(ref s) => emit_length_and_string_sync(s, w)?,
            Self::BSTRING(ref v) => emit_length_and_bytes_sync(v, w)?,
        }
        Ok(())
    }

    pub async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
        &self,
        w: &mut W,
    ) -> std::io::Result<()> {
        w.write_u8(self.type_id()).await?; // I1
        match *self {
            // variable
            Self::INT(i) => w.write_all(&i.to_le_bytes()).await?,
            Self::BIGINT(i) => w.write_all(&i.to_le_bytes()).await?,
            Self::DOUBLE(f) => w.write_all(&f.to_le_bytes()).await?,
            Self::BOOLEAN(b) => w.write_u8(if b { 1 } else { 0 }).await?,
            Self::STRING(ref s) => async_emit_length_and_string(s, w).await?,
            Self::BSTRING(ref v) => async_emit_length_and_bytes(v, w).await?,
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        1 + match *self {
            Self::INT(_) => 4,
            Self::BIGINT(_) | Self::DOUBLE(_) => 8,
            Self::BOOLEAN(_) => 1,
            Self::STRING(ref s) => util::cesu8_length(s) + 2,
            Self::BSTRING(ref v) => v.len() + 2,
        }
    }

    pub fn type_id(&self) -> u8 {
        match *self {
            Self::INT(_) => 3,
            Self::BIGINT(_) => 4,
            Self::DOUBLE(_) => 7,
            Self::BOOLEAN(_) => 28,
            Self::STRING(_) => 29,
            Self::BSTRING(_) => 33,
        }
    }

    pub fn parse_sync(rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        let value_type = rdr.read_u8()?; // U1
        Self::parse_value_sync(value_type, rdr)
    }

    pub async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        rdr: &mut R,
    ) -> std::io::Result<Self> {
        let value_type = rdr.read_u8().await?; // U1
        Self::parse_value_async(value_type, rdr).await
    }

    fn parse_value_sync(typecode: u8, rdr: &mut dyn std::io::Read) -> std::io::Result<Self> {
        match typecode {
            3 => Ok(Self::INT(rdr.read_i32::<LittleEndian>()?)), // I4
            4 => Ok(Self::BIGINT(rdr.read_i64::<LittleEndian>()?)), // I8
            7 => Ok(Self::DOUBLE(rdr.read_f64::<LittleEndian>()?)), // F8
            28 => Ok(Self::BOOLEAN(rdr.read_u8()? > 0)),         // B1
            29 => Ok(Self::STRING(parse_length_and_string_sync(rdr)?)),
            33 => Ok(Self::BSTRING(parse_length_and_binary_sync(rdr)?)),
            _ => Err(util::io_error(format!(
                "OptionValue::parse_value() not implemented for type code {}",
                typecode
            ))),
        }
    }

    async fn parse_value_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        typecode: u8,
        rdr: &mut R,
    ) -> std::io::Result<Self> {
        match typecode {
            3 => Ok(Self::INT(util_async::read_i32(rdr).await?)), // I4
            4 => Ok(Self::BIGINT(util_async::read_i64(rdr).await?)), // I8
            7 => Ok(Self::DOUBLE(util_async::read_f64(rdr).await?)), // F8
            28 => Ok(Self::BOOLEAN(rdr.read_u8().await? > 0)),    // B1
            29 => Ok(Self::STRING(parse_length_and_string_async(rdr).await?)),
            33 => Ok(Self::BSTRING(parse_length_and_binary_async(rdr).await?)),
            _ => Err(util::io_error(format!(
                "OptionValue::parse_value() not implemented for type code {}",
                typecode
            ))),
        }
    }
}

fn emit_length_and_string_sync(s: &str, w: &mut dyn std::io::Write) -> std::io::Result<()> {
    emit_length_and_bytes_sync(&cesu8::to_cesu8(s), w)
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
fn emit_length_and_bytes_sync(v: &[u8], w: &mut dyn std::io::Write) -> std::io::Result<()> {
    w.write_i16::<LittleEndian>(v.len() as i16)?; // I2: length of value
    w.write_all(v)?; // B (varying)
    Ok(())
}

async fn async_emit_length_and_string<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    s: &str,
    w: &mut W,
) -> std::io::Result<()> {
    async_emit_length_and_bytes(&cesu8::to_cesu8(s), w).await
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
async fn async_emit_length_and_bytes<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    v: &[u8],
    w: &mut W,
) -> std::io::Result<()> {
    w.write_all(&(v.len() as i16).to_le_bytes()).await?; // I2: length of value
    w.write_all(v).await?; // B (varying)
    Ok(())
}

fn parse_length_and_string_sync(rdr: &mut dyn std::io::Read) -> std::io::Result<String> {
    util::string_from_cesu8(parse_length_and_binary_sync(rdr)?).map_err(util::io_error)
}

async fn parse_length_and_string_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<String> {
    util::string_from_cesu8(parse_length_and_binary_async(rdr).await?).map_err(util::io_error)
}

#[allow(clippy::clippy::cast_sign_loss)]
fn parse_length_and_binary_sync(rdr: &mut dyn std::io::Read) -> std::io::Result<Vec<u8>> {
    let len = rdr.read_i16::<LittleEndian>()? as usize; // I2: length of value
    util_sync::parse_bytes(len, rdr) // B (varying)
}

#[allow(clippy::clippy::cast_sign_loss)]
async fn parse_length_and_binary_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<Vec<u8>> {
    let len = util_async::read_i16(rdr).await? as usize; // I2: length of value
    util_async::parse_bytes(len, rdr).await // B (varying)
}

impl std::fmt::Display for OptionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Self::INT(v) => write!(f, "{v}"),
            Self::BIGINT(v) => write!(f, "{v}"),
            Self::DOUBLE(v) => write!(f, "{v}"),
            Self::BOOLEAN(v) => write!(f, "{v}"),
            Self::STRING(v) => write!(f, "{v}"),
            Self::BSTRING(v) => write!(f, "{v:?}"),
        }
    }
}
