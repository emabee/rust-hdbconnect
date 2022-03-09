use crate::protocol::util::io_error;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};

pub(crate) const MAX_1_BYTE_LENGTH: u8 = 245;
pub(crate) const MAX_2_BYTE_LENGTH: i16 = i16::max_value();
const LENGTH_INDICATOR_2BYTE: u8 = 246;
const LENGTH_INDICATOR_4BYTE: u8 = 247;
pub(crate) const LENGTH_INDICATOR_NULL: u8 = 255;

#[allow(clippy::cast_possible_truncation)]
pub(crate) fn emit_sync(l: usize, w: &mut dyn std::io::Write) -> std::io::Result<()> {
    match l {
        l if l <= MAX_1_BYTE_LENGTH as usize => w.write_u8(l as u8)?,
        l if l <= 0xFFFF => {
            w.write_u8(LENGTH_INDICATOR_2BYTE)?;
            w.write_u16::<LittleEndian>(l as u16)?;
        }
        l if l <= 0xFFFF_FFFF => {
            w.write_u8(LENGTH_INDICATOR_4BYTE)?;
            w.write_u32::<LittleEndian>(l as u32)?;
        }
        l => {
            return Err(io_error(format!("Value too big: {l}")));
        }
    }
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
pub(crate) async fn emit_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    l: usize,
    w: &mut W,
) -> std::io::Result<()> {
    match l {
        l if l <= MAX_1_BYTE_LENGTH as usize => w.write_u8(l as u8).await?,
        l if l <= 0xFFFF => {
            w.write_u8(LENGTH_INDICATOR_2BYTE).await?;
            w.write_u16_le(l as u16).await?;
        }
        l if l <= 0xFFFF_FFFF => {
            w.write_u8(LENGTH_INDICATOR_4BYTE).await?;
            w.write_u32_le(l as u32).await?;
        }
        l => {
            return Err(io_error(format!("Value too big: {l}")));
        }
    }
    Ok(())
}

// #[allow(clippy::cast_sign_loss)]
pub(crate) fn parse_sync(l8: u8, rdr: &mut dyn std::io::Read) -> std::io::Result<usize> {
    match l8 {
        0..=MAX_1_BYTE_LENGTH => Ok(l8 as usize),
        LENGTH_INDICATOR_2BYTE => Ok(rdr.read_u16::<LittleEndian>()? as usize),
        LENGTH_INDICATOR_4BYTE => Ok(rdr.read_u32::<LittleEndian>()? as usize),
        LENGTH_INDICATOR_NULL => Ok(rdr.read_u16::<BigEndian>()? as usize),
        _ => Err(io_error(format!(
            "Unknown length indicator for AuthField: {}",
            l8
        ))),
    }
}

// #[allow(clippy::cast_sign_loss)]
pub(crate) async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    l8: u8,
    rdr: &mut R,
) -> std::io::Result<usize> {
    match l8 {
        0..=MAX_1_BYTE_LENGTH => Ok(l8 as usize),
        LENGTH_INDICATOR_2BYTE => Ok(rdr.read_u16_le().await? as usize),
        LENGTH_INDICATOR_4BYTE => Ok(rdr.read_u32_le().await? as usize),
        LENGTH_INDICATOR_NULL => Ok(rdr.read_u16().await? as usize),
        _ => Err(io_error(format!(
            "Unknown length indicator for AuthField: {}",
            l8
        ))),
    }
}
