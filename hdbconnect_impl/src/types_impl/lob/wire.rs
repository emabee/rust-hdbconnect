use crate::{
    conn::AmConnCore,
    protocol::{
        parts::{am_rs_core::AmRsCore, TypeId},
        util,
    },
    HdbValue,
};

#[cfg(feature = "async")]
use crate::protocol::util_async;
#[cfg(feature = "sync")]
use crate::protocol::util_sync;
#[cfg(feature = "sync")]
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[cfg(feature = "sync")]
pub(crate) fn parse_blob_sync(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1_sync(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null BLOB column"))
        }
    } else {
        let (_, length, locator_id, data) = parse_lob_2_sync(rdr, is_data_included)?;
        Ok(HdbValue::SYNC_BLOB(crate::sync::BLob::new(
            am_conn_core,
            o_am_rscore,
            is_last_data,
            length,
            locator_id,
            data,
        )))
    }
}

#[cfg(feature = "async")]
pub(crate) async fn parse_blob_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1_async(rdr).await?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null BLOB column"))
        }
    } else {
        let (_, length, locator_id, data) = parse_lob_2_async(rdr, is_data_included).await?;
        Ok(HdbValue::ASYNC_BLOB(crate::a_sync::BLob::new(
            am_conn_core,
            o_am_rscore,
            is_last_data,
            length,
            locator_id,
            data,
        )))
    }
}

#[cfg(feature = "sync")]
pub(crate) fn parse_clob_sync(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1_sync(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null CLOB column"))
        }
    } else {
        let (char_length, byte_length, locator_id, data) = parse_lob_2_sync(rdr, is_data_included)?;
        Ok(HdbValue::SYNC_CLOB(
            crate::sync::CLob::new(
                am_conn_core,
                o_am_rscore,
                is_last_data,
                char_length,
                byte_length,
                locator_id,
                data,
            )
            .map_err(|e| util::io_error(e.to_string()))?,
        ))
    }
}

#[cfg(feature = "async")]
pub(crate) async fn parse_clob_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1_async(rdr).await?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null CLOB column"))
        }
    } else {
        let (char_length, byte_length, locator_id, data) =
            parse_lob_2_async(rdr, is_data_included).await?;
        Ok(HdbValue::ASYNC_CLOB(
            crate::a_sync::CLob::new(
                am_conn_core,
                o_am_rscore,
                is_last_data,
                char_length,
                byte_length,
                locator_id,
                data,
            )
            .map_err(|e| util::io_error(e.to_string()))?,
        ))
    }
}

#[cfg(feature = "sync")]
pub(crate) fn parse_nclob_sync(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    type_id: TypeId,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1_sync(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null NCLOB column"))
        }
    } else {
        let (char_length, byte_length, locator_id, data) = parse_lob_2_sync(rdr, is_data_included)?;
        Ok(match type_id {
            TypeId::TEXT | TypeId::NCLOB => HdbValue::SYNC_NCLOB(
                crate::sync::NCLob::new(
                    am_conn_core,
                    o_am_rscore,
                    is_last_data,
                    char_length,
                    byte_length,
                    locator_id,
                    data,
                )
                .map_err(|e| util::io_error(e.to_string()))?,
            ),
            _ => return Err(util::io_error("unexpected type id for nclob")),
        })
    }
}

#[cfg(feature = "async")]
pub(crate) async fn parse_nclob_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    type_id: TypeId,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1_async(rdr).await?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null NCLOB column"))
        }
    } else {
        let (char_length, byte_length, locator_id, data) =
            parse_lob_2_async(rdr, is_data_included).await?;
        Ok(match type_id {
            TypeId::TEXT | TypeId::NCLOB => HdbValue::ASYNC_NCLOB(
                crate::a_sync::NCLob::new(
                    am_conn_core,
                    o_am_rscore,
                    is_last_data,
                    char_length,
                    byte_length,
                    locator_id,
                    data,
                )
                .map_err(|e| util::io_error(e.to_string()))?,
            ),
            _ => return Err(util::io_error("unexpected type id for nclob")),
        })
    }
}

#[cfg(feature = "sync")]
fn parse_lob_1_sync(rdr: &mut dyn std::io::Read) -> std::io::Result<(bool, bool, bool)> {
    let _data_type = rdr.read_u8()?; // I1
    let options = rdr.read_u8()?; // I1
    let is_null = (options & 0b1_u8) != 0;
    let is_data_included = (options & 0b_10_u8) != 0;
    let is_last_data = (options & 0b100_u8) != 0;
    Ok((is_null, is_data_included, is_last_data))
}

#[cfg(feature = "async")]
async fn parse_lob_1_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
) -> std::io::Result<(bool, bool, bool)> {
    let _data_type = rdr.read_u8().await?; // I1
    let options = rdr.read_u8().await?; // I1
    let is_null = (options & 0b1_u8) != 0;
    let is_data_included = (options & 0b10_u8) != 0;
    let is_last_data = (options & 0b100_u8) != 0;
    Ok((is_null, is_data_included, is_last_data))
}

#[cfg(feature = "sync")]
fn parse_lob_2_sync(
    rdr: &mut dyn std::io::Read,
    is_data_included: bool,
) -> std::io::Result<(u64, u64, u64, Vec<u8>)> {
    util_sync::skip_bytes(2, rdr)?; // U2 (filler)
    let total_char_length = rdr.read_u64::<LittleEndian>()?; // I8
    let total_byte_length = rdr.read_u64::<LittleEndian>()?; // I8
    let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
    let chunk_length = rdr.read_u32::<LittleEndian>()?; // I4

    if is_data_included {
        let data = util_sync::parse_bytes(chunk_length as usize, rdr)?; // B[chunk_length]
        Ok((total_char_length, total_byte_length, locator_id, data))
    } else {
        Ok((
            total_char_length,
            total_byte_length,
            locator_id,
            Vec::<u8>::new(),
        ))
    }
}

#[cfg(feature = "async")]
async fn parse_lob_2_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    rdr: &mut R,
    is_data_included: bool,
) -> std::io::Result<(u64, u64, u64, Vec<u8>)> {
    util_async::skip_bytes(2, rdr).await?; // U2 (filler)
    let total_char_length = util_async::read_u64(rdr).await?; // I8
    let total_byte_length = util_async::read_u64(rdr).await?; // I8
    let locator_id = util_async::read_u64(rdr).await?; // I8
    let chunk_length = util_async::read_u32(rdr).await?; // I4

    if is_data_included {
        let data = util_async::parse_bytes(chunk_length as usize, rdr).await?; // B[chunk_length]
        Ok((total_char_length, total_byte_length, locator_id, data))
    } else {
        Ok((
            total_char_length,
            total_byte_length,
            locator_id,
            Vec::<u8>::new(),
        ))
    }
}

#[cfg(feature = "sync")]
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn emit_lob_header_sync(
    length: u64,
    offset: &mut i32,
    w: &mut dyn std::io::Write,
) -> std::io::Result<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b000_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(length as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*offset)?; // I4           position
    *offset += length as i32;
    Ok(())
}

#[cfg(feature = "async")]
#[allow(clippy::cast_possible_truncation)]
pub(crate) async fn emit_lob_header_async<W: std::marker::Unpin + tokio::io::AsyncWriteExt>(
    length: u64,
    offset: &mut i32,
    w: &mut W,
) -> std::io::Result<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b000_u8).await?; // I1           Bit set for options
    w.write_i32_le(length as i32).await?; // I4           LENGTH OF VALUE
    w.write_i32_le(*offset).await?; // I4           position
    *offset += length as i32;
    Ok(())
}
