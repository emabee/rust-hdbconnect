use crate::conn_core::AmConnCore;
use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::resultset::AmRsCore;
use crate::protocol::parts::type_id::TypeId;
use crate::protocol::util;
use crate::types_impl::lob::{BLob, CLob, NCLob};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub(crate) fn parse_blob(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    rdr: &mut dyn std::io::BufRead,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null BLOB column"))
        }
    } else {
        let (_, length, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
        Ok(HdbValue::BLOB(BLob::new(
            am_conn_core,
            o_am_rscore,
            is_last_data,
            length,
            locator_id,
            data,
        )))
    }
}

pub(crate) fn parse_clob(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    rdr: &mut dyn std::io::BufRead,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null CLOB column"))
        }
    } else {
        let (char_length, byte_length, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
        Ok(HdbValue::CLOB(CLob::new(
            am_conn_core,
            o_am_rscore,
            is_last_data,
            char_length,
            byte_length,
            locator_id,
            data,
        )))
    }
}

pub(crate) fn parse_nclob(
    am_conn_core: &AmConnCore,
    o_am_rscore: &Option<AmRsCore>,
    nullable: bool,
    type_id: TypeId,
    rdr: &mut dyn std::io::BufRead,
) -> std::io::Result<HdbValue<'static>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error("found null value for not-null NCLOB column"))
        }
    } else {
        let (char_length, byte_length, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
        Ok(match type_id {
            TypeId::TEXT | TypeId::NCLOB => HdbValue::NCLOB(NCLob::new(
                am_conn_core,
                o_am_rscore,
                is_last_data,
                char_length,
                byte_length,
                locator_id,
                data,
            )),
            _ => return Err(util::io_error("unexpected type id for nclob")),
        })
    }
}

fn parse_lob_1(rdr: &mut dyn std::io::BufRead) -> std::io::Result<(bool, bool, bool)> {
    let _data_type = rdr.read_u8()?; // I1
    let options = rdr.read_u8()?; // I1
    let is_null = (options & 0b_1_u8) != 0;
    let is_data_included = (options & 0b_10_u8) != 0;
    let is_last_data = (options & 0b_100_u8) != 0;
    Ok((is_null, is_data_included, is_last_data))
}

fn parse_lob_2(
    rdr: &mut dyn std::io::BufRead,
    is_data_included: bool,
) -> std::io::Result<(u64, u64, u64, Vec<u8>)> {
    util::skip_bytes(2, rdr)?; // U2 (filler)
    let total_char_length = rdr.read_u64::<LittleEndian>()?; // I8
    let total_byte_length = rdr.read_u64::<LittleEndian>()?; // I8
    let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
    let chunk_length = rdr.read_u32::<LittleEndian>()?; // I4

    if is_data_included {
        let data = util::parse_bytes(chunk_length as usize, rdr)?; // B[chunk_length]
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

#[allow(clippy::cast_possible_truncation)]
pub(crate) fn emit_lob_header(
    length: u64,
    offset: &mut i32,
    w: &mut dyn std::io::Write,
) -> std::io::Result<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_000_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(length as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*offset as i32)?; // I4           position
    *offset += length as i32;
    Ok(())
}
