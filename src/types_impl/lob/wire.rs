use conn_core::AmConnCore;
use protocol::util;
use types_impl::lob::blob::{new_blob_from_db, BLob};
use types_impl::lob::clob::{new_clob_from_db, CLob};
use types_impl::lob::nclob::{new_nclob_from_db, NCLob};
use {HdbError, HdbResult};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

pub fn parse_blob(am_conn_core: &AmConnCore, rdr: &mut io::BufRead) -> HdbResult<BLob> {
    match parse_nullable_blob(am_conn_core, rdr)? {
        Some(blob) => Ok(blob),
        None => Err(HdbError::Impl(
            "Null value found for non-null blob column".to_owned(),
        )),
    }
}

pub fn parse_nullable_blob(
    am_conn_core: &AmConnCore,
    rdr: &mut io::BufRead,
) -> HdbResult<Option<BLob>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        Ok(None)
    } else {
        let (_, length_b, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
        Ok(Some(new_blob_from_db(
            am_conn_core,
            is_last_data,
            length_b,
            locator_id,
            data,
        )))
    }
}

pub fn parse_clob(am_conn_core: &AmConnCore, rdr: &mut io::BufRead) -> HdbResult<CLob> {
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
) -> HdbResult<Option<CLob>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        Ok(None)
    } else {
        let (length_c, length_b, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
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

pub fn parse_nclob(am_conn_core: &AmConnCore, rdr: &mut io::BufRead) -> HdbResult<NCLob> {
    match parse_nullable_nclob(am_conn_core, rdr)? {
        Some(nclob) => Ok(nclob),
        None => Err(HdbError::Impl(
            "Null value found for non-null nclob column".to_owned(),
        )),
    }
}

pub fn parse_nullable_nclob(
    am_conn_core: &AmConnCore,
    rdr: &mut io::BufRead,
) -> HdbResult<Option<NCLob>> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        Ok(None)
    } else {
        let (length_c, length_b, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
        Ok(Some(new_nclob_from_db(
            am_conn_core,
            is_last_data,
            length_c,
            length_b,
            locator_id,
            &data,
        )))
    }
}

fn parse_lob_1(rdr: &mut io::BufRead) -> HdbResult<(bool, bool, bool)> {
    let _data_type = rdr.read_u8()?; // I1
    let options = rdr.read_u8()?; // I1
    let is_null = (options & 0b_1_u8) != 0;
    let is_data_included = (options & 0b_10_u8) != 0;
    let is_last_data = (options & 0b_100_u8) != 0;
    Ok((is_null, is_data_included, is_last_data))
}

fn parse_lob_2(
    rdr: &mut io::BufRead,
    is_data_included: bool,
) -> HdbResult<(u64, u64, u64, Vec<u8>)> {
    util::skip_bytes(2, rdr)?; // U2 (filler)
    let length_c = rdr.read_u64::<LittleEndian>()?; // I8
    let length_b = rdr.read_u64::<LittleEndian>()?; // I8
    let locator_id = rdr.read_u64::<LittleEndian>()?; // I8
    let chunk_length = rdr.read_u32::<LittleEndian>()?; // I4

    if is_data_included {
        let data = util::parse_bytes(chunk_length as usize, rdr)?; // B[chunk_length]
        Ok((length_c, length_b, locator_id, data))
    } else {
        Ok((length_c, length_b, locator_id, Vec::<u8>::new()))
    }
}

pub fn serialize_blob_header(v_len: usize, data_pos: &mut i32, w: &mut io::Write) -> HdbResult<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_110_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(v_len as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*data_pos as i32)?; // I4           position
    *data_pos += v_len as i32;
    Ok(())
}

pub fn serialize_clob_header(s_len: usize, data_pos: &mut i32, w: &mut io::Write) -> HdbResult<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_110_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(s_len as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*data_pos as i32)?; // I4           position
    *data_pos += s_len as i32;
    Ok(())
}

pub fn serialize_nclob_header(
    s_len: usize,
    data_pos: &mut i32,
    w: &mut io::Write,
) -> HdbResult<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_110_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(s_len as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*data_pos as i32)?; // I4           position
    *data_pos += s_len as i32;
    Ok(())
}
