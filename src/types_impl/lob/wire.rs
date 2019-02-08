use crate::conn_core::AmConnCore;
use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::type_id::TypeId;
use crate::protocol::util;
use crate::types_impl::lob::blob::new_blob_from_db;
use crate::types_impl::lob::clob::new_clob_from_db;
use crate::types_impl::lob::nclob::new_nclob_from_db;
use crate::{HdbError, HdbResult};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

pub(crate) fn parse_blob(
    am_conn_core: &AmConnCore,
    nullable: bool,
    rdr: &mut io::BufRead,
) -> HdbResult<HdbValue> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found null value for not-null BLOB column".to_owned(),
            ))
        }
    } else {
        let (_, length_b, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
        Ok(HdbValue::BLOB(new_blob_from_db(
            am_conn_core,
            is_last_data,
            length_b,
            locator_id,
            data,
        )))
    }
}

pub(crate) fn parse_clob(
    am_conn_core: &AmConnCore,
    nullable: bool,
    rdr: &mut io::BufRead,
) -> HdbResult<HdbValue> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found null value for not-null CLOB column".to_owned(),
            ))
        }
    } else {
        let (length_c, length_b, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
        Ok(HdbValue::CLOB(new_clob_from_db(
            am_conn_core,
            is_last_data,
            length_c,
            length_b,
            locator_id,
            data,
        )))
    }
}

pub(crate) fn parse_nclob(
    am_conn_core: &AmConnCore,
    nullable: bool,
    type_id: TypeId,
    rdr: &mut io::BufRead,
) -> HdbResult<HdbValue> {
    let (is_null, is_data_included, is_last_data) = parse_lob_1(rdr)?;
    if is_null {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found null value for not-null NCLOB column".to_owned(),
            ))
        }
    } else {
        let (length_c, length_b, locator_id, data) = parse_lob_2(rdr, is_data_included)?;
        let nclob = new_nclob_from_db(
            am_conn_core,
            is_last_data,
            length_c,
            length_b,
            locator_id,
            data,
        );
        Ok(match type_id {
            TypeId::TEXT | TypeId::NCLOB => HdbValue::NCLOB(nclob),
            _ => return Err(HdbError::Impl("unexpected type id for nclob".to_owned())),
        })
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

pub(crate) fn emit_blob_header(
    v_len: usize,
    data_pos: &mut i32,
    w: &mut io::Write,
) -> HdbResult<()> {
    // bit 0: not used; bit 1: data is included; bit 2: no more data remaining
    w.write_u8(0b_110_u8)?; // I1           Bit set for options
    w.write_i32::<LittleEndian>(v_len as i32)?; // I4           LENGTH OF VALUE
    w.write_i32::<LittleEndian>(*data_pos as i32)?; // I4           position
    *data_pos += v_len as i32;
    Ok(())
}

pub(crate) fn emit_clob_header(
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

pub(crate) fn emit_nclob_header(
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
