use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::type_id::TypeId;
use crate::types_impl::hdb_decimal::HdbDecimal;
use crate::{HdbError, HdbResult};
use bigdecimal::BigDecimal;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use num::bigint::BigInt;
use num::{FromPrimitive, ToPrimitive};

pub fn parse_decimal(
    nullable: bool,
    type_id: TypeId,
    scale: i16,
    rdr: &mut dyn std::io::BufRead,
) -> HdbResult<HdbValue<'static>> {
    match type_id {
        TypeId::DECIMAL => HdbDecimal::parse_hdb_decimal(nullable, scale, rdr),

        TypeId::FIXED8 => Ok(if parse_null(nullable, rdr)? {
            HdbValue::NULL
        } else {
            trace!("parse FIXED8");
            let i = rdr.read_i64::<LittleEndian>()?;
            let bigint = BigInt::from_i64(i)
                .ok_or_else(|| HdbError::Impl("invalid value of type FIXED8".to_owned()))?;
            let bd = BigDecimal::new(bigint, i64::from(scale));
            HdbValue::DECIMAL(bd)
        }),

        TypeId::FIXED12 => Ok(if parse_null(nullable, rdr)? {
            HdbValue::NULL
        } else {
            trace!("parse FIXED12");
            let bytes = crate::protocol::util::parse_bytes(12, rdr)?;
            let bigint = BigInt::from_signed_bytes_le(&bytes);
            let bd = BigDecimal::new(bigint, i64::from(scale));
            HdbValue::DECIMAL(bd)
        }),

        TypeId::FIXED16 => Ok(if parse_null(nullable, rdr)? {
            HdbValue::NULL
        } else {
            trace!("parse FIXED16");
            let i = rdr.read_i128::<LittleEndian>()?;
            let bi = BigInt::from_i128(i)
                .ok_or_else(|| HdbError::Impl("invalid value of type FIXED16".to_owned()))?;
            let bd = BigDecimal::new(bi, i64::from(scale));
            HdbValue::DECIMAL(bd)
        }),
        _ => Err(HdbError::Impl("unexpected type id for decimal".to_owned())),
    }
}

fn parse_null(nullable: bool, rdr: &mut dyn std::io::BufRead) -> HdbResult<bool> {
    let is_null = rdr.read_u8()? == 0;
    if is_null && !nullable {
        Err(HdbError::Impl(
            "found null value for not-null column".to_owned(),
        ))
    } else {
        Ok(is_null)
    }
}

pub(crate) fn emit_decimal(
    bd: &BigDecimal,
    type_id: TypeId,
    scale: i16,
    w: &mut dyn std::io::Write,
) -> HdbResult<()> {
    match type_id {
        TypeId::DECIMAL => {
            trace!("emit DECIMAL");
            let hdb_decimal = HdbDecimal::from_bigdecimal(bd)?;
            w.write_all(&hdb_decimal.into_raw())?;
        }
        TypeId::FIXED8 => {
            trace!("emit FIXED8");
            let bd = bd.with_scale(i64::from(scale));
            let (bigint, _exponent) = bd.as_bigint_and_exponent();
            w.write_i64::<LittleEndian>(bigint.to_i64().unwrap())?;
        }
        TypeId::FIXED12 => {
            trace!("emit FIXED12");
            // if we get less than 12 bytes, we need to append bytes with either value
            // 0_u8 or 255_u8, depending on the value of the highest bit of the last byte.
            let bd = bd.with_scale(i64::from(scale));
            let (bigint, _exponent) = bd.as_bigint_and_exponent();
            let mut bytes = bigint.to_signed_bytes_le();
            let l = bytes.len();
            if l < 12 {
                let filler = if bytes[l - 1] & 0b_1000_0000_u8 == 0 {
                    0_u8
                } else {
                    255_u8
                };
                bytes.reserve(12 - l);
                for _ in l..12 {
                    bytes.push(filler);
                }
            }
            w.write_all(&bytes)?;
        }
        TypeId::FIXED16 => {
            trace!("emit FIXED16");
            let bd = bd.with_scale(i64::from(scale));
            let (bigint, _exponent) = bd.as_bigint_and_exponent();
            w.write_i128::<LittleEndian>(bigint.to_i128().unwrap())?;
        }
        _ => return Err(HdbError::Impl("unexpected type id for decimal".to_owned())),
    }
    Ok(())
}
