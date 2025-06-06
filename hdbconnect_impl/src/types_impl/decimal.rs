use crate::{
    HdbResult, HdbValue, TypeId, impl_err,
    types_impl::wire_decimal::{big_decimal_to_wire_decimal, wire_decimal_to_hdbvalue},
};
use bigdecimal::BigDecimal;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use num::{FromPrimitive, ToPrimitive};
use num_bigint::BigInt;

pub fn parse(
    nullable: bool,
    type_id: TypeId,
    scale: i16,
    rdr: &mut dyn std::io::Read,
) -> HdbResult<HdbValue<'static>> {
    match type_id {
        TypeId::DECIMAL => {
            trace!("parse DECIMAL");
            let mut raw = [0_u8; 16];
            rdr.read_exact(&mut raw[..])?;
            wire_decimal_to_hdbvalue(raw, nullable, scale)
        }

        TypeId::FIXED8 => Ok({
            trace!("parse FIXED8");
            if parse_null(nullable, rdr)? {
                HdbValue::NULL
            } else {
                let i = rdr.read_i64::<LittleEndian>()?;
                let bigint =
                    BigInt::from_i64(i).ok_or_else(|| impl_err!("invalid value of type FIXED8"))?;
                let bd = BigDecimal::new(bigint, i64::from(scale));
                HdbValue::DECIMAL(bd)
            }
        }),

        TypeId::FIXED12 => Ok({
            trace!("parse FIXED12");
            if parse_null(nullable, rdr)? {
                HdbValue::NULL
            } else {
                let bytes = crate::protocol::util_sync::parse_bytes(12, rdr)?;
                let bigint = BigInt::from_signed_bytes_le(&bytes);
                let bd = BigDecimal::new(bigint, i64::from(scale));
                HdbValue::DECIMAL(bd)
            }
        }),

        TypeId::FIXED16 => Ok({
            trace!("parse FIXED16");
            if parse_null(nullable, rdr)? {
                HdbValue::NULL
            } else {
                let i = rdr.read_i128::<LittleEndian>()?;
                let bi = BigInt::from_i128(i)
                    .ok_or_else(|| impl_err!("invalid value of type FIXED16"))?;
                let bd = BigDecimal::new(bi, i64::from(scale));
                HdbValue::DECIMAL(bd)
            }
        }),
        _ => Err(impl_err!("unexpected type id for decimal")),
    }
}

fn parse_null(nullable: bool, rdr: &mut dyn std::io::Read) -> HdbResult<bool> {
    let is_null = rdr.read_u8()? == 0;
    if is_null && !nullable {
        Err(impl_err!("found null value for not-null column"))
    } else {
        Ok(is_null)
    }
}

pub(crate) fn emit(
    big_decimal: &BigDecimal,
    type_id: TypeId,
    scale: i16,
    w: &mut dyn std::io::Write,
) -> HdbResult<()> {
    match type_id {
        TypeId::DECIMAL => {
            trace!("emit DECIMAL");
            let buffer = big_decimal_to_wire_decimal(big_decimal).map_err(|e| impl_err!("{e}"))?;
            w.write_all(&buffer)?;
        }
        TypeId::FIXED8 => {
            trace!("emit FIXED8");
            let bd = big_decimal.with_scale(i64::from(scale));
            let (bigint, _exponent) = bd.as_bigint_and_exponent();
            w.write_i64::<LittleEndian>(
                bigint
                    .to_i64()
                    .ok_or_else(|| impl_err!("conversion to FIXED8 fails"))?,
            )?;
        }
        TypeId::FIXED12 => {
            trace!("emit FIXED12");
            // if we get less than 12 bytes, we need to append bytes with either value
            // 0_u8 or 255_u8, depending on the value of the highest bit of the last byte.
            let bd = big_decimal.with_scale(i64::from(scale));
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
            let bd = big_decimal.with_scale(i64::from(scale));
            let (bigint, _exponent) = bd.as_bigint_and_exponent();
            w.write_i128::<LittleEndian>(
                bigint
                    .to_i128()
                    .ok_or_else(|| impl_err!("conversion to FIXED16 fails"))?,
            )?;
        }
        _ => return Err(impl_err!("unexpected type id for decimal")),
    }
    Ok(())
}
