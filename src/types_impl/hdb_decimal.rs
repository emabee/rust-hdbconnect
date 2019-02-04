use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::type_id::TypeId;
use crate::{HdbError, HdbResult};
use bigdecimal::{BigDecimal, Zero};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use num::bigint::{BigInt, Sign};
use num::{FromPrimitive, ToPrimitive};
use serde_db::ser::SerializationError;
use std::io;

// MANTISSA     113-bit     Integer mantissa
//                          (byte 0; byte 14, lowest bit)
// EXPONENT      14-bit     Exponent, biased with 6176, leading to a range -6143 to +6144
//                          (byte 14, above lowest bit; byte 15, below highest bit)
// SIGN           1-bit     Sign: 0 is positive, 1 is negative
//                          (byte 15, highest bit)
//
// The represented number is (10^EXPONENT)*MANTISSA.
// It is expected that MANTISSA is not a multiple of 10.

// Intermediate representation of HANA's DECIMAL type; is only used when reading
// from or writing to the wire.
#[derive(Clone, Debug)]
struct HdbDecimal {
    raw: [u8; 16],
}
impl HdbDecimal {
    // Creates an HdbDecimal from a BigDecimal.
    pub fn from_bigdecimal(bigdecimal: &BigDecimal) -> Result<HdbDecimal, SerializationError> {
        let ten = BigInt::from(10_u8);
        let (sign, mantissa, exponent) = {
            let (mut bigint, neg_exponent) = bigdecimal.as_bigint_and_exponent();
            let mut exponent = -neg_exponent;

            // HANA does not like mantissas that are multiples of 10
            while !bigint.is_zero() && (&bigint % &ten).is_zero() {
                bigint /= 10;
                exponent += 1;
            }

            // HANA accepts only mantissas up to 113 bits, so we round if necessary
            loop {
                let (_, mantissa) = bigint.to_bytes_le();
                let l = mantissa.len();
                if (l > 15) || ((l == 15) && (mantissa[14] & 0b1111_1110) != 0) {
                    bigint /= 10;
                    exponent += 1;
                } else {
                    break;
                }
            }

            if exponent < -6143 || exponent > 6144 {
                return Err(SerializationError::Serde(format!(
                    "exponent '{}' out of range",
                    exponent
                )));
            }
            let (sign, mantissa) = bigint.to_bytes_le();
            (sign, mantissa, exponent)
        };

        let mut raw = [0_u8; 16];
        (&mantissa)
            .iter()
            .enumerate()
            .for_each(|(i, b)| raw[i] = *b);

        let biased_exponent: u16 = (exponent + 6176) as u16; // bounds are checked above
        LittleEndian::write_u16(&mut raw[14..=15], biased_exponent * 2);

        if let Sign::Minus = sign {
            raw[15] |= 0b_1000_0000_u8;
        }
        let hdbdecimal = HdbDecimal { raw };
        Ok(hdbdecimal)
    }

    // Creates a `BigDecimal` representation.
    fn as_bigdecimal(&self, scale: i16) -> BigDecimal {
        let (sign, mantissa, exponent) = self.elements();
        let bd = match sign {
            Sign::Minus => -BigDecimal::new(mantissa, -exponent),
            Sign::NoSign | Sign::Plus => BigDecimal::new(mantissa, -exponent),
        };
        bd.with_scale(i64::from(scale))
    }

    // Retrieve the ingredients of the HdbDecimal
    pub fn elements(&self) -> (Sign, BigInt, i64) {
        let mut raw_bytes = self.raw;

        let sign = if (raw_bytes[15] & 0b_1000_0000_u8) == 0 {
            Sign::Plus
        } else {
            Sign::Minus
        };

        raw_bytes[15] &= 0b_0111_1111_u8;
        let exponent = i64::from(LittleEndian::read_u16(&raw_bytes[14..=15]) >> 1) - 6176;

        raw_bytes[14] &= 0b_0000_0001_u8;
        let mantissa = BigInt::from_bytes_le(Sign::Plus, &raw_bytes[0..=14]);

        (sign, mantissa, exponent)
    }
}

pub fn parse_decimal(
    nullable: bool,
    type_id: TypeId,
    scale: i16,
    rdr: &mut io::BufRead,
) -> HdbResult<HdbValue> {
    match type_id {
        TypeId::SMALLDECIMAL | TypeId::DECIMAL => {
            let mut raw = [0_u8; 16];
            rdr.read_exact(&mut raw[..])?;
            let is_null = raw[15] == 112
                && raw[14] == 0
                && raw[13] == 0
                && raw[12] == 0
                && raw[11] == 0
                && raw[10] == 0
                && raw[9] == 0
                && raw[8] == 0
                && raw[7] == 0
                && raw[6] == 0
                && raw[5] == 0
                && raw[4] == 0
                && raw[3] == 0
                && raw[2] == 0
                && raw[1] == 0
                && raw[0] == 0;

            if is_null {
                if nullable {
                    Ok(HdbValue::NULL(type_id))
                } else {
                    Err(HdbError::Impl(
                        "found null value for not-null column".to_owned(),
                    ))
                }
            } else {
                trace!("parse {}", type_id);
                let bd = HdbDecimal { raw }.as_bigdecimal(scale);
                Ok(match type_id {
                    TypeId::SMALLDECIMAL => HdbValue::DECIMAL(bd, TypeId::SMALLDECIMAL, scale),
                    TypeId::DECIMAL => HdbValue::DECIMAL(bd, TypeId::DECIMAL, scale),
                    _ => return Err(HdbError::Impl("unexpected type id for decimal".to_owned())),
                })
            }
        }
        TypeId::FIXED8 => Ok(if parse_null(nullable, rdr)? {
            HdbValue::NULL(TypeId::FIXED8)
        } else {
            trace!("parse FIXED8");
            let i = rdr.read_i64::<LittleEndian>()?;
            let bi = BigInt::from_i64(i)
                .ok_or_else(|| HdbError::Impl("invalid value of type FIXED8".to_owned()))?;
            let bd = BigDecimal::new(bi, i64::from(scale));
            HdbValue::DECIMAL(bd, TypeId::FIXED8, scale)
        }),

        TypeId::FIXED12 => Ok(if parse_null(nullable, rdr)? {
            HdbValue::NULL(TypeId::FIXED12)
        } else {
            trace!("parse FIXED12");
            let bytes = crate::protocol::util::parse_bytes(12, rdr)?;
            let bigint = BigInt::from_signed_bytes_le(&bytes);
            let bd = BigDecimal::new(bigint, i64::from(scale));
            HdbValue::DECIMAL(bd, TypeId::FIXED12, scale)
        }),

        TypeId::FIXED16 => Ok(if parse_null(nullable, rdr)? {
            HdbValue::NULL(TypeId::FIXED16)
        } else {
            trace!("parse FIXED16");
            let i = rdr.read_i128::<LittleEndian>()?;
            let bi = BigInt::from_i128(i)
                .ok_or_else(|| HdbError::Impl("invalid value of type FIXED16".to_owned()))?;
            let bd = BigDecimal::new(bi, i64::from(scale));
            HdbValue::DECIMAL(bd, TypeId::FIXED8, scale)
        }),
        _ => Err(HdbError::Impl("unexpected type id for decimal".to_owned())),
    }
}

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

pub(crate) fn emit_decimal(
    bd: &BigDecimal,
    type_id: TypeId,
    scale: i16,
    w: &mut io::Write,
) -> HdbResult<()> {
    match type_id {
        TypeId::SMALLDECIMAL | TypeId::DECIMAL => {
            let hdb_decimal = HdbDecimal::from_bigdecimal(bd)?;
            w.write_all(&hdb_decimal.raw)?;
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

pub fn decimal_size(type_id: TypeId) -> HdbResult<usize> {
    Ok(match type_id {
        TypeId::FIXED8 => 8,
        TypeId::FIXED12 => 12,
        TypeId::FIXED16 => 16,
        TypeId::DECIMAL | TypeId::SMALLDECIMAL => 16,
        _ => return Err(HdbError::Impl("unexpected type id for decimal".to_owned())),
    })
}

#[cfg(test)]
mod tests {
    use super::HdbDecimal;
    use bigdecimal::BigDecimal;
    use num::bigint::BigInt;
    use std::str::FromStr;

    #[test]
    fn test_all() {
        flexi_logger::Logger::with_str("info").start().unwrap();

        str_2_big_2_hdb_2_big("1234.56780000");
        str_2_big_2_hdb_2_big("1234.5678");
        str_2_big_2_hdb_2_big("-1234.5678");

        str_2_big_2_hdb_2_big("123456789");
        str_2_big_2_hdb_2_big("123456789.0000");
        str_2_big_2_hdb_2_big("0.1234567890000");
        str_2_big_2_hdb_2_big(
            "0.000000000000000000000000000000000000000000000000000001234567890000",
        );

        str_2_big_2_hdb_2_big("-123456789");
        str_2_big_2_hdb_2_big("-123456789.0000");
        str_2_big_2_hdb_2_big("-0.1234567890000");
        str_2_big_2_hdb_2_big(
            "-0.000000000000000000000000000000000000000000000000000001234567890000",
        );

        str_2_big_2_hdb_2_big("123456789123456789");
        str_2_big_2_hdb_2_big("1234567890012345678900000");
        str_2_big_2_hdb_2_big("1234567890000000000000000123456789");

        me_2_big_2_hdb_2_big(BigInt::from_str("0").unwrap(), 0);
        me_2_big_2_hdb_2_big(BigInt::from_str("1234567890").unwrap(), -5);
        me_2_big_2_hdb_2_big(BigInt::from_str("1234567890000").unwrap(), -8);

        me_2_big_2_hdb_2_big(
            BigInt::from_str("123456789012345678901234567890").unwrap(),
            0,
        );
        me_2_big_2_hdb_2_big(
            BigInt::from_str("1234567890123456789012345678901234000").unwrap(),
            0,
        );
        me_2_big_2_hdb_2_big(
            BigInt::from_str("1234567890123456789012345678901234").unwrap(),
            3,
        );
    }

    fn str_2_big_2_hdb_2_big(input: &str) {
        debug!("input:  {}", input);
        let bigdec = BigDecimal::from_str(input).unwrap();
        big_2_hdb_2_big(bigdec);
    }

    fn me_2_big_2_hdb_2_big(mantissa: BigInt, exponent: i64) {
        debug!("mantissa: {}, exponent: {}", mantissa, exponent);
        let bigdec = BigDecimal::new(mantissa, -exponent);
        big_2_hdb_2_big(bigdec);
    }

    fn big_2_hdb_2_big(bigdec: BigDecimal) {
        let hdbdec = HdbDecimal::from_bigdecimal(&bigdec).unwrap();
        let (s, m, e) = hdbdec.elements();
        let bigdec2 = hdbdec.as_bigdecimal();
        debug!("bigdec:  {:?}", bigdec);
        debug!("hdbdec:  {:?}", hdbdec);
        debug!("s: {:?}, m: {}, e: {}", s, m, e);
        debug!("bigdec2: {:?}\n", bigdec2);
        assert_eq!(bigdec, bigdec2, "start != end");
    }
}
