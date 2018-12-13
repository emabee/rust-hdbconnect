use bigdecimal::{BigDecimal, Zero};
use byteorder::{ByteOrder, LittleEndian};
use num::bigint::{BigInt, Sign};
use serde_db::ser::SerializationError;
use std::fmt;
use std::io;
use crate::HdbResult;

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
pub struct HdbDecimal {
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
        trace!("hdbdecimal.as_bigdecimal(): {}", hdbdecimal.as_bigdecimal());
        Ok(hdbdecimal)
    }

    // Creates a `BigDecimal` representation.
    pub fn as_bigdecimal(&self) -> BigDecimal {
        let (sign, mantissa, exponent) = self.elements();
        match sign {
            Sign::Minus => -BigDecimal::new(mantissa, -exponent),
            _ => BigDecimal::new(mantissa, -exponent),
        }
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

impl fmt::Display for HdbDecimal {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.as_bigdecimal())
    }
}

pub fn parse_decimal(rdr: &mut io::BufRead) -> HdbResult<BigDecimal> {
    let mut raw = [0_u8; 16];
    rdr.read_exact(&mut raw[..])?;
    Ok(HdbDecimal { raw }.as_bigdecimal())
}

pub fn parse_nullable_decimal(rdr: &mut io::BufRead) -> HdbResult<Option<BigDecimal>> {
    let mut raw = [0_u8; 16];
    rdr.read_exact(&mut raw[..])?;

    if raw[15] == 112
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
        && raw[0] == 0
    {
        Ok(None)
    } else {
        Ok(Some(HdbDecimal { raw }.as_bigdecimal()))
    }
}

pub fn serialize_decimal(bd: &BigDecimal, w: &mut io::Write) -> HdbResult<()> {
    let hdb_decimal = HdbDecimal::from_bigdecimal(bd)?;
    w.write_all(&hdb_decimal.raw)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    extern crate flexi_logger;
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
