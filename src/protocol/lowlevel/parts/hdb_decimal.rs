use super::PrtResult;
use byteorder::{ByteOrder, LittleEndian};
use num::ToPrimitive;
use num::bigint::{BigInt, Sign};
use rust_decimal::Decimal;
use std::fmt;
use std::io;
use std::ops::Mul;
use std::str::FromStr;
use serde_db::ser::SerializationError;



// SIGN           1-bit     (byte 15, highest bit)  Sign: 0 is positive, 1 is negative
// EXPONENT      14-bit     (byte 14, above lowest bit; byte 15, below highest bit) Exponent,
//                          biased with 6176, leading to a range -6143 to +6144
// MANTISSA     113-bit     Integer mantissa
//
// The number represented is (10^EXPONENT)*MANTISSA.
// It is expected that MANTISSA is not a multiple of 10
/// Representation of HANA's DECIMAL type.
#[derive(Clone, Debug)]
pub struct HdbDecimal {
    raw: [u8; 16],
}
impl HdbDecimal {
    /// Parse from a string representation.
    pub fn parse_from_str(s: &str) -> Result<HdbDecimal, SerializationError> {
        let decimal = Decimal::from_str(s.trim()).map_err(|_| {
            SerializationError::GeneralError(
                "Cannot serialize decimal string to Decimal".to_string(),
            )
        })?;
        trace!("HdbDecimal::from_str() called with: {}, converted to decimal {}", s, decimal);
        let raw: [u8; 16] = decimal.serialize();
        let mantissa: &[u8] = &raw[4..16];

        let mut bytes = [0_u8; 16];
        mantissa.iter().enumerate().for_each(|(i, b)| bytes[i] = *b);

        let scale: u16 = (6176 - decimal.scale()) as u16;
        LittleEndian::write_u16(&mut bytes[14..16], scale * 2);

        if decimal.is_negative() {
            bytes[15] |= 0b_1000_0000_u8;
        }
        let result = HdbDecimal { raw: bytes };
        trace!("result.as_decimal(): {}", result.as_decimal());
        Ok(result)
    }

    /// Creates a HdbDecimal from a `rust_decimal::Decimal`.
    pub fn from_decimal(decimal: Decimal) -> Result<HdbDecimal, SerializationError> {
        // FIXME improve this: do bit shuffling rather than going through the String representation
        let s = format!("{}", decimal);
        HdbDecimal::parse_from_str(&s)
    }

    /// Converts into a `rust_decimal::Decimal` representation.
    pub fn into_decimal(mut self) -> Decimal {
        let sign: Sign = if self.raw[15] & 0b_1000_0000_u8 == 0 {
            Sign::Plus
        } else {
            Sign::Minus
        };

        self.raw[15] &= !0b_1000_0000_u8;
        let tmp = i32::from(LittleEndian::read_u16(&self.raw[14..16]) >> 1) - 6176;
        let (factor, exponent): (u32, u32) = if tmp < 0 {
            (0, -tmp as u32)
        } else {
            (tmp as u32, 0)
        };

        self.raw[14] = 0b_0000_0000_u8;
        self.raw[15] = 0b_0000_0000_u8;
        let mut mantissa = BigInt::from_bytes_le(sign, &self.raw);
        for _ in 0..factor {
            mantissa = mantissa.mul(BigInt::from(10_usize));
        }
        Decimal::new(mantissa.to_i64().unwrap(), exponent)
    }

    /// Returns a `rust_decimal::Decimal` representation.
    pub fn as_decimal(&self) -> Decimal {
        self.clone().into_decimal()
    }
}

impl fmt::Display for HdbDecimal {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.as_decimal())
    }
}

pub fn parse_decimal(rdr: &mut io::BufRead) -> PrtResult<HdbDecimal> {
    let mut bytes: [u8; 16] = [0; 16];
    rdr.read_exact(&mut bytes[..])?;
    Ok(HdbDecimal { raw: bytes })
}

pub fn parse_nullable_decimal(rdr: &mut io::BufRead) -> PrtResult<Option<HdbDecimal>> {
    let mut bytes: [u8; 16] = [0; 16];
    rdr.read_exact(&mut bytes[..])?;

    if bytes[15] == 112 && bytes[14] == 0 && bytes[13] == 0 && bytes[12] == 0 && bytes[11] == 0
        && bytes[10] == 0 && bytes[9] == 0 && bytes[8] == 0 && bytes[7] == 0 && bytes[6] == 0
        && bytes[5] == 0 && bytes[4] == 0 && bytes[3] == 0 && bytes[2] == 0 && bytes[1] == 0
        && bytes[0] == 0
    {
        Ok(None)
    } else {
        Ok(Some(HdbDecimal { raw: bytes }))
    }
}

pub fn serialize_decimal(d: &HdbDecimal, w: &mut io::Write) -> PrtResult<()> {
    w.write_all(&d.raw)?;
    Ok(())
}
