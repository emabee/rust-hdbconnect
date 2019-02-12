use crate::protocol::parts::hdb_value::HdbValue;
use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt};
use serde_derive::Serialize;
use std::cmp;
use std::fmt;
use std::io;

const NULL_REPRESENTATION: i64 = 315_538_070_401;

const SECOND_FACTOR: i64 = 1;
const MINUTE_FACTOR: i64 = 60;
const HOUR_FACTOR: i64 = 3_600;
const DAY_FACTOR: i64 = 86_400;

const ZEITENWENDE: i64 = 1_721_424;
const JGREG: i64 = 2_299_161;
// const IGREG: i64 = 18_994;             // Julian day of 01.01.0001 n. Chr.

/// Implementation of HANA's `SecondDate`.
///
/// The type is used internally to implement serialization to the wire.
/// It is agnostic of timezones.
#[derive(Clone, Debug, Serialize)]
pub struct SecondDate(i64);

impl fmt::Display for SecondDate {
    // The format chosen supports the conversion to chrono types.
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (year, month, day, hour, minute, second) = self.as_ymd_hms();
        write!(
            fmt,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        )
    }
}

impl cmp::PartialEq<SecondDate> for SecondDate {
    fn eq(&self, other: &SecondDate) -> bool {
        self.0 == other.0
    }
}

impl SecondDate {
    pub(crate) fn new(raw: i64) -> SecondDate {
        SecondDate(raw)
    }

    pub(crate) fn ref_raw(&self) -> &i64 {
        &self.0
    }

    /// Convert into tuple of "elements".
    pub(crate) fn as_ymd_hms(&self) -> (i32, u32, u32, u32, u32, u32) {
        let value = match self.0 {
            0 => 0, // maps the special value '' == 0 to '0001-01-01 00:00:00.000000000' = 1
            v => v - 1,
        };

        let datevalue = value / DAY_FACTOR;
        let mut timevalue = value - (datevalue * DAY_FACTOR);
        let hour: u32 = (timevalue / HOUR_FACTOR) as u32;
        timevalue -= HOUR_FACTOR * (i64::from(hour));
        let minute: u32 = (timevalue / MINUTE_FACTOR) as u32;
        timevalue -= MINUTE_FACTOR * (i64::from(minute));
        let second: u32 = (timevalue / SECOND_FACTOR) as u32;

        let julian: i64 = datevalue + ZEITENWENDE;
        let ja: i64 = if julian >= JGREG {
            let jalpha: i64 = (((julian - 1_867_216) as f64 - 0.25_f64) / 36_524.25_f64) as i64;
            julian + 1 + jalpha - ((0.25_f64 * jalpha as f64) as i64)
        } else {
            julian
        };

        let jb: i64 = ja + 1524;
        let jc: i64 = (6680_f64 + ((jb - 2_439_870) as f64 - 122.1_f64) / 365.25_f64) as i64;
        let jd: i64 = ((365 * jc) as f64 + (0.25_f64 * jc as f64)) as i64;
        let je: i64 = ((jb - jd) as f64 / 30.6001) as i64;

        let day: u32 = (jb - jd - ((30.6001 * je as f64) as i64)) as u32;
        let mut month: u32 = je as u32 - 1;
        let mut year: i32 = jc as i32 - 4715;

        if month > 12 {
            month -= 12;
        }
        if month > 2 {
            year -= 1;
        }
        if year <= 0 {
            year -= 1;
        }
        (year, month, day, hour, minute, second)
    }
}

pub(crate) fn parse_seconddate(nullable: bool, rdr: &mut io::BufRead) -> HdbResult<HdbValue> {
    let i = rdr.read_i64::<LittleEndian>()?;
    if i == NULL_REPRESENTATION {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found NULL value for NOT NULL longdate column".to_owned(),
            ))
        }
    } else {
        Ok(HdbValue::SECONDDATE(SecondDate::new(i)))
    }
}
