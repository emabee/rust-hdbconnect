use crate::protocol::parts::hdb_value::HdbValue;
use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt};
use serde_derive::Serialize;
use std::cmp;
use std::fmt;
use std::io;

const NULL_REPRESENTATION: i32 = 3_652_062;

const ZEITENWENDE: i32 = 1_721_424;
const JGREG: i32 = 2_299_161;
// const IGREG: i64 = 18_994;             // Julian day of 01.01.0001 n. Chr.

/// Implementation of HANA's `DayDate`.
///
/// The type is used internally to implement serialization to the wire.
/// It is agnostic of timezones.
#[derive(Clone, Debug, Serialize)]
pub struct DayDate(i32);

impl fmt::Display for DayDate {
    // The format chosen supports the conversion to chrono types.
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (year, month, day) = self.as_ymd();
        write!(fmt, "{:04}-{:02}-{:02}", year, month, day)
    }
}

impl cmp::PartialEq<DayDate> for DayDate {
    fn eq(&self, other: &DayDate) -> bool {
        self.0 == other.0
    }
}

impl DayDate {
    pub(crate) fn new(raw: i32) -> DayDate {
        assert!(raw < NULL_REPRESENTATION && raw >= 0);
        DayDate(raw)
    }
    pub(crate) fn ref_raw(&self) -> &i32 {
        &self.0
    }

    /// Convert into tuple of "elements".
    pub(crate) fn as_ymd(&self) -> (i32, u32, u32) {
        let datevalue = match self.0 {
            0 => 0, // maps the special value '' == 0 to '0001-01-01' = 1
            v => v - 1,
        };

        let julian: i32 = datevalue as i32 + ZEITENWENDE;
        let ja: i32 = if julian >= JGREG {
            let jalpha: i32 = ((f64::from(julian - 1_867_216) - 0.25_f64) / 36_524.25_f64) as i32;
            julian + 1 + jalpha - ((0.25_f64 * f64::from(jalpha)) as i32)
        } else {
            julian
        };

        let jb: i32 = ja + 1524;
        let jc: i32 = (6680_f64 + (f64::from(jb - 2_439_870) - 122.1_f64) / 365.25_f64) as i32;
        let jd: i32 = (f64::from(365 * jc) + (0.25_f64 * f64::from(jc))) as i32;
        let je: i32 = (f64::from(jb - jd) / 30.6001) as i32;

        let day: u32 = (jb - jd - ((30.6001 * f64::from(je)) as i32)) as u32;
        let mut month: u32 = je as u32 - 1;
        let mut year: i32 = jc - 4715;

        if month > 12 {
            month -= 12;
        }
        if month > 2 {
            year -= 1;
        }
        if year <= 0 {
            year -= 1;
        }
        (year, month, day)
    }

}

pub(crate) fn parse_daydate(nullable: bool, rdr: &mut io::BufRead) -> HdbResult<HdbValue> {
    let i = rdr.read_i32::<LittleEndian>()?;
    if i == NULL_REPRESENTATION {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found NULL value for NOT NULL longdate column".to_owned(),
            ))
        }
    } else {
        Ok(HdbValue::DAYDATE(DayDate::new(i)))
    }
}
