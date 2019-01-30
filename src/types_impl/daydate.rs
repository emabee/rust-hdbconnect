use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::type_id::TypeId;
use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt};
use chrono::{Datelike, NaiveDate};
use serde_derive::Serialize;
use std::cmp;
use std::error::Error;
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

    /// Factory method for DayDate with all fields.
    pub fn from_ymd(y: i32, m: u32, d: u32) -> HdbResult<DayDate> {
        if y < 1 || y > 9999 {
            return Err(HdbError::Usage(
                "Only years between 1 and 9999 are supported".to_owned(),
            ));
        }
        if m < 1 || m > 12 {
            return Err(HdbError::Usage(
                "Only months between 1 and 12 are supported".to_owned(),
            ));
        }
        if d < 1 || d > 31 {
            return Err(HdbError::Usage(
                "Only days between 1 and 31 are supported".to_owned(),
            ));
        }

        Ok(DayDate(1 + to_day_number(y as u32, m, d)))
    }

    /// Convert into tuple of "elements".
    pub fn as_ymd(&self) -> (i32, u32, u32) {
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

    /// Parses a `DayDate` from a String.
    ///
    /// Note that Chrono types serialize as formatted Strings.
    /// We parse such (and other) Strings and construct a `DayDate`.
    pub fn from_date_string(s: &str) -> HdbResult<DayDate> {
        type FSD = fn(&str) -> HdbResult<DayDate>;

        let funcs: Vec<FSD> = vec![DayDate::from_string_day];

        for func in funcs {
            if let Ok(daydate) = func(s) {
                return Ok(daydate);
            }
        }
        Err(HdbError::Usage(format!(
            "Cannot parse DayDate from given date string\"{}\"",
            s,
        )))
    }

    fn from_string_day(s: &str) -> HdbResult<DayDate> {
        let nd = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| HdbError::Usage(e.description().to_owned()))?;
        DayDate::from_ymd(nd.year(), nd.month(), nd.day())
    }
}

fn to_day_number(y: u32, m: u32, d: u32) -> i32 {
    let (yd, md) = to_day(m);
    let y2 = y as i32 + yd;
    let mut daynr = ((1461 * y2) >> 2) + md + d as i32 - 307;
    if daynr > 577_746_i32 {
        daynr += 2 - ((3 * ((y2 + 100) / 100)) >> 2);
    }
    daynr
}
fn to_day(m: u32) -> (i32, i32) {
    match m {
        1 => (-1, 306),
        2 => (-1, 337),
        3 => (0, 0),
        4 => (0, 31),
        5 => (0, 61),
        6 => (0, 92),
        7 => (0, 122),
        8 => (0, 153),
        9 => (0, 184),
        10 => (0, 214),
        11 => (0, 245),
        12 => (0, 275),
        _ => panic!("unexpected value m = {} in to_day()", m),
    }
}

pub fn parse_daydate(nullable: bool, rdr: &mut io::BufRead) -> HdbResult<HdbValue> {
    let i = rdr.read_i32::<LittleEndian>()?;
    if i == NULL_REPRESENTATION {
        if nullable {
            Ok(HdbValue::NULL(TypeId::DAYDATE))
        } else {
            Err(HdbError::Impl(
                "found NULL value for NOT NULL longdate column".to_owned(),
            ))
        }
    } else {
        Ok(HdbValue::DAYDATE(DayDate::new(i)))
    }
}
