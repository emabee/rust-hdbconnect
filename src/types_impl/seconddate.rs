use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};
use serde_derive::Serialize;
use std::cmp;
use std::error::Error;
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

    /// Factory method for SecondDate with all fields.
    pub fn from_ymd_hms(
        y: i32,
        m: u32,
        d: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> HdbResult<SecondDate> {
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

        Ok(SecondDate(
            1 + to_day_number(y as u32, m, d) * DAY_FACTOR
                + i64::from(hour) * HOUR_FACTOR
                + i64::from(minute) * MINUTE_FACTOR
                + i64::from(second) * SECOND_FACTOR,
        ))
    }

    /// Factory method for SecondDate up to day precision.
    pub fn from_ymd(y: i32, m: u32, d: u32) -> HdbResult<SecondDate> {
        SecondDate::from_ymd_hms(y, m, d, 0, 0, 0)
    }

    /// Convert into tuple of "elements".
    pub fn as_ymd_hms(&self) -> (i32, u32, u32, u32, u32, u32) {
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

    /// Parses a `SecondDate` from a String.
    ///
    /// Note that Chrono types serialize as formatted Strings.
    /// We parse such (and other) Strings and construct a `SecondDate`.
    pub fn from_date_string(s: &str) -> HdbResult<SecondDate> {
        type FSD = fn(&str) -> HdbResult<SecondDate>;

        let funcs: Vec<FSD> = vec![
            SecondDate::from_string_second,
            SecondDate::from_string_day,
            SecondDate::from_utc_string,
        ];

        for func in funcs {
            if let Ok(seconddate) = func(s) {
                return Ok(seconddate);
            }
        }
        Err(HdbError::Usage(format!(
            "Cannot parse SecondDate from given date string \"{}\"",
            s,
        )))
    }

    fn from_string_second(s: &str) -> HdbResult<SecondDate> {
        let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
            .map_err(|e| HdbError::Usage(e.description().to_owned()))?;
        SecondDate::from_ymd_hms(
            ndt.year(),
            ndt.month(),
            ndt.day(),
            ndt.hour(),
            ndt.minute(),
            ndt.second(),
        )
    }

    fn from_string_day(s: &str) -> HdbResult<SecondDate> {
        let ndt = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| HdbError::Usage(e.description().to_owned()))?;
        SecondDate::from_ymd(ndt.year(), ndt.month(), ndt.day())
    }

    // 2012-02-02T02:02:02.200Z
    fn from_utc_string(s: &str) -> HdbResult<SecondDate> {
        let ndt = DateTime::parse_from_rfc3339(s)
            .map_err(|e| HdbError::Usage(e.description().to_owned()))?
            .naive_utc();
        SecondDate::from_ymd_hms(
            ndt.year(),
            ndt.month(),
            ndt.day(),
            ndt.hour(),
            ndt.minute(),
            ndt.second(),
        )
    }
}

fn to_day_number(y: u32, m: u32, d: u32) -> i64 {
    let (yd, md) = to_day(m);
    let y2 = y as i32 + yd;
    let mut daynr = i64::from(((1461 * y2) >> 2) + md + d as i32 - 307);
    if daynr > 577_746_i64 {
        daynr += 2 - i64::from((3 * ((y2 + 100) / 100)) >> 2);
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

pub fn parse_seconddate(rdr: &mut io::BufRead) -> HdbResult<SecondDate> {
    let i = rdr.read_i64::<LittleEndian>()?;
    match i {
        NULL_REPRESENTATION => Err(HdbError::Impl(
            "Null value found for non-null longdate column".to_owned(),
        )),
        _ => Ok(SecondDate::new(i)),
    }
}

pub fn parse_nullable_seconddate(rdr: &mut io::BufRead) -> HdbResult<Option<SecondDate>> {
    let i = rdr.read_i64::<LittleEndian>()?;
    match i {
        NULL_REPRESENTATION => Ok(None),
        _ => Ok(Some(SecondDate::new(i))),
    }
}
