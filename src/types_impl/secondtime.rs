use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt};
use chrono::{NaiveTime, Timelike};
use serde_derive::Serialize;
use std::cmp;
use std::error::Error;
use std::fmt;
use std::io;

const NULL_REPRESENTATION: i32 = 86_402;

const MINUTE_FACTOR: u32 = 60;
const HOUR_FACTOR: u32 = 3_600;

/// Implementation of HANA's `SecondTime`.
///
/// The type is used internally to implement serialization to the wire.
///
/// HANA allows input of empty strings, they are mapped to 0, all other legal values are mapped to
/// Hours * 60*60 + Minutes * 60 + Seconds  + 1 < 86400.
///
/// When reading, we treat 0 and 1 as "00:00:00".
#[derive(Clone, Debug, Serialize)]
pub struct SecondTime(u32);

impl fmt::Display for SecondTime {
    // The format chosen supports the conversion to chrono types.
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (hour, minute, second) = self.as_hms();
        write!(fmt, "{:02}:{:02}:{:02}", hour, minute, second)
    }
}

impl cmp::PartialEq<SecondTime> for SecondTime {
    fn eq(&self, other: &SecondTime) -> bool {
        self.0 == other.0
    }
}

impl SecondTime {
    pub(crate) fn new(raw: i32) -> SecondTime {
        assert!(raw < NULL_REPRESENTATION && raw >= 0);
        SecondTime(raw as u32)
    }

    pub(crate) fn ref_raw(&self) -> &u32 {
        &self.0
    }

    /// Factory method for SecondTime with all fields.
    pub fn from_hms(hour: u32, minute: u32, second: u32) -> HdbResult<SecondTime> {
        if hour > 23 || minute > 59 || second > 59 {
            Err(HdbError::Usage(
                "illegal value of hour, minute or second".to_owned(),
            ))
        } else {
            Ok(SecondTime(
                hour * HOUR_FACTOR + minute * MINUTE_FACTOR + second + 1,
            ))
        }
    }

    /// Convert into tuple of "elements".
    pub fn as_hms(&self) -> (u32, u32, u32) {
        let mut second = if self.0 == 0 { 0 } else { self.0 - 1 };
        let hour = second / HOUR_FACTOR;
        second -= HOUR_FACTOR * hour;
        let minute = second / MINUTE_FACTOR;
        second -= MINUTE_FACTOR * minute;

        (hour, minute, second)
    }

    /// Parses a `SecondTime` from a String.
    ///
    /// Note that Chrono types serialize as formatted Strings.
    /// We parse such (and other) Strings and construct a `SecondTime`.
    pub fn from_date_string(s: &str) -> HdbResult<SecondTime> {
        type FSD = fn(&str) -> HdbResult<SecondTime>;

        let funcs: Vec<FSD> = vec![SecondTime::from_string_second];

        for func in funcs {
            if let Ok(secondtime) = func(s) {
                return Ok(secondtime);
            }
        }
        Err(HdbError::Usage(format!(
            "Cannot parse SecondTime from given date string \"{}\"",
            s,
        )))
    }

    fn from_string_second(s: &str) -> HdbResult<SecondTime> {
        let nt = NaiveTime::parse_from_str(s, "%H:%M:%S")
            .map_err(|e| HdbError::Usage(e.description().to_owned()))?;
        SecondTime::from_hms(nt.hour(), nt.minute(), nt.second())
    }
}

pub fn parse_secondtime(rdr: &mut io::BufRead) -> HdbResult<SecondTime> {
    let st = rdr.read_i32::<LittleEndian>()?;
    match st {
        NULL_REPRESENTATION => Err(HdbError::Impl(
            "Null value found for non-null secondtime column".to_owned(),
        )),
        _ => Ok(SecondTime::new(st)),
    }
}

pub fn parse_nullable_secondtime(rdr: &mut io::BufRead) -> HdbResult<Option<SecondTime>> {
    let st = rdr.read_i32::<LittleEndian>()?;
    match st {
        NULL_REPRESENTATION => Ok(None),
        _ => Ok(Some(SecondTime::new(st))),
    }
}
