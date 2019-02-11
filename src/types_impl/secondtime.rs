use crate::protocol::parts::hdb_value::HdbValue;
use crate::{HdbError, HdbResult};
use byteorder::{LittleEndian, ReadBytesExt};
use serde_derive::Serialize;
use std::cmp;
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

    /// Convert into tuple of "elements".
    pub(crate) fn as_hms(&self) -> (u32, u32, u32) {
        let mut second = if self.0 == 0 { 0 } else { self.0 - 1 };
        let hour = second / HOUR_FACTOR;
        second -= HOUR_FACTOR * hour;
        let minute = second / MINUTE_FACTOR;
        second -= MINUTE_FACTOR * minute;

        (hour, minute, second)
    }

}

pub(crate) fn parse_secondtime(nullable: bool, rdr: &mut io::BufRead) -> HdbResult<HdbValue> {
    let i = rdr.read_i32::<LittleEndian>()?;
    if i == NULL_REPRESENTATION {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(HdbError::Impl(
                "found NULL value for NOT NULL scondtime column".to_owned(),
            ))
        }
    } else {
        Ok(HdbValue::SECONDTIME(SecondTime::new(i)))
    }
}
