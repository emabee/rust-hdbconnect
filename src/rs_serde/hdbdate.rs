use chrono::{Datelike, DateTime, NaiveDate, NaiveDateTime, Timelike};

use types::LongDate;
use super::ser::SerializationError;


/// Serializes a date string into a `LongDate`.
///
/// Chrono types serialize as formatted Strings. We try to parse such a string
/// to convert back into the type we had originally, and construct a `LongDate`.
pub fn longdate_from_str(s: &str) -> Result<LongDate, SerializationError> {
    let funcs: Vec<fn(&str) -> Result<LongDate, ()>> = vec![
        from_naivedt_string_full,
        from_naivedt_string_second,
        from_naivedt_string_day,
        from_utc_string,
    ];

    for func in funcs {
        if let Ok(longdate) = func(s) {
            return Ok(longdate);
        }
    }
    Err(SerializationError::InvalidValue("Cannot serialize date-string to LongDate".to_string()))
}

// 2012-02-02T02:02:02.200
fn from_naivedt_string_full(s: &str) -> Result<LongDate, ()> {
    trace!("from_naivedt_string_full");
    match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(ndt_parsed) => {
            let ld = LongDate::from_ymd_hms_n(
                ndt_parsed.year(),
                ndt_parsed.month(),
                ndt_parsed.day(),
                ndt_parsed.hour(),
                ndt_parsed.minute(),
                ndt_parsed.second(),
                ndt_parsed.nanosecond(),
            )
                     .or(Err(()))?;
            trace!("NaiveDateTime::from_naivedt_string_full(): OK with ld = {}", ld);
            Ok(ld)
        }
        Err(e) => {
            trace!("{:?}", e);
            Err(())
        }
    }
}

fn from_naivedt_string_second(s: &str) -> Result<LongDate, ()> {
    trace!("from_naivedt_string_second");
    match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        Ok(ndt_parsed) => {
            let ld = LongDate::from_ymd_hms(
                ndt_parsed.year(),
                ndt_parsed.month(),
                ndt_parsed.day(),
                ndt_parsed.hour(),
                ndt_parsed.minute(),
                ndt_parsed.second(),
            )
                     .or(Err(()))?;
            trace!("NaiveDateTime::from_naivedt_string_second(): OK with ld = {}", ld);
            Ok(ld)
        }
        Err(e) => {
            trace!("{:?}", e);
            Err(())
        }
    }
}

fn from_naivedt_string_day(s: &str) -> Result<LongDate, ()> {
    trace!("from_naivedt_string_day with {}", s);
    match NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        Ok(ndt_parsed) => {
            let ld = try!(
                LongDate::from_ymd(ndt_parsed.year(), ndt_parsed.month(), ndt_parsed.day())
                    .or(Err(()))
            );
            trace!("NaiveDateTime::from_naivedt_string_day(): OK with ld = {}", ld);
            Ok(ld)
        }
        Err(e) => {
            trace!("{:?}", e);
            Err(())
        }
    }
}

// 2012-02-02T02:02:02.200Z
fn from_utc_string(s: &str) -> Result<LongDate, ()> {
    trace!("from_utc_string");
    match DateTime::parse_from_rfc3339(s) {
        Ok(dt) => {
            trace!("DateTime::parse_from_rfc3339(s): {}", dt);
            let ndt = dt.naive_utc();
            let ld = try!(
                LongDate::from_ymd_hms_n(
                    ndt.year(),
                    ndt.month(),
                    ndt.day(),
                    ndt.hour(),
                    ndt.minute(),
                    ndt.second(),
                    ndt.nanosecond(),
                )
                .or(Err(()))
            );
            trace!("DateTime::parse_from_rfc3339(): OK with ld = {}", ld);
            Ok(ld)
        }
        Err(e) => {
            trace!("{:?}", e);
            Err(())
        }
    }
}
