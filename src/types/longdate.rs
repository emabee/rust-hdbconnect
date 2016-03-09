use chrono::{Datelike, DateTime, Timelike, UTC};
use chrono::offset::TimeZone;
use std::cmp;
use std::fmt;

const SECOND_FACTOR: i64 = 10_000_000;
const MINUTE_FACTOR: i64 = 600_000_000;   // 10_000_000 * 60;
const HOUR_FACTOR: i64 = 36_000_000_000;   // 10_000_000 * 60 * 60;
const DAY_FACTOR: i64 = 864_000_000_000;   // 10_000_000 * 60 * 60 * 24;

const ZEITENWENDE: i64 = 1_721_424;
const JGREG: i64 = 2_299_161;
// const IGREG: i64         =    18_994;   /* Julianischer Tag des 01.01.0001 n. Chr. */

/// Implementation of HANA's LongDate.
///
/// The type is used in ResultSets and can be used as input parameters.
#[derive(Clone,Debug,Deserialize,Serialize)]
pub struct LongDate(pub i64);

impl LongDate {
    // see  UnifiedTypes/impl/Longdate.cpp, int Longdate::set(const DateRepresentation &dr)
    /// Converts a chrono DateTime<UTC> into a LongDate that represents the same value.
    pub fn from(dt_utc: DateTime<UTC>) -> Result<LongDate, &'static str> {
        if dt_utc.year() < 1 || dt_utc.year() > 9999 {
            return Err("Only years between 1 and 9999 are supported");
        } else {
            let mut m: u32 = dt_utc.month();
            let mut d: u32 = dt_utc.day();
            if m == 0 {
                m = 1;
            }
            if d == 0 {
                d = 1;
            }

            Ok(LongDate(to_day_number(dt_utc.year() as u32, m, d) * DAY_FACTOR + dt_utc.hour() as i64 * HOUR_FACTOR +
                        dt_utc.minute() as i64 * MINUTE_FACTOR +
                        dt_utc.second() as i64 * SECOND_FACTOR +
                        dt_utc.nanosecond() as i64 / 100 + 1))
        }
    }

    /// Factory method for LongDate.
    pub fn ymd(y: u32, m: u32, d: u32) -> Result<LongDate, &'static str> {
        LongDate::ymd_hms(y, m, d, 0, 0, 0)
    }

    /// Factory method for LongDate.
    pub fn ymd_hms(y: u32, m: u32, d: u32, hour: u32, minute: u32, second: u32) -> Result<LongDate, &'static str> {
        if y < 1 || y > 9999 {
            return Err("Only years between 1 and 9999 are supported");
        }
        if m < 1 || m > 12 {
            return Err("Only months between 1 and 12 are supported");
        }
        if d < 1 || d > 31 {
            return Err("Only days between 1 and 31 are supported");
        }

        Ok(LongDate(to_day_number(y, m, d) * DAY_FACTOR + hour as i64 * HOUR_FACTOR +
                    minute as i64 * MINUTE_FACTOR + second as i64 * SECOND_FACTOR + 1))
    }

    // see jdbc/translators/LongDateTranslator.java, getTimestamp()
    /// Converts a LongDate into a chrono DateTime<UTC>.
    pub fn to_datetime_utc(&self) -> Option<DateTime<UTC>> {
        trace!("Entering to_datetime_utc()");
        let value = match self.0 {
            0 => 0,             // with this we map the special value '' == 0 to '0001-01-01 00:00:00.000000000' = 1
            v => v - 1,
        };

        let datevalue = value / DAY_FACTOR;
        let mut timevalue = value - (datevalue * DAY_FACTOR);
        let hour: u32 = (timevalue / HOUR_FACTOR) as u32;
        timevalue -= HOUR_FACTOR * (hour as i64);
        let minute: u32 = (timevalue / MINUTE_FACTOR) as u32;
        timevalue -= MINUTE_FACTOR * (minute as i64);
        let second: u32 = (timevalue / SECOND_FACTOR) as u32;
        timevalue -= SECOND_FACTOR * (second as i64);
        let fraction: u32 = timevalue as u32; // 10**-7

        let julian: i64 = datevalue + ZEITENWENDE;
        let ja: i64 = if julian >= JGREG {
            let jalpha: i64 = (((julian - 1867216) as f64 - 0.25_f64) / 36524.25_f64) as i64;
            julian + 1 + jalpha - ((0.25_f64 * jalpha as f64) as i64)
        } else {
            julian
        };

        let jb: i64 = ja + 1524;
        let jc: i64 = (6680_f64 + ((jb - 2439870) as f64 - 122.1_f64) / 365.25_f64) as i64;
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

        trace!("Leaving to_datetime_utc(): year {}, month {}, day {}, hour {}, minute {}, second {}, fraction {}",
               year,
               month,
               day,
               hour,
               minute,
               second,
               fraction);
        Some(UTC.ymd(year, month, day).and_hms_nano(hour, minute, second, fraction * 100))
    }
}

fn to_day_number(y: u32, m: u32, d: u32) -> i64 {
    let (yd, md) = to_day(m);
    let y2 = y as i32 + yd;
    let mut daynr: i64 = (((1461 * y2) >> 2) + md + d as i32 - 307) as i64;
    if daynr > 577746_i64 {
        daynr += 2 - ((3 * ((y2 + 100) / 100)) >> 2) as i64;
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


impl fmt::Display for LongDate {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self.to_datetime_utc() {
            Some(dtu) => write!(fmt, "{}", dtu),
            None => write!(fmt, "Unprintable Longdate ({:?})", self),
        }
    }
}

impl cmp::PartialEq<LongDate> for LongDate {
    fn eq(&self, other: &LongDate) -> bool {
        self.0 == other.0
    }
}

#[cfg(test)]
mod test {
    use super::LongDate;
    use chrono::UTC;
    use chrono::offset::TimeZone;
    use flexi_logger;

    #[test]
    fn longdate_conversion() {
        flexi_logger::init(flexi_logger::LogConfig::new(), Some("info".to_string())).unwrap();


        info!("test consistency of conversions between chrono::DateTime<UTC> and hdbconnect::LongDate");

        // LongDate => DateTime<UTC> => LongDate
        for longdate in vec!(   LongDate(1234567890123456789_i64),
                                LongDate(1010101010101010101_i64),
                                LongDate( 635895889133394319_i64),
                                LongDate(  77777777777777777_i64),
        ) {
            let datetime_utc = longdate.to_datetime_utc().unwrap();
            debug!("1 (LongDate => chrono => LongDate): {:?} == {} ?", longdate, datetime_utc);
            assert_eq!(longdate, LongDate::from(datetime_utc).unwrap());
        }

        // DateTime<UTC> => LongDate => DateTime<UTC>
        for datetime_utc in vec!(   UTC::now(),
                                    UTC.ymd(   1, 1, 1).and_hms_nano(0, 0, 0, 0),
                                    UTC.ymd(  22, 2, 2).and_hms_nano(0, 0, 0, 0),
                                    UTC.ymd( 333, 3, 3).and_hms_nano(0, 0, 0, 0),
                                    UTC.ymd(4444, 4, 4).and_hms_nano(0, 0, 0, 0),
        ) {
            let longdate = LongDate::from(datetime_utc).unwrap();
            debug!("2 (chrono => LongDate => chrono): {:?} == {} ?", longdate, datetime_utc);
            assert_eq!(datetime_utc, longdate.to_datetime_utc().unwrap());
        }
    }
}
