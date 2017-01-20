use std::cmp;
use std::fmt;

const SECOND_FACTOR: i64 = 10_000_000;
const MINUTE_FACTOR: i64 = 600_000_000; // 10_000_000 * 60;
const HOUR_FACTOR: i64 = 36_000_000_000; // 10_000_000 * 60 * 60;
const DAY_FACTOR: i64 = 864_000_000_000; // 10_000_000 * 60 * 60 * 24;

const ZEITENWENDE: i64 = 1_721_424;
const JGREG: i64 = 2_299_161;
// const IGREG: i64 = 18_994;             // Julian day of 01.01.0001 n. Chr.

/// Implementation of HANA's LongDate.
///
/// The type is used internally to implement serialization to the wire.
/// It is agnostic of timezones.
#[derive(Clone,Debug)]
pub struct LongDate(pub i64);

impl fmt::Display for LongDate {
    // The format chosen supports the conversion to chrono types.
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (year, month, day, hour, minute, second, fraction) = self.as_ymd_hms_f();
        write!(fmt,
               "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:07}",
               year,
               month,
               day,
               hour,
               minute,
               second,
               fraction)
    }
}

impl cmp::PartialEq<LongDate> for LongDate {
    fn eq(&self, other: &LongDate) -> bool {
        self.0 == other.0
    }
}


impl LongDate {
    /// Factory method for LongDate with all fields.
    pub fn from_ymd_hms_n(y: i32, m: u32, d: u32, hour: u32, minute: u32, second: u32,
                          nanosecond: u32)
                          -> Result<LongDate, &'static str> {
        if y < 1 || y > 9999 {
            return Err("Only years between 1 and 9999 are supported");
        }
        if m < 1 || m > 12 {
            return Err("Only months between 1 and 12 are supported");
        }
        if d < 1 || d > 31 {
            return Err("Only days between 1 and 31 are supported");
        }

        Ok(LongDate(1 + to_day_number(y as u32, m, d) * DAY_FACTOR + hour as i64 * HOUR_FACTOR +
                    minute as i64 * MINUTE_FACTOR +
                    second as i64 * SECOND_FACTOR + nanosecond as i64 / 100))
    }

    /// Factory method for LongDate up to second precision.
    pub fn from_ymd_hms(y: i32, m: u32, d: u32, hour: u32, minute: u32, second: u32)
                        -> Result<LongDate, &'static str> {
        LongDate::from_ymd_hms_n(y, m, d, hour, minute, second, 0)
    }

    /// Factory method for LongDate up to day precision.
    pub fn from_ymd(y: i32, m: u32, d: u32) -> Result<LongDate, &'static str> {
        LongDate::from_ymd_hms_n(y, m, d, 0, 0, 0, 0)
    }


    fn as_ymd_hms_f(&self) -> (i32, u32, u32, u32, u32, u32, u32) {
        let value = match self.0 {
            0 => 0,       // maps the special value '' == 0 to '0001-01-01 00:00:00.000000000' = 1
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
        (year, month, day, hour, minute, second, fraction)
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
