use chrono::{DateTime,UTC};
use chrono::offset::TimeZone;

const SECOND_FACTOR: i64 =      10_000_000;
const MINUTE_FACTOR: i64 =     600_000_000;   // 10_000_000 * 60;
const HOUR_FACTOR: i64   =  36_000_000_000;   // 10_000_000 * 60 * 60;
const DAY_FACTOR: i64    = 864_000_000_000;   // 10_000_000 * 60 * 60 * 24;

const ZEITENWENDE: i64   = 1721424;
const JGREG: i64         = 2299161;

#[derive(Clone,Debug,Deserialize,Serialize)]
pub struct LongDate(pub i64);

impl LongDate {
    // see jdbc/translators/LongDateTranslator.java, getTimestamp()
    pub fn datetime_utc(&self) -> DateTime<UTC> {
        let value = self.0 - 1;

        let datevalue = value / DAY_FACTOR;
        let mut timevalue = value - (datevalue * DAY_FACTOR);
        let hour: u32 = (timevalue / HOUR_FACTOR) as u32;
        timevalue -= HOUR_FACTOR * (hour as i64);
        let minute:u32 = (timevalue / MINUTE_FACTOR) as u32;
        timevalue -= MINUTE_FACTOR * (minute as i64);
        let second:u32 = (timevalue / SECOND_FACTOR) as u32;
        timevalue -= SECOND_FACTOR * (second as i64);
        let fraction: u32 = timevalue as u32; // 10**-7

        let julian: i64 = datevalue + ZEITENWENDE;
        let ja: i64 = if julian >= JGREG {
            let jalpha: i64 = (((julian - 1867216) as f64 - 0.25_f64) / 36524.25_f64) as i64;
            julian + 1 + jalpha - ( (0.25_f64 * jalpha as f64) as i64)
        } else {
            julian
        };

        let jb: i64 = ja + 1524;
        let jc: i64 = (6680_f64 + ((jb - 2439870) as f64 - 122.1_f64) / 365.25_f64) as i64;
        let jd: i64 = ((365 * jc) as f64 + (0.25_f64 * jc as f64)) as i64;
        let je: i64 = ((jb - jd) as f64 / 30.6001) as i64;

        let day: u32 = (jb - jd - ((30.6001 * je as f64) as i64)) as u32;
        let mut month: u32 = je as u32 - 1;
        let mut year: i32 = jc as i32- 4715;

        if month > 12 { month -= 12; }
        if month >  2 { year -= 1; }
        if year <=  0 { year -= 1; }

        UTC.ymd(year, month, day).and_hms_nano(hour, minute, second, fraction*100)
    }
}
