use crate::protocol::{util, util_async};
use crate::HdbValue;
use byteorder::{LittleEndian, ReadBytesExt};

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

impl std::fmt::Display for SecondDate {
    // The format chosen supports the conversion to chrono types.
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        let (year, month, day, hour, minute, second) = self.as_ymd_hms();
        write!(
            fmt,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        )
    }
}

impl std::cmp::PartialEq<SecondDate> for SecondDate {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl SecondDate {
    pub(crate) fn new(raw: i64) -> Self {
        Self(raw)
    }

    pub(crate) fn ref_raw(&self) -> &i64 {
        &self.0
    }

    // Convert into tuple of "elements".
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn as_ymd_hms(&self) -> (i32, u8, u8, u8, u8, u8) {
        let value = match self.0 {
            0 => 0, // maps the special value '' == 0 to '0001-01-01 00:00:00.000000000' = 1
            v => v - 1,
        };

        let datevalue = value / DAY_FACTOR;
        let mut timevalue = value - (datevalue * DAY_FACTOR);
        let hour: u8 = (timevalue / HOUR_FACTOR) as u8;
        timevalue -= HOUR_FACTOR * (i64::from(hour));
        let minute: u8 = (timevalue / MINUTE_FACTOR) as u8;
        timevalue -= MINUTE_FACTOR * (i64::from(minute));
        let second: u8 = (timevalue / SECOND_FACTOR) as u8;

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

        let day: u8 = (jb - jd - ((30.6001 * je as f64) as i64)) as u8;
        let mut month: u8 = je as u8 - 1;
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

pub(crate) fn parse_seconddate_sync(
    nullable: bool,
    rdr: &mut dyn std::io::Read,
) -> std::io::Result<HdbValue<'static>> {
    let i = rdr.read_i64::<LittleEndian>()?;
    if i == NULL_REPRESENTATION {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error(
                "found NULL value for NOT NULL SECONDDATE column",
            ))
        }
    } else {
        Ok(HdbValue::SECONDDATE(SecondDate::new(i)))
    }
}

pub(crate) async fn parse_seconddate_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
    nullable: bool,
    rdr: &mut R,
) -> std::io::Result<HdbValue<'static>> {
    let i = util_async::read_i64(rdr).await?;
    if i == NULL_REPRESENTATION {
        if nullable {
            Ok(HdbValue::NULL)
        } else {
            Err(util::io_error(
                "found NULL value for NOT NULL SECONDDATE column",
            ))
        }
    } else {
        Ok(HdbValue::SECONDDATE(SecondDate::new(i)))
    }
}
