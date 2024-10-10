use crate::ToHana;
use serde::ser::Error as _;
use std::str::FromStr;
use time::{format_description::FormatItem, macros::format_description, Date};

/// Wraps a `time::Date`, helps with serializing from and deserializing into `time::Date`.
///
/// # Example for serialization
/// ```rust, no_run
/// use hdbconnect::ToHana;
/// use time::{macros::date, Date};
/// # let connection = hdbconnect::Connection::new("...").unwrap();
/// # let stmt = "";
/// let ts: Date = date!(2012-02-02);
/// let response = connection.prepare_and_execute(stmt, &(ts.to_hana())).unwrap();
/// ```
///
/// # Example for deserialization
///
/// Deserialize into `HanaDate`,
/// then use `deref()` or `to_inner()` to access the contained `Date`.
///
/// ```rust, no_run
///  use hdbconnect::{time::HanaDate, Connection, HdbResult};
///  # fn main() -> HdbResult<()> {
///  # let mut connection = Connection::new("...")?;
///  # let the_query = "...";
///
///  let times: Vec<HanaDate> = connection.query(the_query)?.try_into()?;
///  let day = (*times[0]).day();
///  Ok(())
///  # }
/// ```
#[derive(Debug)]
pub struct HanaDate(pub Date);
impl HanaDate {
    /// Consumes the `HanaDate`, returning the wrapped `Date`.
    #[must_use]
    pub fn into_inner(self) -> Date {
        self.0
    }
}
impl std::ops::Deref for HanaDate {
    type Target = Date;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// ***********
// deserialize
// ***********
impl<'de> serde::de::Deserialize<'de> for HanaDate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(HanaTimeVisitor)
    }
}
impl FromStr for HanaDate {
    type Err = time::error::Parse;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // subsecond is optional
        const DATE: &[FormatItem<'static>] = format_description!("[year]-[month]-[day]");

        Date::parse(s, &DATE).map(HanaDate)
    }
}

pub(in crate::serde_db_impl) struct HanaTimeVisitor;
impl serde::de::Visitor<'_> for HanaTimeVisitor {
    type Value = HanaDate;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a String in the form [year]-[month]-[day]")
    }

    fn visit_str<E>(self, value: &str) -> Result<HanaDate, E>
    where
        E: serde::de::Error,
    {
        HanaDate::from_str(value).map_err(E::custom)
    }
}

/// Helper method for deserializing database values
/// into values of type `time::Date`.
///
/// # Example
///
/// Use serde's annotation `serde(deserialize_with = "..")` to refer to this method:
///
/// ```rust
///     #[derive(serde::Deserialize)]
///     struct WithTs {
///         #[serde(deserialize_with = "hdbconnect::time::to_date")]
///         ts_o: time::Date,
///     }
/// ```
///
/// Unfortunately, the serde-annotation `deserialize_with` does not cover all cases,
/// since it can only be applied to struct fields;
/// it cannot be applied if you want to deserialize into a `Vec<Date>`
/// or a plain `Date`.
/// The best you can do then is to deserialize instead into [`HanaDate`] and use
/// `deref()` or `into_inner()` to access the contained `time::Date`.
#[allow(clippy::missing_errors_doc)]
pub fn to_date<'de, D>(input: D) -> Result<Date, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    input
        .deserialize_str(HanaTimeVisitor)
        .map(HanaDate::into_inner)
}

//
// serialize
//

impl ToHana<HanaDate> for Date {
    fn to_hana(self) -> HanaDate {
        HanaDate(self)
    }
}

impl serde::ser::Serialize for HanaDate {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        const DATE: &[FormatItem<'static>] = format_description!("[year]-[month]-[day]");

        serializer.serialize_str(
            &self
                .0
                .format(DATE)
                .map_err(|_| S::Error::custom("failed formatting `Date`"))?,
        )
    }
}
