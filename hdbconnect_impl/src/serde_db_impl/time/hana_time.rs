use crate::ToHana;
use serde::ser::Error as _;
use std::str::FromStr;
use time::{Time, format_description::FormatItem, macros::format_description};

/// Wraps a `time::Time`, helps with serializing from and deserializing into `time::Time`.
///
/// # Example for serialization
/// ```rust, no_run
/// use hdbconnect::ToHana;
/// use time::{macros::time,Time};
/// # let stmt = "...";
/// # let mut connection = hdbconnect::Connection::new("...").unwrap();
/// let ts: Time = time!(02:02:02.200000000);
/// let response = connection.prepare_and_execute(stmt, &(ts.to_hana())).unwrap();
/// ```
///
/// # Example for deserialization
///
/// Deserialize into `HanaTime`,
/// then use `deref()` or `to_inner()` to access the contained `Time`.
///
/// ```rust, no_run
///  use hdbconnect::time::HanaTime;
/// # let the_query = "...";
/// # let mut connection = hdbconnect::Connection::new("...").unwrap();
///  let times: Vec<HanaTime> = connection.query(the_query).unwrap().try_into().unwrap();
///  let hour = (*times[0]).hour();
/// ```
#[derive(Debug)]
pub struct HanaTime(pub Time);
impl HanaTime {
    /// Consumes the `HanaTime`, returning the wrapped `Time`.
    #[must_use]
    pub fn into_inner(self) -> Time {
        self.0
    }
}
impl std::ops::Deref for HanaTime {
    type Target = Time;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// ***********
// deserialize
// ***********
impl<'de> serde::de::Deserialize<'de> for HanaTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(HanaTimeVisitor)
    }
}
impl FromStr for HanaTime {
    type Err = time::error::Parse;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // subsecond is optional
        const DATE_T_TIME: &[FormatItem<'static>] = format_description!("[hour]:[minute]:[second]");
        const DATE_T_TIME_SUB: &[FormatItem<'static>] =
            format_description!("[hour]:[minute]:[second].[subsecond]");

        Time::parse(s, &DATE_T_TIME_SUB)
            .or_else(|_| Time::parse(s, &DATE_T_TIME))
            .map(HanaTime)
    }
}

pub(in crate::serde_db_impl) struct HanaTimeVisitor;
impl serde::de::Visitor<'_> for HanaTimeVisitor {
    type Value = HanaTime;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "a String in the form [hour]:[minute]:[second].[subsecond]"
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<HanaTime, E>
    where
        E: serde::de::Error,
    {
        HanaTime::from_str(value).map_err(E::custom)
    }
}

/// Helper method for deserializing database values into values of type `time::Time`.
///
/// # Example
///
/// Use serde's annotation `serde(deserialize_with = "..")` to refer to this method:
///
/// ```rust
///     use time::Time;
///     #[derive(serde::Deserialize)]
///     struct WithTs {
///         #[serde(deserialize_with = "hdbconnect::time::to_time")]
///         ts_o: Time,
///     }
/// ```
///
/// Unfortunately, the serde-annotation `deserialize_with` does not cover all cases,
/// since it can only be applied to struct fields;
/// it cannot be applied if you want to deserialize into a `Vec<Time>`
/// or a plain `Time`.
/// The best you can do then is to deserialize instead into [`HanaTime`] and use
/// `deref()` or `into_inner()` to access the contained `time::Time`.
#[allow(clippy::missing_errors_doc)]
pub fn to_time<'de, D>(input: D) -> Result<Time, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    input
        .deserialize_str(HanaTimeVisitor)
        .map(HanaTime::into_inner)
}

//
// serialize
//

impl ToHana<HanaTime> for Time {
    fn to_hana(self) -> HanaTime {
        HanaTime(self)
    }
}

impl serde::ser::Serialize for HanaTime {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        const TIME_9: &[FormatItem<'static>] =
            format_description!("[hour]:[minute]:[second].[subsecond digits:9]");

        serializer.serialize_str(
            &self
                .0
                .format(TIME_9)
                .map_err(|_| S::Error::custom("failed formatting `Time`"))?,
        )
    }
}
