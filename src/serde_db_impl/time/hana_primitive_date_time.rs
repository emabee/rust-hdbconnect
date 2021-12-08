use crate::ToHana;
use serde::ser::Error as _;
use std::str::FromStr;
use time::{format_description::FormatItem, macros::format_description, PrimitiveDateTime};

/// Wraps a `time::PrimitiveDateTime`, helps with serializing from and deserializing
/// into `time::PrimitiveDateTime`.
///
/// # Example for serialization
/// ```rust, no_run
/// use hdbconnect::time::ToHana;
/// let ts: PrimitiveDateTime = datetime!(2012-02-02 02:02:02.200000000 +2);
/// let response = connection.prepare_and_execute(stmt, &(ts.to_hana()))?;
/// ```
///
/// # Example for deserialization
///
/// Deserialize into `HanaPrimitiveDateTime`,
/// then use `deref()` or `to_inner()` to access the contained `PrimitiveDateTime`.
///
/// ```rust, no_run
///  let dates: Vec<HanaPrimitiveDateTime> = connection.query(the_query)?.try_into()?;
///  let year = (*dates[0]).year();
/// ```
#[derive(Debug)]
pub struct HanaPrimitiveDateTime(pub PrimitiveDateTime);
impl HanaPrimitiveDateTime {
    /// Consumes the `HanaPrimitiveDateTime`, returning the wrapped `PrimitiveDateTime`.
    pub fn into_inner(self) -> PrimitiveDateTime {
        self.0
    }
}
impl std::ops::Deref for HanaPrimitiveDateTime {
    type Target = PrimitiveDateTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// ***********
// deserialize
// ***********
impl<'de> serde::de::Deserialize<'de> for HanaPrimitiveDateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(HanaPrimitiveDateTimeVisitor)
    }
}
impl FromStr for HanaPrimitiveDateTime {
    type Err = time::error::Parse;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // subsecond is optional
        const DATE_T_TIME: &[FormatItem<'static>] =
            format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
        const DATE_T_TIME_SUB: &[FormatItem<'static>] =
            format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]");

        PrimitiveDateTime::parse(s, &DATE_T_TIME_SUB)
            .or_else(|_| PrimitiveDateTime::parse(s, &DATE_T_TIME))
            .map(HanaPrimitiveDateTime)
    }
}

pub(in crate::serde_db_impl) struct HanaPrimitiveDateTimeVisitor;
impl<'de> serde::de::Visitor<'de> for HanaPrimitiveDateTimeVisitor {
    type Value = HanaPrimitiveDateTime;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "a String in the form [year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]"
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<HanaPrimitiveDateTime, E>
    where
        E: serde::de::Error,
    {
        HanaPrimitiveDateTime::from_str(value).map_err(E::custom)
    }
}

/// Helper method for deserializing database values
/// into values of type `time::PrimitiveDateTime`.
///
/// Since HANA's types [`LongDate`](crate::types::LongDate) and
/// [`SecondDate`](crate::types::SecondDate) have no understanding of time zones,
/// they deserialize naturally into `PrimitiveDateTime` values.
///
/// # Example
///
/// Use serde's annotation `serde(deserialize_with = "..")` to refer to this method:
///
/// ```rust
///     #[derive(Deserialize)]
///     struct WithTs {
///         #[serde(deserialize_with = "hdbconnect::time::to_primitive_date_time")]
///         ts_o: PrimitiveDateTime,
///     }
/// ```
///
/// Unfortunately, the serde-annotation `deserialize_with` does not cover all cases,
/// since it can only be applied to struct fields;
/// it cannot be applied if you want to deserialize into a `Vec<PrimitiveDateTime>`
/// or a plain `PrimitiveDateTime`.
/// The best you can do then is to deserialize instead into [`HanaPrimitiveDateTime`] and use
/// `deref()` or `into_inner()` to access the contained `time::PrimitivetDateTime`.
#[allow(clippy::missing_errors_doc)]
pub fn to_primitive_date_time<'de, D>(input: D) -> Result<PrimitiveDateTime, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    input
        .deserialize_str(HanaPrimitiveDateTimeVisitor)
        .map(HanaPrimitiveDateTime::into_inner)
}

//
// serialize
//

impl ToHana<HanaPrimitiveDateTime> for PrimitiveDateTime {
    fn to_hana(self) -> HanaPrimitiveDateTime {
        HanaPrimitiveDateTime(self)
    }
}

impl serde::ser::Serialize for HanaPrimitiveDateTime {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        const DATE_T_TIME_9: &[FormatItem<'static>] = format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]"
        );

        serializer.serialize_str(
            &self
                .0
                .format(DATE_T_TIME_9)
                .map_err(|_| S::Error::custom("failed formatting `PrimitiveDateTime`"))?,
        )
    }
}
