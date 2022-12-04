use super::hana_primitive_date_time::HanaPrimitiveDateTimeVisitor;
use crate::ToHana;
use serde::ser::Error as _;
use time::{format_description::FormatItem, macros::format_description, OffsetDateTime};

/// Wraps a `time::OffsetDateTime`, helps with serializing from and deserializing
/// into `time::OffsetDateTime`.
///
/// Note that this is completely based on
/// [`time::HanaPrimitiveDateTime`](crate::time::HanaPrimitiveDateTime),
/// since HANA's own date formats have no understanding of timezones.
/// All deserialized instances of `OffsetDateTime` have offset `UTC`.
/// All serialized instances of `OffsetDateTime` _must_ have offset `UTC`.
///
/// # Example for serialization
/// ```rust, no_run
/// # let stmt = "...";
/// use hdbconnect::ToHana;
/// use time::{macros::datetime, OffsetDateTime};
/// # let connection = hdbconnect::Connection::new("...").unwrap();
/// let ts: OffsetDateTime = datetime!(2012-02-02 02:02:02.200000000 +2);
/// let response = connection.prepare_and_execute(stmt, &(ts.to_hana())).unwrap();
/// ```
///
/// # Example for deserialization
///
/// Deserialize into `HanaOffsetDateTime`,
/// then use `deref` or `to_inner()` to access the contained `OffsetDateTime`.
///
/// ```rust, no_run
/// use hdbconnect::time::HanaOffsetDateTime;
/// # let the_query = "...";
/// # let mut connection = hdbconnect::Connection::new("...").unwrap();
/// let dates: Vec<HanaOffsetDateTime> = connection.query(the_query).unwrap().try_into().unwrap();
/// let year = (*dates[0]).year();
/// ```
#[derive(Debug)]
pub struct HanaOffsetDateTime(OffsetDateTime);
impl HanaOffsetDateTime {
    /// Consumes the `HanaOffsetDateTime`, returning the wrapped `OffsetDateTime`.
    pub fn into_inner(self) -> OffsetDateTime {
        self.0
    }
}
impl std::ops::Deref for HanaOffsetDateTime {
    type Target = OffsetDateTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// ***********
// deserialize
// ***********
impl<'de> serde::de::Deserialize<'de> for HanaOffsetDateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_str(HanaOffsetDateTimeVisitor)
    }
}

struct HanaOffsetDateTimeVisitor;
impl<'de> serde::de::Visitor<'de> for HanaOffsetDateTimeVisitor {
    type Value = HanaOffsetDateTime;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        HanaPrimitiveDateTimeVisitor.expecting(formatter)
    }

    fn visit_str<E>(self, value: &str) -> Result<HanaOffsetDateTime, E>
    where
        E: serde::de::Error,
    {
        Ok(HanaOffsetDateTime(
            OffsetDateTime::now_utc()
                .replace_date_time(HanaPrimitiveDateTimeVisitor.visit_str(value)?.into_inner()),
        ))
    }
}

/// Helper method for deserializing database values
/// into values of type `time::OffsetDateTime`.
///
/// Since HANA's types [`LongDate`](crate::types::LongDate) and
/// [`SecondDate`](crate::types::SecondDate) have no understanding of time zones,
/// they deserialize only into `OffsetDateTime` values with zero offset
/// (offset =
/// [`time::UtcOffset::UTC`](https://docs.rs/time/latest/time/struct.UtcOffset.html#associatedconstant.UTC)).
///
/// # Example
///
/// Use serde's annotation `serde(deserialize_with = "..")` to refer to this method:
///
/// ```rust
///     #[derive(serde::Deserialize)]
///     struct WithTs {
///         #[serde(deserialize_with = "hdbconnect::time::to_offset_date_time")]
///         ts_o: time::OffsetDateTime,
///     }
/// ```
///
/// Unfortunately, the serde-annotation `deserialize_with` does not cover all cases,
/// since it can only be applied to struct fields;
/// it cannot be applied if you want to deserialize into a `Vec<OffsetDateTime>`
/// or a plain `OffsetDateTime`.
/// The best you can do then is deserialize instead into [`HanaOffsetDateTime`] and use
/// `deref()` or `into_inner()` to access the contained `time::OffsetDateTime`.
#[allow(clippy::missing_errors_doc)]
pub fn to_offset_date_time<'de, D>(input: D) -> Result<OffsetDateTime, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    input
        .deserialize_str(HanaOffsetDateTimeVisitor)
        .map(HanaOffsetDateTime::into_inner)
}

//
// serialize
//

impl ToHana<HanaOffsetDateTime> for OffsetDateTime {
    fn to_hana(self) -> HanaOffsetDateTime {
        HanaOffsetDateTime(self)
    }
}

impl serde::ser::Serialize for HanaOffsetDateTime {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        const DATE_T_TIME: &[FormatItem<'static>] = format_description!(
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]"
        );

        serializer.serialize_str(
            &self
                .0
                .format(DATE_T_TIME)
                .map_err(|_| S::Error::custom("failed formatting `OffsetDateTime`"))?,
        )
    }
}
