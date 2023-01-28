/// Helper trait for serialization.
///
/// Helps providing parameters to prepared statements.
///
/// # Example for serialization
/// ```rust, no_run
/// use hdbconnect::{ToHana,time::HanaOffsetDateTime};
///
/// use time::{macros::datetime,OffsetDateTime};
///
/// # let stmt = "...";
/// # let mut connection = hdbconnect::Connection::new("...").unwrap();
/// let ts: OffsetDateTime = datetime!(2012-02-02 02:02:02.200000000 +2);
/// let response = connection.prepare_and_execute(stmt, &(ts.to_hana())).unwrap();
/// ```
pub trait ToHana<T> {
    ///
    fn to_hana(self) -> T;
}
