/// Helper trait for serialization.
///
/// # Example for serialization
/// ```rust, no_run
/// use hdbconnect::ToHana;
/// let ts: OffsetDateTime = datetime!(2012-02-02 02:02:02.200000000 +2);
/// let response = connection.prepare_and_execute(stmt, &(ts.to_hana()))?;
/// ```
pub trait ToHana<T> {
    ///
    fn to_hana(self) -> T;
}
