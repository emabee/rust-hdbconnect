use crate::{
    base::{RsState, XMutexed},
    protocol::{parts::ResultSetMetadata, ServerUsage},
    HdbResult, HdbValue, Row, Rows,
};
use serde_db::de::DeserializableResultSet;
use std::sync::Arc;

/// The result of a database query.
///
/// This behaves essentially like a set of `Row`s, and each `Row` is a set of `HdbValue`s.
///
/// In fact, result sets with a larger number of rows are not returned from the database
/// in a single roundtrip. `ResultSet` automatically fetches outstanding data
/// as soon as they are required.
///
/// The method [`try_into`](#method.try_into) converts the data from this generic format
/// in a singe step into your application specific format.
///
/// Due to the chunk-wise data transfer, which has to happen asynchronously,
/// `ResultSet` cannot implement the synchronous trait `std::iter::Iterator`.
/// Use method [`next_row()`](#method.next_row) as a replacement.
///
/// ```
///
// (see also <https://rust-lang.github.io/rfcs/2996-async-iterator.html>)
#[derive(Debug)]
pub struct ResultSet {
    metadata: Arc<ResultSetMetadata>,
    state: Arc<XMutexed<RsState>>,
}

impl ResultSet {
    pub(crate) fn new(a_rsmd: Arc<ResultSetMetadata>, rs_state: RsState) -> Self {
        Self {
            metadata: a_rsmd,
            state: Arc::new(XMutexed::new_async(rs_state)),
        }
    }

    /// Conveniently translates the complete result set into a rust type that implements
    /// `serde::Deserialize` and has an adequate structure.
    /// The implementation of this method uses
    /// [`serde_db::de`](https://docs.rs/serde_db/latest/serde_db/de/index.html).
    ///
    /// A result set is essentially a two-dimensional structure, given as a list
    /// of rows, where each row is a list of fields; the name of each field is
    /// given in the metadata of the result set.
    ///
    /// The method supports a variety of target data structures, with the only
    /// strong limitation that no data loss is supported.
    ///
    /// It depends on the dimension of the result set what target data
    /// structure   you can choose for deserialization:
    ///
    /// * You can always use a `Vec<line_struct>`, if the elements of
    ///   `line_struct` match the field list of the result set.
    ///
    /// * If the result set contains only a single line (e.g. because you
    ///   specified `TOP 1` in your select clause),
    ///   then you can optionally choose to deserialize directly into a plain
    ///   `line_struct`.
    ///
    /// * If the result set contains only a single column, then you can
    ///   optionally choose to deserialize directly into a
    ///   `Vec<plain_field>`.
    ///
    /// * If the result set contains only a single value (one row with one
    ///   column), then you can optionally choose to deserialize into a
    ///   plain `line_struct`, or a `Vec<plain_field>`, or a `plain_field`.
    ///
    /// Also the translation of the individual field values provides flexibility.
    ///
    /// * You can e.g. convert values from a nullable column
    ///   into a plain field, provided that no NULL values are given in the
    ///   result set.
    ///
    /// * Vice versa, you can use an `Option<plain_field>`, even if the column is
    ///   marked as NOT NULL.
    ///
    /// * Similarly, integer types can differ, as long as the concrete values
    ///   can be assigned without loss.
    ///
    /// As usual with serde deserialization, you need to specify the type of your target variable
    /// explicitly, so that `try_into()` can derive the type it needs to instantiate:
    ///
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct Entity {
    ///     ...
    /// }
    /// let typed_result: Vec<Entity> = result_set.try_into()?;
    /// ```
    ///
    /// # Errors
    ///
    /// `HdbError::Deserialization` if the deserialization into the target type is not possible.
    pub async fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("ResultSet::try_into()");
        Ok(DeserializableResultSet::try_into(self.into_rows().await?)?)
    }

    /// Fetches all rows and all data of contained LOBs
    ///
    /// # Errors
    ///
    /// Various errors can occur.
    pub async fn into_rows(self) -> HdbResult<Rows> {
        self.state
            .lock_async()
            .await
            .as_rows_async(Arc::clone(&self.metadata))
            .await
    }

    /// Converts the result set into a single row.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the result set contains more than a single row, or is empty.
    pub async fn into_single_row(self) -> HdbResult<Row> {
        let mut state = self.state.lock_async().await;
        state.single_row_async().await
    }

    /// Converts the result set into a single value.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the result set contains more than a single value, or is empty.
    pub async fn into_single_value(self) -> HdbResult<HdbValue<'static>> {
        let mut state = self.state.lock_async().await;
        state.single_row_async().await?.into_single_value()
    }

    /// Access to metadata.
    ///
    /// ## Examples
    ///
    /// ```rust,ignore
    /// let rs: ResultSet;
    /// //...
    /// // get the precision of the second field
    /// let prec: i16 = result_set.metadata()[1].precision();
    /// ```
    ///
    /// or
    ///
    /// ```rust,ignore
    /// let rs: ResultSet;
    /// //...
    /// for field_metadata in &*rs.metadata() {
    ///     // evaluate metadata of a field
    /// }
    /// ```
    #[must_use]
    pub fn metadata(&self) -> Arc<ResultSetMetadata> {
        Arc::clone(&self.metadata)
    }

    /// Returns the total number of rows in the result set,
    /// including those that still need to be fetched from the database,
    /// but excluding those that have already been removed from the result set.
    ///
    /// This method can be expensive, and it can fail, since it fetches all yet
    /// outstanding rows from the database.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub async fn total_number_of_rows(&self) -> HdbResult<usize> {
        self.state
            .lock_async()
            .await
            .total_number_of_rows_async(&self.metadata)
            .await
    }

    /// Removes the next row and returns it, or `Ok(None)` if the `ResultSet` is empty.
    ///
    /// Consequently, the `ResultSet` has one row less after the call.
    /// May need to fetch further rows from the database, which can fail.
    ///
    /// ```rust, no_run
    /// # use hdbconnect::{Connection,ConnectParams,HdbResult};
    /// # use serde::Deserialize;
    /// # fn main() -> HdbResult<()> {
    /// # #[derive(Debug, Deserialize)]
    /// # struct Entity();
    /// # let mut connection = Connection::new(ConnectParams::builder().build()?)?;
    /// # let query_str = "";
    /// let mut rs = connection.query(query_str).await?;
    /// while let Some(row) = rs.next_row().await? {
    ///     let entity: Entity = row.try_into()?;
    ///     println!("Got entity: {:?}", entity);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub async fn next_row(&mut self) -> HdbResult<Option<Row>> {
        self.state
            .lock_async()
            .await
            .next_row_async(&self.metadata)
            .await
    }

    /// Fetches all not yet transported result lines from the server.
    ///
    /// Bigger result sets are typically not transported in one roundtrip from the database;
    /// the number of roundtrips depends on the total number of rows in the result set
    /// and the configured fetch-size of the connection.
    ///
    /// Fetching can fail, e.g. if the network connection is broken.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub async fn fetch_all(&self) -> HdbResult<()> {
        self.state
            .lock_async()
            .await
            .fetch_all_async(&self.metadata)
            .await
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `ResultSet` object.
    pub async fn server_usage(&self) -> ServerUsage {
        *self.state.lock_async().await.server_usage()
    }
}

impl std::fmt::Display for ResultSet {
    // Writes a header and then the data
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(fmt, "{}\n", &self.metadata)?;
        // hard to do, because of the async lock we'd need to acquire
        writeln!(fmt, "Display not implemented for async result set\n")?;

        Ok(())
    }
}
