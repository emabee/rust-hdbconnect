use crate::{
    base::{RsState, XMutexed},
    protocol::{parts::ResultSetMetadata, ServerUsage},
    HdbResult, Row, Rows,
};

use serde_db::de::DeserializableResultset;
use std::sync::Arc;

/// The result of a database query.
///
/// This is essentially a set of `Row`s, and each `Row` is a set of `HdbValue`s.
///
/// The method [`try_into`](#method.try_into) converts the data from this generic format
/// in a singe step into your application specific format.
///
/// `ResultSet` implements `std::iter::Iterator`, so you can
/// directly iterate over the rows of a resultset.
/// While iterating, the not yet transported rows are fetched "silently" on demand, which can fail.
/// The Iterator-Item is thus not `Row`, but `HdbResult<Row>`.
///
/// ```rust, no_run
/// # use hdbconnect::{Connection,ConnectParams,HdbResult};
/// # use serde::Deserialize;
/// # fn main() -> HdbResult<()> {
/// # #[derive(Debug, Deserialize)]
/// # struct Entity();
/// # let mut connection = Connection::new(ConnectParams::builder().build()?)?;
/// # let query_string = "";
/// for row in connection.query(query_string)? {
///     // handle fetch errors and convert each line individually:
///     let entity: Entity = row?.try_into()?;
///     println!("Got entity: {:?}", entity);
/// }
/// # Ok(())
/// # }
///
/// ```
///
#[derive(Debug)]
pub struct ResultSet {
    metadata: Arc<ResultSetMetadata>,
    state: Arc<XMutexed<RsState>>,
}

impl ResultSet {
    // pub(crate) fn new(
    //     am_conn_core: &AmConnCore,
    //     attrs: PartAttributes,
    //     rs_id: u64,
    //     a_rsmd: Arc<ResultSetMetadata>,
    //     o_stmt_ctx: Option<StatementContext>,
    // ) -> Self {
    //     Self::new_1(
    //         a_rsmd,
    //         RsState::new_sync(o_stmt_ctx, am_conn_core, attrs, rs_id),
    //     )
    // }

    // FIXME rename
    pub(crate) fn new_1(a_rsmd: Arc<ResultSetMetadata>, rs_state: RsState) -> Self {
        Self {
            metadata: a_rsmd,
            state: Arc::new(XMutexed::new_sync(rs_state)),
        }
    }

    /// Conveniently translates the complete resultset into a rust type that implements
    /// `serde::Deserialize` and has an adequate structure.
    /// The implementation of this method uses
    /// [`serde_db::de`](https://docs.rs/serde_db/latest/serde_db/de/index.html).
    ///
    /// A resultset is essentially a two-dimensional structure, given as a list
    /// of rows, where each row is a list of fields; the name of each field is
    /// given in the metadata of the resultset.
    ///
    /// The method supports a variety of target data structures, with the only
    /// strong limitation that no data loss is supported.
    ///
    /// It depends on the dimension of the resultset what target data
    /// structure   you can choose for deserialization:
    ///
    /// * You can always use a `Vec<line_struct>`, if the elements of
    /// `line_struct` match the field list of the resultset.
    ///
    /// * If the resultset contains only a single line (e.g. because you
    /// specified `TOP 1` in your select clause),
    /// then you can optionally choose to deserialize directly into a plain
    /// `line_struct`.
    ///
    /// * If the resultset contains only a single column, then you can
    /// optionally choose to deserialize directly into a
    /// `Vec<plain_field>`.
    ///
    /// * If the resultset contains only a single value (one row with one
    /// column), then you can optionally choose to deserialize into a
    /// plain `line_struct`, or a `Vec<plain_field>`, or a `plain_field`.
    ///
    /// Also the translation of the individual field values provides flexibility.
    ///
    /// * You can e.g. convert values from a nullable column
    /// into a plain field, provided that no NULL values are given in the
    /// resultset.
    ///
    /// * Vice versa, you can use an `Option<plain_field>`, even if the column is
    /// marked as NOT NULL.
    ///
    /// * Similarly, integer types can differ, as long as the concrete values
    /// can   be assigned without loss.
    ///
    /// As usual with serde deserialization, you need to specify the type of your target variable
    /// explicitly, so that `try_into()` can derive the type it needs to instantiate:
    ///
    /// ```ignore
    /// #[derive(Deserialize)]
    /// struct Entity {
    ///     ...
    /// }
    /// let typed_result: Vec<Entity> = resultset.try_into()?;
    /// ```
    ///
    /// # Errors
    ///
    /// `HdbError::Deserialization` if the deserialization into the target type is not possible.
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Resultset::try_into()");
        let rows: Rows = self
            .state
            .lock_sync()?
            .into_rows_sync(Arc::clone(&self.metadata))?;
        Ok(DeserializableResultset::try_into(rows)?)
    }

    /// Converts the resultset into a single row.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the resultset contains more than a single row, or is empty.
    pub fn into_single_row(self) -> HdbResult<Row> {
        let mut state = self.state.lock_sync()?;
        state.single_row_sync()
    }

    /// Access to metadata.
    ///
    /// ## Examples
    ///
    /// ```rust,ignore
    /// let rs: ResultSet;
    /// //...
    /// // get the precision of the second field
    /// let prec: i16 = resultset.metadata()[1].precision();
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
    pub fn metadata(&self) -> Arc<ResultSetMetadata> {
        Arc::clone(&self.metadata)
    }

    /// Returns the total number of rows in the resultset,
    /// including those that still need to be fetched from the database,
    /// but excluding those that have already been removed from the resultset.
    ///
    /// This method can be expensive, and it can fail, since it fetches all yet
    /// outstanding rows from the database.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub fn total_number_of_rows(&self) -> HdbResult<usize> {
        self.state
            .lock_sync()?
            .total_number_of_rows_sync(&self.metadata)
    }

    /// Removes the next row and returns it, or None if the `ResultSet` is empty.
    ///
    /// Consequently, the `ResultSet` has one row less after the call.
    /// May need to fetch further rows from the database, which can fail.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub fn next_row(&mut self) -> HdbResult<Option<Row>> {
        self.state.lock_sync()?.next_row_sync(&self.metadata)
    }

    /// Fetches all not yet transported result lines from the server.
    ///
    /// Bigger resultsets are typically not transported in one roundtrip from the database;
    /// the number of roundtrips depends on the total number of rows in the resultset
    /// and the configured fetch-size of the connection.
    ///
    /// Fetching can fail, e.g. if the network connection is broken.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    pub fn fetch_all(&self) -> HdbResult<()> {
        self.state.lock_sync()?.fetch_all_sync(&self.metadata)
    }

    // resultsets can be part of the response in three cases which differ
    // in regard to metadata handling:
    //
    // a) a response to a plain "execute" will contain the metadata in one of the
    //    other parts; the metadata parameter will thus have the variant None
    //
    // b) a response to an "execute prepared" will only contain data;
    //    the metadata had beeen returned already to the "prepare" call, and are
    //    provided with parameter metadata
    //
    // c) a response to a "fetch more lines" is triggered from an older resultset
    //    which already has its metadata
    //
    // For first resultset packets, we create and return a new ResultSet object.
    // We then expect the previous three parts to be
    // a matching ResultSetMetadata, a ResultSetId, and a StatementContext.
    // pub(crate) fn parse(
    //     no_of_rows: usize,
    //     attributes: PartAttributes,
    //     parts: &mut Parts,
    //     am_conn_core: &AmConnCore,
    //     o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
    //     o_rs: &mut Option<&mut RsState>,
    //     rdr: &mut dyn std::io::Read,
    // ) -> HdbResult<Option<Self>> {
    //     match o_rs {
    //         None => {
    //             // case a) or b)
    //             let o_stmt_ctx = match parts.pop_if_kind(PartKind::StatementContext) {
    //                 Some(Part::StatementContext(stmt_ctx)) => Some(stmt_ctx),
    //                 None => None,
    //                 Some(_) => return Err(HdbError::Impl("Inconsistent StatementContext")),
    //             };

    //             let Some(Part::ResultSetId(rs_id)) = parts.pop() else {
    //                 return Err(HdbError::Impl("ResultSetId missing"));
    //             };

    //             let a_rsmd = match parts.pop_if_kind(PartKind::ResultSetMetadata) {
    //                 Some(Part::ResultSetMetadata(rsmd)) => Arc::new(rsmd),
    //                 None => match o_a_rsmd {
    //                     Some(a_rsmd) => Arc::clone(a_rsmd),
    //                     None => return Err(HdbError::Impl("No metadata provided for ResultSet")),
    //                 },
    //                 Some(_) => {
    //                     return Err(HdbError::Impl(
    //                         "Inconsistent metadata part found for ResultSet",
    //                     ));
    //                 }
    //             };

    //             let rs = Self::new(am_conn_core, attributes, rs_id, a_rsmd, o_stmt_ctx);
    //             rs.parse_rows(no_of_rows, rdr)?;
    //             Ok(Some(rs))
    //         }

    //         Some(fetching_state) => {
    //             match parts.pop_if_kind(PartKind::StatementContext) {
    //                 Some(Part::StatementContext(stmt_ctx)) => {
    //                     fetching_state.update_server_usage(stmt_ctx);
    //                 }
    //                 None => {}
    //                 Some(_) => {
    //                     return Err(HdbError::Impl(
    //                         "Inconsistent StatementContext part found for ResultSet",
    //                     ));
    //                 }
    //             };

    //             fetching_state.set_attributes_sync(attributes).ok();

    //             let a_rsmd = if let Some(a_rsmd) = o_a_rsmd {
    //                 Arc::clone(a_rsmd)
    //             } else {
    //                 return Err(HdbError::Impl("RsState provided without RsMetadata"));
    //             };
    //             fetching_state.parse_rows_sync(no_of_rows, &a_rsmd, rdr)?;
    //             Ok(None)
    //         }
    //     }
    // }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `ResultSet` object.
    pub fn server_usage(&self) -> HdbResult<ServerUsage> {
        Ok(self.state.lock_sync()?.server_usage().clone())
    }

    // fn parse_rows(&self, no_of_rows: usize, rdr: &mut dyn std::io::Read) -> HdbResult<()> {
    //     self.state
    //         .lock_sync()?
    //         .parse_rows_sync(no_of_rows, &self.metadata, rdr)
    // }
}

impl std::fmt::Display for ResultSet {
    // Writes a header and then the data
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(fmt, "{}\n", &self.metadata)?;

        writeln!(
            fmt,
            "{}",
            self.state
                .lock_sync()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        )?;
        Ok(())
    }
}

impl Iterator for ResultSet {
    type Item = HdbResult<Row>;
    fn next(&mut self) -> Option<HdbResult<Row>> {
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
