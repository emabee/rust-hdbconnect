#[cfg(feature = "sync")]
use crate::conn::SyncAmConnCore;
#[cfg(feature = "sync")]
use crate::sync_prepared_statement_core::AmPsCore;
#[cfg(feature = "sync")]
use std::sync::Mutex;

#[cfg(feature = "async")]
use crate::async_prepared_statement_core::AmPsCore;
#[cfg(feature = "async")]
use crate::conn::AsyncAmConnCore;
#[cfg(feature = "async")]
use tokio::sync::Mutex;

use super::rs_state::ResultSetCore;
use super::RsState;
use crate::protocol::parts::{Parts, ResultSetMetadata, StatementContext};
use crate::protocol::{util, Part, PartAttributes, PartKind, ServerUsage};
use crate::{HdbResult, Row, Rows};
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
    state: Arc<Mutex<RsState>>,
}

impl ResultSet {
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
    #[cfg(feature = "sync")]
    pub fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Resultset::try_into()");
        let rows: Rows = self.state.lock()?.into_rows(Arc::clone(&self.metadata))?;
        Ok(DeserializableResultset::try_into(rows)?)
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
    #[cfg(feature = "async")]
    pub async fn try_into<'de, T>(self) -> HdbResult<T>
    where
        T: serde::de::Deserialize<'de>,
    {
        trace!("Resultset::async_try_into()");
        Ok(DeserializableResultset::try_into(self.into_rows().await?)?)
    }

    // fetches all rows and all data of contained LOBs
    #[cfg(feature = "async")]
    pub async fn into_rows(self) -> HdbResult<Rows> {
        self.state
            .lock()
            .await
            .into_rows(Arc::clone(&self.metadata))
            .await
    }

    /// Converts the resultset into a single row.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the resultset contains more than a single row, or is empty.
    #[cfg(feature = "sync")]
    pub fn into_single_row(self) -> HdbResult<Row> {
        let mut state = self.state.lock()?;
        state.single_row()
    }

    /// Converts the resultset into a single row.
    ///
    /// # Errors
    ///
    /// `HdbError::Usage` if the resultset contains more than a single row, or is empty.
    #[cfg(feature = "async")]
    pub async fn into_single_row(self) -> HdbResult<Row> {
        let mut state = self.state.lock().await;
        state.single_row().await
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
    #[cfg(feature = "sync")]
    pub fn total_number_of_rows(&self) -> HdbResult<usize> {
        self.state.lock()?.sync_total_number_of_rows(&self.metadata)
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
    #[cfg(feature = "async")]
    pub async fn total_number_of_rows(&self) -> HdbResult<usize> {
        self.state
            .lock()
            .await
            .async_total_number_of_rows(&self.metadata)
            .await
    }

    /// Removes the next row and returns it, or None if the `ResultSet` is empty.
    ///
    /// Consequently, the `ResultSet` has one row less after the call.
    /// May need to fetch further rows from the database, which can fail.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    #[cfg(feature = "sync")]
    pub fn next_row(&mut self) -> HdbResult<Option<Row>> {
        self.state.lock()?.sync_next_row(&self.metadata)
    }

    /// Removes the next row and returns it, or None if the `ResultSet` is empty.
    ///
    /// Consequently, the `ResultSet` has one row less after the call.
    /// May need to fetch further rows from the database, which can fail.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` are possible.
    #[cfg(feature = "async")]
    pub async fn next_row(&mut self) -> HdbResult<Option<Row>> {
        self.state.lock().await.async_next_row(&self.metadata).await
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
    #[cfg(feature = "sync")]
    pub fn fetch_all(&self) -> HdbResult<()> {
        self.state.lock()?.sync_fetch_all(&self.metadata)
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
    #[cfg(feature = "async")]
    pub async fn fetch_all(&self) -> HdbResult<()> {
        self.state
            .lock()
            .await
            .async_fetch_all(&self.metadata)
            .await
    }

    pub(crate) fn new(
        #[cfg(feature = "sync")] am_conn_core: &SyncAmConnCore,
        #[cfg(feature = "async")] am_conn_core: &AsyncAmConnCore,
        attrs: PartAttributes,
        rs_id: u64,
        a_rsmd: Arc<ResultSetMetadata>,
        o_stmt_ctx: Option<StatementContext>,
    ) -> Self {
        let mut server_usage: ServerUsage = ServerUsage::default();

        if let Some(stmt_ctx) = o_stmt_ctx {
            server_usage.update(
                stmt_ctx.server_processing_time(),
                stmt_ctx.server_cpu_time(),
                stmt_ctx.server_memory_usage(),
            );
        }

        Self {
            metadata: a_rsmd,
            state: Arc::new(Mutex::new(RsState {
                o_am_rscore: Some(ResultSetCore::new_am_rscore(am_conn_core, attrs, rs_id)),
                next_rows: Vec::<Row>::new(),
                row_iter: Vec::<Row>::new().into_iter(),
                server_usage,
            })),
        }
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
    #[cfg(feature = "sync")]
    pub(crate) fn parse_sync(
        no_of_rows: usize,
        attributes: PartAttributes,
        parts: &mut Parts,
        am_conn_core: &SyncAmConnCore,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_rs: &mut Option<&mut RsState>,
        rdr: &mut dyn std::io::Read,
    ) -> std::io::Result<Option<Self>> {
        match *o_rs {
            None => {
                // case a) or b)
                let o_stmt_ctx = match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(stmt_ctx)) => Some(stmt_ctx),
                    None => None,
                    Some(_) => return Err(util::io_error("Inconsistent StatementContext")),
                };

                let Some(Part::ResultSetId(rs_id)) = parts.pop() else {
                    return Err(util::io_error("ResultSetId missing"));
                };

                let a_rsmd = match parts.pop_if_kind(PartKind::ResultSetMetadata) {
                    Some(Part::ResultSetMetadata(rsmd)) => Arc::new(rsmd),
                    None => match o_a_rsmd {
                        Some(a_rsmd) => Arc::clone(a_rsmd),
                        None => return Err(util::io_error("No metadata provided for ResultSet")),
                    },
                    Some(_) => {
                        return Err(util::io_error(
                            "Inconsistent metadata part found for ResultSet",
                        ));
                    }
                };

                let rs = Self::new(am_conn_core, attributes, rs_id, a_rsmd, o_stmt_ctx);
                rs.sync_parse_rows(no_of_rows, rdr)?;
                Ok(Some(rs))
            }

            Some(ref mut fetching_state) => {
                match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(stmt_ctx)) => {
                        fetching_state.server_usage.update(
                            stmt_ctx.server_processing_time(),
                            stmt_ctx.server_cpu_time(),
                            stmt_ctx.server_memory_usage(),
                        );
                    }
                    None => {}
                    Some(_) => {
                        return Err(util::io_error(
                            "Inconsistent StatementContext part found for ResultSet",
                        ));
                    }
                };

                if let Some(ref mut am_rscore) = fetching_state.o_am_rscore {
                    let mut rscore = am_rscore
                        .lock()
                        .map_err(|e| util::io_error(e.to_string()))?;
                    rscore.attributes = attributes;
                }
                let a_rsmd = if let Some(a_rsmd) = o_a_rsmd {
                    Arc::clone(a_rsmd)
                } else {
                    return Err(util::io_error("RsState provided without RsMetadata"));
                };
                fetching_state.parse_rows_sync(no_of_rows, &a_rsmd, rdr)?;
                Ok(None)
            }
        }
    }

    #[cfg(feature = "async")]
    pub(crate) async fn parse_async<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        no_of_rows: usize,
        attributes: PartAttributes,
        parts: &mut Parts<'static>,
        am_conn_core: &AsyncAmConnCore,
        o_a_rsmd: Option<&Arc<ResultSetMetadata>>,
        o_rs: &mut Option<&mut RsState>,
        rdr: &mut R,
    ) -> std::io::Result<Option<Self>> {
        match *o_rs {
            None => {
                // case a) or b)
                let o_stmt_ctx = match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(stmt_ctx)) => Some(stmt_ctx),
                    None => None,
                    Some(_) => return Err(util::io_error("Inconsistent StatementContext")),
                };

                let Some(Part::ResultSetId(rs_id)) = parts.pop() else {
                    return Err(util::io_error("ResultSetId missing"));
                };

                let a_rsmd = match parts.pop_if_kind(PartKind::ResultSetMetadata) {
                    Some(Part::ResultSetMetadata(rsmd)) => Arc::new(rsmd),
                    None => match o_a_rsmd {
                        Some(a_rsmd) => Arc::clone(a_rsmd),
                        None => return Err(util::io_error("No metadata provided for ResultSet")),
                    },
                    Some(_) => {
                        return Err(util::io_error(
                            "Inconsistent metadata part found for ResultSet",
                        ));
                    }
                };

                let rs = Self::new(am_conn_core, attributes, rs_id, a_rsmd, o_stmt_ctx);
                rs.async_parse_rows(no_of_rows, rdr).await?;
                Ok(Some(rs))
            }

            Some(ref mut fetching_state) => {
                match parts.pop_if_kind(PartKind::StatementContext) {
                    Some(Part::StatementContext(stmt_ctx)) => {
                        fetching_state.server_usage.update(
                            stmt_ctx.server_processing_time(),
                            stmt_ctx.server_cpu_time(),
                            stmt_ctx.server_memory_usage(),
                        );
                    }
                    None => {}
                    Some(_) => {
                        return Err(util::io_error(
                            "Inconsistent StatementContext part found for ResultSet",
                        ));
                    }
                };

                if let Some(ref mut am_rscore) = fetching_state.o_am_rscore {
                    let mut rscore = am_rscore.lock().await;
                    rscore.attributes = attributes;
                }
                let a_rsmd = if let Some(a_rsmd) = o_a_rsmd {
                    Arc::clone(a_rsmd)
                } else {
                    return Err(util::io_error("RsState provided without RsMetadata"));
                };
                fetching_state
                    .parse_rows_async(no_of_rows, &a_rsmd, rdr)
                    .await?;
                Ok(None)
            }
        }
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `ResultSet` object.
    #[cfg(feature = "sync")]
    pub fn server_usage(&self) -> ServerUsage {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .server_usage
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `ResultSet` object.
    #[cfg(feature = "async")]
    pub async fn server_usage(&self) -> ServerUsage {
        self.state.lock().await.server_usage
    }

    #[cfg(feature = "sync")]
    fn sync_parse_rows(
        &self,
        no_of_rows: usize,
        rdr: &mut dyn std::io::Read,
    ) -> std::io::Result<()> {
        self.state
            .lock()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
            .parse_rows_sync(no_of_rows, &self.metadata, rdr)
    }

    #[cfg(feature = "async")]
    async fn async_parse_rows<R: std::marker::Unpin + tokio::io::AsyncReadExt>(
        &self,
        no_of_rows: usize,
        rdr: &mut R,
    ) -> std::io::Result<()> {
        self.state
            .lock()
            .await
            .parse_rows_async(no_of_rows, &self.metadata, rdr)
            .await
    }

    #[cfg(feature = "sync")]
    pub fn inject_statement_id(&mut self, am_ps_core: AmPsCore) -> HdbResult<()> {
        if let Some(rs_core) = &(self.state.lock()?).o_am_rscore {
            rs_core.lock()?.inject_statement_id(am_ps_core);
        }
        Ok(())
    }
    #[cfg(feature = "async")]
    pub async fn inject_statement_id(&mut self, am_ps_core: AmPsCore) -> HdbResult<()> {
        if let Some(rs_core) = &(self.state.lock().await).o_am_rscore {
            rs_core.lock().await.inject_statement_id(am_ps_core);
        }
        Ok(())
    }
}

impl std::fmt::Display for ResultSet {
    // Writes a header and then the data
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(fmt, "{}\n", &self.metadata)?;

        #[cfg(feature = "sync")]
        {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for row in state.row_iter.as_slice() {
                writeln!(fmt, "{}\n", &row)?;
            }
            for row in &state.next_rows {
                writeln!(fmt, "{}\n", &row)?;
            }
        }

        #[cfg(feature = "async")]
        {
            writeln!(fmt, "Display not implemented for async\n")?;
        }

        Ok(())
    }
}

#[cfg(feature = "sync")]
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

// This is a poor replacement for an "impl AsyncIterator for ResultSet"
// see https://rust-lang.github.io/rfcs/2996-async-iterator.html for reasoning
#[cfg(feature = "async")]
impl ResultSet {
    pub async fn next(&mut self) -> Option<HdbResult<Row>> {
        match self.next_row().await {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
