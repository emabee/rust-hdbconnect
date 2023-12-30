use crate::{
    a_sync::HdbResponse,
    base::{new_am_async, InternalReturnValue, PreparedStatementCore, AM},
    conn::AmConnCore,
    protocol::parts::{
        HdbValue, LobFlags, ParameterDescriptors, ParameterRows, ResultSetMetadata, TypeId,
    },
    protocol::{MessageType, Part, PartKind, Request, ServerUsage, HOLD_CURSORS_OVER_COMMIT},
    types_impl::lob::async_lob_writer,
    HdbError, HdbResult,
};
use std::sync::Arc;

/// Allows injection-safe SQL execution and repeated calls of the same statement
/// with different parameters with as few roundtrips as possible.
///
/// # Providing Input Parameters
///
/// ## Type systems
///
/// Type system**s**, really? Yes, there are in fact four type systems involved!
/// * Your application is written in rust, and uses the _rust type system_.
/// * `hdbconnect_async`'s _driver API_ represents values with the `enum` [`HdbValue`](crate::HdbValue);
///   this type system aims to be as close to the rust type system as possible
///   and hides the complexity of the following two internal type systems.
/// * The _wire_ has its own type system - it's focus is on efficient data transfer.
///   `hdbconnect_async` deals with these types internally.
/// * The _database type system_ consists of the standard SQL types and proprietary types
///   to represent values, like TINYINT, FLOAT, NVARCHAR, and many others.
///   This type system is NOT directly visible to `hdbconnect_async`.
///
///   [`TypeId`](crate::TypeId) enumerates a somewhat reduced superset
///   of the server-side and the wire type system.
///
/// ## From Rust types to `HdbValue`
///
/// Prepared statements typically take one or more input parameter(s).
/// As part of the statement preparation, the database server provides the client
/// with detailed metadata for these parameters, which are kept by the `PreparedStatement`.
///
/// The parameter values can be handed over to the `PreparedStatement` either as
/// `Serializable` rust types, or explicitly as [`HdbValue`](crate::HdbValue) instances.
/// If they are handed over as `Serializable` rust types, then the built-in
/// [`serde_db`](https://docs.rs/serde_db)-based
/// conversion will convert them directly into those [`HdbValue`](crate::HdbValue) variants
/// that correspond to the `TypeId` that the server has requested.
/// The application can also provide the values explicitly as [`HdbValue`](crate::HdbValue)
/// instances and by that
/// enforce the usage of a different wire type and of server-side type conversions.
///
/// ## Sending `HdbValue`s to the database
///
/// The protocol for sending values can be version-specific. Sending e.g. an
/// `HdbValue::DECIMAL` to the database occurs in different formats:
/// * with older HANA versions, a proprietary DECIMAL format is used that is independent
///   of the number range of the concrete field.
/// * In newer HANA versions, three different formats are used
///   ([`TypeId::FIXED8`](crate::TypeId::FIXED8),
///   [`TypeId::FIXED12`](crate::TypeId::FIXED12) and
///   [`TypeId::FIXED16`](crate::TypeId::FIXED16))
///   that together allow for a wider value range and a lower bandwidth.
///
///  `hdbconnect_async` cares about these details.
///
/// Similarly, an `HdbValue::STRING` is used to transfer values to all string-like wire types.
///
/// The wire protocol sometimes also allows sending data in another wire type than requested.
///
/// If the database e.g. requests an INT, you can also send a String representation of the
/// number, by using `HdbValue::STRING("1088")`, instead of the binary INT representation
/// `HdbValue::INT(1088)`.
#[derive(Debug)]
pub struct PreparedStatement {
    am_ps_core: AM<PreparedStatementCore>,
    server_usage: ServerUsage,
    a_descriptors: Arc<ParameterDescriptors>,
    o_a_rsmd: Option<Arc<ResultSetMetadata>>,
    batch: ParameterRows<'static>,
    _o_table_location: Option<Vec<i32>>,
}

impl<'a> PreparedStatement {
    /// Converts the input into a row of parameters, and
    /// executes the statement with these parameters immediately.
    ///
    /// The row of parameters must be consistent with the input parameter metadata.
    /// The input conversion is done with the help of serde, so the input must implement
    /// `serde::ser::Serialize`.
    ///
    /// ```rust,no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection, HdbResult, IntoConnectParams};
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).await.unwrap();
    /// let mut statement = connection.prepare("select * from phrases where ID = ? and text = ?").await.unwrap();
    /// let hdbresponse = statement.execute(&(42, "Foo is bar")).await.unwrap();
    /// # })
    /// ```
    ///
    /// If the statement has no parameter, you can execute it like this
    ///
    /// ```rust, no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection, HdbResult, IntoConnectParams, Row};
    /// # let mut connection = Connection::new("".into_connect_params().unwrap()).await.unwrap();
    /// # let mut stmt = connection.prepare("").await.unwrap();
    /// let hdbresponse = stmt.execute(&()).await.unwrap();
    /// # })
    /// ```
    ///
    /// or like this
    ///
    /// ```rust, no_run
    /// # tokio_test::block_on(async {
    /// # use hdbconnect_async::{Connection, HdbResult, IntoConnectParams, Row};
    /// # let mut connection = Connection::new("".into_connect_params().unwrap()).await.unwrap();
    /// # let mut stmt = connection.prepare("").await.unwrap();
    /// let hdbresponse = stmt.execute_batch().await.unwrap();
    /// # })
    /// ```
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn execute<T: serde::ser::Serialize>(&mut self, input: &T) -> HdbResult<HdbResponse> {
        trace!("PreparedStatement::execute()");
        if self.a_descriptors.has_in() {
            let mut par_rows = ParameterRows::new();
            par_rows.push(input, &self.a_descriptors)?;
            return self.execute_parameter_rows(Some(par_rows)).await;
        }
        self.execute_parameter_rows(None).await
    }

    /// Consumes the given `HdbValue`s as a row of parameters for immediate execution.
    ///
    /// In most cases
    /// [`PreparedStatement::execute()`](crate::PreparedStatement::execute)
    /// will be more convenient. Streaming LOBs to the database however is an important exception -
    /// it only works with this method!
    ///
    /// Note that with older versions of HANA, streaming LOBs to the database only works
    /// if auto-commit is switched off. Consequently, you need to commit the update then explicitly.
    ///
    /// ## Example for streaming LOBs to the database
    ///
    /// The first parameter in this example inserts a string into a normal NVARCHAR column.
    /// We're using a `HdbValue::STR` here which allows passing the String as reference
    /// (compared to `HdbValue::STRING`, which needs to own the String).
    ///
    /// The second parameter of type [`HdbValue::ASYNC_LOBSTREAM`](crate::HdbValue::ASYNC_LOBSTREAM)
    /// wraps a shared mutable reference to a reader object
    /// which is supposed to produce the content you want to store.
    ///
    /// ``` rust, no_run
    /// # tokio_test::block_on(async {
    /// use hdbconnect_async::{Connection, HdbValue, HdbResult, IntoConnectParams};
    /// use std::io::Cursor;
    /// use std::sync::Arc;
    /// use tokio::sync::Mutex;
    /// # let mut connection = Connection::new("".into_connect_params().unwrap()).await.unwrap();
    /// # connection.set_auto_commit(false).await.unwrap();
    /// # let insert_stmt_string = "insert into TEST_NCLOBS values(?, ?)".to_owned();
    ///   let mut stmt = connection.prepare(&insert_stmt_string).await.unwrap();
    ///
    ///   stmt.execute_row(vec![
    ///       HdbValue::STR("nice descriptive text, could be quite long"),
    ///       HdbValue::ASYNC_LOBSTREAM(Some(Arc::new(Mutex::new(Cursor::new("foo bar"))))),
    ///   ]).await.unwrap();
    /// # connection.commit().await.unwrap();
    /// # })
    /// ```
    ///
    /// `PreparedStatement::execute_row()` first sends the specified statement to the database,
    /// with the given parameter values, where `ASYNC_LOBSTREAM` instances are replaced with placeholders.
    /// Subsequently the data from the readers are transferred to the database in additional
    /// roundtrips. Upon completion of the last LOB chunk transfer, the database really executes
    /// the procedure and returns its results.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn execute_row(
        &'a mut self,
        hdb_values: Vec<HdbValue<'a>>,
    ) -> HdbResult<HdbResponse> {
        if self.a_descriptors.has_in() {
            let ps_core_guard = self.am_ps_core.lock_async().await;

            let mut request = Request::new(MessageType::Execute, HOLD_CURSORS_OVER_COMMIT);

            request.push(Part::StatementId(ps_core_guard.statement_id));

            // If readers were provided, pick them out and replace them with None
            let mut readers: Vec<(HdbValue, TypeId)> = vec![];
            let hdb_values = hdb_values
                .into_iter()
                .zip(self.a_descriptors.iter_in())
                .map(|(v, d)| {
                    if let HdbValue::ASYNC_LOBSTREAM(Some(_)) = v {
                        readers.push((v, d.type_id()));
                        HdbValue::ASYNC_LOBSTREAM(None)
                    } else {
                        v
                    }
                })
                .collect();

            let mut par_rows = ParameterRows::new();
            par_rows.push_hdb_values(hdb_values, &self.a_descriptors)?;
            request.push(Part::ParameterRows(par_rows));

            if ps_core_guard
                .am_conn_core
                .async_lock()
                .await
                .connect_options()
                .get_implicit_lob_streaming()
            {
                request.push(Part::LobFlags(LobFlags::for_implicit_streaming()));
            }

            let mut main_reply = ps_core_guard
                .am_conn_core
                .full_send_async(
                    request,
                    self.o_a_rsmd.as_ref(),
                    Some(&self.a_descriptors),
                    &mut None,
                )
                .await?;

            // if the input was not transferred completely in the same roundtrip,
            // then the statement execution roundtrip cannot bring any of the expected results;
            // instead, the results that belong to the procedure execution roundtrip
            // will be received with the response to the last input-LOB transfer-roundtrip.
            let write_lob_reply = main_reply
                .parts
                .remove_first_of_kind(PartKind::WriteLobReply);

            let (mut internal_return_values, replytype) = (
                main_reply
                    .parts
                    .into_internal_return_values_async(&ps_core_guard.am_conn_core, None)
                    .await?,
                main_reply.replytype,
            );

            if let Some(Part::WriteLobReply(wlr)) = write_lob_reply {
                let locator_ids = wlr.into_locator_ids();
                if locator_ids.len() != readers.len() {
                    return Err(HdbError::UsageDetailed(format!(
                        "{} readers are provided, but {} are required",
                        readers.len(),
                        locator_ids.len()
                    )));
                }

                for (locator_id, (reader, type_id)) in locator_ids.into_iter().zip(readers) {
                    debug!("writing content to locator with id {:?}", locator_id);
                    if let HdbValue::ASYNC_LOBSTREAM(Some(reader)) = reader {
                        let mut reader = reader.lock().await;

                        let _amount = async_lob_writer::copy(
                            &mut *reader,
                            ps_core_guard.am_conn_core.clone(),
                            locator_id,
                            &mut internal_return_values,
                            type_id,
                            self.o_a_rsmd.as_ref(),
                            Some(&self.a_descriptors),
                        )
                        .await?;
                    }
                }
            }

            // inject statement id
            for rv in &mut internal_return_values {
                if let InternalReturnValue::RsState((rs_state, _a_rsmd)) = rv {
                    rs_state
                        .inject_ps_core_async(Arc::clone(&self.am_ps_core))
                        .await?;
                }
            }
            HdbResponse::try_new(internal_return_values, replytype)
        } else {
            self.execute_parameter_rows(None).await
        }
    }

    /// Converts the input into a row of parameters and adds it to the batch of this
    /// `PreparedStatement`, if it is consistent with the metadata.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn add_batch<T: serde::ser::Serialize>(&mut self, input: &T) -> HdbResult<()> {
        trace!("PreparedStatement::add_batch()");
        if self.a_descriptors.has_in() {
            self.batch.push(input, &self.a_descriptors)?;
            return Ok(());
        }
        Err(HdbError::Usage(
            "Batch not usable for PreparedStatements without input parameter",
        ))
    }

    /// Consumes the input as a row of parameters for the batch.
    ///
    /// Useful mainly for generic code.
    /// In most cases [`add_batch()`](crate::PreparedStatement::add_batch)
    /// is more convenient.
    /// Note that LOB streaming can not be combined with using the batch.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub fn add_row_to_batch(&mut self, hdb_values: Vec<HdbValue<'static>>) -> HdbResult<()> {
        trace!("PreparedStatement::add_row_to_batch()");
        if self.a_descriptors.has_in() {
            self.batch
                .push_hdb_values(hdb_values, &self.a_descriptors)?;
            return Ok(());
        }
        Err(HdbError::Usage(
            "Batch not possible, PreparedStatement has no input parameter",
        ))
    }

    /// Executes the statement with the collected batch, and clears the batch.
    ///
    /// Does nothing and returns with an error, if the statement needs input and no batch exists.
    /// If the statement does not need input and the batch is empty,
    /// a single execution is triggered.
    ///
    /// # Errors
    ///
    /// Several variants of `HdbError` can occur.
    pub async fn execute_batch(&mut self) -> HdbResult<HdbResponse> {
        if self.batch.is_empty() && self.a_descriptors.has_in() {
            return Err(HdbError::Usage("Empty batch cannot be executed"));
        }
        let mut rows2 = ParameterRows::new();
        std::mem::swap(&mut self.batch, &mut rows2);
        self.execute_parameter_rows(Some(rows2)).await
    }

    /// Descriptors of all parameters of the prepared statement (in, out, inout).
    #[must_use]
    pub fn parameter_descriptors(&self) -> Arc<ParameterDescriptors> {
        Arc::clone(&self.a_descriptors)
    }

    async fn execute_parameter_rows(
        &mut self,
        o_rows: Option<ParameterRows<'_>>,
    ) -> HdbResult<HdbResponse> {
        trace!("PreparedStatement::execute_parameter_rows()");

        let ps_core_guard = self.am_ps_core.lock_async().await;
        let mut request = Request::new(MessageType::Execute, HOLD_CURSORS_OVER_COMMIT);
        request.push(Part::StatementId(ps_core_guard.statement_id));
        if let Some(rows) = o_rows {
            request.push(Part::ParameterRows(rows));
        }

        let (mut internal_return_values, replytype) = ps_core_guard
            .am_conn_core
            .full_send_async(
                request,
                self.o_a_rsmd.as_ref(),
                Some(&self.a_descriptors),
                &mut None,
            )
            .await?
            .into_internal_return_values_async(&ps_core_guard.am_conn_core, None)
            .await?;

        // inject statement id
        for rv in &mut internal_return_values {
            if let InternalReturnValue::RsState((rs_state, _a_rsmd)) = rv {
                rs_state
                    .inject_ps_core_async(Arc::clone(&self.am_ps_core))
                    .await?;
            }
        }

        HdbResponse::try_new(internal_return_values, replytype)
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `PreparedStatement` object.
    #[must_use]
    pub fn server_usage(&self) -> ServerUsage {
        self.server_usage
    }

    // Prepare a statement.
    pub(crate) async fn try_new(am_conn_core: AmConnCore, stmt: &str) -> HdbResult<Self> {
        let mut request = Request::new(MessageType::Prepare, HOLD_CURSORS_OVER_COMMIT);
        request.push(Part::Command(stmt));

        let reply = am_conn_core.send_async(request).await?;

        let mut o_table_location: Option<Vec<i32>> = None;
        let mut o_stmt_id: Option<u64> = None;
        let mut a_descriptors: Arc<ParameterDescriptors> =
            Arc::new(ParameterDescriptors::default());
        let mut o_a_rsmd: Option<Arc<ResultSetMetadata>> = None;
        let mut server_usage = ServerUsage::default();

        for part in reply.parts {
            match part {
                Part::ParameterMetadata(descriptors) => {
                    a_descriptors = Arc::new(descriptors);
                }
                Part::StatementId(id) => {
                    o_stmt_id = Some(id);
                }
                Part::TransactionFlags(ta_flags) => {
                    let mut guard = am_conn_core.async_lock().await;
                    (*guard).evaluate_ta_flags(ta_flags)?;
                }
                Part::TableLocation(vec_i) => {
                    o_table_location = Some(vec_i);
                }
                Part::ResultSetMetadata(rs_md) => {
                    o_a_rsmd = Some(Arc::new(rs_md));
                }

                Part::StatementContext(ref stmt_ctx) => {
                    let mut guard = am_conn_core.async_lock().await;
                    (*guard).evaluate_statement_context(stmt_ctx);
                    server_usage.update(
                        stmt_ctx.server_processing_time(),
                        stmt_ctx.server_cpu_time(),
                        stmt_ctx.server_memory_usage(),
                    );
                }
                x => warn!("try_new(): Unexpected reply part found {:?}", x),
            }
        }

        let statement_id = o_stmt_id.ok_or_else(|| HdbError::Impl("No StatementId received"))?;
        let am_ps_core = new_am_async(PreparedStatementCore {
            am_conn_core,
            statement_id,
        });
        debug!(
            "PreparedStatement created with parameter descriptors = {:?}",
            a_descriptors
        );
        Ok(Self {
            am_ps_core,
            server_usage,
            batch: ParameterRows::new(),
            a_descriptors,
            o_a_rsmd,
            _o_table_location: o_table_location,
        })
    }
}
