use crate::conn::AmConnCore;
use crate::hdb_response::InternalReturnValue;
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::lob_flags::LobFlags;
use crate::protocol::parts::parameter_descriptor::ParameterDescriptors;
use crate::protocol::parts::parameter_rows::ParameterRows;
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::parts::type_id::TypeId;
use crate::protocol::request::{Request, HOLD_CURSORS_OVER_COMMIT};
use crate::protocol::request_type::RequestType;
use crate::protocol::server_usage::ServerUsage;
use crate::types_impl::lob::LobWriter;
use crate::{HdbError, HdbResponse, HdbResult};
use serde;
use std::io::Write;
use std::sync::{Arc, Mutex};

/// Allows injection-safe SQL execution and repeated calls of the same statement
/// with different parameters with as few roundtrips as possible.
///
/// # Providing Input Parameters
///
/// ## Type systems
///
/// Type system**s**, really? Yes, there are in fact four type systems involved!
/// * Your application is written in rust, and uses the _rust type system_.
/// * `hdbconnect`'s _driver API_ represents values with the `enum` [`HdbValue`](enum.HdbValue.html);
///   this type system aims to be as close to the rust type system as possible
///   and hides the complexity of the following two internal type systems.
/// * The _wire_ has its own type system - it's focus is on efficient data transfer.
///   `hdbconnect` deals with these types internally.
/// * The _database type system_ consists of the standard SQL types and proprietary types
///   to represent values, like TINYINT, FLOAT, NVARCHAR, and many others.
///   This type system is NOT directly visible to `hdbconnect`.
///
///   [`TypeId`](enum.TypeId.html) enumerates a somewhat reduced superset
///   of the server-side and the wire type system.
///
/// ## From Rust types to `HdbValue`
///
/// Prepared statements typically take one or more input parameter(s).
/// As part of the statement preparation, the database server provides the client
/// with detailed metadata for these parameters, which are kept by the `PreparedStatement`.
///
/// The parameter values can be handed over to the `PreparedStatement` either as
/// `Serializable` rust types, or explicitly as [`HdbValue`](enum.HdbValue.html) instances.
/// If they are handed over as `Serializable` rust types, then the built-in
/// [`serde_db`](https://docs.rs/serde_db)-based
/// conversion will convert them directly into those [`HdbValue`](enum.HdbValue.html) variants
/// that correspond to the `TypeId` that the server has requested.
/// The application can also provide the values explicitly as [`HdbValue`](enum.HdbValue.html)
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
///   ([`TypeId::FIXED8`](enum.TypeId.html#variant.FIXED8),
///   [`TypeId::FIXED12`](enum.TypeId.html#variant.FIXED12) and
///   [`TypeId::FIXED16`](enum.TypeId.html#variant.FIXED16))
///   that together allow for a wider value range and a lower bandwidth.
///
///  `hdbconnect` cares about these details.
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
    am_ps_core: Arc<Mutex<PreparedStatementCore>>,
    server_usage: ServerUsage,
    a_descriptors: Arc<ParameterDescriptors>,
    o_a_rsmd: Option<Arc<ResultSetMetadata>>,
    batch: ParameterRows<'static>,
    _o_table_location: Option<Vec<i32>>,
}

pub(crate) type AmPsCore = Arc<Mutex<PreparedStatementCore>>;

// Needs connection for its Drop implementation
#[derive(Debug)]
pub(crate) struct PreparedStatementCore {
    am_conn_core: AmConnCore,
    statement_id: u64,
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
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams};
    /// # fn main() -> HdbResult<()> {
    /// # let params = "hdbsql://my_user:my_passwd@the_host:2222"
    /// #     .into_connect_params()
    /// #     .unwrap();
    /// # let mut connection = Connection::new(params).unwrap();
    /// let mut statement = connection.prepare("select * from phrases where ID = ? and text = ?")?;
    /// let hdbresponse = statement.execute(&(42, "Foo is bar"))?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If the statement has no parameter, you can execute it like this
    ///
    /// ```rust, no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
    /// # fn foo() -> HdbResult<()> {
    /// # let mut connection = Connection::new("".into_connect_params()?)?;
    /// # let mut stmt = connection.prepare("")?;
    /// let hdbresponse = stmt.execute(&())?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// or like this
    ///
    /// ```rust, no_run
    /// # use hdbconnect::{Connection, HdbResult, IntoConnectParams, Row};
    /// # fn foo() -> HdbResult<()> {
    /// # let mut connection = Connection::new("".into_connect_params()?)?;
    /// # let mut stmt = connection.prepare("")?;
    /// let hdbresponse = stmt.execute_batch()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute<T: serde::ser::Serialize>(&mut self, input: &T) -> HdbResult<HdbResponse> {
        trace!("PreparedStatement::execute()");
        if self.a_descriptors.has_in() {
            let mut par_rows = ParameterRows::new();
            par_rows.push(input, &self.a_descriptors)?;
            return self.execute_parameter_rows(Some(par_rows));
        }
        self.execute_parameter_rows(None)
    }

    /// Consumes the given `HdbValue`s as a row of parameters for immediate execution.
    ///
    /// In most cases
    /// [`PreparedStatement::execute()`](struct.PreparedStatement.html#method.execute)
    /// will be more convenient. Streaming LOBs to the database however is an important exception -
    /// it only works with this method!
    ///
    /// Note that with older versions of HANA streaming LOBs to the database only works
    /// if auto-commit is switched off. Consequently, you need to commit the update then explicitly.
    ///
    /// ## Example for streaming LOBs to the database
    ///
    /// The first parameter in this example inserts a string into a normal NVARCHAR column.
    /// We're using a `HdbValue::STR` here which allows passing the String as reference
    /// (compared to `HdbValue::STRING`, which needs to own the String).
    ///
    /// The second parameter of type [`HdbValue::LOBSTREAM`](enum.HdbValue.html#variant.LOBSTREAM)
    /// wraps a shared mutable reference to a reader object
    /// which is supposed to produce the content you want to store.
    ///
    /// ``` rust, no_run
    /// # use hdbconnect::{Connection, HdbValue, HdbResult, IntoConnectParams};
    /// use std::io::Cursor;
    /// use std::sync::{Arc,Mutex};
    /// # fn main() -> HdbResult<()> {
    /// # let mut connection = Connection::new("".into_connect_params()?)?;
    /// # connection.set_auto_commit(false)?;
    /// # let insert_stmt_string = "insert into TEST_NCLOBS values(?, ?)".to_owned();
    ///   let mut stmt = connection.prepare(&insert_stmt_string)?;
    ///
    ///   stmt.execute_row(vec![
    ///       HdbValue::STR("nice descriptive text, could be quite long"),
    ///       HdbValue::LOBSTREAM(Some(Arc::new(Mutex::new(Cursor::new("foo bar"))))),
    ///   ])?;
    /// # connection.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// `PreparedStatement::execute_row()` first sends the specified statement to the database,
    /// with the given parameter values, where LOBSTREAM instances are replaced with placeholders.
    /// Subsequently the data from the readers are transferred to the database in additional
    /// roundtrips. Upon completion of the last LOB chunk transfer, the database really executes
    /// the procedure and returns its results.
    pub fn execute_row(&'a mut self, hdb_values: Vec<HdbValue<'a>>) -> HdbResult<HdbResponse> {
        if self.a_descriptors.has_in() {
            let mut ps_core_guard = self.am_ps_core.lock()?;

            let mut request = Request::new(RequestType::Execute, HOLD_CURSORS_OVER_COMMIT);

            request.push(Part::new(
                PartKind::StatementId,
                Argument::StatementId(ps_core_guard.statement_id),
            ));

            // If readers were provided, pick them out and replace them with None
            let mut readers: Vec<(HdbValue, TypeId)> = vec![];
            let hdb_values = hdb_values
                .into_iter()
                .zip(self.a_descriptors.iter_in())
                .map(|(v, d)| {
                    if let HdbValue::LOBSTREAM(Some(_)) = v {
                        readers.push((v, d.type_id()));
                        HdbValue::LOBSTREAM(None)
                    } else {
                        v
                    }
                })
                .collect();

            let mut par_rows = ParameterRows::new();
            par_rows.push_hdb_values(hdb_values, &self.a_descriptors)?;
            request.push(Part::new(
                PartKind::Parameters,
                Argument::Parameters(par_rows),
            ));

            if ps_core_guard
                .am_conn_core
                .lock()?
                .connect_options()
                .get_implicit_lob_streaming()
                .unwrap_or(false)
            {
                request.push(Part::new(
                    PartKind::LobFlags,
                    Argument::LobFlags(LobFlags::for_implicit_streaming()),
                ));
            }

            let mut main_reply = ps_core_guard.am_conn_core.full_send_sync(
                request,
                self.o_a_rsmd.as_ref(),
                Some(&self.a_descriptors),
                &mut None,
            )?;

            // if the input was not transferred completely in the same roundtrip,
            // then the statement execution roundtrip cannot bring any of the expected results;
            // instead, the results that belong to the procedure execution roundtrip
            // will be received with the response to the last input-LOB transfer-roundtrip.
            let write_lob_reply = main_reply
                .parts
                .remove_first_of_kind(PartKind::WriteLobReply)
                .map(Part::into_arg);

            let (mut internal_return_values, replytype) =
                main_reply.into_internal_return_values(&mut ps_core_guard.am_conn_core, None)?;

            if let Some(Argument::WriteLobReply(wlr)) = write_lob_reply {
                let locator_ids = wlr.into_locator_ids();
                if locator_ids.len() != readers.len() {
                    return Err(HdbError::UsageDetailed(format!(
                        "The number of provided readers ({}) does not match \
                         the number of required readers ({})",
                        readers.len(),
                        locator_ids.len()
                    )));
                }
                for (locator_id, (reader, type_id)) in locator_ids.into_iter().zip(readers) {
                    debug!("writing content to locator with id {:?}", locator_id);
                    if let HdbValue::LOBSTREAM(Some(reader)) = reader {
                        let mut reader = reader.lock()?;
                        let mut writer = LobWriter::new(
                            locator_id,
                            type_id,
                            ps_core_guard.am_conn_core.clone(),
                            self.o_a_rsmd.as_ref(),
                            Some(&self.a_descriptors),
                        )?;
                        std::io::copy(&mut *reader, &mut writer).map_err(HdbError::LobStreaming)?;
                        writer.flush().map_err(HdbError::LobStreaming)?;
                        if let Some(mut irvs) = writer.into_internal_return_values() {
                            internal_return_values.append(&mut irvs);
                        }
                    }
                }
            }

            // inject statement id
            for rv in &mut internal_return_values {
                if let InternalReturnValue::ResultSet(rs) = rv {
                    rs.inject_statement_id(Arc::clone(&self.am_ps_core));
                }
            }
            HdbResponse::try_new(internal_return_values, replytype)
        } else {
            self.execute_parameter_rows(None)
        }
    }

    /// Converts the input into a row of parameters and adds it to the batch of this
    /// `PreparedStatement`, if it is consistent with the metadata.
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
    /// In most cases [`add_batch()`](struct.PreparedStatement.html#method.add_batch)
    /// is more convenient.
    /// Note that LOB streaming can not be combined with using the batch.
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
    pub fn execute_batch(&mut self) -> HdbResult<HdbResponse> {
        if self.batch.is_empty() && self.a_descriptors.has_in() {
            return Err(HdbError::Usage("Empty batch cannot be executed"));
        }
        let mut rows2 = ParameterRows::new();
        std::mem::swap(&mut self.batch, &mut rows2);
        self.execute_parameter_rows(Some(rows2))
    }

    /// Descriptors of all parameters of the prepared statement (in, out, inout).
    pub fn parameter_descriptors(&self) -> Arc<ParameterDescriptors> {
        Arc::clone(&self.a_descriptors)
    }

    fn execute_parameter_rows(&mut self, o_rows: Option<ParameterRows>) -> HdbResult<HdbResponse> {
        trace!("PreparedStatement::execute_parameter_rows()");

        let mut ps_core_guard = self.am_ps_core.lock()?;
        let mut request = Request::new(RequestType::Execute, HOLD_CURSORS_OVER_COMMIT);
        request.push(Part::new(
            PartKind::StatementId,
            Argument::StatementId(ps_core_guard.statement_id),
        ));
        if let Some(rows) = o_rows {
            request.push(Part::new(PartKind::Parameters, Argument::Parameters(rows)));
        }

        let (mut internal_return_values, replytype) = ps_core_guard
            .am_conn_core
            .full_send_sync(
                request,
                self.o_a_rsmd.as_ref(),
                Some(&self.a_descriptors),
                &mut None,
            )?
            .into_internal_return_values(&mut ps_core_guard.am_conn_core, None)?;

        // inject statement id
        for rv in &mut internal_return_values {
            if let InternalReturnValue::ResultSet(rs) = rv {
                rs.inject_statement_id(Arc::clone(&self.am_ps_core));
            }
        }

        HdbResponse::try_new(internal_return_values, replytype)
    }

    /// Provides information about the the server-side resource consumption that
    /// is related to this `PreparedStatement` object.
    pub fn server_usage(&self) -> ServerUsage {
        self.server_usage
    }

    // Prepare a statement.
    pub(crate) fn try_new(mut am_conn_core: AmConnCore, stmt: &str) -> HdbResult<Self> {
        let mut request = Request::new(RequestType::Prepare, HOLD_CURSORS_OVER_COMMIT);
        request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

        let mut reply = am_conn_core.send_sync(request)?;

        // ParameterMetadata, ResultSetMetadata
        // StatementContext, StatementId,
        // TableLocation, TransactionFlags,
        let mut o_table_location: Option<Vec<i32>> = None;
        let mut o_stmt_id: Option<u64> = None;
        let mut a_descriptors: Arc<ParameterDescriptors> = Arc::new(ParameterDescriptors::new());
        let mut o_a_rsmd: Option<Arc<ResultSetMetadata>> = None;
        let mut server_usage = ServerUsage::default();

        while !reply.parts.is_empty() {
            match reply.parts.pop_arg() {
                Some(Argument::ParameterMetadata(descriptors)) => {
                    a_descriptors = Arc::new(descriptors);
                }
                Some(Argument::StatementId(id)) => {
                    o_stmt_id = Some(id);
                }
                Some(Argument::TransactionFlags(ta_flags)) => {
                    let mut guard = am_conn_core.lock()?;
                    (*guard).evaluate_ta_flags(ta_flags)?;
                }
                Some(Argument::TableLocation(vec_i)) => {
                    o_table_location = Some(vec_i);
                }
                Some(Argument::ResultSetMetadata(rs_md)) => {
                    o_a_rsmd = Some(Arc::new(rs_md));
                }

                Some(Argument::StatementContext(ref stmt_ctx)) => {
                    let mut guard = am_conn_core.lock()?;
                    (*guard).evaluate_statement_context(stmt_ctx)?;
                    server_usage.update(
                        stmt_ctx.server_processing_time(),
                        stmt_ctx.server_cpu_time(),
                        stmt_ctx.server_memory_usage(),
                    );
                }
                x => warn!("prepare(): Unexpected reply part found {:?}", x),
            }
        }

        let statement_id = if let Some(id) = o_stmt_id {
            id
        } else {
            return Err(HdbError::Impl("No StatementId received"));
        };

        debug!(
            "PreparedStatement created with parameter descriptors = {:?}",
            a_descriptors
        );

        let am_ps_core = Arc::new(Mutex::new(PreparedStatementCore {
            am_conn_core,
            statement_id,
        }));
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

impl Drop for PreparedStatementCore {
    /// Frees all server-side ressources that belong to this prepared statement.
    fn drop(&mut self) {
        let mut request = Request::new(RequestType::DropStatementId, 0);
        request.push(Part::new(
            PartKind::StatementId,
            Argument::StatementId(self.statement_id),
        ));
        if let Ok(mut reply) = self.am_conn_core.send_sync(request) {
            reply.parts.pop_if_kind(PartKind::StatementContext);
        }
    }
}
