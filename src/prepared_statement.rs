use crate::conn_core::AmConnCore;
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::hdb_value::HdbValue;
use crate::protocol::parts::parameter_descriptor::{ParameterDescriptor, ParameterDirection};
use crate::protocol::parts::parameters::{ParameterRow, Parameters};
use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
use crate::protocol::request::{Request, HOLD_CURSORS_OVER_COMMIT};
use crate::protocol::request_type::RequestType;
use crate::{HdbError, HdbResponse, HdbResult};

use serde;
use serde_db::ser::to_params;
use serde_db::ser::SerializationError;

use std::mem;

/// Allows injection-safe SQL execution and repeated calls of the same statement
/// with different parameters with as few roundtrips as possible.
#[derive(Debug)]
pub struct PreparedStatement {
    am_conn_core: AmConnCore,
    statement_id: u64,
    _o_table_location: Option<Vec<i32>>,
    o_par_md: Option<Vec<ParameterDescriptor>>,
    o_input_md: Option<Vec<ParameterDescriptor>>,
    o_rs_md: Option<ResultSetMetadata>,
    o_batch: Option<Vec<ParameterRow>>,
}

impl PreparedStatement {
    /// Descriptors of all parameters of the prepared statement (in, out, inout), if any.
    pub fn parameter_descriptors(&self) -> Option<&Vec<ParameterDescriptor>> {
        self.o_par_md.as_ref()
    }

    /// Descriptors of the input (in and inout) parameters of the prepared statement, if any.
    pub fn input_parameter_descriptors(&self) -> Option<&Vec<ParameterDescriptor>> {
        self.o_input_md.as_ref()
    }

    /// Converts the input into a row of parameters for the batch,
    /// if it is consistent with the metadata.
    pub fn add_batch<T: serde::ser::Serialize>(&mut self, input: &T) -> HdbResult<()> {
        trace!("PreparedStatement::add_batch()");
        match (&(self.o_input_md), &mut (self.o_batch)) {
            (&Some(ref metadata), &mut Some(ref mut vec)) => {
                vec.push(ParameterRow::new(to_params(input, metadata)?));
                Ok(())
            }
            (_, _) => {
                let s = "no metadata in add_batch()";
                Err(HdbError::Serialization(
                    SerializationError::StructuralMismatch(s),
                ))
            }
        }
    }

    /// Consumes the input as a row of parameters for the batch.
    ///
    /// Useful mainly for generic code.
    /// In most cases [`add_batch()`](struct.PreparedStatement.html#method.add_batch)
    /// is more convenient.
    pub fn add_row_to_batch(&mut self, row: Vec<HdbValue>) -> HdbResult<()> {
        trace!("PreparedStatement::add_row_to_batch()");
        match (&(self.o_input_md), &mut (self.o_batch)) {
            (&Some(ref _metadata), &mut Some(ref mut vec)) => {
                vec.push(ParameterRow::new(row));
                Ok(())
            }
            (_, _) => {
                let s = "no metadata in add_row_to_batch()";
                Err(HdbError::Serialization(
                    SerializationError::StructuralMismatch(s),
                ))
            }
        }
    }

    /// Executes the statement with the collected batch, and clears the batch.
    pub fn execute_batch(&mut self) -> HdbResult<HdbResponse> {
        trace!("PreparedStatement::execute_batch()");
        let mut request = Request::new(RequestType::Execute, HOLD_CURSORS_OVER_COMMIT);
        request.push(Part::new(
            PartKind::StatementId,
            Argument::StatementId(self.statement_id),
        ));
        if let Some(ref mut pars1) = self.o_batch {
            let mut pars2 = Vec::<ParameterRow>::new();
            mem::swap(pars1, &mut pars2);
            request.push(Part::new(
                PartKind::Parameters,
                Argument::Parameters(Parameters::new(pars2)),
            ));
        }

        let reply = self.am_conn_core.full_send(
            request,
            self.o_rs_md.as_ref(),
            self.o_par_md.as_ref(),
            &mut None,
        )?;
        reply.into_hdbresponse(&mut (self.am_conn_core))
    }

    // Prepare a statement.
    pub(crate) fn try_new(
        mut am_conn_core: AmConnCore,
        stmt: &str,
    ) -> HdbResult<PreparedStatement> {
        let mut request = Request::new(RequestType::Prepare, HOLD_CURSORS_OVER_COMMIT);
        request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

        let mut reply = am_conn_core.send(request)?;

        // ParameterMetadata, ResultSetMetadata
        // StatementContext, StatementId,
        // TableLocation, TransactionFlags,
        let mut o_table_location: Option<Vec<i32>> = None;
        let mut o_stmt_id: Option<u64> = None;
        let mut o_par_md: Option<Vec<ParameterDescriptor>> = None;
        let mut o_rs_md: Option<ResultSetMetadata> = None;

        while !reply.parts.is_empty() {
            match reply.parts.pop_arg() {
                Some(Argument::ParameterMetadata(par_md)) => {
                    o_par_md = Some(par_md);
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
                    o_rs_md = Some(rs_md);
                }

                Some(Argument::StatementContext(ref stmt_ctx)) => {
                    let mut guard = am_conn_core.lock()?;
                    (*guard).evaluate_statement_context(stmt_ctx)?;
                }
                x => warn!("prepare(): Unexpected reply part found {:?}", x),
            }
        }

        let statement_id = match o_stmt_id {
            Some(id) => id,
            None => {
                return Err(HdbError::Impl(
                    "PreparedStatement needs a StatementId".to_owned(),
                ))
            }
        };

        let o_input_md = if let Some(ref mut metadata) = o_par_md {
            let mut input_metadata = Vec::<ParameterDescriptor>::new();
            for pd in metadata {
                match pd.direction() {
                    ParameterDirection::IN | ParameterDirection::INOUT => {
                        input_metadata.push((*pd).clone())
                    }
                    ParameterDirection::OUT => {}
                }
            }
            if !input_metadata.is_empty() {
                Some(input_metadata)
            } else {
                None
            }
        } else {
            None
        };

        debug!(
            "PreparedStatement created with parameter_metadata = {:?}",
            o_par_md
        );

        Ok(PreparedStatement {
            am_conn_core,
            statement_id,
            o_batch: match o_par_md {
                Some(_) => Some(Vec::<ParameterRow>::new()),
                None => None,
            },
            o_par_md,
            o_input_md,
            o_rs_md,
            _o_table_location: o_table_location,
        })
    }
}

impl Drop for PreparedStatement {
    /// Frees all server-side ressources that belong to this prepared statement.
    fn drop(&mut self) {
        let mut request = Request::new(RequestType::DropStatementId, 0);
        request.push(Part::new(
            PartKind::StatementId,
            Argument::StatementId(self.statement_id),
        ));
        if let Ok(mut reply) = self.am_conn_core.send(request) {
            reply.parts.pop_arg_if_kind(PartKind::StatementContext);
        }
    }
}
