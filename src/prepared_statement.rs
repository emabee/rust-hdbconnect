use {HdbError, HdbResult, HdbResponse};
use protocol::lowlevel::conn_core::ConnCoreRef;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::Request;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::parameter_metadata::{ParameterDescriptor, ParameterMetadata,
                                                    ParMode};
use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
use protocol::lowlevel::parts::parameters::{ParameterRow, Parameters};
use rs_serde::ser::SerializationError;
use rs_serde::ser::Serializer;

use serde;
use std::mem;

/// Allows injection-safe SQL execution and repeated calls with different parameters
/// with as few roundtrips as possible.
pub struct PreparedStatement {
    conn_ref: ConnCoreRef,
    statement_id: u64,
    auto_commit: bool,
    #[allow(dead_code)]
    o_table_location: Option<Vec<i32>>,
    o_par_md: Option<ParameterMetadata>, // optional, because there will not always be parameters
    #[allow(dead_code)]
    o_rs_md: Option<ResultSetMetadata>, // optional, because there will not always be a resultset
    o_batch: Option<Vec<ParameterRow>>,
    acc_server_proc_time: i32,
}

impl PreparedStatement {
    /// Adds the values from the rust-typed input to the batch,
    /// if it is consistent with the metadata.
    pub fn add_batch<T: serde::ser::Serialize>(&mut self, input: &T) -> HdbResult<()> {
        trace!("PreparedStatement::add_batch() called");
        match (&(self.o_par_md), &mut (self.o_batch)) {
            (&Some(ref metadata), &mut Some(ref mut vec)) => {

                let mut input_metadata = Vec::<ParameterDescriptor>::new();
                for pd in &metadata.descriptors {
                    match pd.mode {
                        ParMode::IN | ParMode::INOUT => input_metadata.push((*pd).clone()),
                        ParMode::OUT => {}
                    }
                }

                let row = Serializer::into_row(input, input_metadata)?;
                vec.push(row);
                Ok(())
            }
            (_, _) => {
                let s = "no metadata in add_batch()";
                Err(HdbError::SerializationError(SerializationError::StructuralMismatch(s)))
            }
        }
    }

    /// Executes the statement with the collected batch, and clears the batch.
    pub fn execute_batch(&mut self) -> HdbResult<HdbResponse> {
        let mut request =
            Request::new(&(self.conn_ref), RequestType::Execute, self.auto_commit, 8_u8)?;
        request.push(Part::new(PartKind::StatementId, Argument::StatementId(self.statement_id)));
        if let Some(ref mut pars1) = self.o_batch {
            let mut pars2 = Vec::<ParameterRow>::new();
            mem::swap(pars1, &mut pars2);
            debug!("pars: {:?}", pars2);
            request.push(
                Part::new(PartKind::Parameters, Argument::Parameters(Parameters::new(pars2))),
            );
        }
        let rs_md = match self.o_rs_md {
            Some(ref rs_md) => Some(rs_md),
            None => None,
        };
        let par_md = match self.o_par_md {
            Some(ref par_md) => Some(par_md),
            None => None,
        };
        request.send_and_get_response(
            rs_md,
            par_md,
            &(self.conn_ref),
            None,
            &mut self.acc_server_proc_time,
        )
    }

    /// Sets the prepared statement's auto-commit behavior for future calls.
    pub fn set_auto_commit(&mut self, ac: bool) {
        self.auto_commit = ac;
    }
}


impl Drop for PreparedStatement {
    /// Frees all server-side ressources that belong to this prepared statement.
    fn drop(&mut self) {
        match Request::new(&(self.conn_ref), RequestType::DropStatementId, false, 0) {
            Err(_) => {}
            Ok(mut request) => {
                request.push(
                    Part::new(PartKind::StatementId, Argument::StatementId(self.statement_id)),
                );
                if let Ok(mut reply) = request.send_and_receive(&(self.conn_ref), None) {
                    reply.parts.pop_arg_if_kind(PartKind::StatementContext);
                }
            }
        }
    }
}

pub mod factory {
    use {HdbError, HdbResult};
    use protocol::lowlevel::conn_core::ConnCoreRef;
    use protocol::lowlevel::argument::Argument;
    use protocol::lowlevel::message::Request;
    use protocol::lowlevel::request_type::RequestType;
    use protocol::lowlevel::part::Part;
    use protocol::lowlevel::partkind::PartKind;
    use protocol::lowlevel::parts::option_value::OptionValue;
    use protocol::lowlevel::parts::parameters::ParameterRow;
    use protocol::lowlevel::parts::parameter_metadata::ParameterMetadata;
    use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;
    use protocol::lowlevel::parts::statement_context::StatementContext;
    use protocol::lowlevel::parts::transactionflags::TransactionFlag;
    use super::PreparedStatement;


    /// Prepare a statement.
    pub fn prepare(conn_ref: ConnCoreRef, stmt: String, auto_commit: bool)
                   -> HdbResult<PreparedStatement> {
        let command_options: u8 = 8;
        let mut request =
            Request::new(&conn_ref, RequestType::Prepare, auto_commit, command_options)?;
        request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

        let mut reply = request.send_and_receive(&conn_ref, None)?;

        // TableLocation, TransactionFlags, StatementContext,
        // StatementId, ParameterMetadata, ResultSetMetadata
        let mut o_table_location: Option<Vec<i32>> = None;
        let mut o_ta_flags: Option<Vec<TransactionFlag>> = None;
        let mut o_stmt_ctx: Option<StatementContext> = None;
        let mut o_stmt_id: Option<u64> = None;
        let mut o_par_md: Option<ParameterMetadata> = None;
        let mut o_rs_md: Option<ResultSetMetadata> = None;

        while !reply.parts.0.is_empty() {
            match reply.parts.pop_arg() {
                Some(Argument::ParameterMetadata(par_md)) => {
                    o_par_md = Some(par_md);
                }
                Some(Argument::StatementId(id)) => {
                    o_stmt_id = Some(id);
                }
                Some(Argument::StatementContext(stmt_ctx)) => {
                    o_stmt_ctx = Some(stmt_ctx);
                }
                Some(Argument::TransactionFlags(vec)) => o_ta_flags = Some(vec),
                Some(Argument::TableLocation(vec_i)) => {
                    o_table_location = Some(vec_i);
                }
                Some(Argument::ResultSetMetadata(rs_md)) => {
                    o_rs_md = Some(rs_md);
                }
                x => warn!("prepare(): Unexpected reply part found {:?}", x),
            }
        }

        if let Some(vec) = o_ta_flags {
            let mut guard = conn_ref.lock()?;
            for ta_flag in vec {
                (*guard).set_transaction_state(ta_flag)?;
            }
        }

        let mut acc_server_proc_time = 0;
        if let Some(stmt_ctx) = o_stmt_ctx {
            if let Some(OptionValue::INT(i)) = stmt_ctx.server_processing_time {
                acc_server_proc_time = i;
            }
        };

        let statement_id = match o_stmt_id {
            Some(id) => id,
            None => return Err(HdbError::EvaluationError("PreparedStatement needs a StatementId")),
        };

        debug!(
            "PreparedStatement created with auto_commit = {}, parameter_metadata = {:?}",
            auto_commit,
            o_par_md
        );

        Ok(PreparedStatement {
            conn_ref: conn_ref,
            statement_id: statement_id,
            auto_commit: auto_commit,
            o_batch: match o_par_md {
                Some(_) => Some(Vec::<ParameterRow>::new()),
                None => None,
            },
            o_par_md: o_par_md,
            o_rs_md: o_rs_md,
            o_table_location: o_table_location,
            acc_server_proc_time: acc_server_proc_time,
        })
    }
}
