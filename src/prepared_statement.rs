use {DbcError,DbcResult};
use protocol::lowlevel::conn_core::ConnRef;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::{Request,Metadata};
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::parameter_metadata::ParameterMetadata;


pub struct PreparedStatement {
    conn_ref: ConnRef,
    statement_id: u64,
    auto_commit: bool,
    par_md: Option<ParameterMetadata>,
}

impl PreparedStatement {
    /// Prepare a statement
    pub fn prepare(conn_ref: ConnRef, stmt: String, auto_commit: bool, command_options: u8,) -> DbcResult<PreparedStatement> {
        let mut request = Request::new(0, RequestType::Prepare, auto_commit, command_options);
        request.push(Part::new(PartKind::Command, Argument::Command(stmt)));

        let mut reply = try!(request.send_and_receive(&Metadata::None, &mut None, &conn_ref, None));

        let part = try!(reply.retrieve_first_part_of_kind(PartKind::StatementId));
        let statement_id = match part.arg {
            Argument::StatementId(statement_id) => statement_id,
            _ => return Err(DbcError::EvaluationError(String::from("No StatementId in PreparedStatement::prepare()"))),
        };

        let part = try!(reply.retrieve_first_part_of_kind(PartKind::ParameterMetadata));
        let par_md = match part.arg {
            Argument::ParameterMetadata(par_md) => Some(par_md),
            _ => None,
        };

        Ok(PreparedStatement {
            conn_ref: conn_ref,
            statement_id: statement_id,
            auto_commit: auto_commit,
            par_md: par_md,
        })
    }

    // Execute the statement with the collected params
    // pub fn execute(&self, pars: &[&ToHdb]) -> DbcResult<RowsAffected> {
    //     let in_parameters =
    //
    //     let mut request = Request::new(0, RequestType::Prepare, false, HOLD_OVER_COMMIT);
    //     request.push(Part::new(PartKind::StatementId, Argument::StatementId(&self.statement_id)));
    //     request.push(Part::new(PartKind::Parameters, Argument::Parameters(in_parameters)));
    //
    //     let mut reply = try!(request.send_and_receive(&(self.par_md), &mut None, self.conn_ref, None));
    //     - TransactionFlags
    //     - RowsAffected
    //     - (StatementContext)
    //     - OutputParameters
    // }
}


impl Drop for PreparedStatement {
    fn drop(&mut self) {
        // request=Request{
        // 	session_id: 1289496156256637,
        // 	request_type: DropStatementId,
        // 	auto_commit: false,
        // 	command_options: 0,
        // 	parts: [Part{
        // 		kind: StatementId,
        // 		arg: StatementId(1289497357203401)
        // 	}]
        // }
        //
        // reply=Reply{
        // 	session_id: 1289496156256637,
        // 	reply_type: Nil,
        // 	parts: [Part{
        // 		kind: StatementContext,
        // 		arg: StatementContext(StatementContext{
        // 			statement_sequence_info: Some(BSTRING([])),
        // 			server_processing_time: Some(BIGINT(165)),
        // 			schema_name: None
        // 		})
        // 	}]
        // }

    }
}
