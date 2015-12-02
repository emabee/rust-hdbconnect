use {DbcError,DbcResult};
use protocol::lowlevel::conn_core::ConnRef;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::Message;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::resultset::ResultSet;
use protocol::lowlevel::rows_affected::RowsAffected;
use protocol::lowlevel::segment;
use protocol::lowlevel::util;

///
pub struct CallableStatement {
    conn_ref: ConnRef,
    stmt: String,
}
impl CallableStatement {
    pub fn new(conn_ref: ConnRef, stmt: String) -> DbcResult<CallableStatement> {
        Ok(CallableStatement { conn_ref: conn_ref, stmt: stmt })
    }
}

impl CallableStatement {
    pub fn execute(&self, auto_commit: bool) -> DbcResult<CallableStatementResult> {
        trace!("CallableStatement::execute({})",self.stmt);
        // build the request
        let command_options = 0b_1000;
        let mut segment = segment::new_request_seg(segment::MessageType::ExecuteDirect, auto_commit, command_options);
        let fetch_size = { self.conn_ref.borrow().get_fetch_size() };
        segment.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));
        segment.push(Part::new(PartKind::Command, Argument::Command(self.stmt.clone())));
        let mut message = Message::new();
        message.segments.push(segment);

        // send it
        let mut response = try!(message.send_and_receive(&mut None, &(self.conn_ref)));

        // digest response
        assert!(response.segments.len() == 1, "Wrong count of segments");
        let mut segment = response.segments.swap_remove(0);
        match &segment.function_code {
            &Some(segment::FunctionCode::Select) => {
                let part = match util::get_first_part_of_kind(PartKind::ResultSet, &segment.parts) {
                    Some(idx) => segment.parts.swap_remove(idx),
                    None => return Err(DbcError::EvaluationError("no part of kind ResultSet".to_string())),
                };

                match part.arg {
                    Argument::ResultSet(Some(mut resultset)) => {
                        try!(resultset.fetch_all());  // FIXME fetching remaining data should be done more lazily
                        Ok(CallableStatementResult::ResultSet(resultset))
                    },
                    _ => Err(DbcError::EvaluationError("unexpected error in CallableStatement::execute() 1".to_string())),
                }
            },

            &Some(segment::FunctionCode::Ddl)
            | &Some(segment::FunctionCode::Insert) => {
                let part = match util::get_first_part_of_kind(PartKind::RowsAffected, &segment.parts) {
                    Some(idx) => segment.parts.remove(idx),
                    None => return Err(DbcError::EvaluationError("no part of kind RowsAffected".to_string())),
                };

                match part.arg {
                    Argument::RowsAffected(vec) => {
                        Ok(CallableStatementResult::RowsAffected(vec))
                    },
                    _ => Err(DbcError::EvaluationError("unexpected error in CallableStatement::execute() 2".to_string())),
                }

            },

            _ => {
                return Err(DbcError::EvaluationError(
                    format!("CallableStatement: unexpected function code {:?}", &segment.function_code)
                ));
            },
        }
    }

    pub fn execute_rs(&self, auto_commit: bool) -> DbcResult<ResultSet> {
        try!(self.execute(auto_commit)).as_resultset()
    }
    pub fn execute_ra(&self, auto_commit: bool) -> DbcResult<Vec<RowsAffected>> {
        try!(self.execute(auto_commit)).as_rows_affected()
    }
}

pub enum CallableStatementResult {
    ResultSet(ResultSet),
    RowsAffected(Vec<RowsAffected>)
}

impl CallableStatementResult {
    pub fn as_resultset(self) -> DbcResult<ResultSet> {
        match self {
            CallableStatementResult::RowsAffected(_) => {
                Err(DbcError::EvaluationError("The statement returned a RowsAffected, not a ResultSet".to_string()))
            },
            CallableStatementResult::ResultSet(rs) => Ok(rs),
        }
    }
    pub fn as_rows_affected(self) -> DbcResult<Vec<RowsAffected>> {
        match self {
            CallableStatementResult::RowsAffected(v) => Ok(v),
            CallableStatementResult::ResultSet(_) => {
                Err(DbcError::EvaluationError("The statement returned a ResultSet, not a RowsAffected".to_string()))
            },
        }
    }
}
