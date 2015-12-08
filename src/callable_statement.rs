use {DbcError,DbcResult};
use protocol::lowlevel::conn_core::ConnRef;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::function_code::FunctionCode;
use protocol::lowlevel::message::RequestMessage;
use protocol::lowlevel::message_type::MessageType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::resultset::ResultSet;
use protocol::lowlevel::rows_affected::RowsAffected;
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
        let mut message = RequestMessage::new(0, MessageType::ExecuteDirect, auto_commit, command_options);
        let fetch_size = { self.conn_ref.borrow().get_fetch_size() };
        message.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));
        message.push(Part::new(PartKind::Command, Argument::Command(self.stmt.clone())));


        // send it
        let mut response = try!(message.send_and_receive(&mut None, &(self.conn_ref), None));

        // digest response
        match &response.function_code {
            &FunctionCode::Select => {
                let part = match util::get_first_part_of_kind(PartKind::ResultSet, &response.parts) {
                    Some(idx) => response.parts.swap_remove(idx),
                    None => return Err(DbcError::EvaluationError(String::from("no part of kind ResultSet"))),
                };
                match part.arg {
                    Argument::ResultSet(Some(mut resultset)) => {
                        try!(resultset.fetch_all());  // FIXME fetching remaining data should be done more lazily
                        Ok(CallableStatementResult::ResultSet(resultset))
                    },
                    _ => Err(DbcError::EvaluationError(String::from("unexpected error in CallableStatement::execute() 1"))),
                }
            },

            &FunctionCode::Ddl | &FunctionCode::Insert => {
                let part = match util::get_first_part_of_kind(PartKind::RowsAffected, &response.parts) {
                    Some(idx) => response.parts.remove(idx),
                    None => return Err(DbcError::EvaluationError(String::from("no part of kind RowsAffected"))),
                };
                match part.arg {
                    Argument::RowsAffected(vec) => {
                        Ok(CallableStatementResult::RowsAffected(vec))
                    },
                    _ => Err(DbcError::EvaluationError(String::from("unexpected error in CallableStatement::execute() 2"))),
                }
            },

            fc => Err(DbcError::EvaluationError(format!("CallableStatement: unexpected function code {:?}", fc))),
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
                Err(DbcError::EvaluationError(String::from("The statement returned a RowsAffected, not a ResultSet")))
            },
            CallableStatementResult::ResultSet(rs) => Ok(rs),
        }
    }
    pub fn as_rows_affected(self) -> DbcResult<Vec<RowsAffected>> {
        match self {
            CallableStatementResult::RowsAffected(v) => Ok(v),
            CallableStatementResult::ResultSet(_) => {
                Err(DbcError::EvaluationError(String::from("The statement returned a ResultSet, not a RowsAffected")))
            },
        }
    }
}
