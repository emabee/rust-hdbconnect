use {DbcError,DbcResult,DbResult};
use protocol::lowlevel::conn_core::ConnRef;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::{Request,Metadata};
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;

///
pub struct CallableStatement {
    conn_ref: ConnRef,
    stmt: String,
    auto_commit: bool,
}
impl CallableStatement {
    pub fn new(conn_ref: ConnRef, stmt: String, auto_commit: bool) -> CallableStatement {
        CallableStatement { conn_ref: conn_ref, stmt: stmt, auto_commit: auto_commit }
    }
}

impl CallableStatement {
    pub fn execute(&self) -> DbcResult<DbResult> {
        trace!("CallableStatement::execute({})",self.stmt);
        // build the request
        let command_options = 0b_1000;
        let mut request = Request::new(0, RequestType::ExecuteDirect, self.auto_commit, command_options);
        let fetch_size = { self.conn_ref.borrow().get_fetch_size() };
        request.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));
        request.push(Part::new(PartKind::Command, Argument::Command(self.stmt.clone())));


        // send it
        let mut reply = try!(request.send_and_receive(&Metadata::None, &mut None, &(self.conn_ref), None));

        // digest reply
        match reply.type_ {
            ReplyType::Select => {
                let part = try!(reply.retrieve_first_part_of_kind(PartKind::ResultSet));
                match part.arg {
                    Argument::ResultSet(Some(mut resultset)) => {
                        Ok(DbResult::ResultSet(resultset))
                    },
                    _ => Err(DbcError::EvaluationError(String::from("No ResultSet in CallableStatement::execute()"))),
                }
            },

            ReplyType::Ddl | ReplyType::Insert => {
                let part = try!(reply.retrieve_first_part_of_kind(PartKind::RowsAffected));
                match part.arg {
                    Argument::RowsAffected(vec) => {
                        Ok(DbResult::RowsAffected(vec))
                    },
                    _ => Err(DbcError::EvaluationError(String::from("No RowsAffected in CallableStatement::execute()"))),
                }
            },

            rt => Err(DbcError::EvaluationError(format!("CallableStatement: unexpected reply type {:?}", rt))),
        }
    }
}
