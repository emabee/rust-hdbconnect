use {DbcResult,DbResponses};
use protocol::lowlevel::conn_core::ConnRef;
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::message::{Request,Metadata};
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;

///
pub struct AdhocStatement {
    conn_ref: ConnRef,
    stmt: String,
    auto_commit: bool,
}
impl AdhocStatement {
    pub fn new(conn_ref: ConnRef, stmt: String, auto_commit: bool) -> AdhocStatement {
        AdhocStatement { conn_ref: conn_ref, stmt: stmt, auto_commit: auto_commit }
    }
}

impl AdhocStatement {
    pub fn execute(&self) -> DbcResult<DbResponses> {
        trace!("AdhocStatement::execute({})",self.stmt);
        // build the request
        let command_options = 0b_1000;
        let mut request = try!(Request::new(&(self.conn_ref), RequestType::ExecuteDirect, self.auto_commit, command_options));
        let fetch_size = { self.conn_ref.borrow().get_fetch_size() };
        request.push(Part::new(PartKind::FetchSize, Argument::FetchSize(fetch_size)));
        request.push(Part::new(PartKind::Command, Argument::Command(self.stmt.clone())));

        // send it
        request.send_and_get_response(Metadata::None, &(self.conn_ref), None)
    }
}
