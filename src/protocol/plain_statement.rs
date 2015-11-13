use {DbcError,DbcResult};
use super::super::connection::ConnectionState;
use super::lowlevel::argument::Argument;
use super::lowlevel::message::Message;
use super::lowlevel::part::Part;
use super::lowlevel::partkind::PartKind;
use super::lowlevel::resultset::ResultSet;
use super::lowlevel::rows_affected::RowsAffected;
use super::lowlevel::segment;
use super::lowlevel::util;

///
pub fn execute(conn_state: &mut ConnectionState, stmt: String, auto_commit: bool)
   -> DbcResult<PlainStatementResult>
{
    trace!("plain_statement::execute({})",stmt);
    // build the request
    let mut segment = segment::new_request_seg(segment::MessageType::ExecuteDirect, auto_commit);
    segment.push(Part::new(PartKind::Command, Argument::Command(stmt)));
    let mut message = Message::new(conn_state.session_id, conn_state.get_next_seq_number());
    message.segments.push(segment);

    // send it
    let mut response = try!(message.send_and_receive(&mut None, &mut (conn_state.stream)));

    // digest response
    assert!(response.segments.len() == 1, "Wrong count of segments");
    let mut segment = response.segments.swap_remove(0);
    match &segment.function_code {
        &Some(segment::FunctionCode::Select) => {
            let part = match util::get_first_part_of_kind(PartKind::ResultSet, &segment.parts) {
                Some(idx) => segment.parts.swap_remove(idx),
                None => return Err(DbcError::ProtocolError("no part of kind ResultSet".to_string())),
            };

            match part.arg {
                Argument::ResultSet(Some(mut resultset)) => {
                    try!(resultset.fetch_all(conn_state));  // FIXME fetching remaining data should done more lazily
                    Ok(PlainStatementResult::Select(resultset))
                },
                _ => Err(DbcError::ProtocolError("unexpected error in plain_statement::execute() 1".to_string())),
            }
        },

        &Some(segment::FunctionCode::Ddl)
        | &Some(segment::FunctionCode::Insert) => {
            let part = match util::get_first_part_of_kind(PartKind::RowsAffected, &segment.parts) {
                Some(idx) => segment.parts.remove(idx),
                None => return Err(DbcError::ProtocolError("no part of kind RowsAffected".to_string())),
            };

            match part.arg {
                Argument::RowsAffected(vec) => {
                    Ok(PlainStatementResult::Ddl(vec))
                },
                _ => Err(DbcError::ProtocolError("unexpected error in plain_statement::execute() 2".to_string())),
            }

        },

        _ => {
            return Err(DbcError::ProtocolError(
                format!("plain_statement: unexpected function code {:?}", &segment.function_code)
            ));
        },
    }
}

pub enum PlainStatementResult {
    Select(ResultSet),
    Ddl(Vec<RowsAffected>)
}

impl PlainStatementResult {
    pub fn as_resultset(self) -> ResultSet {
        match self {
            PlainStatementResult::Ddl(_) => {panic!("The statement returned a RowsAffected, not a ResultSet");},
            PlainStatementResult::Select(rs) => rs,
        }
    }
    pub fn as_rows_affected(self) -> Vec<RowsAffected> {
        match self {
            PlainStatementResult::Ddl(v) => v,
            PlainStatementResult::Select(_) => {panic!("The statement returned a ResultSet, not a RowsAffected");},
        }
    }
}
