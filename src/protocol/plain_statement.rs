use {DbcError,DbcResult};
use super::super::connection::ConnectionState;
use super::lowlevel::argument::Argument;
use super::lowlevel::message::Message;
use super::lowlevel::part::Part;
use super::lowlevel::partkind::PartKind;
use super::lowlevel::resultset::ResultSet;
use super::lowlevel::segment;
use super::lowlevel::util;


///
pub fn execute(conn_state: &mut ConnectionState, stmt: String, auto_commit: bool) -> DbcResult<ResultSet> {
    trace!("plain_statement::execute()");
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
    match (&segment.kind, &segment.function_code) {
        (&segment::Kind::Reply, &Some(segment::FunctionCode::Select)) => {},
        _ => return Err(DbcError::ProtocolError(format!("plain_statement: unexpected segment kind {:?} or function code {:?}",
                                                 &segment.kind, &segment.function_code))),
    }

    let part = match util::get_first_part_of_kind(PartKind::ResultSet, &segment.parts) {
        Some(idx) => segment.parts.swap_remove(idx),
        None => return Err(DbcError::ProtocolError("no part of kind ResultSet".to_string())),
    };

    match part.arg {
        Argument::ResultSet(Some(mut resultset)) => {
            try!(resultset.fetch_all(conn_state));
            Ok(resultset)
        },
        _ => Err(DbcError::ProtocolError("unexpected error in plain_statement::execute()".to_string())),
    }
}
