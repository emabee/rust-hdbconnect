use super::lowlevel::argument::Argument;
use super::lowlevel::message::*;
use super::lowlevel::part;
use super::lowlevel::partkind::*;
use super::lowlevel::resultset::ResultSet;
use super::lowlevel::segment;
use super::lowlevel::util;

use std::io;
use std::net::TcpStream;

///
pub fn execute(tcp_stream: &mut TcpStream, mut message: Message, stmt: String, auto_commit: bool)
    -> io::Result<ResultSet>
{
    trace!("plain_statement::execute()");
    // build the request
    let mut segment = segment::new_request_seg(segment::MessageType::ExecuteDirect, auto_commit);
    segment.push(part::new(PartKind::Command, 0, Argument::Command(stmt)));
    message.segments.push(segment);

    // send it
    let response = try!(message.send_and_receive(tcp_stream));

    // digest response
    assert!(response.segments.len() == 1, "Wrong count of segments");
    let segment = response.segments.get(0).unwrap();
    match (&segment.kind, &segment.function_code) {
        (&segment::Kind::Reply, &Some(segment::FunctionCode::Select)) => {},
        _ => return Err(util::io_error(&format!("unexpected segment kind {:?} or function code {:?}",
                                                 &segment.kind, &segment.function_code))),
    }

    let part = match util::get_first_part_of_kind(PartKind::ResultSet, &segment.parts) {
        Some(idx) => segment.parts.get(idx).unwrap(),
        None => return Err(util::io_error("no part of kind ResultSet")),
    };

    match part.arg {
        Argument::ResultSet(ref resultset) => Ok((*resultset).clone()),
        _ => Err(util::io_error("wrong Argument variant found in response from DB")),
    }
}
