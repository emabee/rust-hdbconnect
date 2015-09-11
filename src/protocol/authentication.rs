use super::lowlevel::argument::Argument;
use super::lowlevel::authfield::*;
use super::lowlevel::hdboption::*;
use super::lowlevel::message;
use super::lowlevel::part;
use super::lowlevel::partkind::*;
use super::lowlevel::segment;

use rand::{Rng,thread_rng};
use std::io::{Error,ErrorKind,Result};
use std::net::TcpStream;

/// authenticate with the scram_sha256 method
pub fn scram_sha256(tcp_stream: &mut TcpStream, username: &str, password: &str) -> Result<()> {

    let response = try!(auth1_request(username).send_and_receive(tcp_stream));

    debug!("Got a message: {:?}", response);

    // FIXME digest the response

    // FIXME send client proof

    // FIXME digest the response

    Err(Error::new(ErrorKind::Other, "scram_sha256: remainder to be implemented"))  // FIXME
}


/// Build the auth1-request: message_header + (segment_header + (part1+options) + (part2+authrequest)
fn auth1_request (username: &str) -> message::Message {
    let mut arg_v = Vec::<HdbOption>::new();
    arg_v.push( HdbOption {
        id: HdbOptionId::Version,
        value: HdbOptionValue::BSTRING(b"1.50.00.000000".to_vec()),
    });
    arg_v.push( HdbOption {
        id: HdbOptionId::ClientType,
        value: HdbOptionValue::BSTRING(b"JDBC".to_vec()),
    });
    arg_v.push( HdbOption {
        id: HdbOptionId::ClientApplicationProgram,
        value: HdbOptionValue::BSTRING(b"UNKNOWN".to_vec()),
    });

    let mut part1 = part::new(PartKind::ClientContext);
    part1.set_arg(Argument::HdbOptions(arg_v));


    let mut auth_fields = Vec::<AuthField>::with_capacity(5);
    auth_fields.push(AuthField {v: username.as_bytes().to_vec() });
    auth_fields.push(AuthField {v: b"SCRAMSHA256".to_vec() });
    auth_fields.push(AuthField {v: get_client_challenge() });
    auth_fields.push(AuthField {v: b"SCRAMMD5".to_vec() });
    auth_fields.push(AuthField {v: get_client_challenge() });

    let mut part2 = part::new(PartKind::Authentication);
    part2.set_arg(Argument::Auth(auth_fields));


    let mut segment = segment::new(segment::Kind::Request, segment::Type::Authenticate) ;
    segment.push(part1);
    segment.push(part2);

    let (session_id, packet_seq_number) = (-1i64, 0i32);
    let mut message = message::new(session_id, packet_seq_number);
    message.push(segment);
    trace!("Message: {:?}", message);
    message
}

fn get_client_challenge() -> Vec<u8>{
    let mut rng = thread_rng();
    let mut client_challenge: [u8;64] = [0;64];
    rng.fill_bytes(&mut client_challenge);
    let mut res = Vec::<u8>::with_capacity(64);
    for i in 0..64 {
        res.push(client_challenge[i]);
    }
    res
}
