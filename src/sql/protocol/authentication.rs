use super::dberr::*;
use super::lowlevel::argument::*;
use super::lowlevel::message::*;
use super::lowlevel::part::*;
use super::lowlevel::segment::*;
use super::dbstream::*;

use log::LogLevel::Trace;
use rand::{Rng,thread_rng};

/// authenticate with the scram_sha256 method
pub fn scram_sha256(dbstream: &mut DbStream, username: &str, password: &str) -> DbResult<()> {

    // Build the request: message_header + (segment_header + (part1+options) + (part2+authrequest)
    let mut arg_v = Vec::<Option>::new();
    arg_v.push( Option {
        id: OptionId::Version,
        value: b"1.50.00.000000".to_vec(),
    });
    arg_v.push( Option {
        id: OptionId::ClientType,
        value: b"JDBC".to_vec()
    });
    arg_v.push( Option {
        id: OptionId::ClientApplicationProgram,
        value: b"UNKNOWN".to_vec()
    });
    let part1 = Part::new(PartKind::ClientContext, Argument::Options(arg_v));

    let username = username.as_bytes();
    let mut auth_s = AuthS {
        user: Vec::<u8>::with_capacity(username.len()),
        methods: Vec::<AuthMethod>::new(),
    };
    for b in username{
        auth_s.user.push(*b);
    }

    auth_s.methods.push( AuthMethod {
        name: b"SCRAMSHA256".to_vec(),
        client_challenge: get_client_challenge(),
    });
    auth_s.methods.push( AuthMethod {
        name: b"SCRAMMD5".to_vec(),
        client_challenge: get_client_challenge(),
    });
    let part2 = Part::new(PartKind::Authentication, Argument::Auth(auth_s));


    let mut segment = Segment::new(SegmentKind::Request, MessageType::Authenticate) ;
    segment.push(part1);
    segment.push(part2);

    let (session_id, packet_seq_number) = (-1i64, 0i32);
    let mut message = Message::new(session_id, packet_seq_number);
    message.push(segment);

    // Serialize the request
    let mut request_buffer = Vec::<u8>::with_capacity(300);
    let mut response_buffer = Vec::<u8>::with_capacity(300);

    debug!("Message: {:?}", message);
    message.encode(&mut request_buffer);
    let mut i = 0;
    if log_enabled!(Trace) {
        for b in &request_buffer {
            i+=1;
            trace!("Request: {:3} = {:0>2x}",i, b);
        }
    }
    //let received = try!(dbstream.send_and_receive(&request_buffer, &mut response_buffer));
    // for i in 0..received {
    //     info!("Response: {} = {:X} = {}",i,resp_buffer[i],resp_buffer[i]);
    // }

    // FIXME digest the response

    // FIXME send client proof

    // FIXME digest the response

    Err(DbError::from_str("to be implemented"))  // FIXME
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

// fn get_client_challenge() -> Vec<u8>{
//     let mut rng = thread_rng();
//     let mut client_challenge: [u8;64] = [0;64];
//     rng.fill_bytes(&mut client_challenge);
//     let res = (*as_vec(&client_challenge)).clone();
//     res
// }
