use super::lowlevel::argument::Argument;
use super::lowlevel::authfield::*;
use super::lowlevel::clientcontext_option::*;
use super::lowlevel::connect_option::*;
use super::lowlevel::message;
use super::lowlevel::part;
use super::lowlevel::partkind::*;
use super::lowlevel::segment;
use super::lowlevel::typed_value::*;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use rand::{Rng,thread_rng};
use std::io::{Cursor,Read,Result};
use std::net::TcpStream;

use std::iter::repeat;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::digest::Digest;
use crypto::sha2::Sha256;


/// authenticate with the scram_sha256 method
pub fn scram_sha256(tcp_stream: &mut TcpStream, username: &str, password: &str) -> Result<()> {
    trace!("Entering scram_sha256()");

    let client_challenge = create_client_challenge();
    let response1 = try!(auth1_request(&client_challenge, &create_client_challenge(), username)
                         .send_and_receive(tcp_stream));

    let server_challenge: Vec<u8> = get_server_challenge(response1);

    let client_proof = try!(get_client_proof(server_challenge, client_challenge, password));

    let response2 = try!(auth2_request(&client_proof, username)
                         .send_and_receive(tcp_stream));

    // FIXME retrieve and digest the server_proof
    Ok(())
}

/// Build the auth1-request: message_header + (segment_header + (part1+options) + (part2+authrequest)
/// the md5 stuff seems obsolete!
fn auth1_request (chllng_sha256: &Vec<u8>, chllng_md5: &Vec<u8>, username: &str) -> message::Message {
    trace!("Entering auth1_request()");
    let mut message = message::new(-1i64, 0i32);

    let mut arg_v = Vec::<CcOption>::new();
    arg_v.push( CcOption {
        id: CcOptionId::Version,
        value: TypedValue::STRING("1.50.00.000000".to_string()),
    });
    arg_v.push( CcOption {
        id: CcOptionId::ClientType,
        value: TypedValue::STRING("JDBC".to_string()),
    });
    arg_v.push( CcOption {
        id: CcOptionId::ClientApplicationProgram,
        value: TypedValue::STRING("UNKNOWN".to_string()),
    });

    let mut part1 = part::new(PartKind::ClientContext);
    part1.set_arg(Argument::CcOptions(arg_v));


    let mut auth_fields = Vec::<AuthField>::with_capacity(5);
    auth_fields.push(AuthField {v: username.as_bytes().to_vec() });
    auth_fields.push(AuthField {v: b"SCRAMSHA256".to_vec() });
    auth_fields.push(AuthField {v: chllng_sha256.clone() });
    auth_fields.push(AuthField {v: b"SCRAMMD5".to_vec() });
    auth_fields.push(AuthField {v: chllng_md5.clone() });

    let mut part2 = part::new(PartKind::Authentication);
    part2.set_arg(Argument::Auth(auth_fields));


    let mut segment = segment::new(segment::Kind::Request, segment::Type::Authenticate) ;
    segment.push(part1);
    segment.push(part2);

    message.segments.push(segment);
    trace!("Message: {:?}", message);
    message
}

fn auth2_request (client_proof: &Vec<u8>, username: &str) -> message::Message {
    trace!("Entering auth2_request()");
    let mut message = message::new(0i64, 1i32);
    let mut segment = segment::new(segment::Kind::Request, segment::Type::Connect) ;

    let mut part1 = part::new(PartKind::ItabShm);  // What is this!?
    let mut auth_fields = Vec::<AuthField>::with_capacity(3);
    auth_fields.push(AuthField {v: username.as_bytes().to_vec() });
    auth_fields.push(AuthField {v: b"SCRAMSHA256".to_vec() });
    auth_fields.push(AuthField {v: client_proof.clone() });
    part1.set_arg(Argument::ItabShm(auth_fields));
    segment.push(part1);

    let mut part2 = part::new(PartKind::ClientID);
    part2.set_arg(Argument::ClientID(get_client_id()));
    segment.push(part2);

    let mut part3 = part::new(PartKind::ConnectOptions);
    let mut conn_opts = Vec::<ConnectOption>::new();
    conn_opts.push( ConnectOption{id: ConnectOptionId::CompleteArrayExecution, value: TypedValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DataFormatVersion2, value: TypedValue::INT(4)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DataFormatVersion, value: TypedValue::INT(1)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::ClientLocale, value: TypedValue::STRING(get_locale())});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DistributionEnabled, value: TypedValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::ClientDistributionMode, value: TypedValue::INT(3)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::SelectForUpdateSupported, value: TypedValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DistributionProtocolVersion, value: TypedValue::INT(1)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::RowSlotImageParameter, value: TypedValue::BOOLEAN(true)});

    part3.set_arg(Argument::ConnectOptions(conn_opts));
    segment.push(part3);

    message.segments.push(segment);
    trace!("Message: {:?}", message);
    message
}

fn get_client_id() -> Vec<u8> {
    // FIXME this is supposed to return something like 10508@WDFN00319537A.EMEA.global.corp.sap
    // i.e., <process id>@<fully qualified hostname>
    // rust has nothing stable to access the process id or the host name
    b"do not know who and where I am, sorry".to_vec()
}

fn get_locale() -> String {
    "en_US".to_string()
}

fn create_client_challenge() -> Vec<u8>{
    let mut client_challenge = [0u8; 64];
    let mut rng = thread_rng();
    rng.fill_bytes(&mut client_challenge);
    client_challenge.to_vec()
}

fn get_server_challenge(response1: message::Message) -> Vec<u8> {
    trace!("Entering get_server_challenge()");
    debug!("Trying to read server_challenge from {:?}", response1);
    assert!(response1.segments.len() == 1, "Wrong count of segments");

    let segment = response1.segments.get(0).unwrap();
    match (&segment.kind, &segment.function_code) {
        (&segment::Kind::Reply, &segment::FunctionCode::INITIAL) => {},
        _ => {panic!("bad segment")}
    }
    assert!(segment.parts.len() == 1, "wrong count of parts");

    let part = segment.parts.get(0).unwrap();
    match part.kind {
        PartKind::Authentication => {},
        _ => {panic!("wrong part kind")}
    }

    if let Argument::Auth(ref vec) = part.arg {
        let server_challenge = vec.get(1).unwrap().v.clone();
        trace!("Leaving get_server_challenge() with {:?}",&server_challenge);
        server_challenge
    } else {
        panic!("wrong Argument variant");
    }
}

const CLIENT_PROOF_SIZE: usize = 32;
fn get_client_proof(server_challenge: Vec<u8>, client_challenge: Vec<u8>, password: &str)
                    -> Result<Vec<u8>> {
    trace!("Entering get_client_proof()");
    let (salts,srv_key) = get_salt_and_key(server_challenge).unwrap();
    let buf = Vec::<u8>::with_capacity(2 + (CLIENT_PROOF_SIZE+1)*salts.len());
    let mut w = Cursor::new(buf);
    try!(w.write_u8(0u8));
    try!(w.write_u8(salts.len() as u8));

    for salt in salts {
        try!(w.write_u8(CLIENT_PROOF_SIZE as u8));
        let scrambled = try!(scramble(&salt, &srv_key, &client_challenge, password));
        for b in scrambled {try!(w.write_u8(b));}                // B variable   VALUE
    }
    Ok(w.into_inner())
}

/// Server_challenge is structured itself into fieldcount and fields
/// the last field is taken as key, all the previous fields are salt (usually 1)
fn get_salt_and_key(server_challenge: Vec<u8>) -> Result<(Vec<Vec<u8>>,Vec<u8>)> {
    trace!("Entering get_salt_and_key()");
    let mut rdr = Cursor::new(server_challenge);
    let fieldcount = rdr.read_i16::<LittleEndian>().unwrap();               // I2

    type BVec = Vec<u8>;
    let mut salts = Vec::<BVec>::new();
    for _ in 0..(fieldcount-1) {
        let len = try!(rdr.read_u8());                                      // B1
        let mut salt: Vec<u8> = repeat(0u8).take(len as usize).collect();
        try!(rdr.read(&mut salt));                                          // variable
        salts.push(salt);
    }
    trace!("fieldcount = {}, salts = {:?}", fieldcount, salts);

    let len = try!(rdr.read_u8());                                          // B1
    let mut key: Vec<u8> = repeat(0u8).take(len as usize).collect();
    try!(rdr.read(&mut key));                                               // variable
    Ok((salts,key))
}

fn scramble(salt: &Vec<u8>, server_key: &Vec<u8>, client_key: &Vec<u8>, password: &str)
            -> Result<Vec<u8>> {
    let length = salt.len() + server_key.len() + client_key.len();
    let mut msg = Vec::<u8>::with_capacity(length);
    for b in salt {msg.push(*b)}
    for b in server_key {msg.push(*b)}
    for b in client_key {msg.push(*b)}

    let tmp = &hmac(&password.as_bytes().to_vec(), salt);
    let key: &Vec<u8> = &sha256(tmp);
    let sig: &Vec<u8> = &hmac(&sha256(key), &msg);
    Ok(xor(&sig,&key))
}

fn hmac(key: &Vec<u8>, message: &Vec<u8>) -> Vec<u8> {
    let mut hmac = Hmac::new(Sha256::new(), &key);
    hmac.input(message);
    hmac.result().code().to_vec()
}

fn sha256(input: &Vec<u8>) -> Vec<u8> {
    let mut sha = Sha256::new();
    sha.input(input);

    let mut bytes: Vec<u8> = repeat(0u8).take(sha.output_bytes()).collect();
    sha.result(&mut bytes[..]);
    bytes
}

fn xor(a: &Vec<u8>, b: &Vec<u8>) -> Vec<u8> {
    assert!(a.len() == b.len(), "xor needs two equally long parameters");

    let mut bytes: Vec<u8> = repeat(0u8).take(a.len()).collect();
    for i in (0..a.len()) {
        bytes[i] = a[i] ^ b[i];
    }
    bytes
}
