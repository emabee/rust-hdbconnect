use super::lowlevel::argument::Argument;
use super::lowlevel::authfield::*;
use super::lowlevel::clientcontext_option::*;
use super::lowlevel::connect_option::*;
use super::lowlevel::message::Message;
use super::lowlevel::option_value::*;
use super::lowlevel::part;
use super::lowlevel::partkind::*;
use super::lowlevel::segment;
use super::lowlevel::topology_attribute::*;
use super::lowlevel::util;

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
pub fn authenticate_with_scram_sha256(tcp_stream: &mut TcpStream, username: &str, password: &str)
    -> Result<(Vec<ConnectOption>,Vec<TopologyAttr>,i64)> {
    trace!("Entering scram_sha256()");

    let client_challenge = create_client_challenge();
    let response1 = try!(build_auth1_request(&client_challenge, username).send_and_receive(tcp_stream));
    let server_challenge: Vec<u8> = get_server_challenge(response1);
    let client_proof = try!(calculate_client_proof(server_challenge, client_challenge, password));
    let response2 = try!(build_auth2_request(&client_proof, username).send_and_receive(tcp_stream));
    evaluate_resp2(response2)
}

/// Build the auth1-request: message_header + (segment_header + (part1+options) + (part2+authrequest)
/// the md5 stuff seems obsolete!
fn build_auth1_request (chllng_sha256: &Vec<u8>, username: &str) -> Message {
    trace!("Entering auth1_request()");
    let mut message = Message::new(-1i64, 0i32);

    let mut cc_options = Vec::<CcOption>::new();
    cc_options.push( CcOption {
        id: CcOptionId::Version,
        value: OptionValue::STRING("1.50.00.000000".to_string()),
    });
    cc_options.push( CcOption {
        id: CcOptionId::ClientType,
        value: OptionValue::STRING("JDBC".to_string()),
    });
    cc_options.push( CcOption {
        id: CcOptionId::ClientApplicationProgram,
        value: OptionValue::STRING("UNKNOWN".to_string()),
    });

    let part1 = part::new(PartKind::ClientContext, 0, Argument::CcOptions(cc_options));

    let mut auth_fields = Vec::<AuthField>::with_capacity(5);
    auth_fields.push(AuthField {v: username.as_bytes().to_vec() });
    auth_fields.push(AuthField {v: b"SCRAMSHA256".to_vec() });
    auth_fields.push(AuthField {v: chllng_sha256.clone() });

    let part2 = part::new(PartKind::Authentication, 0, Argument::Auth(auth_fields));

    let mut segment = segment::new_request_seg(segment::MessageType::Authenticate,true);
    segment.push(part1);
    segment.push(part2);

    message.segments.push(segment);
    trace!("Message: {:?}", message);
    message
}

fn build_auth2_request (client_proof: &Vec<u8>, username: &str) -> Message {
    trace!("Entering auth2_request()");
    let mut message = Message::new(0i64, 1i32);
    let mut segment = segment::new_request_seg(segment::MessageType::Connect, true);

    let mut auth_fields = Vec::<AuthField>::with_capacity(3);
    auth_fields.push(AuthField {v: username.as_bytes().to_vec() });
    auth_fields.push(AuthField {v: b"SCRAMSHA256".to_vec() });
    auth_fields.push(AuthField {v: client_proof.clone() });
    segment.push(part::new(PartKind::Authentication, 0, Argument::Auth(auth_fields)));
    segment.push(part::new(PartKind::ClientID, 0, Argument::ClientID(get_client_id())));

    let mut conn_opts = Vec::<ConnectOption>::new();
    conn_opts.push( ConnectOption{id: ConnectOptionId::CompleteArrayExecution, value: OptionValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DataFormatVersion2, value: OptionValue::INT(4)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DataFormatVersion, value: OptionValue::INT(1)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::ClientLocale, value: OptionValue::STRING(get_locale())});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DistributionEnabled, value: OptionValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::ClientDistributionMode, value: OptionValue::INT(3)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::SelectForUpdateSupported, value: OptionValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DistributionProtocolVersion, value: OptionValue::INT(1)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::RowSlotImageParameter, value: OptionValue::BOOLEAN(true)});
    segment.push(part::new(PartKind::ConnectOptions, 0, Argument::ConnectOptions(conn_opts)));

    message.segments.push(segment);
    trace!("Message: {:?}", message);
    message
}

fn get_client_id() -> Vec<u8> {
    // FIXME this is supposed to return something like 10508@WDFN00319537A.EMEA.global.corp.sap
    // i.e., <process id>@<fully qualified hostname>
    // rust has nothing stable to access the process id or the host name

    //b"do not know who and where I am, sorry".to_vec()
    b"10509@WDFN00319537A.emea.global.corp.sap".to_vec()
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

fn get_server_challenge(response1: Message) -> Vec<u8> {
    trace!("Entering get_server_challenge()");
    debug!("Trying to read server_challenge from {:?}", response1);
    assert!(response1.segments.len() == 1, "Wrong count of segments");

    let segment = response1.segments.get(0).unwrap();
    match (&segment.kind, &segment.function_code) {
        (&segment::Kind::Reply, &Some(segment::FunctionCode::Nil)) => {},
        _ => {panic!("unexpected segment kind {:?} or function code {:?} at 1", &segment.kind, &segment.function_code)}
    }

    let part = match util::get_first_part_of_kind(PartKind::Authentication, &segment.parts) {
        Some(idx) => segment.parts.get(idx).unwrap(),
        None => panic!("no part of kind Authentication"),
    };

    if let Argument::Auth(ref vec) = part.arg {
        let server_challenge = vec.get(1).unwrap().v.clone();
        trace!("Leaving get_server_challenge() with {:?}",&server_challenge);
        server_challenge
    } else {
        panic!("wrong Argument variant");
    }
}

fn evaluate_resp2(response2: Message) -> Result<(Vec<ConnectOption>,Vec<TopologyAttr>,i64)> {
    trace!("Entering evaluate_resp2()");
    assert!(response2.segments.len() == 1, "Wrong count of segments");

    let segment = response2.segments.get(0).unwrap();
    match (&segment.kind, &segment.function_code) {
        (&segment::Kind::Reply, &Some(segment::FunctionCode::Nil)) => {},
        _ => {panic!("unexpected segment kind {:?} or function code {:?} at 2", &segment.kind, &segment.function_code)}
    }
    assert!(segment.parts.len() >= 1, "no part found");

    let mut server_proof = Vec::<u8>::new();
    let mut conn_opts = Vec::<ConnectOption>::new();
    let mut topo_attrs = Vec::<TopologyAttr>::new();

    // FIXME rather than copying the args, we should consume the original values and hand them out
    for ref part in &segment.parts {
        match part.kind {
            PartKind::Authentication => {
                if let Argument::Auth(ref vec) = part.arg {
                    for b in &(vec.get(0).unwrap()).v { server_proof.push(*b); }
                }
            },
            PartKind::ConnectOptions => {
                if let Argument::ConnectOptions(ref vec) = part.arg {
                    for e in vec { conn_opts.push((*e).clone()); } // FIXME avoid clone
                }
            },
            PartKind::TopologyInformation => {
                if let Argument::TopologyInformation(ref vec) = part.arg {
                    for e in vec { topo_attrs.push((*e).clone()); } // FIXME avoid clone
                }
            },
            pk => {
                error!("ignoring unexpected partkind ({}) in auth_response2", pk.to_i8());
            }
        }
    }
    warn!("still don't know what to do with the server proof: {:?}", server_proof);
    Ok((conn_opts,topo_attrs,response2.session_id))
}

fn calculate_client_proof(server_challenge: Vec<u8>, client_challenge: Vec<u8>, password: &str)
                    -> Result<Vec<u8>> {
    let client_proof_size = 32usize;
    trace!("Entering calculate_client_proof()");
    let (salts,srv_key) = get_salt_and_key(server_challenge).unwrap();
    let buf = Vec::<u8>::with_capacity(2 + (client_proof_size+1)*salts.len());
    let mut w = Cursor::new(buf);
    try!(w.write_u8(0u8));
    try!(w.write_u8(salts.len() as u8));

    for salt in salts {
        try!(w.write_u8(client_proof_size as u8));
        trace!("buf: \n{:?}",w.get_ref());
        let scrambled = try!(scramble(&salt, &srv_key, &client_challenge, password));
        for b in scrambled {try!(w.write_u8(b));}                // B variable   VALUE
        trace!("buf: \n{:?}",w.get_ref());
    }
    Ok(w.into_inner())
}

/// Server_challenge is structured itself into fieldcount and fields
/// the last field is taken as key, all the previous fields are salt (usually 1)
fn get_salt_and_key(server_challenge: Vec<u8>) -> Result<(Vec<Vec<u8>>,Vec<u8>)> {
    trace!("Entering get_salt_and_key()");
    let mut rdr = Cursor::new(server_challenge);
    let fieldcount = rdr.read_i16::<LittleEndian>().unwrap();               // I2
    trace!("fieldcount = {}", fieldcount);

    type BVec = Vec<u8>;
    let mut salts = Vec::<BVec>::new();
    for _ in 0..(fieldcount-1) {
        let len = try!(rdr.read_u8());                                      // B1
        let mut salt: Vec<u8> = repeat(0u8).take(len as usize).collect();
        try!(rdr.read(&mut salt));                                          // variable
        trace!("salt: \n{:?}",salt);
        salts.push(salt);
    }

    let len = try!(rdr.read_u8());                                          // B1
    let mut key: Vec<u8> = repeat(0u8).take(len as usize).collect();
    try!(rdr.read(&mut key));                                               // variable
    trace!("key: \n{:?}",key);
    Ok((salts,key))
}

fn scramble(salt: &Vec<u8>, server_key: &Vec<u8>, client_key: &Vec<u8>, password: &str)
            -> Result<Vec<u8>> {
    let length = salt.len() + server_key.len() + client_key.len();
    let mut msg = Vec::<u8>::with_capacity(length);
    for b in salt {msg.push(*b)}
    trace!("salt: \n{:?}",msg);
    for b in server_key {msg.push(*b)}
    trace!("salt + server_key: \n{:?}",msg);
    for b in client_key {msg.push(*b)}
    trace!("salt + server_key + client_key: \n{:?}",msg);

    let tmp = &hmac(&password.as_bytes().to_vec(), salt);
    trace!("tmp = hmac(password, salt): \n{:?}",tmp);

    let key: &Vec<u8> = &sha256(tmp);
    trace!("sha256(tmp): \n{:?}",key);

    let sig: &Vec<u8> = &hmac(&sha256(key), &msg);
    trace!("sig = hmac(sha256(key),msg): \n{:?}",sig);
    let scramble = xor(&sig,&key);
    trace!("scramble = xor(sig,key): \n{:?}",scramble);
    Ok(scramble)
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
    for i in 0..a.len() {
        bytes[i] = a[i] ^ b[i];
    }
    bytes
}


#[cfg(test)]
mod tests {
    use super::calculate_client_proof;
//    use flexi_logger;

    // run exclusively with
    // cargo test protocol::authentication::tests::test_client_proof -- --nocapture
    #[test]
    fn test_client_proof() {

//        flexi_logger::init(flexi_logger::LogConfig::new(), Some("hdbconnect=warn".to_string())).unwrap();

        let client_challenge: Vec<u8> = b"\xb5\xab\x3a\x90\xc5\xad\xb8\x04\x15\x27\x37\x66\x54\xd7\x5c\x31\x94\xd8\x61\x50\x3f\xe0\x8d\xff\x8b\xea\xd5\x1b\xc3\x5a\x07\xcc\x63\xed\xbf\xa9\x5d\x03\x62\xf5\x6f\x1a\x48\x2e\x4c\x3f\xb8\x32\xe4\x1c\x89\x74\xf9\x02\xef\x87\x38\xcc\x74\xb6\xef\x99\x2e\x8e"
        .to_vec();
        let server_challenge: Vec<u8> =
        b"\x02\x00\x10\x12\x41\xe5\x8f\x39\x23\x4e\xeb\x77\x3e\x90\x90\x33\xe5\xcb\x6e\x30\x1a\xce\xdc\xdd\x05\xc1\x90\xb0\xf0\xd0\x7d\x81\x1a\xdb\x0d\x6f\xed\xa8\x87\x59\xc2\x94\x06\x0d\xae\xab\x3f\x62\xea\x4b\x16\x6a\xc9\x7e\xfc\x9a\x6b\xde\x4f\xe9\xe5\xda\xcc\xb5\x0a\xcf\xce\x56"
        .to_vec();
        let password: &str = "manager";
        let correct_client_proof: Vec<u8> = b"\x00\x01\x20\x17\x26\x25\xab\x29\x71\xd8\x58\x74\x32\x5d\x21\xbc\x3d\x68\x37\x71\x80\x5c\x9a\xfe\x38\xd0\x95\x1d\xad\x46\x53\x00\x9c\xc9\x21"
        .to_vec();

        trace!("----------------------------------------------------");
        trace!("client_challenge ({} bytes): \n{:?}",&client_challenge.len(),&client_challenge);
        trace!("server_challenge ({} bytes): \n{:?}",&server_challenge.len(),&server_challenge);

        let my_client_proof = calculate_client_proof(server_challenge, client_challenge, password).unwrap();

        trace!("my_client_proof ({} bytes): \n{:?}",&my_client_proof.len(),&my_client_proof);
        trace!("correct_client_proof ({} bytes): \n{:?}",&correct_client_proof.len(),&correct_client_proof);
        trace!("----------------------------------------------------");
        assert_eq!(my_client_proof,correct_client_proof);
    }
}
