use connection::{ConnProps};
use protocol::protocol_error::{PrtResult,prot_err};
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::ConnRef;
use protocol::lowlevel::function_code::FunctionCode;
use protocol::lowlevel::message::{Request,Reply};
use protocol::lowlevel::message_type::MessageType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::authfield::AuthField;
use protocol::lowlevel::parts::connect_option::{ConnectOption,ConnectOptionId};
use protocol::lowlevel::parts::clientcontext_option::{CcOption,CcOptionId};
use protocol::lowlevel::parts::option_value::OptionValue;
use protocol::lowlevel::util;

use byteorder::{LittleEndian,ReadBytesExt,WriteBytesExt};
use rand::{Rng,thread_rng};
use std::io::{self,Read};

use std::iter::repeat;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::digest::Digest;
use crypto::sha2::Sha256;

// FIXME we do too much clone/copy here - working with slices and references should be possible

/// authenticate with the scram_sha256 method
pub fn authenticate(conn_ref: &ConnRef, conn_props: &mut ConnProps, username: &str, password: &str)
-> PrtResult<()> {
    trace!("Entering authenticate()");

    let client_challenge = create_client_challenge();
    let response1 = try!(build_auth1_request(&client_challenge, username)
                         .send_and_receive(&mut None, conn_ref, Some(FunctionCode::Nil)));
    let server_challenge: Vec<u8> = try!(get_server_challenge(response1));

    let client_proof = try!(calculate_client_proof(server_challenge, client_challenge, password));

    let response2 = try!(build_auth2_request(&client_proof, username)
                         .send_and_receive(&mut None, conn_ref, Some(FunctionCode::Nil)));

    conn_ref.borrow_mut().session_id = response2.session_id;
    evaluate_resp2(response2, conn_props)
}

/// Build the auth1-request: message_header + (segment_header + (part1+options) + (part2+authrequest)
fn build_auth1_request (chllng_sha256: &Vec<u8>, username: &str) -> Request {
    trace!("Entering auth1_request()");

    let mut cc_options = Vec::<CcOption>::new();
    cc_options.push( CcOption {
        id: CcOptionId::Version,
        value: OptionValue::STRING(String::from("1.50.00.000000")),
    });  // FIXME encapsulate this into a CcOptions object
    cc_options.push( CcOption {
        id: CcOptionId::ClientType,
        value: OptionValue::STRING(String::from("JDBC")),
    });
    cc_options.push( CcOption {
        id: CcOptionId::ClientApplicationProgram,
        value: OptionValue::STRING(String::from("UNKNOWN")),
    });

    let part1 = Part::new(PartKind::ClientContext, Argument::CcOptions(cc_options));

    let mut auth_fields = Vec::<AuthField>::with_capacity(3);
    auth_fields.push( AuthField(username.as_bytes().to_vec()) );
    auth_fields.push( AuthField(b"SCRAMSHA256".to_vec()) );
    auth_fields.push( AuthField(chllng_sha256.clone()) );

    let part2 = Part::new(PartKind::Authentication, Argument::Auth(auth_fields));

    let mut request = Request::new(0, MessageType::Authenticate, true, 0);
    request.push(part1);
    request.push(part2);

    trace!("Request: {:?}", request);
    request
}

fn build_auth2_request (client_proof: &Vec<u8>, username: &str) -> Request {
    trace!("Entering auth2_request()");
    let mut request = Request::new(0, MessageType::Connect, true, 0);

    let mut auth_fields = Vec::<AuthField>::with_capacity(3);
    auth_fields.push( AuthField(username.as_bytes().to_vec()) );
    auth_fields.push( AuthField(b"SCRAMSHA256".to_vec()) );
    auth_fields.push( AuthField(client_proof.clone()) );
    request.push(Part::new(PartKind::Authentication, Argument::Auth(auth_fields)));
    request.push(Part::new(PartKind::ClientID, Argument::ClientID(get_client_id())));

    let mut conn_opts = Vec::<ConnectOption>::new(); // FIXME shouldn't we put this into the conn_props, too?
    conn_opts.push( ConnectOption{id: ConnectOptionId::CompleteArrayExecution, value: OptionValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DataFormatVersion2, value: OptionValue::INT(4)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DataFormatVersion, value: OptionValue::INT(1)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::ClientLocale, value: OptionValue::STRING(get_locale())});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DistributionEnabled, value: OptionValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::ClientDistributionMode, value: OptionValue::INT(3)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::SelectForUpdateSupported, value: OptionValue::BOOLEAN(true)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::DistributionProtocolVersion, value: OptionValue::INT(1)});
    conn_opts.push( ConnectOption{id: ConnectOptionId::RowSlotImageParameter, value: OptionValue::BOOLEAN(true)});
    request.push(Part::new(PartKind::ConnectOptions, Argument::ConnectOptions(conn_opts)));

    trace!("request: {:?}", request);
    request
}

fn get_client_id() -> Vec<u8> {
    // FIXME IMPORTANT this is supposed to return something like
    // 10508@WDFN00319537A.EMEA.global.corp.sap
    // i.e., <process id>@<fully qualified hostname>
    // rust has nothing stable to access the process id or the host name

    b"4711@somemachine.nirwana".to_vec()
    // b"10509@WDFN00319537A.emea.global.corp.sap".to_vec()
}

fn get_locale() -> String {
    String::from("en_US")
}

fn create_client_challenge() -> Vec<u8>{
    let mut client_challenge = [0u8; 64];
    let mut rng = thread_rng();
    rng.fill_bytes(&mut client_challenge);
    client_challenge.to_vec()
}

fn get_server_challenge(response: Reply) -> PrtResult<Vec<u8>> {
    trace!("Entering get_server_challenge()");
    let part = match util::get_first_part_of_kind(PartKind::Authentication, &response.parts) {
        Some(idx) => response.parts.get(idx).unwrap(),
        None => return Err(prot_err("no part of kind Authentication")),
    };

    if let Argument::Auth(ref vec) = part.arg {
        let server_challenge = vec.get(1).unwrap().0.clone();
        debug!("get_server_challenge(): returning {:?}",&server_challenge);
        Ok(server_challenge)
    } else {
        Err(prot_err("wrong Argument variant"))
    }
}

fn evaluate_resp2(response: Reply, conn_props: &mut ConnProps) -> PrtResult<()> {
    trace!("Entering evaluate_resp2()");
    assert!(response.parts.len() >= 1, "no part found");

    let mut server_proof = Vec::<u8>::new();

    for part in response.parts {
        match part.kind {
            PartKind::Authentication => {
                if let Argument::Auth(ref vec) = part.arg {
                    for b in &(vec.get(0).unwrap()).0 { server_proof.push(*b); }
                }
            },
            PartKind::ConnectOptions => {
                if let Argument::ConnectOptions(vec) = part.arg {
                    for e in vec { conn_props.connect_options.push(e); }
                }
            },
            PartKind::TopologyInformation => {
                if let Argument::TopologyInformation(vec) = part.arg {
                    for e in vec { conn_props.topology_attributes.push(e); }
                }
            },
            pk => {
                error!("ignoring unexpected partkind ({}) in auth_response2", pk.to_i8());
            }
        }
    }
    warn!("the server proof is not evaluated: {:?}", server_proof);
    Ok(())
}

fn calculate_client_proof(server_challenge: Vec<u8>, client_challenge: Vec<u8>, password: &str)
-> PrtResult<Vec<u8>> {
    let client_proof_size = 32usize;
    trace!("Entering calculate_client_proof()");
    let (salts,srv_key) = get_salt_and_key(server_challenge).unwrap();
    let buf = Vec::<u8>::with_capacity(2 + (client_proof_size+1)*salts.len());
    let mut w = io::Cursor::new(buf);
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
fn get_salt_and_key(server_challenge: Vec<u8>) -> PrtResult<(Vec<Vec<u8>>,Vec<u8>)> {
    trace!("Entering get_salt_and_key()");
    let mut rdr = io::Cursor::new(server_challenge);
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
            -> PrtResult<Vec<u8>> {
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
