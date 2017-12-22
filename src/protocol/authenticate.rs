use protocol::protocol_error::{prot_err, PrtResult};
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::ConnCoreRef;
use protocol::lowlevel::message::{Reply, Request};
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request_type::RequestType;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::authfield::AuthField;
use protocol::lowlevel::parts::connect_option::{ConnectOptionId, ConnectOptions};
use protocol::lowlevel::parts::prt_option_value::PrtOptionValue;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use rand::{thread_rng, Rng};

use std::env;
use std::io::{self, Read};
use std::iter::repeat;

use username;

/// authenticate with user and password, using the `scram_sha256` method
pub fn user_pw(conn_ref: &ConnCoreRef, username: &str, password: &str) -> PrtResult<()> {
    trace!("Entering authenticate()");

    let client_challenge = create_client_challenge();
    let reply1 = auth1_request(conn_ref, &client_challenge, username)?;
    let server_challenge: Vec<u8> = get_server_challenge(reply1)?;

    let client_proof = calculate_client_proof(server_challenge, &client_challenge, password)?;

    let reply2 = auth2_request(conn_ref, &client_proof, username)?;
    evaluate_reply2(reply2, conn_ref)
}

fn auth1_request(conn_ref: &ConnCoreRef, chllng_sha256: &[u8], username: &str) -> PrtResult<Reply> {
    trace!("Entering auth1_request()");
    let mut auth_fields = Vec::<AuthField>::with_capacity(3);
    auth_fields.push(AuthField::new(username.as_bytes().to_vec()));
    auth_fields.push(AuthField::new(b"SCRAMSHA256".to_vec()));
    auth_fields.push(AuthField::new(chllng_sha256.to_owned()));

    let part2 = Part::new(PartKind::Authentication, Argument::Auth(auth_fields));

    let mut request = Request::new(conn_ref, RequestType::Authenticate, 0)?;
    request.push(part2);

    request.send_and_receive(conn_ref, Some(ReplyType::Nil))
}

fn auth2_request(conn_ref: &ConnCoreRef, client_proof: &[u8], username: &str) -> PrtResult<Reply> {
    trace!("Entering auth2_request()");
    let mut request = Request::new(conn_ref, RequestType::Connect, 0)?;

    let mut auth_fields = Vec::<AuthField>::with_capacity(3);
    auth_fields.push(AuthField::new(username.as_bytes().to_vec()));
    auth_fields.push(AuthField::new(b"SCRAMSHA256".to_vec()));
    auth_fields.push(AuthField::new(client_proof.to_owned()));
    request.push(Part::new(
        PartKind::Authentication,
        Argument::Auth(auth_fields),
    ));

    let mut conn_opts = ConnectOptions::default();
    conn_opts.push(
        ConnectOptionId::CompleteArrayExecution,
        PrtOptionValue::BOOLEAN(true),
    );
    conn_opts.push(ConnectOptionId::DataFormatVersion2, PrtOptionValue::INT(4));
    conn_opts.push(ConnectOptionId::DataFormatVersion, PrtOptionValue::INT(1));
    conn_opts.push(
        ConnectOptionId::ClientLocale,
        PrtOptionValue::STRING(get_locale()),
    );
    conn_opts.push(
        ConnectOptionId::EnableArrayType,
        PrtOptionValue::BOOLEAN(true),
    );
    conn_opts.push(
        ConnectOptionId::DistributionEnabled,
        PrtOptionValue::BOOLEAN(true),
    );
    conn_opts.push(
        ConnectOptionId::ClientDistributionMode,
        PrtOptionValue::INT(3),
    );
    // conn_opts.push(ConnectOptionId::SelectForUpdateOK, PrtOptionValue::BOOLEAN(true));
    conn_opts.push(
        ConnectOptionId::DistributionProtocolVersion,
        PrtOptionValue::INT(1),
    );
    conn_opts.push(
        ConnectOptionId::RowSlotImageParameter,
        PrtOptionValue::BOOLEAN(true),
    );
    conn_opts.push(
        ConnectOptionId::OSUser,
        PrtOptionValue::STRING(get_username()),
    );
    request.push(Part::new(
        PartKind::ConnectOptions,
        Argument::ConnectOptions(conn_opts),
    ));

    request.send_and_receive(conn_ref, Some(ReplyType::Nil))
}

fn get_locale() -> String {
    let locale = match env::var("LANG") {
        Ok(l) => l,
        Err(_) => String::from("en_US"),
    };
    debug!("Using locale {}", locale);
    locale
}

fn get_username() -> String {
    let username = username::get_user_name().unwrap_or_default();
    debug!("Username: {}", username);
    username
}

fn create_client_challenge() -> Vec<u8> {
    let mut client_challenge = [0u8; 64];
    let mut rng = thread_rng();
    rng.fill_bytes(&mut client_challenge);
    client_challenge.to_vec()
}

fn get_server_challenge(mut reply: Reply) -> PrtResult<Vec<u8>> {
    trace!("Entering get_server_challenge()");
    match reply.parts.pop_arg_if_kind(PartKind::Authentication) {
        Some(Argument::Auth(mut vec)) => {
            let server_challenge = vec.remove(1).into_data();
            debug!("get_server_challenge(): returning {:?}", &server_challenge);
            Ok(server_challenge)
        }
        _ => Err(prot_err(
            "get_server_challenge(): expected Authentication part",
        )),
    }
}

fn evaluate_reply2(mut reply: Reply, conn_ref: &ConnCoreRef) -> PrtResult<()> {
    trace!("Entering evaluate_reply2()");
    let mut guard = conn_ref.lock()?;
    let conn_core = &mut *guard;
    conn_core.set_session_id(reply.session_id());

    match reply.parts.pop_arg_if_kind(PartKind::TopologyInformation) {
        Some(Argument::TopologyInformation(mut vec)) => {
            conn_core.swap_topology_attributes(&mut vec)
        }
        _ => {
            return Err(prot_err(
                "evaluate_reply2(): expected TopologyInformation part",
            ))
        }
    }

    match reply.parts.pop_arg_if_kind(PartKind::ConnectOptions) {
        Some(Argument::ConnectOptions(ConnectOptions(mut vec))) => {
            conn_core.swap_server_connect_options(&mut vec)
        }
        _ => return Err(prot_err("evaluate_reply2(): expected ConnectOptions part")),
    }

    let mut server_proof = Vec::<u8>::new();
    debug!("parts before: {:?}", reply.parts);
    match reply.parts.pop_arg_if_kind(PartKind::Authentication) {
        Some(Argument::Auth(mut vec)) => vec[0].swap_data(&mut server_proof),
        _ => return Err(prot_err("evaluate_reply2(): expected Authentication part")),
    }
    // FIXME the server proof is not evaluated

    conn_core.set_authenticated(true);
    debug!("parts after: {:?}", reply.parts);
    Ok(())
}

fn calculate_client_proof(
    server_challenge: Vec<u8>,
    client_challenge: &[u8],
    password: &str,
) -> PrtResult<Vec<u8>> {
    let client_proof_size = 32usize;
    trace!("Entering calculate_client_proof()");
    let (salts, srv_key) = get_salt_and_key(server_challenge).unwrap();
    let buf = Vec::<u8>::with_capacity(2 + (client_proof_size + 1) * salts.len());
    let mut w = io::Cursor::new(buf);
    w.write_u8(0u8)?;
    w.write_u8(salts.len() as u8)?;

    for salt in salts {
        w.write_u8(client_proof_size as u8)?;
        trace!("buf: \n{:?}", w.get_ref());
        let scrambled = scramble(&salt, &srv_key, client_challenge, password)?;
        for b in scrambled {
            w.write_u8(b)?;
        } // B variable   VALUE
        trace!("buf: \n{:?}", w.get_ref());
    }
    Ok(w.into_inner())
}

/// `Server_challenge` is structured itself into fieldcount and fields
/// the last field is taken as key, all the previous fields are salt (usually 1)
fn get_salt_and_key(server_challenge: Vec<u8>) -> PrtResult<(Vec<Vec<u8>>, Vec<u8>)> {
    trace!("Entering get_salt_and_key()");
    let mut rdr = io::Cursor::new(server_challenge);
    let fieldcount = rdr.read_i16::<LittleEndian>().unwrap(); // I2
    trace!("fieldcount = {}", fieldcount);

    type BVec = Vec<u8>;
    let mut salts = Vec::<BVec>::new();
    for _ in 0..(fieldcount - 1) {
        let len = rdr.read_u8()?; // B1
        let mut salt: Vec<u8> = repeat(0u8).take(len as usize).collect();
        rdr.read_exact(&mut salt)?; // variable
        trace!("salt: \n{:?}", salt);
        salts.push(salt);
    }

    let len = rdr.read_u8()?; // B1
    let mut key: Vec<u8> = repeat(0u8).take(len as usize).collect();
    rdr.read_exact(&mut key)?; // variable
    trace!("key: \n{:?}", key);
    Ok((salts, key))
}

fn scramble(
    salt: &[u8],
    server_key: &[u8],
    client_key: &[u8],
    password: &str,
) -> PrtResult<Vec<u8>> {
    let length = salt.len() + server_key.len() + client_key.len();
    let mut msg = Vec::<u8>::with_capacity(length);
    for b in salt {
        msg.push(*b)
    }
    trace!("salt: \n{:?}", msg);
    for b in server_key {
        msg.push(*b)
    }
    trace!("salt + server_key: \n{:?}", msg);
    for b in client_key {
        msg.push(*b)
    }
    trace!("salt + server_key + client_key: \n{:?}", msg);

    let tmp = &hmac(&password.as_bytes().to_vec(), salt);
    trace!("tmp = hmac(password, salt): \n{:?}", tmp);

    let key: &Vec<u8> = &sha256(tmp);
    trace!("sha256(tmp): \n{:?}", key);

    let sig: &Vec<u8> = &hmac(&sha256(key), &msg);
    trace!("sig = hmac(sha256(key),msg): \n{:?}", sig);
    let scramble = xor(sig, key);
    trace!("scramble = xor(sig,key): \n{:?}", scramble);
    Ok(scramble)
}

fn hmac(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut hmac = Hmac::new(Sha256::new(), key);
    hmac.input(message);
    hmac.result().code().to_vec()
}

fn sha256(input: &[u8]) -> Vec<u8> {
    let mut sha = Sha256::new();
    sha.input(input);

    let mut bytes: Vec<u8> = repeat(0u8).take(sha.output_bytes()).collect();
    sha.result(&mut bytes[..]);
    bytes
}

fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert_eq!(a.len(), b.len(), "xor needs two equally long parameters");

    let mut bytes: Vec<u8> = repeat(0u8).take(a.len()).collect();
    for i in 0..a.len() {
        bytes[i] = a[i] ^ b[i];
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::calculate_client_proof;

    // cargo test protocol::authentication::tests::test_client_proof -- --nocapture
    #[test]
    fn test_client_proof() {
        info!("test calculation of client proof");
        #[cfg_attr(rustfmt, rustfmt_skip)]
        let client_challenge: Vec<u8> = b"\xb5\xab\x3a\x90\xc5\xad\xb8\x04\x15\x27\
                                          \x37\x66\x54\xd7\x5c\x31\x94\xd8\x61\x50\
                                          \x3f\xe0\x8d\xff\x8b\xea\xd5\x1b\xc3\x5a\
                                          \x07\xcc\x63\xed\xbf\xa9\x5d\x03\x62\xf5\
                                          \x6f\x1a\x48\x2e\x4c\x3f\xb8\x32\xe4\x1c\
                                          \x89\x74\xf9\x02\xef\x87\x38\xcc\x74\xb6\
                                          \xef\x99\x2e\x8e"
                                        .to_vec();
        let server_challenge: Vec<u8> = b"\x02\x00\x10\x12\x41\xe5\x8f\x39\x23\x4e\
                                          \xeb\x77\x3e\x90\x90\x33\xe5\xcb\x6e\x30\
                                          \x1a\xce\xdc\xdd\x05\xc1\x90\xb0\xf0\xd0\
                                          \x7d\x81\x1a\xdb\x0d\x6f\xed\xa8\x87\x59\
                                          \xc2\x94\x06\x0d\xae\xab\x3f\x62\xea\x4b\
                                          \x16\x6a\xc9\x7e\xfc\x9a\x6b\xde\x4f\xe9\
                                          \xe5\xda\xcc\xb5\x0a\xcf\xce\x56"
            .to_vec();
        let password: &str = "manager";
        let correct_client_proof: Vec<u8> = b"\x00\x01\x20\x17\x26\x25\xab\x29\x71\xd8\
                                              \x58\x74\x32\x5d\x21\xbc\x3d\x68\x37\x71\
                                              \x80\x5c\x9a\xfe\x38\xd0\x95\x1d\xad\x46\
                                              \x53\x00\x9c\xc9\x21"
            .to_vec();

        trace!("----------------------------------------------------");
        trace!(
            "client_challenge ({} bytes): \n{:?}",
            &client_challenge.len(),
            &client_challenge
        );
        trace!(
            "server_challenge ({} bytes): \n{:?}",
            &server_challenge.len(),
            &server_challenge
        );

        let my_client_proof =
            calculate_client_proof(server_challenge, &client_challenge, password).unwrap();

        trace!(
            "my_client_proof ({} bytes): \n{:?}",
            &my_client_proof.len(),
            &my_client_proof
        );
        trace!(
            "correct_client_proof ({} bytes): \n{:?}",
            &correct_client_proof.len(),
            &correct_client_proof
        );
        trace!("----------------------------------------------------");
        assert_eq!(my_client_proof, correct_client_proof);
    }
}
