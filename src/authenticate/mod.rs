mod authenticator;
mod scram_pbkdf2_sha256;
mod scram_sha256;

use self::authenticator::Authenticator;
use self::scram_pbkdf2_sha256::ScramPbkdf2Sha256;
use self::scram_sha256::ScramSha256;
use hdb_error::{HdbError, HdbResult};
use protocol::lowlevel::argument::Argument;
use protocol::lowlevel::conn_core::AmConnCore;
use protocol::lowlevel::part::Part;
use protocol::lowlevel::partkind::PartKind;
use protocol::lowlevel::parts::authfield::AuthField;
use protocol::lowlevel::parts::connect_options::ConnectOptions;
use protocol::lowlevel::reply::{Reply, SkipLastSpace};
use protocol::lowlevel::reply_type::ReplyType;
use protocol::lowlevel::request::Request;
use protocol::lowlevel::request_type::RequestType;
use secstr::SecStr;
use username;

pub fn authenticate(
    am_conn_core: &mut AmConnCore,
    db_user: &str,
    password: &SecStr,
    clientlocale: &Option<String>,
) -> HdbResult<()> {
    trace!("authenticate()");

    let authenticators: Vec<Box<dyn Authenticator>> = vec![
        // Cookie,  Gss, Saml, SapLogon, Jwt, Ldap,
        ScramPbkdf2Sha256::new(),
        ScramSha256::new(),
    ];

    let mut request1 = Request::new(RequestType::Authenticate, 0);
    // FIXME add clientcontext

    let mut auth_fields = Vec::<AuthField>::with_capacity(4);
    auth_fields.push(AuthField::new(db_user.as_bytes().to_vec()));

    for authenticator in &authenticators {
        debug!("trying {}", authenticator.name());
        auth_fields.push(AuthField::new(authenticator.name_as_bytes()));
        auth_fields.push(AuthField::new(authenticator.client_challenge().to_owned()));
    }

    request1.push(Part::new(
        PartKind::Authentication,
        Argument::Auth(auth_fields),
    ));

    let reply1 = request1.send_and_get_reply_simplified(
        am_conn_core,
        Some(ReplyType::Nil),
        SkipLastSpace::Hard,
    )?;

    let (authenticator_name, server_challenge_data): (String, Vec<u8>) = evaluate_reply1(reply1)?;

    let chosen_authenticator: Box<dyn Authenticator> = authenticators
        .into_iter()
        .find(|a| a.name() == authenticator_name)
        .unwrap();
    debug!(
        "authenticate(): authenticating with {}",
        chosen_authenticator.name()
    );

    let mut request2 = Request::new(RequestType::Connect, 0);

    let mut auth_fields = Vec::<AuthField>::with_capacity(3);
    auth_fields.push(AuthField::new(db_user.as_bytes().to_vec()));
    auth_fields.push(AuthField::new(chosen_authenticator.name_as_bytes()));
    auth_fields.push(AuthField::new(
        chosen_authenticator.client_proof(server_challenge_data, password)?
    ));
    request2.push(Part::new(
        PartKind::Authentication,
        Argument::Auth(auth_fields),
    ));

    request2.push(Part::new(
        PartKind::ConnectOptions,
        Argument::ConnectOptions(ConnectOptions::for_server(clientlocale, get_os_user())),
    ));

    let reply2 = request2.send_and_get_reply_simplified(
        am_conn_core,
        Some(ReplyType::Nil),
        SkipLastSpace::Hard,
    )?;
    let _server_proof = evaluate_reply2(reply2, am_conn_core)?;
    // FIXME the server proof is not evaluated

    Ok(())
}

fn evaluate_reply1(mut reply: Reply) -> HdbResult<(String, Vec<u8>)> {
    trace!("Entering evaluate_reply1()");
    match reply.parts.pop_arg_if_kind(PartKind::Authentication) {
        Some(Argument::Auth(mut auth_fields)) => {
            if auth_fields.len() < 2 {
                Err(HdbError::Impl(
                    "evaluate_reply1(): expected Authentication part".to_owned(),
                ))
            } else {
                let server_challenge_data: Vec<u8> = auth_fields.remove(1).into_data();
                let authenticator_name: String =
                    String::from_utf8_lossy(&auth_fields.remove(0).into_data()).to_string();
                if !auth_fields.is_empty() {
                    warn!("evaluate_reply1(): auth_fields has extra info")
                }
                debug!("evaluate_reply1(): returning {:?}", &server_challenge_data);
                Ok((authenticator_name, server_challenge_data))
            }
        }
        _ => Err(HdbError::Impl(
            "evaluate_reply1(): expected Authentication part".to_owned(),
        )),
    }
}

fn get_os_user() -> String {
    let os_user = username::get_user_name().unwrap_or_default();
    debug!("Username: {}", os_user);
    os_user
}

fn evaluate_reply2(mut reply: Reply, am_conn_core: &AmConnCore) -> HdbResult<Vec<u8>> {
    trace!("Entering evaluate_reply2()");
    let mut guard = am_conn_core.lock()?;
    let conn_core = &mut *guard;
    conn_core.set_session_id(reply.session_id());

    match reply.parts.pop_arg_if_kind(PartKind::TopologyInformation) {
        Some(Argument::TopologyInformation(topology)) => conn_core.set_topology(topology),
        _ => {
            return Err(HdbError::Impl(
                "evaluate_reply2(): expected TopologyInformation part".to_owned(),
            ))
        }
    }

    match reply.parts.pop_arg_if_kind(PartKind::ConnectOptions) {
        Some(Argument::ConnectOptions(conn_opts)) => {
            conn_core.transfer_server_connect_options(conn_opts)?
        }
        _ => {
            return Err(HdbError::Impl(
                "evaluate_reply2(): expected ConnectOptions part".to_owned(),
            ))
        }
    }

    let mut server_proof = Vec::<u8>::new();
    match reply.parts.pop_arg_if_kind(PartKind::Authentication) {
        Some(Argument::Auth(mut vec)) => vec[0].swap_data(&mut server_proof),
        _ => {
            return Err(HdbError::Impl(
                "evaluate_reply2(): expected Authentication part".to_owned(),
            ))
        }
    }
    if reply.parts.is_empty() {
        conn_core.set_authenticated(true);
        Ok(server_proof)
    } else {
        Err(HdbError::Impl(
            "evaluate_reply2(): extra parts detected".to_owned(),
        ))
    }
}
