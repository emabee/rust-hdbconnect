use super::authenticator::Authenticator;

use hdb_error::{HdbError, HdbResult};
use protocol::argument::Argument;
use protocol::conn_core::AmConnCore;
use protocol::part::Part;
use protocol::partkind::PartKind;
use protocol::parts::authfields::AuthFields;
use protocol::parts::client_context::ClientContext;
use protocol::parts::connect_options::ConnectOptions;
use protocol::reply::SkipLastSpace;
use protocol::reply_type::ReplyType;
use protocol::request::Request;
use protocol::request_type::RequestType;
use secstr::SecStr;
use username;

pub fn first_auth_request(
    am_conn_core: &mut AmConnCore,
    db_user: &str,
    authenticators: &[Box<dyn Authenticator>],
) -> HdbResult<(String, Vec<u8>)> {
    let mut request1 = Request::new(RequestType::Authenticate, 0);
    request1.push(Part::new(
        PartKind::ClientContext,
        Argument::ClientContext(ClientContext::new()),
    ));

    let mut auth_fields = AuthFields::with_capacity(4);
    auth_fields.push(db_user.as_bytes().to_vec());
    for authenticator in authenticators {
        debug!("proposing {}", authenticator.name());
        auth_fields.push(authenticator.name_as_bytes());
        auth_fields.push(authenticator.client_challenge().to_vec());
    }
    request1.push(Part::new(
        PartKind::Authentication,
        Argument::Auth(auth_fields),
    ));

    let mut reply = request1.send_and_get_reply_simplified(
        am_conn_core,
        Some(ReplyType::Nil),
        SkipLastSpace::Hard,
    )?;

    match reply.parts.pop_arg_if_kind(PartKind::Authentication) {
        Some(Argument::Auth(mut auth_fields)) => {
            if auth_fields.len() != 2 {
                Err(HdbError::Impl(format!(
                    "first_auth_request(): got {} auth_fields, expected 2",
                    auth_fields.len()
                )))
            } else {
                let server_challenge_data: Vec<u8> = auth_fields.pop().unwrap();
                let authenticator_name: String =
                    String::from_utf8_lossy(&auth_fields.pop().unwrap()).to_string();
                Ok((authenticator_name, server_challenge_data))
            }
        }
        _ => Err(HdbError::Impl(
            "first_auth_request(): expected Authentication part".to_owned(),
        )),
    }
}

pub fn second_auth_request(
    am_conn_core: &mut AmConnCore,
    db_user: &str,
    password: &SecStr,
    clientlocale: &Option<String>,
    mut chosen_authenticator: Box<dyn Authenticator>,
    server_challenge_data: &[u8],
) -> HdbResult<()> {
    let mut request2 = Request::new(RequestType::Connect, 0);

    debug!("authenticating with {}", chosen_authenticator.name());

    let mut auth_fields = AuthFields::with_capacity(3);
    auth_fields.push(db_user.as_bytes().to_vec());
    auth_fields.push(chosen_authenticator.name_as_bytes());
    auth_fields.push(chosen_authenticator.client_proof(server_challenge_data, password)?);
    request2.push(Part::new(
        PartKind::Authentication,
        Argument::Auth(auth_fields),
    ));

    request2.push(Part::new(
        PartKind::ConnectOptions,
        Argument::ConnectOptions(ConnectOptions::for_server(clientlocale, get_os_user())),
    ));

    let mut reply = request2.send_and_get_reply_simplified(
        am_conn_core,
        Some(ReplyType::Nil),
        SkipLastSpace::Hard,
    )?;

    let mut conn_core = am_conn_core.lock()?;
    conn_core.set_session_id(reply.session_id());

    match reply.parts.pop_arg_if_kind(PartKind::TopologyInformation) {
        Some(Argument::TopologyInformation(topology)) => conn_core.set_topology(topology),
        _ => {
            return Err(HdbError::Impl(
                "second_auth_request(): expected TopologyInformation part".to_owned(),
            ))
        }
    }

    match reply.parts.pop_arg_if_kind(PartKind::ConnectOptions) {
        Some(Argument::ConnectOptions(conn_opts)) => {
            conn_core.transfer_server_connect_options(conn_opts)?
        }
        _ => {
            return Err(HdbError::Impl(
                "second_auth_request(): expected ConnectOptions part".to_owned(),
            ))
        }
    }

    match reply.parts.pop_arg_if_kind(PartKind::Authentication) {
        Some(Argument::Auth(mut af)) => {
            if af.len() == 2 {
                let server_proof = af.pop().unwrap();
                let method = af.pop().unwrap();
                chosen_authenticator.evaluate_second_response(&method, &server_proof)
            } else {
                Err(HdbError::Impl(format!(
                    "second_auth_request(): got {} authfields, expected 2",
                    af.len()
                )))
            }
        }
        _ => Err(HdbError::Impl(
            "second_auth_request(): expected Authentication part".to_owned(),
        )),
    }
}

fn get_os_user() -> String {
    let os_user = username::get_user_name().unwrap_or_default();
    trace!("OS user: {}", os_user);
    os_user
}
