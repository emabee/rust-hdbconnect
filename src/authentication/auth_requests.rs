use super::authenticator::Authenticator;

use crate::conn_core::AmConnCore;
use crate::hdb_error::{HdbError, HdbResult};
use crate::protocol::argument::Argument;
use crate::protocol::part::Part;
use crate::protocol::partkind::PartKind;
use crate::protocol::parts::authfields::AuthFields;
use crate::protocol::parts::client_context::ClientContext;
use crate::protocol::reply_type::ReplyType;
use crate::protocol::request::Request;
use crate::protocol::request_type::RequestType;
use secstr::SecStr;

pub(crate) fn first_auth_request(
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

    let mut reply = am_conn_core.send(request1)?;
    reply.assert_expected_reply_type(ReplyType::Nil)?;

    match reply
        .parts
        .pop_if_kind(PartKind::Authentication)
        .map(Part::into_arg)
    {
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

pub(crate) fn second_auth_request(
    am_conn_core: &mut AmConnCore,
    db_user: &str,
    password: &SecStr,
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
        Argument::ConnectOptions(am_conn_core.lock()?.connect_options().clone()),
    ));

    let mut reply = am_conn_core.send(request2)?;
    reply.assert_expected_reply_type(ReplyType::Nil)?;

    let mut conn_core = am_conn_core.lock()?;
    conn_core.set_session_id(reply.session_id());

    match reply
        .parts
        .pop_if_kind(PartKind::TopologyInformation)
        .map(Part::into_arg)
    {
        Some(Argument::TopologyInformation(topology)) => conn_core.set_topology(topology),
        _ => {
            return Err(HdbError::Impl(
                "second_auth_request(): expected TopologyInformation part".to_owned(),
            ));
        }
    }

    match reply
        .parts
        .pop_if_kind(PartKind::ConnectOptions)
        .map(Part::into_arg)
    {
        Some(Argument::ConnectOptions(received_co)) => conn_core
            .connect_options_mut()
            .digest_server_connect_options(received_co)?,
        _ => {
            return Err(HdbError::Impl(
                "second_auth_request(): expected ConnectOptions part".to_owned(),
            ));
        }
    }

    match reply
        .parts
        .pop_if_kind(PartKind::Authentication)
        .map(Part::into_arg)
    {
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
