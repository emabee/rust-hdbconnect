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

pub(crate) fn first_auth_request(
    am_conn_core: &mut AmConnCore,
    authenticators: &[Box<dyn Authenticator>],
) -> HdbResult<(String, Vec<u8>)> {
    let mut request1 = Request::new(RequestType::Authenticate, 0);
    request1.push(Part::new(
        PartKind::ClientContext,
        Argument::ClientContext(ClientContext::new()),
    ));

    let mut auth_fields = AuthFields::with_capacity(4);
    auth_fields.push_string(am_conn_core.lock()?.connect_params().dbuser());
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
            match (auth_fields.pop(), auth_fields.pop(), auth_fields.pop()) {
                (Some(server_challenge_data), Some(raw_name), None) => {
                    let authenticator_name = String::from_utf8_lossy(&raw_name).to_string();
                    Ok((authenticator_name, server_challenge_data))
                }
                (_, _, _) => Err(HdbError::imp("expected 2 auth_fields")),
            }
        }
        _ => Err(HdbError::imp("expected Authentication part")),
    }
}

pub(crate) fn second_auth_request(
    am_conn_core: &mut AmConnCore,
    mut chosen_authenticator: Box<dyn Authenticator>,
    server_challenge_data: &[u8],
) -> HdbResult<()> {
    let mut request2 = Request::new(RequestType::Connect, 0);

    debug!("authenticating with {}", chosen_authenticator.name());

    let mut auth_fields = AuthFields::with_capacity(3);
    {
        let acc = am_conn_core.lock()?;
        auth_fields.push_string(acc.connect_params().dbuser());
        auth_fields.push(chosen_authenticator.name_as_bytes());
        auth_fields.push(
            chosen_authenticator
                .client_proof(server_challenge_data, acc.connect_params().password())?,
        );
    }
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

    if let Some(Argument::TopologyInformation(topology)) = reply
        .parts
        .pop_if_kind(PartKind::TopologyInformation)
        .map(Part::into_arg)
    {
        conn_core.set_topology(topology)
    } else {
        return Err(HdbError::imp("Expected TopologyInformation part"));
    }

    if let Some(Argument::ConnectOptions(received_co)) = reply
        .parts
        .pop_if_kind(PartKind::ConnectOptions)
        .map(Part::into_arg)
    {
        conn_core
            .connect_options_mut()
            .digest_server_connect_options(received_co)?
    } else {
        return Err(HdbError::imp("Expected ConnectOptions part"));
    }

    match reply
        .parts
        .pop_if_kind(PartKind::Authentication)
        .map(Part::into_arg)
    {
        Some(Argument::Auth(mut af)) => match (af.pop(), af.pop(), af.pop()) {
            (Some(server_proof), Some(method), None) => {
                chosen_authenticator.evaluate_second_response(&method, &server_proof)
            }
            (_, _, _) => Err(HdbError::imp("Expected 2 authfields")),
        },
        _ => Err(HdbError::imp("Expected Authentication part")),
    }
}
