use crate::authentication::Authenticator;
use crate::conn::AmConnCore;
use crate::protocol::parts::{AuthFields, ClientContext};
use crate::protocol::{Part, ReplyType, Request, RequestType};
use crate::{HdbError, HdbResult};

pub(crate) fn first_auth_request(
    am_conn_core: &mut AmConnCore,
    authenticators: &[Box<dyn Authenticator>],
) -> HdbResult<(String, Vec<u8>)> {
    let mut request1 = Request::new(RequestType::Authenticate, 0);
    request1.push(Part::ClientContext(ClientContext::new()));

    let mut auth_fields = AuthFields::with_capacity(4);
    auth_fields.push_string(am_conn_core.lock()?.connect_params().dbuser());
    for authenticator in authenticators {
        debug!("proposing {}", authenticator.name());
        auth_fields.push(authenticator.name_as_bytes());
        auth_fields.push(authenticator.client_challenge().to_vec());
    }
    request1.push(Part::Auth(auth_fields));

    let reply = am_conn_core.send_sync(request1)?;
    reply.assert_expected_reply_type(ReplyType::Nil)?;

    let mut result = None;
    for part in reply.parts.into_iter() {
        if let Part::Auth(mut auth_fields) = part {
            match (auth_fields.pop(), auth_fields.pop(), auth_fields.pop()) {
                (Some(server_challenge_data), Some(raw_name), None) => {
                    let authenticator_name = String::from_utf8_lossy(&raw_name).to_string();
                    result = Some((authenticator_name, server_challenge_data));
                }
                (_, _, _) => return Err(HdbError::Impl("expected 2 auth_fields")),
            }
        } else {
            warn!("first_auth_request: ignoring unexpected part = {:?}", part);
        }
    }
    result.ok_or(HdbError::Impl("No Authentication part found"))
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
    request2.push(Part::Auth(auth_fields));

    request2.push(Part::ConnectOptions(
        am_conn_core.lock()?.connect_options().clone(),
    ));

    let reply = am_conn_core.send_sync(request2)?;
    reply.assert_expected_reply_type(ReplyType::Nil)?;

    let mut conn_core = am_conn_core.lock()?;
    conn_core.set_session_id(reply.session_id());

    for part in reply.parts.into_iter() {
        match part {
            Part::TopologyInformation(topology) => conn_core.set_topology(topology),
            Part::ConnectOptions(received_co) => conn_core
                .connect_options_mut()
                .digest_server_connect_options(received_co)?,
            Part::Auth(mut af) => match (af.pop(), af.pop(), af.pop()) {
                (Some(server_proof), Some(method), None) => {
                    chosen_authenticator.evaluate_second_response(&method, &server_proof)?
                }
                (_, _, _) => return Err(HdbError::Impl("Expected 2 authfields")),
            },
            _ => warn!("second_auth_request: ignoring unexpected part = {:?}", part),
        }
    }
    Ok(())
}
