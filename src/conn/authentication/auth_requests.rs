use super::Authenticator;
use crate::conn::ConnectionCore;
use crate::protocol::parts::{AuthFields, ClientContext, ConnOptId, OptionValue};
use crate::protocol::{Part, ReplyType, Request, RequestType};
use crate::{HdbError, HdbResult};

// TODO for HC-redirect, we might need a returntype like HdbResult<Enum[(String, Vec)|DbConnectInfo]>
pub(crate) fn first_auth_request(
    conn_core: &mut ConnectionCore,
    authenticators: &[Box<dyn Authenticator>],
) -> HdbResult<(String, Vec<u8>)> {
    let mut request1 = Request::new(RequestType::Authenticate, 0);
    request1.push(Part::ClientContext(ClientContext::new()));

    let mut auth_fields_out = AuthFields::with_capacity(3);
    auth_fields_out.push_string(conn_core.connect_params().dbuser());
    for authenticator in authenticators {
        debug!("proposing {}", authenticator.name());
        auth_fields_out.push(authenticator.name_as_bytes());
        auth_fields_out.push(authenticator.client_challenge().to_vec());
    }
    request1.push(Part::Auth(auth_fields_out));

    // TODO for HC-redirect, we might get here an ErrorPart + DBConnectInfoPart
    // but roundtrip_sync() calls already handle_db_error(), which must not be done in this case
    // we should evaluate explicitly
    // if first is part::Error
    //     if second is DbConnectInfo
    //        -> HC-redirect!
    //     else
    //        error
    // else if first is part::Auth
    //     evaluate like now
    // endif
    // digest excess parts, with warning

    let reply = conn_core.roundtrip_sync(&request1, None, None, None, &mut None)?;
    reply.assert_expected_reply_type(ReplyType::Nil)?;

    let mut result = None;
    // match reply.parts.remove_first_of_kind(PartKind::Error) {
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
    conn_core: &mut ConnectionCore,
    mut chosen_authenticator: Box<dyn Authenticator>,
    server_challenge_data: &[u8],
    reconnect: bool,
) -> HdbResult<()> {
    let mut request2 = Request::new(RequestType::Connect, 0);

    debug!("authenticating with {}", chosen_authenticator.name());

    let mut auth_fields = AuthFields::with_capacity(3);
    {
        auth_fields.push_string(conn_core.connect_params().dbuser());
        auth_fields.push(chosen_authenticator.name_as_bytes());
        auth_fields.push(
            chosen_authenticator
                .client_proof(server_challenge_data, conn_core.connect_params().password())?,
        );
    }
    request2.push(Part::Auth(auth_fields));

    let mut conn_opts = conn_core.connect_options().clone();
    if reconnect {
        conn_opts.insert(
            ConnOptId::OriginalAnchorConnectionID,
            OptionValue::INT(conn_core.connect_options().get_connection_id()?),
        );
    }
    request2.push(Part::ConnectOptions(conn_opts));

    let reply = conn_core.roundtrip_sync(&request2, None, None, None, &mut None)?;
    reply.assert_expected_reply_type(ReplyType::Nil)?;

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
