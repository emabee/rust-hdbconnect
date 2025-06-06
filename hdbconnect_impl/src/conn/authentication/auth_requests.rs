use crate::{
    HdbError, HdbResult,
    conn::{CommandOptions, ConnectionCore, authentication::Authenticator},
    impl_err,
    protocol::{
        MessageType, Part, Reply, ReplyType, Request,
        parts::{AuthFields, ClientContext, ConnOptId, ConnectOptionsPart, DbConnectInfo},
    },
};
use secstr::SecUtf8;

pub(crate) enum FirstAuthResponse {
    AuthenticatorAndChallenge(String, Vec<u8>),
    RedirectInfo(DbConnectInfo),
}

fn first_request(
    db_user: &str,
    authenticators: &[Box<dyn Authenticator + Send + Sync>],
) -> Request<'static> {
    let mut request1 = Request::new(MessageType::Authenticate, CommandOptions::EMPTY);
    request1.push(Part::ClientContext(ClientContext::new()));

    let mut auth_fields_out = AuthFields::with_capacity(3);
    auth_fields_out.push_string(db_user);
    for authenticator in authenticators {
        debug!("proposing {}", authenticator.name());
        auth_fields_out.push(authenticator.name_as_bytes());
        auth_fields_out.push(authenticator.client_challenge().to_vec());
    }
    request1.push(Part::Auth(auth_fields_out));
    request1
}

fn evaluate_first_response(reply: Reply) -> HdbResult<FirstAuthResponse> {
    reply.assert_expected_reply_type(ReplyType::Nil)?;
    let mut parts_iter = reply.parts.into_iter();
    let result = match (parts_iter.next(), parts_iter.next()) {
        (Some(Part::Auth(mut auth_fields)), p2) => {
            if let Some(part) = p2 {
                warn!("first_auth_request: ignoring unexpected part = {part:?}");
            }
            match (auth_fields.pop(), auth_fields.pop(), auth_fields.pop()) {
                (Some(server_challenge_data), Some(raw_name), None) => {
                    let authenticator_name = String::from_utf8_lossy(&raw_name).to_string();
                    Ok(FirstAuthResponse::AuthenticatorAndChallenge(
                        authenticator_name,
                        server_challenge_data,
                    ))
                }
                (_, _, _) => return Err(impl_err!("expected 2 auth_fields")),
            }
        }
        (Some(Part::Error(_vec_server_error)), Some(Part::DbConnectInfo(db_connect_info))) => {
            // for HANA Cloud redirect, we get here an Error and a DBConnectInfo
            Ok(FirstAuthResponse::RedirectInfo(db_connect_info))
        }
        (Some(Part::Error(mut server_errors)), None) => {
            Err(HdbError::from(server_errors.remove(0)))
        }
        (p1, p2) => Err(impl_err!(
            "Unexpected db response with parts: {p1:?}, {p2:?}"
        )),
    };

    for part in parts_iter {
        warn!("first_auth_request(): ignoring unexpected part = {part:?}",);
    }

    result
}

#[cfg(feature = "sync")]
pub(crate) fn first_auth_request_sync(
    conn_core: &mut ConnectionCore,
    authenticators: &[Box<dyn Authenticator + Send + Sync>],
) -> HdbResult<FirstAuthResponse> {
    let request1 = first_request(conn_core.connect_params().dbuser(), authenticators);

    // For RequestType::Authenticate, the default error handling in roundtrip_sync is switched off:
    let reply = conn_core.roundtrip_sync(&request1, None, None, None, &mut None)?;
    evaluate_first_response(reply)
}

#[cfg(feature = "async")]
pub(crate) async fn first_auth_request_async(
    conn_core: &mut ConnectionCore,
    authenticators: &[Box<dyn Authenticator + Send + Sync>],
) -> HdbResult<FirstAuthResponse> {
    let request1 = first_request(conn_core.connect_params().dbuser(), authenticators);

    // For RequestType::Authenticate, the default error handling in roundtrip_sync is switched off:
    let reply = conn_core
        .roundtrip_async(&request1, None, None, None, &mut None)
        .await?;
    evaluate_first_response(reply)
}

fn second_request(
    db_user: &str,
    db_password: &SecUtf8,
    mut connect_options: ConnectOptionsPart,
    chosen_authenticator: &mut dyn Authenticator,
    server_challenge_data: &[u8],
    reconnect: bool,
) -> HdbResult<Request<'static>> {
    let mut request2 = Request::new(MessageType::Connect, CommandOptions::EMPTY);

    debug!("authenticating with {}", chosen_authenticator.name());

    let mut auth_fields = AuthFields::with_capacity(3);
    {
        auth_fields.push_string(db_user);
        auth_fields.push(chosen_authenticator.name_as_bytes());
        auth_fields.push(chosen_authenticator.client_proof(server_challenge_data, db_password)?);
    }
    request2.push(Part::Auth(auth_fields));

    if reconnect {
        connect_options.insert(
            ConnOptId::OriginalAnchorConnectionID,
            connect_options
                .get(&ConnOptId::ConnectionID)
                .unwrap()
                .clone(),
        );
    }
    request2.push(Part::ConnectOptions(connect_options));
    Ok(request2)
}

fn evaluate_second_response(
    reply: Reply,
    chosen_authenticator: &(dyn Authenticator + Send + Sync),
    conn_core: &mut ConnectionCore,
) -> HdbResult<()> {
    reply.assert_expected_reply_type(ReplyType::Nil)?;

    conn_core.set_session_id(reply.session_id());

    for part in reply.parts {
        match part {
            Part::TopologyInformation(topology) => conn_core.set_topology(topology),
            Part::ConnectOptions(received_co) => {
                conn_core
                    .connect_options_mut()
                    .digest_server_connect_options(received_co)?;
            }
            Part::Auth(mut af) => match (af.pop(), af.pop(), af.pop()) {
                (Some(server_proof), Some(method), None) => {
                    chosen_authenticator.evaluate_second_response(&method, &server_proof)?;
                }
                (_, _, _) => return Err(impl_err!("Expected 2 authfields")),
            },
            _ => warn!("second_auth_request: ignoring unexpected part = {part:?}"),
        }
    }
    Ok(())
}

#[cfg(feature = "sync")]
pub(crate) fn second_auth_request_sync(
    conn_core: &mut ConnectionCore,
    chosen_authenticator: &mut (dyn Authenticator + Send + Sync),
    server_challenge_data: &[u8],
    reconnect: bool,
) -> HdbResult<()> {
    let second_request = second_request(
        conn_core.connect_params().dbuser(),
        conn_core.connect_params().password(),
        conn_core.connect_options().for_server(),
        &mut *chosen_authenticator,
        server_challenge_data,
        reconnect,
    )?;

    let reply = conn_core
        .roundtrip_sync(&second_request, None, None, None, &mut None)
        .map_err(|e| HdbError::Authentication {
            source: Box::new(e),
        })?;
    evaluate_second_response(reply, chosen_authenticator, conn_core)
}

#[cfg(feature = "async")]
pub(crate) async fn second_auth_request_async(
    conn_core: &mut ConnectionCore,
    chosen_authenticator: &mut (dyn Authenticator + Send + Sync),
    server_challenge_data: &[u8],
    reconnect: bool,
) -> HdbResult<()> {
    let second_request = second_request(
        conn_core.connect_params().dbuser(),
        conn_core.connect_params().password(),
        conn_core.connect_options().for_server(),
        &mut *chosen_authenticator,
        server_challenge_data,
        reconnect,
    )?;

    let reply = conn_core
        .roundtrip_async(&second_request, None, None, None, &mut None)
        .await
        .map_err(|e| HdbError::Authentication {
            source: Box::new(e),
        })?;
    evaluate_second_response(reply, chosen_authenticator, conn_core)
}
