use super::{
    first_auth_request, second_auth_request, Authenticator, ScramPbkdf2Sha256, ScramSha256,
};
use crate::conn::ConnectionCore;
use crate::hdb_error::{HdbError, HdbResult};

// Do the authentication.
//
// Manages a list of supported authenticators.
// So far we only support two; if more are implemented, the password might
// become optional; if then the password is not given, the pw-related
// authenticators mut not be added to the list.
pub(crate) fn authenticate(conn_core: &mut ConnectionCore, reconnect: bool) -> HdbResult<()> {
    trace!("authenticate()");
    // Propose some authenticators...
    let authenticators: Vec<Box<dyn Authenticator>> = vec![
        // Cookie,  Gss, Saml, SapLogon, Jwt, Ldap,
        ScramSha256::boxed_authenticator(),
        ScramPbkdf2Sha256::boxed_authenticator(),
    ];

    // ...with the first request.
    let (selected, server_challenge) = first_auth_request(conn_core, &authenticators)?;

    // Find the selected authenticator ...
    let authenticator: Box<dyn Authenticator> = authenticators
        .into_iter()
        .find(|authenticator| authenticator.name() == selected)
        .ok_or(HdbError::Impl(
            "None of the available authenticators was accepted",
        ))?;

    // ...and use it for the second request
    second_auth_request(conn_core, authenticator, &server_challenge, reconnect)?;

    conn_core.set_authenticated();
    trace!("session_id: {}", conn_core.session_id());
    Ok(())
}
