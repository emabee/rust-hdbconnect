use crate::authentication::auth_requests::{first_auth_request, second_auth_request};
use crate::authentication::authenticator::Authenticator;
use crate::authentication::scram_pbkdf2_sha256::ScramPbkdf2Sha256;
use crate::authentication::scram_sha256::ScramSha256;
use crate::conn_core::AmConnCore;
use crate::hdb_error::HdbResult;
use secstr::SecStr;

// Do the authentication.
//
// Manages a list of supported authenticators.
// So far we only support two; if more are implemented, the password might
// become optional; if then the password is not given, the pw-related
// authenticators mut not be added to the list.
pub(crate) fn authenticate(
    am_conn_core: &mut AmConnCore,
    db_user: &str,
    password: &SecStr,
) -> HdbResult<()> {
    trace!("authenticate()");

    // Propose some authenticators...
    let authenticators: Vec<Box<dyn Authenticator>> = vec![
        // Cookie,  Gss, Saml, SapLogon, Jwt, Ldap,
        ScramSha256::boxed_authenticator(),
        ScramPbkdf2Sha256::boxed_authenticator(),
    ];

    // ...with the first request.
    let (selected, server_challenge_data) =
        first_auth_request(am_conn_core, db_user, &authenticators)?;

    // Find the selected authenticator ...
    let chosen_authenticator: Box<dyn Authenticator> = authenticators
        .into_iter()
        .find(|a11r| a11r.name() == selected)
        .unwrap();

    // ...and use it for the second request
    second_auth_request(
        am_conn_core,
        db_user,
        password,
        chosen_authenticator,
        &server_challenge_data,
    )?;

    let mut conn_core = am_conn_core.lock()?;
    conn_core.set_authenticated();

    Ok(())
}
