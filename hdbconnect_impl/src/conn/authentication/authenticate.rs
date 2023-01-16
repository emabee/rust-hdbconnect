#[cfg(feature = "async")]
use super::{async_first_auth_request, async_second_auth_request};
#[cfg(feature = "sync")]
use super::{sync_first_auth_request, sync_second_auth_request};
use super::{Authenticator, FirstAuthResponse, ScramPbkdf2Sha256, ScramSha256};
use crate::conn::ConnectionCore;
use crate::hdb_error::{HdbError, HdbResult};
use crate::protocol::parts::DbConnectInfo;

#[must_use]
pub(crate) enum AuthenticationResult {
    Ok,
    Redirect(DbConnectInfo),
}

// Do the authentication.
//
// Manages a list of supported authenticators.
// So far we only support two; if more are implemented, the password might
// become optional; if then the password is not given, the pw-related
// authenticators mut not be added to the list.
#[cfg(feature = "sync")]
pub(crate) fn sync_authenticate(
    conn_core: &mut ConnectionCore,
    reconnect: bool,
) -> HdbResult<AuthenticationResult> {
    trace!("authenticate()");
    // Propose some authenticators...
    let authenticators: [Box<dyn Authenticator + Send + Sync>; 2] = [
        // Cookie,  Gss, Saml, SapLogon, Jwt, Ldap,
        ScramSha256::boxed_authenticator(),
        ScramPbkdf2Sha256::boxed_authenticator(),
    ];

    // ...with the first request.
    match sync_first_auth_request(conn_core, &authenticators)? {
        FirstAuthResponse::AuthenticatorAndChallenge(selected, server_challenge) => {
            // Find the selected authenticator ...
            let mut authenticator: Box<dyn Authenticator + Send + Sync> = authenticators
                .into_iter()
                .find(|authenticator| authenticator.name() == selected)
                .ok_or_else(|| {
                    HdbError::Impl("None of the available authenticators was accepted")
                })?;
            // ...and use it for the second request
            sync_second_auth_request(conn_core, &mut *authenticator, &server_challenge, reconnect)?;
            conn_core.set_authenticated();
            trace!("session_id: {}", conn_core.session_id());
            Ok(AuthenticationResult::Ok)
        }
        FirstAuthResponse::RedirectInfo(db_connect_info) => {
            Ok(AuthenticationResult::Redirect(db_connect_info))
        }
    }
}

#[cfg(feature = "async")]
pub(crate) async fn async_authenticate(
    conn_core: &mut ConnectionCore,
    reconnect: bool,
) -> HdbResult<AuthenticationResult> {
    trace!("authenticate()");
    // Propose some authenticators...
    let authenticators: [Box<dyn Authenticator + Send + Sync>; 2] = [
        // Cookie,  Gss, Saml, SapLogon, Jwt, Ldap,
        ScramSha256::boxed_authenticator(),
        ScramPbkdf2Sha256::boxed_authenticator(),
    ];

    // ...with the first request.
    match async_first_auth_request(conn_core, &authenticators).await? {
        FirstAuthResponse::AuthenticatorAndChallenge(selected, server_challenge) => {
            // Find the selected authenticator ...
            let mut authenticator: Box<dyn Authenticator + Send + Sync> = authenticators
                .into_iter()
                .find(|authenticator| authenticator.name() == selected)
                .ok_or_else(|| {
                    HdbError::Impl("None of the available authenticators was accepted")
                })?;
            // ...and use it for the second request
            async_second_auth_request(conn_core, &mut *authenticator, &server_challenge, reconnect)
                .await?;
            conn_core.set_authenticated();
            trace!("session_id: {}", conn_core.session_id());
            Ok(AuthenticationResult::Ok)
        }
        FirstAuthResponse::RedirectInfo(db_connect_info) => {
            Ok(AuthenticationResult::Redirect(db_connect_info))
        }
    }
}
