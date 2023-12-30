mod auth_requests;
mod authenticate;
mod authenticator;
mod crypto_util;
mod scram_pbkdf2_sha256;
mod scram_sha256;

#[cfg(feature = "sync")]
pub(super) use self::{
    auth_requests::{first_auth_request_sync, second_auth_request_sync},
    authenticate::authenticate_sync,
};

#[cfg(feature = "async")]
pub(super) use self::{
    auth_requests::{first_auth_request_async, second_auth_request_async},
    authenticate::authenticate_async,
};

pub(super) use self::{
    auth_requests::FirstAuthResponse, authenticate::AuthenticationResult,
    authenticator::Authenticator, scram_pbkdf2_sha256::ScramPbkdf2Sha256,
    scram_sha256::ScramSha256,
};
