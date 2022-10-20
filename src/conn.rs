// The database connection, the parameters for creating one, and authenticaton.

mod am_conn_core;
mod authentication;
mod connection_core;
mod initial_request;
mod params;
mod session_state;
mod tcp_client;

pub(crate) use am_conn_core::AmConnCore;
use authentication::AuthenticationResult;
pub(crate) use connection_core::ConnectionCore;
pub use params::cp_url::url;
pub use params::{
    connect_params::{ConnectParams, ServerCerts, Tls},
    connect_params_builder::ConnectParamsBuilder,
    into_connect_params::IntoConnectParams,
    into_connect_params_builder::IntoConnectParamsBuilder,
};
use session_state::SessionState;
use tcp_client::TcpClient;
