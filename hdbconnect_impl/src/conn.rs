// The database connection, the parameters for creating one, and authenticaton.

mod authentication;
mod connection_core;
mod initial_request;
mod params;
mod session_state;

#[cfg(feature = "async")]
mod async_am_conn_core;
#[cfg(feature = "sync")]
mod sync_am_conn_core;

#[cfg(feature = "async")]
pub use async_am_conn_core::AsyncAmConnCore;
#[cfg(feature = "sync")]
pub use sync_am_conn_core::SyncAmConnCore;
#[cfg(feature = "async")]
mod async_tcp_client;
#[cfg(feature = "sync")]
mod sync_tcp_client;

use authentication::AuthenticationResult;
pub use connection_core::ConnectionCore;
pub use params::cp_url::url;
pub use params::{
    connect_params::{ConnectParams, ServerCerts, Tls},
    connect_params_builder::ConnectParamsBuilder,
    into_connect_params::IntoConnectParams,
    into_connect_params_builder::IntoConnectParamsBuilder,
};
use session_state::SessionState;

#[cfg(feature = "async")]
use async_tcp_client::AsyncTcpClient;
#[cfg(feature = "sync")]
use sync_tcp_client::SyncTcpClient;
