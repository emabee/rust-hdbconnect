// The database connection, the parameters for creating one, and authenticaton.

mod am_conn_core;
mod authentication;
mod command_options;
mod connection_configuration;
mod connection_core;
mod connection_statistics;
mod initial_request;
mod params;
mod session_state;
mod tcp_client;

pub mod url;

pub(crate) use {
    am_conn_core::AmConnCore, command_options::CommandOptions, connection_core::ConnectionCore,
    params::Compression, tcp_client::TcpClient,
};
pub use {
    command_options::CursorHoldability,
    connection_configuration::ConnectionConfiguration,
    connection_statistics::ConnectionStatistics,
    params::{
        connect_params::{ConnectParams, ServerCerts, Tls},
        connect_params_builder::ConnectParamsBuilder,
        into_connect_params::IntoConnectParams,
        into_connect_params_builder::IntoConnectParamsBuilder,
    },
};

use authentication::AuthenticationResult;
use session_state::SessionState;
