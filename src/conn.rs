// The low-level database connection.
// Depending on the ConnectParams, the physical connection is either a plain
// TcpStream or a TlsTcpStream.

mod am_conn_core;
mod connection_core;
mod initial_request;
mod params;
mod session_state;
mod tcp;

pub(crate) use am_conn_core::AmConnCore;
pub use params::connect_params::{ConnectParams, ServerCerts};
pub use params::connect_params_builder::ConnectParamsBuilder;
pub use params::into_connect_params::IntoConnectParams;
pub use params::into_connect_params_builder::IntoConnectParamsBuilder;
pub(crate) use session_state::SessionState;
pub(crate) use tcp::TcpClient;
