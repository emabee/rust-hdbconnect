// The low-level database connection.
// Depending on the ConnectParams, the physical connection is either a plain
// TcpStream or a TlsStream, which are the two variants of Buffalo.

mod am_conn_core;
mod connect_params;
mod connect_params_builder;
mod connection_core;
mod initial_request;
mod into_connect_params;
mod session_state;
mod tcp;
mod tcp_conn;

pub(crate) use am_conn_core::AmConnCore;
pub use connect_params::{ConnectParams, ServerCerts};
pub use connect_params_builder::ConnectParamsBuilder;
pub use into_connect_params::IntoConnectParams;
pub(crate) use session_state::SessionState;
pub(crate) use tcp_conn::TcpConn;
