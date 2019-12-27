// The low-level database connection.
// Depending on the ConnectParams, the physical connection is either a plain
// TcpStream or a TlsStream, which are the two variants of Buffalo.

mod am_conn_core;
mod buffalo;
pub mod connect_params;
pub mod connect_params_builder;
mod connection_core;
mod initial_request;
pub mod into_connect_params;
mod session_state;

pub(crate) use self::am_conn_core::AmConnCore;
