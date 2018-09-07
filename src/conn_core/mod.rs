//! The low-level database connection.
//! Depending on the ConnectParams, the physical connection is either a plain
//! TcpStream or a TlsStream, which are the two variants of Buffalo.

mod buffalo;
pub mod connect_params;
mod connection_core;
mod initial_request;

pub use self::connection_core::{AmConnCore, ConnectionCore};
