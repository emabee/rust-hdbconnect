//! Native rust database driver for SAP HANA (TM).
//!
//! `hdbconnect` provides a lean, fast, and easy-to-use rust-API for working with
//! SAP HANA. The driver is written completely in rust. It comes
//! with an API that interoperates elegantly with all data types that implement the standard
//! `serde::Serialize` and/or `serde::Deserialize` traits, for input and output respectively.
//!
//! Instead of iterating over a resultset by rows and columns, you can
//! assign the complete query result directly to any rust structure that fits the data
//! semantics.
//!
//! `hdbconnect` implements this with the help of [`serde_db`](https://docs.rs/serde_db),
//! a reusable library for simplifying the data exchange between application code
//! and database drivers, both for input parameters (e.g. to prepared statements)
//! and for results that are returned from the database.
//!
//! In contrast to typical ORM mapping variants does this approach allow
//! using the full flexibility of SQL (projection lists, all kinds of joins,
//! unions, etc, etc). Whatever query you need, you just use it, without further ado
//! for defining object models etc, and whatever result structure you want to read,
//! you just use a corresponding rust structure into
//! which you deserialize the data. It's hard to use less code!
//!
//! See [code examples](code_examples/index.html) for an overview.
//!

#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde_derive;

pub use r2d2;
pub use serde_db;

mod authentication;
mod conn_core;
mod connection;
mod connection_manager;
mod hdb_error;
mod hdb_response;
mod hdb_return_value;
mod impl_serde_db;
mod prepared_statement;
mod protocol;
mod xa_impl;

pub mod code_examples;

pub use crate::conn_core::connect_params::{ConnectParams, IntoConnectParams};
pub use crate::conn_core::connect_params_builder::ConnectParamsBuilder;

#[cfg(feature = "tls")]
pub use crate::conn_core::connect_params::ServerCerts;

pub use crate::connection::Connection;
pub use crate::connection_manager::ConnectionManager;
pub use crate::hdb_error::{HdbError, HdbErrorKind, HdbResult};
pub use crate::hdb_response::HdbResponse;
pub use crate::hdb_return_value::HdbReturnValue;
pub use crate::prepared_statement::PreparedStatement;
pub use crate::protocol::parts::execution_result::ExecutionResult;
pub use crate::protocol::parts::output_parameters::OutputParameters;
pub use crate::protocol::parts::parameter_descriptor::{
    ParameterBinding, ParameterDescriptor, ParameterDescriptors, ParameterDirection,
};
pub use crate::protocol::parts::resultset::ResultSet;
pub use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
pub use crate::protocol::parts::row::Row;
pub use crate::protocol::parts::server_error::{ServerError, Severity};
pub use crate::protocol::parts::type_id::TypeId;
pub use crate::protocol::server_usage::ServerUsage;

mod types_impl;

/// Non-standard types that are used within the
/// [`HdbValue`](enum.HdbValue.html)s in a [`ResultSet`](struct.ResultSet.html).
///
/// A `ResultSet` contains a sequence of Rows, each row is a sequence of
/// `HdbValue`s. Some of the `HdbValue`s are implemented using `LongDate`,
/// BLOB, etc.
pub mod types {
    pub use crate::types_impl::lob::{BLob, CLob, CharLobSlice, NCLob};

    pub use crate::types_impl::daydate::DayDate;
    pub use crate::types_impl::longdate::LongDate;
    pub use crate::types_impl::seconddate::SecondDate;
    pub use crate::types_impl::secondtime::SecondTime;
}
pub use crate::protocol::parts::hdb_value::HdbValue;

/// Default value for the number of resultset lines that are fetched
/// with a single FETCH roundtrip; the constant's value is 100,000.
///
/// The value used at runtime can be changed with
/// [Connection::set_fetch_size()](struct.Connection.html#method.set_fetch_size).
pub const DEFAULT_FETCH_SIZE: u32 = 100_000;

/// Number of bytes (for BLOBS and CLOBS) or 1-2-3-byte sequences (for NCLOBS)
/// that are fetched in a single LOB READ roundtrip; the constant's value is 16,000,000.
///
/// The value used at runtime can be changed with
/// [Connection::set_lob_read_length()](struct.Connection.html#method.set_lob_read_length).
pub const DEFAULT_LOB_READ_LENGTH: u32 = 16_000_000;

/// Number of bytes that are written in a single LOB WRITE roundtrip;
/// the constant's value is 16,000,000.
///
/// The value used at runtime can be changed with
/// [Connection::set_lob_write_length()](struct.Connection.html#method.set_lob_write_length).
pub const DEFAULT_LOB_WRITE_LENGTH: usize = 16_000_000;
