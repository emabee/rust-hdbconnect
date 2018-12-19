//! Native rust database driver for SAP HANA(TM).
//!
//! `hdbconnect` uses [`serde_db`](https://docs.rs/serde_db)
//! to simplify the data exchange between application code
//! and the driver, both for input parameters to prepared statements
//! and for results that are returned from the database.
//! There is no need to iterate over a resultset by rows and columns, just
//! assign query results directly to rust structures that fit the data
//! semantics. This approach allows, in contrast to many ORM mapping variants,
//! using the full flexibility of SQL (projection lists, all kinds of joins,
//! unions, etc, etc). Whatever query you need, you just use it, and whatever
//! result structure you need, you just use a corresponding rust structure into
//! which you deserialize the data.
//!
//! See
//! [code examples](code_examples/index.html)
//! for an overview.
//!

// #![feature(bufreader_buffer)]
#![warn(missing_docs)]
#![deny(missing_debug_implementations)]

extern crate bigdecimal;
extern crate byteorder;
extern crate cesu8;
extern crate chrono;
extern crate hex;
extern crate hmac;
extern crate num;
extern crate pbkdf2;
extern crate sha2;
#[macro_use]
extern crate log;

extern crate r2d2;
extern crate rand;
extern crate secstr;

extern crate serde;
#[macro_use]
extern crate serde_derive;

#[cfg(feature = "tls")]
extern crate rustls;
extern crate url;
extern crate username;
extern crate vec_map;
#[cfg(feature = "tls")]
extern crate webpki;

extern crate dist_tx;
extern crate serde_db;

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
pub use crate::hdb_error::{HdbError, HdbResult};
pub use crate::hdb_response::HdbResponse;
pub use crate::hdb_return_value::HdbReturnValue;
pub use crate::prepared_statement::PreparedStatement;
pub use crate::protocol::parts::output_parameters::OutputParameters;
pub use crate::protocol::parts::resultset::ResultSet;
pub use crate::protocol::parts::row::Row;
pub use crate::protocol::parts::server_error::{ServerError, Severity};

pub use crate::protocol::parts::execution_result::ExecutionResult;
pub use crate::protocol::parts::parameter_descriptor::{
    ParameterBinding, ParameterDescriptor, ParameterDirection,
};
pub use crate::protocol::parts::resultset_metadata::ResultSetMetadata;
pub use crate::protocol::parts::type_id::{BaseTypeId, TypeId};

mod types_impl;

/// Non-standard types that are used within the
/// [`HdbValue`](enum.HdbValue.html)s in a [`ResultSet`](struct.ResultSet.html).
///
/// A `ResultSet` contains a sequence of Rows, each row is a sequence of
/// `HdbValue`s. Some of the `HdbValue`s are implemented using `LongDate`,
/// BLOB, etc.
pub mod types {
    pub use crate::types_impl::lob::BLob;
    pub use crate::types_impl::lob::CLob;
    pub use crate::types_impl::lob::NCLob;

    pub use crate::types_impl::daydate::DayDate;
    pub use crate::types_impl::longdate::LongDate;
    pub use crate::types_impl::seconddate::SecondDate;
    pub use crate::types_impl::secondtime::SecondTime;
}
pub use crate::protocol::parts::hdb_value::HdbValue;
