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
//! ## Disclaimer
//!
//! This driver is functionally operable and working well and fast.
//! However, its implementation still lacks some features:
//! some date/time/timestamp datatypes are missing, SSL is not yet supported.
//!

// #![feature(bufreader_buffer)]
#![warn(missing_docs)]

extern crate byteorder;
extern crate chrono;
extern crate crypto;
extern crate dist_tx;
extern crate hex;
extern crate num;
extern crate rust_decimal;

#[macro_use]
extern crate log;

extern crate r2d2;
extern crate rand;

extern crate secstr;

extern crate serde;
extern crate serde_db;
#[macro_use]
extern crate serde_derive;
extern crate username;
extern crate vec_map;

mod authenticate;
mod connect_params;
mod connection;
mod connection_manager;
mod hdb_error;
mod hdb_response;
mod hdb_return_value;
mod impl_serde_db;
mod prepared_statement;
mod protocol;
mod url;
mod xa_impl;

pub mod code_examples;

pub use connect_params::{ConnectParams, ConnectParamsBuilder, IntoConnectParams};
pub use connection::Connection;
pub use connection_manager::ConnectionManager;
pub use hdb_error::{HdbError, HdbResult};
pub use hdb_response::HdbResponse;
pub use prepared_statement::PreparedStatement;
pub use protocol::lowlevel::parts::output_parameters::OutputParameters;
pub use protocol::lowlevel::parts::resultset::ResultSet;
pub use protocol::lowlevel::parts::row::Row;
pub use protocol::lowlevel::parts::server_error::{ServerError, Severity};

pub use protocol::lowlevel::parts::parameter_descriptor::{
    ParameterBinding, ParameterDescriptor, ParameterDirection,
};
pub use protocol::lowlevel::parts::resultset_metadata::ResultSetMetadata;

/// Constants for the IDs of the data types being used by HANA.
pub mod type_id {
    pub use protocol::lowlevel::parts::type_id::*;
}

/// Non-standard types that are used within the
/// [`HdbValue`](enum.HdbValue.html)s in a [`ResultSet`](struct.ResultSet.html).
///
/// A `ResultSet` contains a sequence of Rows, each row is a sequence of
/// `HdbValue`s. Some of the `HdbValue`s are implemented using `LongDate`,
/// BLOB, etc.
pub mod types {
    pub use protocol::lowlevel::parts::hdb_decimal::HdbDecimal;
    pub use protocol::lowlevel::parts::lob::BLOB as BLob;
    pub use protocol::lowlevel::parts::lob::CLOB as CLob;
    pub use protocol::lowlevel::parts::longdate::LongDate;
}
pub use protocol::lowlevel::parts::typed_value::TypedValue as HdbValue;
