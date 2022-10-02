//! Native rust database driver for SAP HANA (TM).
//!
//! `hdbconnect` provides a lean, fast, and easy-to-use rust-API for working with
//! SAP HANA. The driver is written completely in rust.

//! It interoperates elegantly with all data types that implement the standard
//! `serde::Serialize` and/or `serde::Deserialize` traits, for input and output respectively.
//! So, instead of iterating over a resultset by rows and columns, you can
//! assign the complete resultset directly to any rust structure that fits the data
//! semantics.
//!
//! `hdbconnect` implements this with the help of [`serde_db`](https://docs.rs/serde_db),
//! a reusable library for simplifying the data exchange between application code
//! and database drivers, both for input parameters (e.g. to prepared statements)
//! and for results that are returned from the database.
//!
//! In contrast to typical ORM mapping variants, this approach allows
//! using the full flexibility of SQL (projection lists, all kinds of joins,
//! unions, nested queries, etc). Whatever query you need, you just use it, without further ado
//! for defining object models etc., and whatever result structure you want to read,
//! you just use a corresponding rust structure into
//! which you deserialize the data. It's hard to use less code!
//!
//! See [code examples](crate::code_examples) for an overview.
//!

#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::non_ascii_literal)]
#![allow(clippy::must_use_candidate)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

pub use r2d2;
pub use serde_db;

mod conn;
mod hdb_error;
mod hdb_response;
mod hdb_return_value;
mod protocol;
mod serde_db_impl;
mod sync_connection;
mod sync_connection_manager;
mod sync_prepared_statement;
mod types_impl;
mod xa_impl;

pub mod code_examples;

pub use crate::conn::{
    url, ConnectParams, ConnectParamsBuilder, IntoConnectParams, IntoConnectParamsBuilder,
    ServerCerts,
};
pub use crate::hdb_error::{HdbError, HdbResult};
pub use crate::hdb_response::HdbResponse;
pub use crate::hdb_return_value::HdbReturnValue;
pub use crate::protocol::parts::{
    ExecutionResult, FieldMetadata, HdbValue, OutputParameters, ParameterBinding,
    ParameterDescriptor, ParameterDescriptors, ParameterDirection, ResultSet, Row, ServerError,
    Severity, TypeId,
};
pub use crate::protocol::ServerUsage;
pub use crate::sync_connection::Connection;
pub use crate::sync_connection_manager::ConnectionManager;
pub use crate::sync_prepared_statement::PreparedStatement;

/// Non-standard types that are used within the
/// [`HdbValue`](crate::HdbValue)s in a [`ResultSet`](crate::ResultSet).
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

/// Default value for the number of resultset lines that are fetched
/// with a single FETCH roundtrip; the constant's value is 100,000.
///
/// The value used at runtime can be changed with
/// [`Connection::set_fetch_size()`](crate::Connection::set_fetch_size).
pub const DEFAULT_FETCH_SIZE: u32 = 100_000;

/// Number of bytes (for BLOBS and CLOBS) or 1-2-3-byte sequences (for NCLOBS)
/// that are fetched in a single LOB READ roundtrip; the constant's value is 16,000,000.
///
/// The value used at runtime can be changed with
/// [`Connection::set_lob_read_length()`](crate::Connection::set_lob_read_length).
pub const DEFAULT_LOB_READ_LENGTH: u32 = 16_000_000;

/// Number of bytes that are written in a single LOB WRITE roundtrip;
/// the constant's value is 16,000,000.
///
/// The value used at runtime can be changed with
/// [`Connection::set_lob_write_length()`](crate::Connection::set_lob_write_length).
pub const DEFAULT_LOB_WRITE_LENGTH: usize = 16_000_000;

const MAX_1_BYTE_LENGTH: u8 = 245;
const MAX_2_BYTE_LENGTH: i16 = i16::max_value();
const LENGTH_INDICATOR_2BYTE: u8 = 246;
const LENGTH_INDICATOR_4BYTE: u8 = 247;
const LENGTH_INDICATOR_NULL: u8 = 255;
