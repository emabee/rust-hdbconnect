//! Synchronous native rust database driver for SAP HANA (TM).
//!
//! `hdbconnect` provides a lean, fast, and easy-to-use rust-API for working with
//! SAP HANA. The driver is written completely in rust.
//!
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

#[macro_use]
extern crate log;

pub mod code_examples;
mod connection;
mod connection_manager;
mod prepared_statement;

pub use {
    connection::Connection, connection_manager::ConnectionManager,
    prepared_statement::PreparedStatement,
};

pub use hdbconnect_impl::{
    conn::{
        url, ConnectParams, ConnectParamsBuilder, IntoConnectParams, IntoConnectParamsBuilder,
        ServerCerts, Tls,
    },
    hdb_error::{HdbError, HdbResult},
    hdb_response::HdbResponse,
    hdb_return_value::HdbReturnValue,
    protocol::parts::{
        ExecutionResult, FieldMetadata, HdbValue, OutputParameters, ParameterBinding,
        ParameterDescriptor, ParameterDescriptors, ParameterDirection, ResultSet,
        ResultSetMetadata, ServerError, Severity, TypeId,
    },
    row::Row,
    rows::Rows,
};

pub use hdbconnect_impl::protocol::ServerUsage;
pub use hdbconnect_impl::serde_db_impl::{time, ToHana};
pub use r2d2;

/// Non-standard types that are used within the
/// [`HdbValue`](crate::HdbValue)s in a [`ResultSet`](crate::ResultSet).
///
/// A `ResultSet` contains a sequence of `Row`s, each row is a sequence of
/// `HdbValue`s. Some of the `HdbValue`s are implemented using `LongDate`,
/// BLOB, etc.
pub mod types {
    pub use hdbconnect_impl::types_impl::lob::{BLob, CLob, CharLobSlice, NCLob};

    pub use hdbconnect_impl::types_impl::daydate::DayDate;
    pub use hdbconnect_impl::types_impl::longdate::LongDate;
    pub use hdbconnect_impl::types_impl::seconddate::SecondDate;
    pub use hdbconnect_impl::types_impl::secondtime::SecondTime;
}

pub use hdbconnect_impl::DEFAULT_FETCH_SIZE;
pub use hdbconnect_impl::DEFAULT_LOB_READ_LENGTH;
pub use hdbconnect_impl::DEFAULT_LOB_WRITE_LENGTH;
