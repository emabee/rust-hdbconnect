//! Asynchronous native rust database driver for SAP HANA (TM).
//!
//! `hdbconnect_async` provides a lean, fast, and easy-to-use rust-API for working with
//! SAP HANA. The driver is written completely in rust.

//! It interoperates elegantly with all data types that implement the standard
//! `serde::Serialize` and/or `serde::Deserialize` traits, for input and output respectively.
//! So, instead of iterating over a resultset by rows and columns, you can
//! assign the complete resultset directly to any rust structure that fits the data
//! semantics.
//!
//! `hdbconnect_async` implements this with the help of [`serde_db`](https://docs.rs/serde_db),
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

// FIXME rocket_pool is missing
pub use hdbconnect_impl::{
    time, types, url, ConnectParams, ConnectParamsBuilder, ExecutionResult, FieldMetadata,
    HdbError, HdbResponse, HdbResult, HdbReturnValue, HdbValue, IntoConnectParams,
    IntoConnectParamsBuilder, OutputParameters, ParameterBinding, ParameterDescriptor,
    ParameterDescriptors, ParameterDirection, Row, ServerCerts, ServerError, ServerUsage, Severity,
    Tls, ToHana, TypeId, DEFAULT_FETCH_SIZE, DEFAULT_LOB_READ_LENGTH, DEFAULT_LOB_WRITE_LENGTH,
};

pub use hdbconnect_impl::a_sync::{Connection, PreparedStatement};
pub use hdbconnect_impl::AsyncResultSet as ResultSet;
pub mod code_examples;
