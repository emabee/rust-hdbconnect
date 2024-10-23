//! Asynchronous database driver for SAP HANA (TM).
//!
//! `hdbconnect_async` is written completely in rust, its asynchronous model is based on
//! [`tokio`](https://crates.io/crates/tokio).
//! It provides a lean, fast, and easy-to-use API for working with SAP HANA.
//!
//! For usecases where you don't need an asynchronous driver,
//! you might want to use `hdbconnect_async`'s synchronous sibling,
//! [`hdbconnect`](https://docs.rs/hdbconnect).
//! The two drivers have a very similar API and share most of their implementation.
//!
//! `hdbconnect_async` interoperates elegantly with all data types that implement the standard
//! `serde::Serialize` and/or `serde::Deserialize` traits, for input and output respectively.
//! So, instead of iterating over a result set by rows and columns, you can
//! assign the complete result set directly to any rust structure that fits the data
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

// only enables the `doc_cfg` feature when the `docsrs` configuration attribute is defined
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_debug_implementations)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]

#[cfg(feature = "rocket_pool")]
mod rocket_pool;

#[cfg(feature = "bb8_pool")]
mod bb8;

/// Provides some statistics about the use of a concrete connection.
///
/// A snapshot of the statistics can be obtained from [`Connection::statistics`].
/// It is possible to reset the statistics using [`Connection::reset_statistics`].
pub use hdbconnect_impl::ConnectionStatistics;

/// A collection of settings that influence the runtime behavior of a connection.
///
/// To create a connection with non-default settings, use [`Connection::with_configuration`].
///
/// # Example
///
/// ```rust, no_run
/// # use hdbconnect_async::{Connection, HdbResult, ConnectionConfiguration};
/// # async fn foo() -> HdbResult<()> {
/// # let db_url = "hdbsql://my_user:my_passwd@the_host:30815";
/// let config = ConnectionConfiguration::default()
///     .with_lob_read_length(1_000_000)
///     .with_max_buffer_size(1_000_000);
/// let connection = Connection::with_configuration(db_url, &config).await?;
/// # Ok(())
/// # }
/// ````
pub use hdbconnect_impl::ConnectionConfiguration;

/// Holdability of cursors in the database.
pub use hdbconnect_impl::CursorHoldability;

pub use hdbconnect_impl::{
    time, url, ConnectParams, ConnectParamsBuilder, DeserializationError, ExecutionResult,
    FieldMetadata, HdbError, HdbResult, HdbValue, IntoConnectParams, IntoConnectParamsBuilder,
    OutputParameters, ParameterBinding, ParameterDescriptor, ParameterDescriptors,
    ParameterDirection, ResultSetMetadata, Row, SerializationError, ServerCerts, ServerError,
    ServerUsage, Severity, ToHana, TypeId,
};

pub use hdbconnect_impl::a_sync::{
    Connection, HdbResponse, HdbReturnValue, PreparedStatement, ResultSet,
};

/// Non-standard types that are used to represent database values.
///
/// A `ResultSet` contains a sequence of `Row`s, each row is a sequence of `HdbValue`s.
/// Some  variants of `HdbValue` are implemented using plain rust types,
/// others are based on the types in this module.
pub mod types {
    pub use hdbconnect_impl::a_sync::{BLob, CLob, NCLob};
    pub use hdbconnect_impl::types::*;
}

#[cfg_attr(docsrs, doc(cfg(feature = "rocket_pool")))]
#[cfg(feature = "rocket_pool")]
pub use rocket_pool::HanaPoolForRocket;

#[cfg_attr(docsrs, doc(cfg(feature = "bb8_pool")))]
#[cfg(feature = "bb8_pool")]
pub use bb8::ConnectionManager;

pub mod code_examples;

pub use hdbconnect_impl::initialize_crypto;
