//! Do not use this crate directly.
//!
//! This is the immplementation crate for `hdbconnect` and `hdbconnect_async`.
//!
//! If you need a synchronous driver, use `hdbconnect`.
//!
//! If you need an asynchronous driver, use `hdbconnect_async`.
//!

#![deny(missing_debug_implementations)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::non_ascii_literal)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![cfg_attr(not(any(feature = "sync", feature = "async")), allow(unused_imports))]
#![cfg_attr(not(any(feature = "sync", feature = "async")), allow(dead_code))]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

mod conn;
mod hdb_error;
mod hdb_response;
mod hdb_return_value;
mod protocol;
mod row;
mod rows;
mod serde_db_impl;
mod types_impl;
pub mod url;
mod xa_impl;

#[cfg(feature = "async")]
pub mod a_sync;
#[cfg(feature = "sync")]
pub mod sync;

pub use crate::conn::{
    ConnectParams, ConnectParamsBuilder, IntoConnectParams, IntoConnectParamsBuilder, ServerCerts,
    Tls,
};

#[cfg(feature = "async")]
pub use types_impl::lob::async_lob_writer;

pub use crate::hdb_error::{HdbError, HdbResult};
pub use crate::hdb_response::HdbResponse;
pub use crate::hdb_return_value::HdbReturnValue;

#[cfg(feature = "async")]
pub use crate::protocol::parts::AsyncResultSet;

#[cfg(feature = "sync")]
pub use crate::protocol::parts::SyncResultSet;

pub use crate::protocol::parts::{
    ExecutionResult, FieldMetadata, HdbValue, OutputParameters, ParameterBinding,
    ParameterDescriptor, ParameterDescriptors, ParameterDirection, ResultSetMetadata, ServerError,
    Severity, TypeId,
};
pub use crate::{row::Row, rows::Rows};

pub use crate::protocol::ServerUsage;
pub use crate::serde_db_impl::{time, ToHana};

/// Non-standard types that are used within the
/// [`HdbValue`](crate::HdbValue)s in a [`ResultSet`](crate::ResultSet).
///
/// A `ResultSet` contains a sequence of `Row`s, each row is a sequence of
/// `HdbValue`s. Some of the `HdbValue`s are implemented using `LongDate`,
/// BLOB, etc.
pub mod types {
    pub use crate::types_impl::{
        daydate::DayDate,
        lob::{BLob, CLob, CharLobSlice, NCLob},
        longdate::LongDate,
        seconddate::SecondDate,
        secondtime::SecondTime,
    };
}

/// Default value for the number of resultset lines that are fetched with a single FETCH roundtrip.
///
/// The value used at runtime can be changed with
/// [`Connection::set_fetch_size()`](crate::Connection::set_fetch_size).
pub const DEFAULT_FETCH_SIZE: u32 = 100_000;

/// Number of bytes (for BLOBS and CLOBS) or 1-2-3-byte sequences (for NCLOBS)
/// that are fetched in a single LOB READ roundtrip.
///
/// The value used at runtime can be changed with
/// [`Connection::set_lob_read_length()`](crate::Connection::set_lob_read_length).
pub const DEFAULT_LOB_READ_LENGTH: u32 = 16_000_000;

/// Number of bytes that are written in a single LOB WRITE roundtrip.
///
/// The value used at runtime can be changed with
/// [`Connection::set_lob_write_length()`](crate::Connection::set_lob_write_length).
pub const DEFAULT_LOB_WRITE_LENGTH: usize = 16_000_000;
