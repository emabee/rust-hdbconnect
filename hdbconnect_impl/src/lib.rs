//! Do not use this crate directly.
//!
//! This is the immplementation crate for `hdbconnect` and `hdbconnect_async`.
//!
//! If you need a synchronous driver, use `hdbconnect`.
//!
//! If you need an asynchronous driver, use `hdbconnect_async`.
//!

// only enables the `doc_cfg` feature when the `docsrs` configuration attribute is defined
#![cfg_attr(docsrs, feature(doc_cfg))]
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
extern crate log;
#[macro_use]
extern crate serde;

mod base;
mod conn;
mod protocol;
mod serde_db_impl;
mod types_impl;
#[cfg(feature = "dist_tx")]
mod xa_impl;

#[cfg(feature = "async")]
pub mod a_sync;
#[cfg(feature = "sync")]
pub mod sync;

pub use crate::{
    base::{HdbError, HdbResult, Row, Rows},
    conn::{
        url, ConnectParams, ConnectParamsBuilder, ConnectionConfiguration, ConnectionStatistics,
        CursorHoldability, IntoConnectParams, IntoConnectParamsBuilder, ServerCerts, Tls,
    },
    protocol::parts::{
        ExecutionResult, FieldMetadata, HdbValue, OutputParameters, ParameterBinding,
        ParameterDescriptor, ParameterDescriptors, ParameterDirection, ResultSetMetadata,
        ServerError, Severity, TypeId,
    },
    protocol::ServerUsage,
    serde_db_impl::{time, ToHana},
};

pub use serde_db::{de::DeserializationError, ser::SerializationError};

/// Non-standard types that are used to represent database values.
///
/// A `ResultSet` contains a sequence of `Row`s, each row is a sequence of `HdbValue`s.
/// Some  variants of `HdbValue` are implemented using plain rust types,
/// others are based on the types in this module.
pub mod types {
    pub use crate::types_impl::{
        daydate::DayDate, lob::CharLobSlice, longdate::LongDate, seconddate::SecondDate,
        secondtime::SecondTime,
    };
}
