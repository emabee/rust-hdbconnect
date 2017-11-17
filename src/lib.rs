//! Experimental native rust database driver for SAP HANA(TM).
//!
//! Works with SAP HANA 1 and SAP HANA 2.
//!
//! `hdbconnect` uses serde (via [`serde_db`](https://docs.rs/serde_db))
//! to simplify the data exchange between your code
//! and the driver, both for input parameters to prepared statements
//! and for results that you get from the database.
//! There is no need to iterate over a resultset by rows and columns!
//!
//! This approach allows, in contrast to many ORM mapping variants, using
//! the full flexibility of SQL (projection lists, all kinds of joins, unions, etc, etc).
//! Whatever query you need, you just use it, and whatever result structure you need,
//! you just use a corresponding rust structure into which you deserialize the data.
//!
//! See
//! [code examples](code_examples/index.html)
//! for an overview.
//!
//! Although being functionally operable and working well and fast, this driver is
//! still in an incomplete state:
//! some datatypes are missing, SSL is not yet supported.

#![warn(missing_docs)]

extern crate byteorder;
extern crate chrono;
extern crate crypto;
extern crate hex;
extern crate num;
extern crate rust_decimal;

#[macro_use]
extern crate log;

extern crate r2d2;
extern crate rand;

extern crate serde;
extern crate serde_db;
#[macro_use]
extern crate serde_derive;
extern crate username;
extern crate vec_map;

mod connect_params;
mod connection;
mod connection_manager;
mod hdb_response;
mod hdb_error;
mod impl_serde_db;
mod prepared_statement;
mod protocol;
mod url;

pub mod code_examples;

pub use connection_manager::ConnectionManager;
pub use connection::Connection;
pub use connect_params::{ConnectParams, ConnectParamsBuilder, IntoConnectParams};
pub use prepared_statement::PreparedStatement;
pub use hdb_response::{HdbResponse, HdbReturnValue};
pub use protocol::lowlevel::parts::resultset::ResultSet;
pub use protocol::lowlevel::parts::row::Row;
pub use hdb_error::{HdbError, HdbResult};

/// Types for describing metadata.
pub mod metadata {
    pub use protocol::lowlevel::parts::output_parameters::OutputParameters;
    pub use protocol::lowlevel::parts::parameter_metadata::{ParMode, ParameterDescriptor,
                                                            ParameterOption};
    pub use protocol::lowlevel::parts::resultset::factory::new_for_tests as new_resultset_for_tests;
    pub use protocol::lowlevel::parts::resultset_metadata::{FieldMetadata, ResultSetMetadata};
}


/// Types that are used within the content part of a `ResultSet`.
///
/// A `ResultSet` contains a sequence of Rows. A row is a sequence of `HdbValues`.
/// Some of the `HdbValues` are implemented using `LongDate`, BLOB, etc
///
pub mod types {
    pub use protocol::lowlevel::parts::lob::{new_clob_to_db, BLOB, CLOB};
    pub use protocol::lowlevel::parts::hdb_decimal::HdbDecimal;
    pub use protocol::lowlevel::parts::longdate::LongDate;
}
pub use protocol::lowlevel::parts::typed_value::TypedValue as HdbValue;
