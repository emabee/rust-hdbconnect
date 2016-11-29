//! Experimental native rust database driver for SAP HANA(TM).
//!
//! Since the implementation makes use of serde, this crate compiles so far only with rust nightly.
//!
//! The reason for publishing this driver in its immature state is that we want to
//! demonstrate how [serde](https://serde.rs/)
//! can be used to simplify the API of such a driver.
//!
//! Concretely, we use serde to simplify the data exchange between your code and the driver,
//! both for input parameters to prepared statements
//! and for results that you get from the database:
//! there is no need to iterate over a complex resultset by rows and columns!
//!
//! This approach allows, in contrast to many ORM mapping variants, using
//! the full flexibility of SQL (projection lists, all kinds of joins, unions, etc, etc).
//! Whatever query you need, you just use it, and whatever result structure you need,
//! you just use a corresponding rust structure into which you deserialize the data.
//!
//! See [ResultSet.into_typed()](struct.ResultSet.html#method.into_typed) and
//! [code examples](code_examples/index.html) for details.

#![feature(proc_macro)]
#![warn(missing_docs)]

extern crate byteorder;
extern crate chrono;
extern crate crypto;
extern crate flexi_logger;

#[macro_use]
extern crate log;

extern crate num;
extern crate rand;

#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate vec_map;
extern crate user;

mod connection;
mod hdb_response;
mod hdb_error;
mod prepared_statement;
mod protocol;
mod rs_serde;

pub mod code_examples;
pub mod types;


pub use connection::Connection;
pub use prepared_statement::PreparedStatement;
pub use hdb_response::{HdbResponse, HdbReturnValue};
pub use protocol::lowlevel::parts::resultset::{ResultSet, Row};
pub use protocol::lowlevel::parts::typed_value::TypedValue as HdbValue;
pub use protocol::lowlevel::parts::output_parameters::OutputParameters;
pub use protocol::lowlevel::parts::parameter_metadata::{ParameterDescriptor, ParameterOption,
                                                        ParMode};
pub use hdb_error::{HdbError, HdbResult};
