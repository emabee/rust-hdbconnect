#![feature(custom_derive, plugin)]  // necessary for serde
#![feature(associated_consts)]      // necessary for local consts
#![plugin(serde_macros)]

extern crate byteorder;
extern crate chrono;
extern crate crypto;
extern crate flexi_logger;
#[macro_use] extern crate log;
extern crate num;
extern crate rand;
extern crate serde;
extern crate vec_map;
extern crate user;


mod adhoc_statement;
mod connection;
mod db_response;
mod dbc_error;
mod prepared_statement;

mod protocol;
mod rs_serde;
mod tests;

pub mod types;
pub mod test_utils;

pub use connection::Connection;
pub use prepared_statement::PreparedStatement;
pub use db_response::DbResponse;
pub use protocol::lowlevel::parts::resultset::{ResultSet,Row};
pub use protocol::lowlevel::parts::typed_value::TypedValue;
pub use dbc_error::{DbcError,DbcResult};
