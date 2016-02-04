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


mod adhoc_statement;
pub mod log_format;
pub mod connection;
pub mod db_response;
pub mod dbc_error;
pub mod prepared_statement;

pub mod protocol;
pub mod rs_serde;
pub mod types;


pub use connection::Connection;
pub use db_response::DbResponse;
pub use types::longdate::LongDate;
pub use dbc_error::{DbcError,DbcResult};
