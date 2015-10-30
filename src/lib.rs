#![feature(custom_derive, plugin)]  // necessary for serde
#![feature(associated_consts)]      // necessary for local consts
#![plugin(serde_macros)]

extern crate byteorder;
extern crate crypto;
extern crate flexi_logger;
#[macro_use] extern crate log;
extern crate num;
extern crate rand;
extern crate serde;
extern crate time;
extern crate vec_map;

pub use connection::*;

pub mod connection;
pub mod protocol;
