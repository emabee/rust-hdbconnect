#![allow(dead_code)] 

use hdbconnect::Connection;
use hdbconnect::DbcResult;

extern crate flexi_logger;
use self::flexi_logger::*;

// "info, \
// hdbconnect::protocol::lowlevel::resultset=debug,\
// "

pub fn init_logger(log_to_file: bool, log_spec: &str) {
    flexi_logger::init(
        flexi_logger::LogConfig {
            log_to_file: log_to_file,
            .. LogConfig::new()
        },
        Some(log_spec.to_string())
    ).unwrap();
}


pub fn statement_ignore_err(connection: &mut Connection, stmts: Vec<&str>) {
    for s in stmts {
        match connection.any_statement(s) {
            Ok(_) => {}
            Err(_) => {}
        }
    }
}

pub fn multiple_statements(connection: &mut Connection, prep: Vec<&str>) -> DbcResult<()> {
    for s in prep {
        try!(connection.any_statement(s));
    }
    Ok(())
}
