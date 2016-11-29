#![allow(dead_code)] // advisable because not all test modules use all functions of this module

extern crate flexi_logger;
use hdbconnect::{Connection, HdbResult};

pub fn init_logger(log_to_file: bool, log_spec: &str) {
    flexi_logger::LogOptions::new()
        .log_to_file(log_to_file)
        .init(Some(log_spec.to_string()))
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e));
}

pub fn get_connection() -> Connection {
    Connection::new("lu245307.dhcp.wdf.sap.corp", "33715").unwrap()
}

pub fn get_authenticated_connection() -> Connection {
    let mut connection = get_connection();
    connection.authenticate_user_password("SYSTEM", "manager").unwrap();
    connection
}

pub fn statement_ignore_err(connection: &mut Connection, stmts: Vec<&str>) {
    for s in stmts {
        match connection.any_statement(s) {
            Ok(_) => {}
            Err(_) => {}
        }
    }
}

pub fn multiple_statements(connection: &mut Connection, prep: Vec<&str>) -> HdbResult<()> {
    for s in prep {
        try!(connection.any_statement(s));
    }
    Ok(())
}
