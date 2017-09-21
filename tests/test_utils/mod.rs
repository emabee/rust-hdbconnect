// advisable because not all test modules use all functions of this module:
 #![allow(dead_code)]

use flexi_logger::Logger;
use hdbconnect::{Connection, ConnectParamsBuilder, HdbError, HdbResult};
use serde_json;
use std::io::BufReader;
use std::path::Path;
use std::fs::File;

pub fn init_logger(log_spec: &str) {
    Logger::with_str(log_spec)
        .start()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e));
}

pub fn connect_params_builder_from_file() -> HdbResult<ConnectParamsBuilder> {
    let path = Path::new("db_access.json");
    let reader = BufReader::new(File::open(&path)?);
    match serde_json::from_reader(reader) {
        Ok(cpb) => Ok(cpb),
        Err(e) => {
            println!("{:?}", e);
            Err(HdbError::UsageError("Cannot read db_access.json".to_owned()))
        }
    }
}

pub fn get_authenticated_connection() -> HdbResult<Connection> {
    let params = connect_params_builder_from_file()?.build()?;
    Connection::new(params)
}

pub fn statement_ignore_err(connection: &mut Connection, stmts: Vec<&str>) {
    for s in stmts {
        match connection.statement(s) {
            Ok(_) => {}
            Err(_) => {}
        }
    }
}
