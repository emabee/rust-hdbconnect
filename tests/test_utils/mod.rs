// advisable because not all test modules use all functions of this module:
#![allow(dead_code)]

use flexi_logger::{Logger, ReconfigurationHandle};
use hdbconnect::{ConnectParamsBuilder, Connection, HdbError, HdbResult};
use serde_json;
use std::io::BufReader;
use std::path::Path;
use std::fs::File;

pub fn init_logger(log_spec: &str) -> ReconfigurationHandle {
    Logger::with_env_or_str(log_spec)
        .start_reconfigurable()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e))
}

pub fn connect_params_builder_from_file(s: &'static str) -> HdbResult<ConnectParamsBuilder> {
    let path = Path::new(s);
    let reader = BufReader::new(File::open(&path)?);
    match serde_json::from_reader(reader) {
        Ok(cpb) => Ok(cpb),
        Err(e) => {
            println!("{:?}", e);
            Err(HdbError::UsageError(
                "Cannot read db_access.json".to_owned(),
            ))
        }
    }
}

pub fn get_authenticated_connection() -> HdbResult<Connection> {
    let params = connect_params_builder_from_file("db_access.json")?.build()?;
    Connection::new(params)
}

pub fn get_system_connection() -> HdbResult<Connection> {
    let params = connect_params_builder_from_file("db_access_system.json")?.build()?;
    Connection::new(params)
}
