// advisable because not all test modules use all functions of this module:
#![allow(dead_code)]

use flexi_logger::{Logger, ReconfigurationHandle};
use hdbconnect::{ConnectParamsBuilder, Connection, HdbError, HdbResult};
use serde_json;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

pub fn init_logger(log_spec: &str) -> ReconfigurationHandle {
    Logger::with_env_or_str(log_spec)
        .start_reconfigurable()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e))
}

pub fn get_authenticated_connection() -> HdbResult<Connection> {
    let params = get_std_connect_params_builder()?.build()?;
    Connection::new(params)
}

pub fn get_system_connection() -> HdbResult<Connection> {
    let params = get_system_connect_params_builder()?.build()?;
    Connection::new(params)
}

pub fn get_std_connect_params_builder() -> HdbResult<ConnectParamsBuilder> {
    let filename = format!("db_access_{}.json", get_version());
    connect_params_builder_from_file(filename.as_ref())
}

pub fn get_system_connect_params_builder() -> HdbResult<ConnectParamsBuilder> {
    let filename = format!("db_access_system_{}.json", get_version());
    connect_params_builder_from_file(filename.as_ref())
}

fn get_version() -> &'static str {
    // "2_0"
    "2_3"
}

fn connect_params_builder_from_file(s: &str) -> HdbResult<ConnectParamsBuilder> {
    let path = Path::new(s);
    let reader = BufReader::new(File::open(&path)?);
    match serde_json::from_reader(reader) {
        Ok(cpb) => Ok(cpb),
        Err(e) => {
            println!("{:?}", e);
            Err(HdbError::Usage("Cannot read db_access.json".to_owned()))
        }
    }
}
