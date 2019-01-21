// advisable because not all test modules use all functions of this module:
#![allow(dead_code)]

use flexi_logger::{opt_format, Logger, ReconfigurationHandle};
use hdbconnect::{ConnectParams, IntoConnectParams};
use hdbconnect::{Connection, HdbResult};
use std::fs::read_to_string;

// Returns a logger that prints out all info, warn and error messages.
//
// For CI/CD, we could change the code here to e.g. react on env settings
// that allow the CI/CD infrastructure to have the logs written to files in a directory.
pub fn init_logger() -> ReconfigurationHandle {
    Logger::with_env_or_str("info")
        // .log_to_file()
        // .suppress_timestamp()
        // .directory("test_logs")
        // .print_message()
        .format(opt_format)
        .start_reconfigurable()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e))
}

pub fn get_authenticated_connection() -> HdbResult<Connection> {
    let params = get_std_connect_params()?;
    Connection::new(params)
}

pub fn get_system_connection() -> HdbResult<Connection> {
    let params = get_system_connect_params()?;
    Connection::new(params)
}

pub fn get_std_connect_params() -> HdbResult<ConnectParams> {
    let filename = format!("./.private/db_{}_std.url", get_version());
    connect_params_from_file(filename.as_ref())
}

pub fn get_system_connect_params() -> HdbResult<ConnectParams> {
    let filename = format!("./.private/db_{}_system.url", get_version());
    connect_params_from_file(filename.as_ref())
}

pub fn get_wrong_connect_params(user: Option<&str>, pw: Option<&str>) -> HdbResult<ConnectParams> {
    let mut url = get_std_connect_url()?;
    let sep1 = url.find("://").unwrap();
    let sep3 = url[sep1 + 3..].find("@").unwrap();
    let sep2 = url[sep1 + 3..sep3].find(":").unwrap();
    if let Some(pw) = pw {
        url.replace_range((sep1 + 3 + sep2 + 1)..(sep1 + 3 + sep3), pw);
    }
    if let Some(u) = user {
        url.replace_range((sep1 + 3)..(sep1 + 3 + sep2), u);
    }
    url.into_connect_params()
}

fn get_version() -> &'static str {
    // "2_0"
    "2_3"
}

fn connect_params_from_file(s: &str) -> HdbResult<ConnectParams> {
    let url = read_to_string(s)?;
    url.into_connect_params()
}

pub fn get_std_connect_url() -> HdbResult<String> {
    let s = format!("./.private/db_{}_std.url", get_version());
    Ok(read_to_string(s)?)
}
