// advisable because not all test modules use all functions of this module:
#![allow(dead_code)]

use failure::ResultExt;
use flexi_logger::{opt_format, Logger, ReconfigurationHandle};
use hdbconnect::{Connection, HdbErrorKind, HdbResult};

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
        .start()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e))
}

pub fn closing_info(connection: Connection, start: std::time::Instant) -> HdbResult<()> {
    log::info!(
        "{} calls to DB were executed; \
         elapsed time: {:?}, \
         accumulated server processing time: {:?}",
        connection.get_call_count()?,
        std::time::Instant::now().duration_since(start),
        connection.server_usage()?.accum_proc_time
    );
    Ok(())
}

pub fn get_authenticated_connection() -> HdbResult<Connection> {
    Connection::new(get_std_connect_string()?)
}

pub fn get_system_connection() -> HdbResult<Connection> {
    Connection::new(get_system_connect_string()?)
}

pub fn get_wrong_connect_string(user: Option<&str>, pw: Option<&str>) -> HdbResult<String> {
    let mut s = get_std_connect_string()?;
    let sep1 = s.find("://").unwrap();
    let sep3 = s[sep1 + 3..].find("@").unwrap();
    let sep2 = s[sep1 + 3..sep3].find(":").unwrap();
    if let Some(pw) = pw {
        s.replace_range((sep1 + 3 + sep2 + 1)..(sep1 + 3 + sep3), pw);
    }
    if let Some(u) = user {
        s.replace_range((sep1 + 3)..(sep1 + 3 + sep2), u);
    }
    Ok(s)
}

fn get_version() -> &'static str {
    // "2_0"
    "2_3"
    // "pascal"
}

pub fn get_std_connect_string() -> HdbResult<String> {
    let filename = format!("./.private/db_{}_std.url", get_version());
    connect_string_from_file(filename.as_ref())
}

pub fn get_system_connect_string() -> HdbResult<String> {
    let filename = format!("./.private/db_{}_system.url", get_version());
    connect_string_from_file(filename.as_ref())
}

fn connect_string_from_file(s: &str) -> HdbResult<String> {
    Ok(std::fs::read_to_string(s).context(HdbErrorKind::ConnParams)?)
}
