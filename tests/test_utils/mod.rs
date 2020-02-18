// advisable because not all test modules use all functions of this module:
#![allow(dead_code)]

use flexi_logger::{opt_format, Logger, ReconfigurationHandle};
use hdbconnect::{
    ConnectParamsBuilder, Connection, HdbError, HdbResult, IntoConnectParamsBuilder, ServerCerts,
};

// const DB: &str = "./.private/2_0.db";
const DB: &str = "./.private/2_3.db";
// const DB: &str = "./.private/C5_02.db";
// const DB: &str = "./.private/C5_02_insecure.db";
// const DB: &str = "./.private/C5_02_insecure_nonblocking.db";

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
    Connection::new(get_std_cp_builder()?)
}

pub fn get_um_connection() -> HdbResult<Connection> {
    Connection::new(get_um_cp_builder()?)
}

pub fn get_std_cp_builder() -> HdbResult<ConnectParamsBuilder> {
    cp_builder_from_file("std")
}

pub fn get_um_cp_builder() -> HdbResult<ConnectParamsBuilder> {
    cp_builder_from_file("um")
}

fn cp_builder_from_file(purpose: &str) -> HdbResult<ConnectParamsBuilder> {
    let content = std::fs::read_to_string(std::path::Path::new(DB.clone())).map_err(|e| {
        HdbError::ConnParams {
            source: Box::new(e),
        }
    })?;

    #[derive(Deserialize)]
    struct Cred<'a> {
        name: &'a str,
        pw: &'a str,
    }
    #[derive(Deserialize)]
    struct Db<'a> {
        url: &'a str,
        std: Cred<'a>,
        um: Cred<'a>,
    }

    let db: Db = serde_json::from_str(&content).unwrap();
    let (url, std, um) = (db.url, db.std, db.um);
    let mut cp_builder = url.into_connect_params_builder()?;
    match purpose {
        "std" => {
            cp_builder.dbuser(std.name).password(std.pw);
        }
        "um" => {
            cp_builder.dbuser(um.name).password(um.pw);
        }
        _ => panic!("unknown purpose: {}",),
    }
    if let Ok(ref s) = std::env::var("HDBCONNECT_FORCE_TEST_WITH_TLS") {
        match s.as_ref() {
            "DIRECTORY" => {
                cp_builder.tls_with(ServerCerts::Directory(".private/certificates".to_string()));
            }
            "INSECURE" => {
                cp_builder.tls_with(ServerCerts::None);
            }
            _ => {}
        }
    };

    Ok(cp_builder)
}
