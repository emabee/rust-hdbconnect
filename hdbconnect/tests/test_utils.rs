// advisable because not all test modules use all functions of this module:
#![allow(dead_code)]

use flexi_logger::{Logger, LoggerHandle, opt_format};
use hdbconnect::{
    ConnectParamsBuilder, Connection, ConnectionConfiguration, HdbError, HdbResult, ServerCerts,
};
use hdbconnect_impl::usage_err;
use std::{fs::read_to_string, path::PathBuf, str::FromStr};

// Returns a logger that prints out all info, warn and error messages.
//
// For CI/CD, we could change the code here to e.g. react on env settings
// that allow the CI/CD infrastructure to have the logs written to files in a directory.
pub fn init_logger() -> LoggerHandle {
    Logger::try_with_env_or_str("info")
        .unwrap()
        // .log_to_file()
        // .suppress_timestamp()
        // .directory("test_logs")
        // .print_message()
        .format(opt_format)
        .start()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {e}"))
}

pub fn closing_info(connection: Connection, start: std::time::Instant) -> HdbResult<()> {
    log::info!(
        "{}\
        Total elapsed time:          {:?}, \n\
        Accumulated server CPU time: {:?}",
        connection.statistics()?,
        std::time::Instant::now().duration_since(start),
        connection.server_usage()?.accum_proc_time
    );
    Ok(())
}

pub fn get_authenticated_connection() -> HdbResult<Connection> {
    let connection = Connection::new(get_std_cp_builder()?)?;
    log::info!("TESTING WITH {}", connection.connect_string().unwrap());
    Ok(connection)
}

pub fn get_authenticated_connection_with_configuration(
    config: &ConnectionConfiguration,
) -> HdbResult<Connection> {
    let connection = Connection::with_configuration(get_std_cp_builder()?, config)?;
    log::info!("TESTING WITH {}", connection.connect_string().unwrap());
    Ok(connection)
}

pub fn get_um_connection() -> HdbResult<Connection> {
    Connection::new(get_um_cp_builder()?)
}

pub fn get_std_cp_builder() -> HdbResult<ConnectParamsBuilder> {
    let (cp_builder, _) = cp_builder_from_file("std")?;
    Ok(cp_builder)
}

pub fn get_std_redirect_cp_builder() -> HdbResult<ConnectParamsBuilder> {
    let (_, redirect_cp_builder) = cp_builder_from_file("std")?;
    Ok(redirect_cp_builder)
}

pub fn get_um_cp_builder() -> HdbResult<ConnectParamsBuilder> {
    let (cp_builder, _) = cp_builder_from_file("um")?;
    Ok(cp_builder)
}

fn cp_builder_from_file(purpose: &str) -> HdbResult<(ConnectParamsBuilder, ConnectParamsBuilder)> {
    const FOLDERS: [&str; 2] = ["./../.private/", "./.private/"];
    const FILE_CREATION_RECIPE: &str = "\
        A convenient way of creating such a file is provided with `examples/setup_db_for_tests.rs`.\n\
        Run the program with `cargo run --package hdbconnect --example setup_db_for_tests`, \n\
        it asks for host and port and the credentials of a user with user-management privileges,\n\
        and creates two database users for the tests and a corresponding config file.";

    let nick_name = {
        const ENV_VAR: &str = "HDBCONNECT_TEST_DB";
        const ENV_ERROR: &str = "Environment variable HDBCONNECT_TEST_DB not set.";
        const ENV_RECIPE: &str = "\
        Set HDBCONNECT_TEST_DB to the nickname of the test database, e.g.\
        'export HDBCONNECT_TEST_DB=abc' to use '.private/test_abc.db'.";

        std::env::var(ENV_VAR).map_err(|_e| {
            println!(
                "ERROR: {ENV_ERROR}\n\n\
                {ENV_RECIPE}\n\n\
                {FILE_CREATION_RECIPE}'."
            );
            usage_err!("{ENV_ERROR}")
        })?
    };
    let filename = format!("test_{nick_name}.db");

    let filepath0 = {
        let mut p = PathBuf::from_str(FOLDERS[0]).unwrap();
        p.push(filename.clone());
        p
    };
    let filepath1 = {
        let mut p = PathBuf::from_str(FOLDERS[1]).unwrap();
        p.push(filename.clone());
        p
    };
    let filepath = if filepath0.exists() {
        filepath0.clone()
    } else {
        filepath1.clone()
    };

    {
        const FILE_STRUCTURE: &str = r#"
The file can be created manually as well. Its json content must look like this:
    {
        "direct_url":"hdbsql://host_url:34015",
        "redirect_url":"hdbsql://host_url:34013?db=H00",
        "std":{"name":"USER1","pw":"user1_pw"},
        "um":{"name":"USER2","pw":"user1_pw"}
    }
where
- the direct URL will be used for most of the tests,
- the redirect URL can/should point to the same database, but via the redirect-syntax.
- the std-user will be used for most of the tests,
- the um-user for few user-management activities.

See https://docs.rs/hdbconnect/latest/hdbconnect/url/index.html for details of the URL format.
"#;

        assert!(
            filepath.exists(),
            "\nERROR: config file with the db connect info not found: {} or {}.\n\n\
            {FILE_CREATION_RECIPE}\n\
            {FILE_STRUCTURE}\n",
            filepath0.display(),
            filepath1.display()
        );
    }

    let content = read_to_string(filepath.clone()).map_err(|e| HdbError::ConnParams {
        source: Box::new(e),
    })?;

    #[derive(serde::Deserialize)]
    struct Cred {
        name: String,
        pw: String,
    }
    #[derive(serde::Deserialize)]
    struct Db {
        #[serde(rename = "direct_url")]
        cp_builder: ConnectParamsBuilder,
        #[serde(rename = "redirect_url")]
        redirect_cp_builder: ConnectParamsBuilder,
        std: Cred,
        um: Cred,
    }

    let db: Db = serde_json::from_str(&content)
        .map_err(|e| format!("Cannot parse config file {filepath:?}: {e}"))
        .unwrap();

    let (mut cp_builder, mut redirect_cp_builder, std, um) =
        (db.cp_builder, db.redirect_cp_builder, db.std, db.um);
    match purpose {
        "std" => {
            cp_builder.dbuser(&std.name).password(&std.pw);
            redirect_cp_builder.dbuser(&std.name).password(&std.pw);
        }
        "um" => {
            cp_builder.dbuser(&um.name).password(&um.pw);
            redirect_cp_builder.dbuser(&um.name).password(&um.pw);
        }
        _ => panic!("unknown purpose: {purpose}"),
    }
    if let Ok(ref s) = std::env::var("HDBCONNECT_FORCE_TEST_WITH_TLS") {
        match s.as_ref() {
            "DIRECTORY" => {
                cp_builder.tls_with(ServerCerts::Directory(".private/certificates".to_string()));
                redirect_cp_builder
                    .tls_with(ServerCerts::Directory(".private/certificates".to_string()));
            }
            "INSECURE" => {
                cp_builder.tls_without_server_verification();
                redirect_cp_builder.tls_without_server_verification();
            }
            _ => {}
        }
    };

    Ok((cp_builder, redirect_cp_builder))
}
