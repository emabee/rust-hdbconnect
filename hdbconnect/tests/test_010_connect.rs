extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{ConnectParams, Connection, HdbResult, IntoConnectParams};
use log::*;
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Instant;

#[test]
fn test_010_connect() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = Instant::now();
    connect_successfully(&mut log_handle).unwrap();
    redirect(&mut log_handle).unwrap();
    connect_options(&mut log_handle).unwrap();
    client_info(&mut log_handle).unwrap();
    connect_wrong_credentials(&mut log_handle);
    connect_and_select_with_explicit_clientlocale(&mut log_handle).unwrap();
    connect_and_select_with_clientlocale_from_env(&mut log_handle).unwrap();
    command_info(&mut log_handle).unwrap();
    info!("Elapsed time: {:?}", Instant::now().duration_since(start));
    Ok(())
}

fn connect_successfully(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("test a successful connection");
    test_utils::get_authenticated_connection()?;
    Ok(())
}

fn redirect(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("test redirect");
    _log_handle
        .parse_and_push_temp_spec("info, hdbconnect::conn= debug, test=debug")
        .unwrap();
    let cpb = test_utils::get_std_redirect_cp_builder()?;
    debug!("Attempting connect to {}", cpb.to_url_without_password());
    let _conn = Connection::new(cpb)?;
    _log_handle.pop_temp_spec();
    Ok(())
}

fn connect_options(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("test connect options");
    let connection = test_utils::get_authenticated_connection()?;
    debug!(
        "Connection options:\n{}",
        connection.dump_connect_options()?
    );
    Ok(())
}

fn client_info(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("client info");
    _log_handle
        .parse_and_push_temp_spec("info, test = debug")
        .unwrap();
    let connection = test_utils::get_authenticated_connection().unwrap();
    let connection_id: u32 = connection.id().unwrap();

    debug!("verify original client info appears in session context");
    let mut prep_stmt = connection.prepare(
        "\
         SELECT KEY, VALUE \
         FROM M_SESSION_CONTEXT \
         WHERE CONNECTION_ID = ? \
         AND (\
         KEY = 'APPLICATION' \
         OR KEY = 'APPLICATIONSOURCE' \
         OR KEY = 'APPLICATIONUSER' \
         OR KEY = 'APPLICATIONVERSION') \
         ORDER BY KEY",
    )?;
    let result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_result_set()?
        .try_into()?;
    check_session_context(true, &result);

    debug!("overwrite the client info, check that it appears in session context");
    connection.set_application("TEST 1 - 2 - 3")?;
    connection.set_application_user("OTTO")?;
    connection.set_application_version("0.8.15")?;
    connection.set_application_source("dummy.rs")?;

    let result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_result_set()?
        .try_into()?;
    check_session_context(false, &result);

    debug!("verify that the updated client info remains set");
    let _result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_result_set()?
        .try_into()?;

    let _result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_result_set()?
        .try_into()?;
    let result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_result_set()?
        .try_into()?;
    check_session_context(false, &result);
    _log_handle.pop_temp_spec();
    Ok(())
}

fn check_session_context(orig: bool, result: &[SessCtx]) {
    if orig {
        assert_eq!(result.len(), 1);
        assert!(result[0].value.starts_with("test_010_connect"));
    } else {
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], SessCtx::new("APPLICATION", "TEST 1 - 2 - 3"));
        assert_eq!(result[1], SessCtx::new("APPLICATIONSOURCE", "dummy.rs"));
        assert_eq!(result[2], SessCtx::new("APPLICATIONUSER", "OTTO"));
        assert_eq!(result[3], SessCtx::new("APPLICATIONVERSION", "0.8.15"));
    }
}

fn connect_wrong_credentials(_log_handle: &mut LoggerHandle) {
    info!("test connect failure on wrong credentials");
    let start = Instant::now();
    let mut cp_builder = test_utils::get_std_cp_builder().unwrap();
    cp_builder.dbuser("didi").password("blabla");
    let conn_params: ConnectParams = cp_builder.into_connect_params().unwrap();
    assert_eq!(conn_params.password().unsecure(), "blabla");

    let err = Connection::new(conn_params).err().unwrap();
    info!(
        "connect with wrong credentials failed as expected, after {} µs with {}.",
        Instant::now().duration_since(start).as_micros(),
        err
    );
}

fn connect_and_select_with_explicit_clientlocale(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("connect and do some simple select with explicit clientlocale");

    let mut cp_builder = test_utils::get_std_cp_builder()?;
    cp_builder.clientlocale("en_US");
    let conn_params: ConnectParams = cp_builder.build()?;
    assert_eq!(conn_params.clientlocale().unwrap(), "en_US");

    let connection = Connection::new(conn_params)?;
    select_version_and_user(&connection)?;
    Ok(())
}

fn connect_and_select_with_clientlocale_from_env(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("connect and do some simple select with clientlocale from env");
    if env::var("LANG").is_err() {
        unsafe {
            env::set_var("LANG", "en_US.UTF-8");
        }
    }

    let mut cp_builder = test_utils::get_std_cp_builder()?;
    cp_builder.clientlocale_from_env_lang();
    let conn_params: ConnectParams = cp_builder.build()?;
    assert!(conn_params.clientlocale().is_some());

    let connection = Connection::new(conn_params)?;
    select_version_and_user(&connection)?;
    Ok(())
}

fn select_version_and_user(connection: &Connection) -> HdbResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = r#"SELECT VERSION as "version", CURRENT_USER as "current_user" FROM SYS.M_DATABASE"#;
    debug!("calling connection.query(SELECT VERSION as ...)");
    let result_set = connection.query(stmt)?;
    let version_and_user: VersionAndUser = result_set.try_into()?;
    let conn_params: ConnectParams = test_utils::get_std_cp_builder()?.into_connect_params()?;
    assert_eq!(
        version_and_user.current_user,
        conn_params.dbuser().to_uppercase()
    );

    debug!("VersionAndUser: {version_and_user:?}");
    Ok(())
}

#[derive(Eq, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct SessCtx {
    key: String,
    value: String,
}
impl SessCtx {
    fn new(key: &str, value: &str) -> SessCtx {
        SessCtx {
            key: key.to_string(),
            value: value.to_string(),
        }
    }
}

fn command_info(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("command info");
    let connection = test_utils::get_authenticated_connection().unwrap();

    let stmt = r#"SELECT KEY, VALUE FROM M_SESSION_CONTEXT ORDER BY KEY"#;

    let _result: Vec<SessCtx> = connection
        .execute_with_debuginfo(stmt, "BLABLA", 4711)?
        .into_result_set()?
        .try_into()?;

    let stmt = r#"SELECT KEY, NONSENSE FROM M_SESSION_CONTEXT ORDER BY KEY"#;

    assert!(
        connection
            .execute_with_debuginfo(stmt, "BLABLA", 4711)
            .is_err()
    );

    Ok(())
}
