mod test_utils;

use chrono::Local;
use flexi_logger::ReconfigurationHandle;
use hdbconnect::{ConnectParams, Connection, HdbResult, IntoConnectParams};
use log::*;
use serde_derive::{Deserialize, Serialize};
use std::env;

#[test]
pub fn test_010_connect() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    connect_successfully(&mut log_handle);
    connect_options(&mut log_handle)?;
    connect_wrong_password(&mut log_handle);
    connect_and_select_with_explicit_clientlocale(&mut log_handle)?;
    connect_and_select_with_clientlocale_from_env(&mut log_handle)?;
    client_info(&mut log_handle)?;
    command_info(&mut log_handle)?;
    Ok(())
}

fn connect_successfully(_log_handle: &mut ReconfigurationHandle) {
    info!("test a successful connection");
    test_utils::get_authenticated_connection().unwrap();
}

fn connect_options(_log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("test a successful connection");
    _log_handle.parse_and_push_temp_spec("info, test = debug");
    let connection = test_utils::get_authenticated_connection()?;
    let db_name = connection.get_database_name()?.unwrap();
    let system_id = connection.get_system_id()?.unwrap();
    let version = connection.get_full_version_string()?.unwrap();
    debug!(
        "db_name: {}, system_id: {}, version: {}",
        db_name, system_id, version
    );

    debug!(
        "Connection options: \n{}",
        connection.dump_connect_options()?
    );
    _log_handle.pop_temp_spec();
    Ok(())
}

fn connect_wrong_password(_log_handle: &mut ReconfigurationHandle) {
    info!("test connect failure on wrong credentials");
    let start = Local::now();
    let conn_params: ConnectParams =
        test_utils::get_wrong_connect_params(None, Some("blabla")).unwrap();
    assert_eq!(conn_params.password().unsecure(), b"blabla");

    let err = Connection::new(conn_params).err().unwrap();
    info!(
        "connect with wrong password failed as expected, after {} µs with {}.",
        Local::now()
            .signed_duration_since(start)
            .num_microseconds()
            .unwrap(),
        err
    );
}

fn connect_and_select_with_explicit_clientlocale(
    _log_handle: &mut ReconfigurationHandle,
) -> HdbResult<()> {
    info!("connect and do some simple select with explicit clientlocale");

    let mut url = test_utils::get_std_connect_url()?;
    url.push_str("?client_locale=en_US");
    let conn_params = url.into_connect_params()?;
    assert_eq!(conn_params.clientlocale().unwrap(), "en_US");

    let mut connection = Connection::new(conn_params)?;
    select_version_and_user(&mut connection)?;
    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn connect_and_select_with_clientlocale_from_env(
    _log_handle: &mut ReconfigurationHandle,
) -> HdbResult<()> {
    info!("connect and do some simple select with clientlocale from env");
    if env::var("LANG").is_err() {
        env::set_var("LANG", "en_US.UTF-8");
    }

    let mut url = test_utils::get_std_connect_url()?;
    url.push_str("?client_locale_from_env=1");
    let conn_params: ConnectParams = url.into_connect_params()?;
    assert!(conn_params.clientlocale().is_some());

    let mut connection = Connection::new(conn_params)?;
    select_version_and_user(&mut connection)?;
    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn select_version_and_user(connection: &mut Connection) -> HdbResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = r#"SELECT VERSION as "version", CURRENT_USER as "current_user" FROM SYS.M_DATABASE"#;
    debug!("calling connection.query(SELECT VERSION as ...)");
    let resultset = connection.query(stmt)?;
    let version_and_user: VersionAndUser = resultset.try_into()?;
    assert_eq!(
        &version_and_user.current_user,
        test_utils::get_std_connect_params()?.dbuser()
    );

    debug!("VersionAndUser: {:?}", version_and_user);
    Ok(())
}

#[allow(non_snake_case)]
#[derive(Eq, PartialEq, Serialize, Deserialize, Debug)]
struct SessCtx {
    KEY: String,
    VALUE: String,
}
impl SessCtx {
    fn new(key: &str, value: &str) -> SessCtx {
        SessCtx {
            KEY: key.to_string(),
            VALUE: value.to_string(),
        }
    }
}

fn client_info(_log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("client info");
    let mut connection = test_utils::get_authenticated_connection().unwrap();
    let connection_id: i32 = connection.id()?;

    debug!("make sure the client info is set ...");
    connection.set_application_user("OTTO")?;
    connection.set_application_version("0.8.15")?;
    connection.set_application_source("dummy.rs")?;

    let mut prep_stmt = connection.prepare(
        "\
         SELECT KEY, VALUE \
         FROM M_SESSION_CONTEXT \
         WHERE CONNECTION_ID = ? \
         AND (\
         KEY = 'APPLICATIONSOURCE' \
         OR KEY = 'APPLICATIONUSER' \
         OR KEY = 'APPLICATIONVERSION') \
         ORDER BY KEY",
    )?;
    let result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_resultset()?
        .try_into()?;
    check_result(&result);

    debug!("... and remains set");
    let _result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_resultset()?
        .try_into()?;

    let _result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_resultset()?
        .try_into()?;
    let result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)?
        .into_resultset()?
        .try_into()?;
    check_result(&result);

    Ok(())
}

fn check_result(result: &[SessCtx]) {
    assert_eq!(result[0], SessCtx::new("APPLICATIONSOURCE", "dummy.rs"));
    assert_eq!(result[1], SessCtx::new("APPLICATIONUSER", "OTTO"));
    assert_eq!(result[2], SessCtx::new("APPLICATIONVERSION", "0.8.15"));
}

fn command_info(_log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("command info");
    let mut connection = test_utils::get_authenticated_connection().unwrap();

    let stmt = r#"SELECT KEY, VALUE FROM M_SESSION_CONTEXT ORDER BY KEY"#;

    let _result: Vec<SessCtx> = connection
        .execute_with_debuginfo(stmt, "BLABLA", 4711)?
        .into_resultset()?
        .try_into()?;

    let stmt = r#"SELECT KEY, NONSENSE FROM M_SESSION_CONTEXT ORDER BY KEY"#;

    assert!(connection
        .execute_with_debuginfo(stmt, "BLABLA", 4711)
        .is_err());

    Ok(())
}
