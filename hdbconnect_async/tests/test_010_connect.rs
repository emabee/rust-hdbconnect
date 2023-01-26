extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{ConnectParams, Connection, HdbResult, IntoConnectParams};
use log::*;
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Instant;

#[tokio::test]
async fn test_010_connect() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = Instant::now();
    connect_successfully(&mut log_handle).await.unwrap();
    reconnect(&mut log_handle).await.unwrap();
    connect_options(&mut log_handle).await.unwrap();
    client_info(&mut log_handle).await.unwrap();
    connect_wrong_credentials(&mut log_handle).await;
    connect_and_select_with_explicit_clientlocale(&mut log_handle)
        .await
        .unwrap();
    connect_and_select_with_clientlocale_from_env(&mut log_handle)
        .await
        .unwrap();
    command_info(&mut log_handle).await.unwrap();
    info!("Elapsed time: {:?}", Instant::now().duration_since(start));
    Ok(())
}

async fn connect_successfully(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("test a successful connection");
    test_utils::get_authenticated_connection().await?;
    Ok(())
}

async fn reconnect(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("test reconnect");
    _log_handle
        .parse_and_push_temp_spec("info, hdbconnect::conn= debug, test=debug")
        .unwrap();
    let cpb = test_utils::get_std_redirect_cp_builder()?;
    debug!("Attempting connect to {}", cpb.to_url()?);
    let _conn = Connection::new(cpb).await?;
    _log_handle.pop_temp_spec();
    Ok(())
}

async fn connect_options(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("test connect options");
    let connection = test_utils::get_authenticated_connection().await?;

    debug!(
        "Connection options:\n{}",
        connection.dump_connect_options().await?
    );
    Ok(())
}

async fn client_info(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("client info");
    _log_handle
        .parse_and_push_temp_spec("info, test = debug")
        .unwrap();
    let mut connection = test_utils::get_authenticated_connection().await.unwrap();
    let connection_id: i32 = connection.id().await?;

    debug!("verify original client info appears in session context");
    let mut prep_stmt = connection
        .prepare(
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
        )
        .await?;
    let result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)
        .await?
        .into_aresultset()?
        .try_into()
        .await?;
    check_session_context(true, &result);

    debug!("overwrite the client info, check that it appears in session context");
    connection.set_application("TEST 1 - 2 - 3").await?;
    connection.set_application_user("OTTO").await?;
    connection.set_application_version("0.8.15").await?;
    connection.set_application_source("dummy.rs").await?;

    let result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)
        .await?
        .into_aresultset()?
        .try_into()
        .await?;
    check_session_context(false, &result);

    debug!("verify that the updated client info remains set");
    let _result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)
        .await?
        .into_aresultset()?
        .try_into()
        .await?;

    let _result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)
        .await?
        .into_aresultset()?
        .try_into()
        .await?;
    let result: Vec<SessCtx> = prep_stmt
        .execute(&connection_id)
        .await?
        .into_aresultset()?
        .try_into()
        .await?;
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

async fn connect_wrong_credentials(_log_handle: &mut LoggerHandle) {
    info!("test connect failure on wrong credentials");
    let start = Instant::now();
    let mut cp_builder = test_utils::get_std_cp_builder().unwrap();
    cp_builder.dbuser("didi").password("blabla");
    let conn_params: ConnectParams = cp_builder.into_connect_params().unwrap();
    assert_eq!(conn_params.password().unsecure(), "blabla");

    let err = Connection::new(conn_params).await.err().unwrap();
    info!(
        "connect with wrong credentials failed as expected, after {} µs with {}.",
        Instant::now().duration_since(start).as_micros(),
        err
    );
}

async fn connect_and_select_with_explicit_clientlocale(
    _log_handle: &mut LoggerHandle,
) -> HdbResult<()> {
    info!("connect and do some simple select with explicit clientlocale");

    let mut cp_builder = test_utils::get_std_cp_builder()?;
    cp_builder.clientlocale("en_US");
    let conn_params: ConnectParams = cp_builder.build()?;
    assert_eq!(conn_params.clientlocale().unwrap(), "en_US");

    let mut connection = Connection::new(conn_params).await?;
    select_version_and_user(&mut connection).await?;
    Ok(())
}

async fn connect_and_select_with_clientlocale_from_env(
    _log_handle: &mut LoggerHandle,
) -> HdbResult<()> {
    info!("connect and do some simple select with clientlocale from env");
    if env::var("LANG").is_err() {
        env::set_var("LANG", "en_US.UTF-8");
    }

    let mut cp_builder = test_utils::get_std_cp_builder()?;
    cp_builder.clientlocale_from_env_lang();
    let conn_params: ConnectParams = cp_builder.build()?;
    assert!(conn_params.clientlocale().is_some());

    let mut connection = Connection::new(conn_params).await?;
    select_version_and_user(&mut connection).await?;
    Ok(())
}

async fn select_version_and_user(connection: &mut Connection) -> HdbResult<()> {
    #[derive(Serialize, Deserialize, Debug)]
    struct VersionAndUser {
        version: Option<String>,
        current_user: String,
    }

    let stmt = r#"SELECT VERSION as "version", CURRENT_USER as "current_user" FROM SYS.M_DATABASE"#;
    debug!("calling connection.query(SELECT VERSION as ...)");
    let resultset = connection.query(stmt).await?;
    let version_and_user: VersionAndUser = resultset.try_into().await?;
    let conn_params: ConnectParams = test_utils::get_std_cp_builder()?.into_connect_params()?;
    assert_eq!(
        version_and_user.current_user,
        conn_params.dbuser().to_uppercase()
    );

    debug!("VersionAndUser: {:?}", version_and_user);
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

async fn command_info(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    info!("command info");
    let mut connection = test_utils::get_authenticated_connection().await.unwrap();

    let stmt = r#"SELECT KEY, VALUE FROM M_SESSION_CONTEXT ORDER BY KEY"#;

    let _result: Vec<SessCtx> = connection
        .execute_with_debuginfo(stmt, "BLABLA", 4711)
        .await?
        .into_aresultset()?
        .try_into()
        .await?;

    let stmt = r#"SELECT KEY, NONSENSE FROM M_SESSION_CONTEXT ORDER BY KEY"#;

    assert!(connection
        .execute_with_debuginfo(stmt, "BLABLA", 4711)
        .await
        .is_err());

    Ok(())
}
