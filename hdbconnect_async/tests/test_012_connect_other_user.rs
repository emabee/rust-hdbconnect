extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::HdbResult;
use log::*;
use std::time::Instant;

#[tokio::test]
async fn test_012_connect_other_user() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = Instant::now();
    connect_other_user(&mut log_handle).await?;
    info!("Elapsed time: {:?}", Instant::now().duration_since(start));
    Ok(())
}

async fn connect_other_user(_log_handle: &mut LoggerHandle) -> HdbResult<()> {
    // _log_handle.parse_and_push_temp_spec("test = debug, info");

    let other_user = "THEOTHERONE".to_string();
    let mut sys_conn = test_utils::get_um_connection().await.unwrap();

    sys_conn
        .multiple_statements_ignore_err(vec![
            &format!("drop user {}", other_user),
            &format!(
                "create user {} password \"Theother1234\" NO FORCE_FIRST_PASSWORD_CHANGE",
                other_user
            ),
        ])
        .await;

    let before: String = sys_conn
        .query("SELECT CURRENT_USER FROM DUMMY")
        .await?
        .async_try_into()
        .await?;
    assert_eq!(before, "SYSTEM".to_string());

    let response = sys_conn
        .statement(format!("CONNECT {} PASSWORD Theother1234", other_user))
        .await?;
    debug!("Response: {:?}", response);

    let after: String = sys_conn
        .query("SELECT CURRENT_USER FROM DUMMY")
        .await?
        .async_try_into()
        .await?;
    assert_eq!(after, "THEOTHERONE".to_string());

    // _log_handle.pop_temp_spec();
    Ok(())
}
