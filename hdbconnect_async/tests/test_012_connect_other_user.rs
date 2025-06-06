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
    let sys_conn = test_utils::get_um_connection().await.unwrap();

    sys_conn
        .multiple_statements_ignore_err(vec![
            &format!("drop user {other_user}"),
            &format!(
                "create user {other_user} password \"Theother1234\" NO FORCE_FIRST_PASSWORD_CHANGE",
            ),
        ])
        .await;

    let before: String = sys_conn
        .query("SELECT CURRENT_USER FROM DUMMY")
        .await?
        .try_into()
        .await?;

    let response = sys_conn
        .statement(format!("CONNECT {other_user} PASSWORD Theother1234"))
        .await?;
    debug!("Response: {response:?}");

    let after: String = sys_conn
        .query("SELECT CURRENT_USER FROM DUMMY")
        .await?
        .try_into()
        .await?;
    assert_eq!(
        after,
        "THEOTHERONE".to_string(),
        "before we had {before}, now we should have THEOTHERONE"
    );
    Ok(())
}
