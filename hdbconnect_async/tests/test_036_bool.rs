extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info};

// cargo test test_036_bool -- --nocapture
#[tokio::test]
async fn test_036_bool() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    test_text(&mut log_handle, &connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn test_text(_logger_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    info!("create a bool in the database, and read it");

    debug!("setup...");

    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_BOOL"])
        .await;
    let stmts = vec![
        "create table TEST_BOOL ( \
         ob0 BOOLEAN, ob1 BOOLEAN, ob2 BOOLEAN, b3 BOOLEAN NOT NULL, b4 BOOLEAN NOT NULL \
         )",
    ];
    connection.multiple_statements(stmts).await?;

    let mut insert_stmt = connection
        .prepare("insert into TEST_BOOL (ob0, ob1, ob2, b3, b4) values (?,?,?,?,?)")
        .await?;

    debug!("trying add batch");
    let none: Option<bool> = None;
    insert_stmt.add_batch(&(true, false, none, true, false))?;

    debug!("trying execute_batch");
    insert_stmt.execute_batch().await?;

    debug!("trying query");
    let result_set = connection.query("select * FROM TEST_BOOL").await?;
    debug!("trying deserialize result set: {result_set:?}");
    let tuple: (Option<bool>, Option<bool>, Option<bool>, bool, bool) =
        result_set.try_into().await?;
    assert_eq!(Some(true), tuple.0);
    assert_eq!(Some(false), tuple.1);
    assert_eq!(None, tuple.2);
    assert!(tuple.3);
    assert!(!tuple.4);

    Ok(())
}
