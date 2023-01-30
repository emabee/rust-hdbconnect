extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use std::thread;
use std::time::Duration;

#[tokio::test] // cargo test --test test_016_selectforupdate -- --nocapture
pub async fn test_016_selectforupdate() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let mut connection = test_utils::get_authenticated_connection().await?;

    prepare(&mut log_handle, &mut connection).await?;
    produce_conflicts(&mut log_handle, &mut connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn prepare(_log_handle: &mut LoggerHandle, connection: &mut Connection) -> HdbResult<()> {
    log::info!("prepare");
    log::debug!("prepare the db table");
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_SELFORUPDATE"])
        .await;
    let stmts = vec![
        "create table TEST_SELFORUPDATE ( f1_s NVARCHAR(100) primary key, f2_i INT, f3_i \
         INT not null, f4_dt LONGDATE)",
        "insert into TEST_SELFORUPDATE (f1_s, f2_i, f3_i, f4_dt) values('Hello', null, \
         1,'01.01.1900')",
        "insert into TEST_SELFORUPDATE (f1_s, f2_i, f3_i, f4_dt) values('world!', null, \
         20,'01.01.1901')",
        "insert into TEST_SELFORUPDATE (f1_s, f2_i, f3_i, f4_dt) values('I am here.', \
         null, 300,'01.01.1902')",
    ];
    connection.multiple_statements(stmts).await?;

    log::debug!("insert some mass data");
    for i in 100..200 {
        connection
            .dml(format!(
                "insert into TEST_SELFORUPDATE (f1_s, f2_i, f3_i, \
             f4_dt) values('{i}', {i}, {i},'01.01.1900')",
            ))
            .await?;
    }
    Ok(())
}

async fn produce_conflicts(
    _log_handle: &mut LoggerHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    log::info!("verify that locking with 'select for update' works");
    connection.set_auto_commit(false).await?;

    log::debug!("get two more connections");
    let mut connection2 = connection.spawn().await?;
    let mut connection3 = connection.spawn().await?;

    log::debug!("conn1: select * for update");
    connection
        .query("select * from TEST_SELFORUPDATE where F1_S = 'Hello' for update")
        .await?;

    log::debug!("try conflicting update with second connection");
    tokio::spawn(async move {
        {
            log::debug!("conn2: select * for update");
            connection2
                .query("select * from TEST_SELFORUPDATE where F1_S = 'Hello' for update")
                .await
                .unwrap();
            connection2
                .dml("update TEST_SELFORUPDATE set F2_I = 2 where F1_S = 'Hello'")
                .await
                .unwrap();
            connection2.commit().await.unwrap();
        }
    });

    log::debug!("do new update with first connection");
    connection
        .dml("update TEST_SELFORUPDATE set F2_I = 1 where F1_S = 'Hello'")
        .await?;

    let i: i32 = connection
        .query("select F2_I from TEST_SELFORUPDATE where F1_S = 'Hello'")
        .await?
        .try_into()
        .await?;
    assert_eq!(i, 1);

    log::debug!("commit the change of the first connection");
    connection.commit().await?;

    log::debug!(
        "verify the change of the second connection is visible (because the other thread \
            had to wait with its update until the first was committed"
    );

    let mut val: i32 = 0;
    for i in 1..=5 {
        thread::sleep(Duration::from_millis(i * 200));
        val = connection3
            .query("select F2_I from TEST_SELFORUPDATE where F1_S = 'Hello'")
            .await?
            .try_into()
            .await?;
        if val == 2 {
            break;
        }
        log::warn!("Repeating test, waiting even longer");
    }
    assert_eq!(val, 2);

    Ok(())
}
