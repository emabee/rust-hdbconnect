extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, ConnectionConfiguration, HdbError, HdbResult};
use log::{debug, info};
use std::{io::ErrorKind, time::Duration};

const TIMEOUT: Duration = Duration::from_millis(150);
const DROP: &str = "drop table TEST_TIMEOUT";
const CREATE: &str = "create table TEST_TIMEOUT (F1_S NVARCHAR(20), F2_I INT)";
const QUERY: &str = "select * from TEST_TIMEOUT for update";

#[tokio::test] // cargo test --test test_130_timeout -- --nocapture
async fn test_130_timeout() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    // log_handle.parse_new_spec("info, test=debug");

    // open conn1 and conn2, with auto_commit off
    let config = ConnectionConfiguration::default()
        .with_auto_commit(false)
        .with_read_timeout(Some(TIMEOUT));
    let conn1 = test_utils::get_authenticated_connection_with_configuration(&config).await?;
    let conn2 = test_utils::get_authenticated_connection_with_configuration(&config).await?;

    // create DB table with some entries
    prepare_table(&mut log_handle, &conn1).await.unwrap();

    let result = conn1.query(QUERY).await;
    assert!(result.is_ok());

    // use thread1 to retrieve DB lock for that table from conn1 and sleep for 3*TIMEOUT seconds
    let task_1 = tokio::task::spawn(async move {
        tokio::time::sleep(3 * TIMEOUT).await;
        0
    });

    // move conn2 into thread2, try to retrieve same DB lock, assert timeout error
    let task_2 = tokio::task::spawn(async move {
        info!("thread_2: start");
        let result = conn2.query(QUERY).await;
        info!("thread_2: query returned with {result:?}");
        if let Err(HdbError::ConnectionBroken { source }) = result {
            if let HdbError::Io { source: io_error } = source.as_deref().unwrap() {
                assert_eq!(io_error.kind(), ErrorKind::TimedOut);
                return 0;
            }
        }
        -1
    });

    // join both, to ensure both run completely, and verify success
    assert_eq!(0, task_1.await.unwrap());
    assert_eq!(0, task_2.await.unwrap());

    Ok(())
}

async fn prepare_table(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    info!("prepare table");
    connection.multiple_statements_ignore_err(vec![DROP]).await;
    connection.multiple_statements(vec![CREATE]).await?;
    connection.commit().await?;

    let insert_stmt_str = "insert into TEST_TIMEOUT (F1_S, F2_I) values(?, ?)";

    debug!("prepare & execute");
    let mut insert_stmt = connection.prepare(insert_stmt_str).await?;
    insert_stmt.add_batch(&("conn1", 45_i32))?;
    insert_stmt.add_batch(&("conn1", 46_i32))?;
    insert_stmt.execute_batch().await?;
    connection.commit().await?;

    Ok(())
}
