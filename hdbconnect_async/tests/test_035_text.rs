extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info};

// cargo test test_035_text -- --nocapture
#[tokio::test]
async fn test_035_text() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    if !prepare_test(&connection).await {
        info!("TEST ABANDONED since database does not support TEXT columns");
        return Ok(());
    }

    test_text(&mut log_handle, &connection).await?;
    test_text_bug_issue_60(&mut log_handle, &connection).await?;
    test_utils::closing_info(connection, start).await
}

async fn prepare_test(connection: &Connection) -> bool {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_TEXT"])
        .await;
    let stmts = vec!["create table TEST_TEXT (chardata TEXT, chardata_nn TEXT NOT NULL)"];
    connection.multiple_statements(stmts).await.is_ok() // in HANA Cloud we get sql syntax error: incorrect syntax near "TEXT"
}

async fn test_text(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    info!("create a TEXT in the database, and read it");
    debug!("setup...");
    connection.set_lob_read_length(1_000_000).await;

    let test_text = "blablaいっぱいおでぶ𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀cesu-8𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀";

    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_TEXT (chardata, chardata_nn) values (?,?)")
        .await?;
    debug!("execute...");
    insert_stmt.execute(&(test_text, test_text)).await?;

    debug!("query...");
    let result_set = connection
        .query("select chardata, chardata_nn FROM TEST_TEXT")
        .await?;
    debug!("deserialize...");
    let ret_text: (Option<String>, String) = result_set.try_into().await?;
    assert_eq!(test_text, ret_text.0.expect("expected string but got None"));
    assert_eq!(test_text, ret_text.1);

    debug!("Also test NULL values");
    let none: Option<&str> = None;
    insert_stmt.add_batch(&(none, test_text))?;
    insert_stmt.execute_batch().await?;
    let ret_text: (Option<String>, String) = connection
        .query("select chardata, chardata_nn FROM TEST_TEXT WHERE chardata IS NULL")
        .await?
        .try_into()
        .await?;
    assert_eq!(None, ret_text.0);
    assert_eq!(test_text, ret_text.1);

    Ok(())
}

async fn test_text_bug_issue_60(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("test_text_bug_issue_60");
    /*this bytes length is 33000 is larger than the default 32768,i16:MAX. currently the function  binary_length
    in hdb_value.rs file will add 5 to calculate the length, so the length will be 33005, that is wrong.it will happen an
    an error ConnectionBroken "{ source: Some(Io { source: Os { code: 104, kind: ConnectionReset, message: "Connection reset by peer" } }) }“
    because of the wrong length calculation. The correct  calculation should be if length < 65535, then length is add 3.
    so we need to change  const MAX_2_BYTE_LENGTH: i16 = i16::MAX;32768 to const MAX_2_BYTE_LENGTH: u16 = u16::MAX;65535 in file length_indicator.rs
    */
    let large_text = "这是一段非常大的文本...".repeat(1000);
    // info!("large_text: {}", large_text.len());
    let test_text = large_text.as_str();
    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_TEXT (chardata, chardata_nn) values (?,?)")
        .await?;
    debug!("execute...");
    insert_stmt.execute(&(test_text, test_text)).await?;

    Ok(())
}
