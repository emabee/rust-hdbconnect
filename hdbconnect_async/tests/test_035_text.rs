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
    let mut connection = test_utils::get_authenticated_connection().await?;

    if !prepare_test(&mut connection).await {
        info!("TEST ABANDONED since database does not support TEXT columns");
        return Ok(());
    }

    test_text(&mut log_handle, &mut connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn prepare_test(connection: &mut Connection) -> bool {
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_TEXT"])
        .await;
    let stmts = vec!["create table TEST_TEXT (chardata TEXT, chardata_nn TEXT NOT NULL)"];
    connection.multiple_statements(stmts).await.is_ok() // in HANA Cloud we get sql syntax error: incorrect syntax near "TEXT"
}

async fn test_text(_log_handle: &mut LoggerHandle, connection: &mut Connection) -> HdbResult<()> {
    info!("create a TEXT in the database, and read it");
    debug!("setup...");
    connection.set_lob_read_length(1_000_000).await?;

    let test_text = "blablaいっぱいおでぶ𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀cesu-8𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀𐐀";

    debug!("prepare...");
    let mut insert_stmt = connection
        .prepare("insert into TEST_TEXT (chardata, chardata_nn) values (?,?)")
        .await?;
    debug!("execute...");
    insert_stmt.execute(&(test_text, test_text)).await?;

    debug!("query...");
    let resultset = connection
        .query("select chardata, chardata_nn FROM TEST_TEXT")
        .await?;
    debug!("deserialize...");
    let ret_text: (Option<String>, String) = resultset.try_into().await?;
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