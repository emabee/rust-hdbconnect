extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbResult};
use log::{debug, info};

#[tokio::test]
async fn test_070_explain() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    test_explain(&mut log_handle, &connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn test_explain(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    info!("use EXPLAIN and verify it works");

    let result = connection
        .dml("DELETE FROM explain_plan_table WHERE statement_name = 'test_explain'")
        .await?;
    debug!("cleanup (deletion result = {result:?})");

    let count: usize = connection
        .query("select count(*) from EXPLAIN_PLAN_TABLE")
        .await?
        .try_into()
        .await?;
    assert_eq!(count, 0);

    debug!("create the plan");
    connection
        .exec("EXPLAIN PLAN SET STATEMENT_NAME = 'test_explain' FOR select 'FOO' from dummy")
        .await?;

    let count: u32 = connection
        .query("select count(*) from EXPLAIN_PLAN_TABLE")
        .await?
        .try_into()
        .await?;
    debug!("read the plan size (no of lines = {count})");
    assert!(count > 0);

    let result: Vec<(String, String)> = connection
        .query(
            "SELECT Operator_Name, Operator_ID \
             FROM explain_plan_table \
             WHERE statement_name = 'test_explain';",
        )
        .await?
        .try_into()
        .await?;
    debug!("obtain the plan: {result:?}");

    Ok(())
}
