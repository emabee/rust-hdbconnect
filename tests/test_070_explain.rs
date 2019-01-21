mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::{Connection, HdbResult};
use log::{debug, info};

#[test]
pub fn test_070_explain() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let mut connection = test_utils::get_authenticated_connection()?;

    test_explain(&mut log_handle, &mut connection)?;
    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn test_explain(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("use EXPLAIN and verify it works");

    let result =
        connection.dml("DELETE FROM explain_plan_table WHERE statement_name = 'test_explain'")?;
    debug!("cleanup (deletion result = {:?})", result);

    let count: usize = connection
        .query("select count(*) from EXPLAIN_PLAN_TABLE")?
        .try_into()?;
    assert_eq!(count, 0);

    debug!("create the plan");
    connection
        .exec("EXPLAIN PLAN SET STATEMENT_NAME = 'test_explain' FOR select 'FOO' from dummy")?;

    let count: u32 = connection
        .query("select count(*) from EXPLAIN_PLAN_TABLE")?
        .try_into()?;
    debug!("read the plan size (no of lines = {})", count);
    assert!(count > 0);

    let result: Vec<(String, String)> = connection
        .query(
            "SELECT Operator_Name, Operator_ID \
             FROM explain_plan_table \
             WHERE statement_name = 'test_explain';",
        )?
        .try_into()?;
    debug!("obtain the plan: {:?}", result);

    Ok(())
}
