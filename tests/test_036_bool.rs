mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::{Connection, HdbResult};
use log::{debug, info};

// cargo test test_036_bool -- --nocapture
#[test]
pub fn test_036_bool() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let mut connection = test_utils::get_authenticated_connection()?;

    test_text(&mut log_handle, &mut connection)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

fn test_text(
    _logger_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("create a bool in the database, and read it");

    debug!("setup...");

    connection.multiple_statements_ignore_err(vec!["drop table TEST_BOOL"]);
    let stmts = vec![
        "create table TEST_BOOL ( \
         ob0 BOOLEAN, ob1 BOOLEAN, ob2 BOOLEAN, b3 BOOLEAN NOT NULL, b4 BOOLEAN NOT NULL \
         )",
    ];
    connection.multiple_statements(stmts)?;

    let mut insert_stmt =
        connection.prepare("insert into TEST_BOOL (ob0, ob1, ob2, b3, b4) values (?,?,?,?,?)")?;

    debug!("trying add batch");
    let none: Option<bool> = None;
    insert_stmt.add_batch(&(true, false, none, true, false))?;

    debug!("trying execute_batch");
    insert_stmt.execute_batch()?;

    debug!("trying query");
    let resultset = connection.query("select * FROM TEST_BOOL")?;
    debug!("trying deserialize result set: {:?}", resultset);
    let tuple: (Option<bool>, Option<bool>, Option<bool>, bool, bool) = resultset.try_into()?;
    assert_eq!(Some(true), tuple.0);
    assert_eq!(Some(false), tuple.1);
    assert_eq!(None, tuple.2);
    assert_eq!(true, tuple.3);
    assert_eq!(false, tuple.4);

    Ok(())
}
