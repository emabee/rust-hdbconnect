extern crate chrono;
extern crate dist_tx;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate rand;
extern crate serde_json;

mod test_utils;

#[allow(unused_imports)]
use flexi_logger::{LogSpecification, ReconfigurationHandle};
use hdbconnect::{Connection, HdbResult};
use dist_tx::tm::*;

#[test] // cargo test --test test_035_xa_transactions -- --nocapture
pub fn test_035_xa_transactions() {
    let mut log_handle = test_utils::init_logger("info");

    match impl_test_035_xa_transactions(&mut log_handle) {
        Err(e) => {
            error!("impl_test_035_xa_transactions() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => debug!("impl_test_035_xa_transactions() ended successful"),
    }
}

fn impl_test_035_xa_transactions(log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    let mut connection = test_utils::get_authenticated_connection()?;
    prepare(&mut connection, log_handle)?;
    successful_xa(&mut connection, log_handle)?;
    debug!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

// prepare the db table
fn prepare(conn: &mut Connection, _log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("Prepare...");
    test_utils::statement_ignore_err(conn, vec!["drop table TEST_XA"]);
    conn.multiple_statements(vec![
        "create column table TEST_XA (f1 INT primary key, f2 NVARCHAR(20))",
        "insert into TEST_XA (f1, f2) values(-100, 'INITIAL')",
        "insert into TEST_XA (f1, f2) values(-101, 'INITIAL')",
        "insert into TEST_XA (f1, f2) values(-102, 'INITIAL')",
        "insert into TEST_XA (f1, f2) values(-103, 'INITIAL')",
    ])
}

// a) successful XA
fn successful_xa(conn: &mut Connection, _log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    //_log_handle.set_new_spec(LogSpecification::parse("debug"));
    info!("Successful XA");

    // open two connections, auto_commit off
    let mut conn_a = conn.spawn()?;
    conn_a.set_auto_commit(false)?;
    let mut conn_b = conn_a.spawn()?;
    assert!(!conn_a.is_auto_commit()?);
    assert!(!conn_b.is_auto_commit()?);

    // instantiate a SimpleTransactionManager and register Resource Managers for the two connections
    _log_handle.set_new_spec(LogSpecification::parse(
        "info, dist_tx::tm::simple_transaction_manager = trace",
    ));
    let mut tm = SimpleTransactionManager::new("test_035_xa_transactions".to_owned());
    tm.register(conn_a.get_resource_manager(), 22, true)
        .unwrap();
    tm.register(conn_b.get_resource_manager(), 44, true)
        .unwrap();

    // start ta
    tm.start_transaction().unwrap();

    // do some inserts
    conn_a.dml(&insert_stmt(1, "a"))?;
    conn_b.dml(&insert_stmt(2, "b"))?;

    // verify with neutral conn that nothing is visible (count)
    let count: u32 = conn.query("select count(*) from TEST_XA")?.try_into()?;
    assert_eq!(count, 4 /* + 0 */);

    // verify that ta is not listed in server
    // tbd

    // commit ta
    tm.commit_transaction().unwrap();

    // verify that stuff is now visible
    let count: u32 = conn.query("select count(*) from TEST_XA")?.try_into()?;
    assert_eq!(count, 4 + 2);

    // verify that ta is not known anymore
    // tbd


    // add test for suspend/resume??
    // add test for join
    // add test for forget

    Ok(())
}

fn insert_stmt(i: u32, s: &'static str) -> String {
    format!("insert into TEST_XA (f1, f2) values({}, '{}')", i, s)
}

// b) failing XA
// use conn1 to insert a row
// use conn2 to insert the same row
// XA_prepare on both, should NOT be successful
// XA_rollback on both, should be successful
// verify with conn3 that the new line was not inserted
