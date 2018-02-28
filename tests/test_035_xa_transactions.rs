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
use flexi_logger::ReconfigurationHandle;
use hdbconnect::{Connection, HdbResult};
use dist_tx::tm::*;

#[test] // cargo test --test test_035_xa_transactions -- --nocapture
pub fn test_035_xa_transactions() {
    let mut log_handle = test_utils::init_logger("info");

    match impl_test_035_xa_transactions(&mut log_handle) {
        Err(e) => panic!("impl_test_035_xa_transactions() failed with {:?}", e),
        Ok(_) => debug!("impl_test_035_xa_transactions() ended successful"),
    }
}

fn impl_test_035_xa_transactions(log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    let mut connection = test_utils::get_authenticated_connection()?;

    prepare(&mut connection, log_handle)?;

    successful_xa(&mut connection, log_handle)?;
    xa_rollback(&mut connection, log_handle)?;
    xa_repeated(&mut connection, log_handle)?;
    xa_conflicts(&mut connection, log_handle)?;

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

fn successful_xa(conn: &mut Connection, _log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("Successful XA");

    // open two connections, auto_commit off
    let mut conn_a = conn.spawn()?;
    conn_a.set_auto_commit(false)?;
    let mut conn_b = conn_a.spawn()?;
    assert!(!conn_a.is_auto_commit()?);
    assert!(!conn_b.is_auto_commit()?);

    // instantiate a SimpleTransactionManager and register Resource Managers for
    // the two connections
    // log_handle.parse_new_spec("debug,
    // dist_tx::tm::simple_transaction_manager=trace");
    let mut tm = SimpleTransactionManager::new("test_035_xa_transactions".to_owned());
    tm.register(conn_a.get_resource_manager(), 22, true)
        .unwrap();
    tm.register(conn_b.get_resource_manager(), 44, true)
        .unwrap();

    // start ta
    tm.start_transaction().unwrap();

    debug!("do some inserts");
    conn_a.dml(&insert_stmt(1, "a"))?;
    conn_b.dml(&insert_stmt(2, "b"))?;

    debug!("verify with neutral conn that nothing is visible (count)");
    let count_query = "select count(*) from TEST_XA where f1 > 0 and f1 < 9";
    let count: u32 = conn.query(count_query)?.try_into()?;
    assert_eq!(0, count);

    debug!("commit ta");
    tm.commit_transaction().unwrap();

    debug!("verify that stuff is now visible");
    let count: u32 = conn.query(count_query)?.try_into()?;
    assert_eq!(2, count);

    Ok(())
}

fn insert_stmt(i: u32, s: &'static str) -> String {
    format!("insert into TEST_XA (f1, f2) values({}, '{}')", i, s)
}

fn xa_rollback(conn: &mut Connection, _log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("xa_rollback");

    // open two connections, auto_commit off
    let mut conn_a = conn.spawn()?;
    conn_a.set_auto_commit(false)?;
    let mut conn_b = conn_a.spawn()?;
    assert!(!conn_a.is_auto_commit()?);
    assert!(!conn_b.is_auto_commit()?);
    let mut conn_c = conn.spawn()?;

    conn_a
        .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
        .unwrap(); // (milliseconds)
    conn_b
        .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
        .unwrap(); // (milliseconds)
    conn_c
        .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
        .unwrap(); // (milliseconds)

    // instantiate a SimpleTransactionManager and register Resource Managers for
    // the two connections
    // log_handle.parse_new_spec("debug, dist_tx::tm::simple_transaction_manager =
    // trace");
    let mut tm = SimpleTransactionManager::new("test_035_xa_transactions".to_owned());
    tm.register(conn_a.get_resource_manager(), 22, true)
        .unwrap();
    tm.register(conn_b.get_resource_manager(), 44, true)
        .unwrap();

    // start ta
    tm.start_transaction().unwrap();

    debug!("conn_a inserts");
    conn_a.dml(&insert_stmt(10, "a"))?;
    conn_a.dml(&insert_stmt(11, "a"))?;
    debug!("conn_b inserts");
    conn_b.dml(&insert_stmt(12, "b"))?;
    conn_b.dml(&insert_stmt(13, "b"))?;

    // verify with neutral conn that nothing is visible (count)
    let count_query = "select count(*) from TEST_XA where f1 > 9 and f1 < 99";
    let count: u32 = conn.query(count_query)?.try_into()?;
    assert_eq!(0, count);

    debug!("rollback xa");
    tm.rollback_transaction().unwrap();

    // verify that nothing additional was inserted
    let count: u32 = conn.query(count_query)?.try_into()?;
    assert_eq!(0, count);

    debug!("conn_c inserts");
    conn_c.dml(&insert_stmt(10, "c"))?;
    conn_c.dml(&insert_stmt(11, "c"))?;
    conn_c.dml(&insert_stmt(12, "c"))?;
    conn_c.dml(&insert_stmt(13, "c"))?;
    conn_c.commit().unwrap();

    // verify that now the insertions were successful
    let count: u32 = conn.query(count_query)?.try_into()?;
    assert_eq!(4, count);

    Ok(())
}

fn xa_repeated(conn: &mut Connection, _log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("xa_repeated");

    // open two connections, auto_commit off
    let mut conn_a = conn.spawn()?;
    conn_a.set_auto_commit(false)?;
    let mut conn_b = conn_a.spawn()?;
    assert!(!conn_a.is_auto_commit()?);
    assert!(!conn_b.is_auto_commit()?);

    conn_a
        .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
        .unwrap(); // (milliseconds)
    conn_b
        .exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")
        .unwrap(); // (milliseconds)

    // instantiate a SimpleTransactionManager and register Resource Managers for
    // the two connections
    // log_handle.parse_new_spec("debug, dist_tx::tm::simple_transaction_manager =
    // trace");
    let mut tm = SimpleTransactionManager::new("test_035_xa_transactions".to_owned());
    tm.register(conn_a.get_resource_manager(), 22, true)
        .unwrap();
    tm.register(conn_b.get_resource_manager(), 44, true)
        .unwrap();

    for i in 0..5 {
        let j = i * 10 + 20;
        let count_query = format!(
            "select count(*) from TEST_XA where f1 > {} and f1 < {}",
            j,
            j + 9
        );

        tm.start_transaction().unwrap();

        debug!("conn_a inserts {}", j);
        conn_a.dml(&insert_stmt(j + 1, "a"))?;
        conn_a.dml(&insert_stmt(j + 2, "a"))?;
        debug!("conn_b inserts {}", j);
        conn_b.dml(&insert_stmt(j + 3, "b"))?;
        conn_b.dml(&insert_stmt(j + 4, "b"))?;

        // verify with neutral conn that nothing is visible (count)
        let count: u32 = conn.query(&count_query)?.try_into()?;
        assert_eq!(0, count);

        debug!("rollback xa");
        tm.rollback_transaction().unwrap();

        tm.start_transaction().unwrap();
        debug!("conn_a inserts {}", j);
        conn_a.dml(&insert_stmt(j + 1, "a"))?;
        conn_a.dml(&insert_stmt(j + 2, "a"))?;
        debug!("conn_b inserts");
        conn_b.dml(&insert_stmt(j + 3, "b"))?;
        conn_b.dml(&insert_stmt(j + 4, "b"))?;

        // verify with neutral conn that nothing is visible (count)
        let count: u32 = conn.query(&count_query)?.try_into()?;
        assert_eq!(0, count);

        debug!("commit xa");
        tm.commit_transaction().unwrap();

        // verify that now the insertions were successful
        let count: u32 = conn.query(&count_query)?.try_into()?;
        assert_eq!(4, count);
    }

    Ok(())
}

fn xa_conflicts(conn: &mut Connection, _log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("xa_conflicts");

    // open two connections, auto_commit off
    let mut conn_a = conn.spawn()?;
    conn_a.set_auto_commit(false)?;
    let mut conn_b = conn_a.spawn()?;
    assert!(!conn_a.is_auto_commit()?);
    assert!(!conn_b.is_auto_commit()?);

    conn_a.exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")?;
    conn_b.exec("SET TRANSACTION LOCK WAIT TIMEOUT 3000")?;

    // instantiate a SimpleTransactionManager and register Resource Managers for
    // the two connections
    // log_handle.parse_new_spec("debug, dist_tx::tm::simple_transaction_manager =
    // trace");
    let mut tm = SimpleTransactionManager::new("test_035_xa_transactions".to_owned());
    tm.register(conn_a.get_resource_manager(), 22, true)
        .unwrap();
    tm.register(conn_b.get_resource_manager(), 44, true)
        .unwrap();

    // do conflicting inserts
    // catch error response
    // try to commit
    // clean up
    // do clean inserts (to ensure nothing is left over)

    // do conflicting inserts
    // catch error response
    // rollback
    // do clean inserts (to ensure nothing is left over)

    Ok(())
}
