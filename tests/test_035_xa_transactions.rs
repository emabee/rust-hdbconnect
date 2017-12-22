extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
extern crate serde_json;

mod test_utils;

use hdbconnect::{Connection, HdbResult, XatFlag, XatId};
use flexi_logger::{LogSpecification, ReconfigurationHandle};

//#[test] // cargo test --test test_035_xa_transactions -- --nocapture
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

// Test what?
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
    let stmts = vec![
        "create table TEST_XA (f1 INT primary key, f2 NVARCHAR(20))",
        "insert into TEST_XA (f1, f2) values(100, 'INITIAL')",
        "insert into TEST_XA (f1, f2) values(101, 'INITIAL')",
        "insert into TEST_XA (f1, f2) values(102, 'INITIAL')",
        "insert into TEST_XA (f1, f2) values(103, 'INITIAL')",
    ];
    conn.multiple_statements(stmts)
}

// a) successful XA
fn successful_xa(conn: &mut Connection, log_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    info!("Successful XA");
    // open two connections, auto_commit off
    let mut conn_a = conn.spawn()?;
    conn_a.set_auto_commit(false)?;
    let mut conn_b = conn_a.spawn()?;
    assert!(!conn_a.is_auto_commit()?);
    assert!(!conn_b.is_auto_commit()?);

    log_handle.set_new_spec(LogSpecification::parse("debug"));
    debug!("Use conn_a to insert a row, and conn_b to insert another row");
    conn_a.dml("insert into TEST_XA (F1, F2) values (1,'SUCCESS')")?;
    conn_b.dml("insert into TEST_XA (F1, F2) values (2,'SUCCESS')")?;

    debug!("verify with conn that both new lines are not yet visible");
    let sum: Option<i32> = conn.query("SELECT SUM(F1) from TEST_XA where F2 = 'SUCCESS'")?
        .try_into()?;
    assert_eq!(sum, None);

    debug!("xa_start on both, should be successful");
    let xatid = XatId::new(1, vec![1_u8; 16], vec![1_u8; 16])?;

    log_handle.set_new_spec(LogSpecification::parse("trace"));
    assert_eq!(conn_a.xa_start(&xatid, XatFlag::NOFLAG)?, ());
    assert_eq!(conn_b.xa_start(&xatid, XatFlag::JOIN)?, ());

    // debug!("XA_commit on both, should be successful, too");
    // assert_eq!(conn_a.xa_commit(&xatid)?, ());
    // assert_eq!(conn_b.xa_commit(&xatid)?, ());

    // verify with conn that both new lines were inserted
    let sum: Option<i32> = conn.query("SELECT SUM(F1) from TEST_XA where F2 = 'SUCCESS'")?
        .try_into()?;
    assert_eq!(sum, Some(3));
    Ok(())
}



// b) failing XA
// use conn1 to insert a row
// use conn2 to insert the same row
// XA_prepare on both, should NOT be successful
// XA_rollback on both, should be successful
// verify with conn3 that the new line was not inserted
