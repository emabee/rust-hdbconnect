extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod test_utils;

use chrono::NaiveDateTime;
use hdbconnect::{Connection, HdbResult};
use flexi_logger::{Logger, ReconfigurationHandle};
use std::thread;
use std::time::Duration;

#[test] // cargo test --test test_016_selectforupdate -- --nocapture
pub fn test_016_selectforupdate() {
    let mut logger_handle = Logger::with_str("info").start_reconfigurable().unwrap();

    match impl_test_016_selectforupdate(&mut logger_handle) {
        Err(e) => {
            error!("impl_test_016_selectforupdate() failed with {:?}", e);
            assert!(false)
        }
        Ok(_) => debug!("impl_test_016_selectforupdate() ended successful"),
    }
}

// Test the various ways to evaluate a resultset
fn impl_test_016_selectforupdate(logger_handle: &mut ReconfigurationHandle) -> HdbResult<()> {
    let mut connection = test_utils::get_authenticated_connection()?;
    prepare(&mut connection, logger_handle)?;
    produce_conflict(&mut connection, logger_handle)?;
    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct TestData {
    #[serde(rename = "F1_S")] f1: String,
    #[serde(rename = "F2_I")] f2: Option<i32>,
    #[serde(rename = "F3_I")] f3: i32,
    #[serde(rename = "F4_DT")] f4: NaiveDateTime,
}

fn prepare(
    connection: &mut Connection,
    _logger_handle: &mut ReconfigurationHandle,
) -> HdbResult<()> {
    // prepare the db table
    connection.multiple_statements_ignore_err(vec!["drop table TEST_SELFORUPDATE"]);
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
    connection.multiple_statements(stmts)?;

    // insert some mass data
    for i in 100..200 {
        connection.dml(&format!(
            "insert into TEST_SELFORUPDATE (f1_s, f2_i, f3_i, \
             f4_dt) values('{}', {}, {},'01.01.1900')",
            i, i, i
        ))?;
    }
    Ok(())
}

fn produce_conflict(
    connection: &mut Connection,
    logger_handle: &mut ReconfigurationHandle,
) -> HdbResult<()> {
    logger_handle.parse_new_spec("info");
    connection.set_auto_commit(false)?;

    debug!("get two more connection");
    let mut connection2 = connection.spawn()?;
    let mut connection3 = connection.spawn()?;

    debug!("conn1: select * for update");
    connection.query("select * from TEST_SELFORUPDATE where F1_S = 'Hello' for update")?;

    debug!("try conflicting update with second connection");
    thread::spawn(move || {
        debug!("conn2: select * for update");
        connection2
            .query("select * from TEST_SELFORUPDATE where F1_S = 'Hello' for update")
            .unwrap();
        connection2
            .dml("update TEST_SELFORUPDATE set F2_I = 2 where F1_S = 'Hello'")
            .unwrap();
        connection2.commit().unwrap();
    });

    debug!("do update with first connection");
    connection.dml("update TEST_SELFORUPDATE set F2_I = 1 where F1_S = 'Hello'")?;

    let i: i32 = connection
        .query("select F2_I from TEST_SELFORUPDATE where F1_S = 'Hello'")?
        .try_into()?;
    assert_eq!(i, 1);

    debug!("commit the change of the first connection");
    connection.commit()?;

    thread::sleep(Duration::from_millis(200));

    debug!(
        "verify the change of the second connection is visible (because the other thread \
         had to wait with its update until the first was committed"
    );
    let i: i32 = connection3
        .query("select F2_I from TEST_SELFORUPDATE where F1_S = 'Hello'")?
        .try_into()?;
    assert_eq!(i, 2);

    Ok(())
}
