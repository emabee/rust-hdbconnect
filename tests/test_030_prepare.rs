extern crate chrono;
extern crate flexi_logger;
extern crate hdbconnect;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

mod test_utils;

use hdbconnect::{Connection, HdbResult, HdbValue};

#[test] // cargo test --test test_030_prepare -- --nocapture
pub fn test_030_prepare() {
    test_utils::init_logger("test_030_prepare=info, hdbconnect=info");

    match impl_test_030_prepare() {
        Err(e) => {
            error!("test_030_prepare() failed with {:?}", e);
            assert!(false)
        }
        Ok(i) => info!("{} calls to DB were executed", i),
    }
}

// Test prepared statements, transactional correctness,
// incl. parameter serialization (and resultset deserialization)
fn impl_test_030_prepare() -> HdbResult<i32> {
    let mut connection = test_utils::get_authenticated_connection()?;

    prepare_insert_statement(&mut connection)?;
    prepare_statement_use_parameter_row(&mut connection)?;

    Ok(connection.get_call_count()?)
}

fn prepare_insert_statement(connection: &mut Connection) -> HdbResult<()> {
    info!(
        "test statement preparation and transactional correctness (auto_commit on/off, rollbacks)"
    );
    connection.multiple_statements_ignore_err(vec!["drop table TEST_PREPARE"]);
    let stmts = vec!["create table TEST_PREPARE (F1_S NVARCHAR(20), F2_I INT)"];
    connection.multiple_statements(stmts)?;

    #[derive(Deserialize, Debug)]
    struct TestStruct {
        #[serde(rename = "F1_S")]
        f1_s: Option<String>,
        #[serde(rename = "F2_I")]
        f2_i: Option<i32>,
    }

    let insert_stmt_str = "insert into TEST_PREPARE (F1_S, F2_I) values(?, ?)";

    // prepare & execute
    let mut insert_stmt = connection.prepare(insert_stmt_str)?;
    insert_stmt.add_batch(&("conn1-auto1", 45_i32))?;
    insert_stmt.add_batch(&("conn1-auto2", 46_i32))?;
    insert_stmt.execute_batch()?;

    // prepare & execute on second connection
    let connection2 = connection.spawn()?;
    let mut insert_stmt2 = connection2.prepare(insert_stmt_str)?;
    insert_stmt2.add_batch(&("conn2-auto1", 45_i32))?;
    insert_stmt2.add_batch(&("conn2-auto2", 46_i32))?;
    let affrows = insert_stmt2.execute_batch()?.into_affected_rows();
    debug!("affected rows: {:?}", affrows);

    // prepare & execute on first connection with auto_commit off,
    // rollback, do it again and commit
    connection.set_auto_commit(false)?;
    let count = connection.get_call_count()?;
    let mut insert_stmt = connection.prepare(insert_stmt_str)?;
    insert_stmt.add_batch(&("conn1-rollback1", 45_i32))?;
    insert_stmt.add_batch(&("conn1-rollback2", 46_i32))?;
    insert_stmt.add_batch(&("conn1-rollback3", 47_i32))?;
    insert_stmt.add_batch(&("conn1-rollback4", 48_i32))?;
    insert_stmt.add_batch(&("conn1-rollback5", 49_i32))?;
    insert_stmt.add_batch(&("conn1-rollback6", 50_i32))?;
    let affrows = insert_stmt.execute_batch()?.into_affected_rows();
    debug!(
        "affected rows: {:?}, callcount: {}",
        affrows,
        connection.get_call_count()? - count
    );
    assert_eq!(connection.get_call_count()? - count, 2);
    connection.rollback()?;

    insert_stmt.add_batch(&("conn1-commit1", 45_i32))?;
    insert_stmt.add_batch(&("conn1-commit2", 46_i32))?;
    insert_stmt.execute_batch()?;
    connection.commit()?;

    // prepare, execute batch, rollback in new spawn
    let mut connection3 = connection.spawn()?;
    let mut insert_stmt3 = connection3.prepare(insert_stmt_str)?;
    insert_stmt3.add_batch(&("conn3-auto1", 45_i32))?;
    insert_stmt3.add_batch(&("conn3-auto2", 46_i32))?;
    insert_stmt3.add_batch(&("conn3-auto3", 47_i32))?;
    insert_stmt3.execute_batch()?;
    connection3.rollback()?;

    let typed_result: Vec<TestStruct> =
        connection.query("select * from TEST_PREPARE")?.try_into()?;
    assert_eq!(typed_result.len(), 6);
    for ts in typed_result {
        let s = ts.f1_s.as_ref().unwrap();
        assert_eq!(false, s.contains("rollback"));
        assert_eq!(true, s.contains("comm") || s.contains("auto"));
    }
    Ok(())
}

fn prepare_statement_use_parameter_row(connection: &mut Connection) -> HdbResult<()> {
    info!("test statement preparation with direct use of parameter row");
    connection.multiple_statements_ignore_err(vec!["drop table TEST_PREPARE"]);
    let stmts = vec!["create table TEST_PREPARE (F1_S NVARCHAR(20), F2_I INT)"];
    connection.multiple_statements(stmts)?;

    let insert_stmt_str = "insert into TEST_PREPARE (F1_S, F2_I) values(?, ?)";

    // prepare & execute with rust types
    let mut insert_stmt = connection.prepare(insert_stmt_str)?;
    insert_stmt.add_batch(&("conn1-auto1", 45_i32))?;
    insert_stmt.add_batch(&("conn1-auto2", 46_i32))?;
    insert_stmt.execute_batch()?;

    let typed_result: i32 = connection
        .query("select sum(F2_I) from TEST_PREPARE")?
        .try_into()?;
    assert_eq!(typed_result, 91);

    // prepare & execute with rust types
    let mut stmt = connection.prepare(insert_stmt_str)?;
    let my_string = String::from("foo");
    stmt.add_batch(&vec![
        HdbValue::STRING(my_string.clone()),
        HdbValue::INT(1000_i32),
    ])?;
    stmt.add_batch(&vec![
        HdbValue::STRING(my_string.clone()),
        HdbValue::INT(2100_i32),
    ])?;
    stmt.add_batch(&vec![
        HdbValue::STRING(my_string),
        HdbValue::STRING("25".to_string()),
    ])?;

    stmt.execute_batch()?;
    connection.commit()?;

    let typed_result: i32 = connection
        .query("select sum(F2_I) from TEST_PREPARE")?
        .try_into()?;
    assert_eq!(typed_result, 3216);

    Ok(())
}
