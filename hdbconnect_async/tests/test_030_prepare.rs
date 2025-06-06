extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect_async::{Connection, HdbError, HdbResult, HdbValue};
use log::{debug, info};
use serde::Deserialize;

// Test prepared statements, transactional correctness,
// incl. parameter serialization (and result set deserialization)

#[tokio::test] // cargo test --test test_030_prepare -- --nocapture
pub async fn test_030_prepare() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    // log_handle.parse_new_spec("info, test=debug");
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection().await?;

    prepare_insert_statement(&mut log_handle, &connection).await?;
    prepare_statement_use_parameter_row(&mut log_handle, &connection).await?;
    prepare_multiple_errors(&mut log_handle, &connection).await?;
    prepare_select_with_pars(&mut log_handle, &connection).await?;
    prepare_select_without_pars(&mut log_handle, &connection).await?;
    prepare_and_execute_with_fetch(&mut log_handle, &connection).await?;

    test_utils::closing_info(connection, start).await
}

async fn prepare_insert_statement(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("statement preparation and transactional correctness (auto_commit on/off, rollbacks)");
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_PREPARE"])
        .await;
    let stmts = vec!["create table TEST_PREPARE (F1_S NVARCHAR(20), F2_I INT)"];
    connection.multiple_statements(stmts).await?;

    #[allow(dead_code)]
    #[derive(Deserialize, Debug)]
    struct TestStruct {
        #[serde(rename = "F1_S")]
        f1_s: Option<String>,
        #[serde(rename = "F2_I")]
        f2_i: Option<i32>,
    }

    let insert_stmt_str = "insert into TEST_PREPARE (F1_S, F2_I) values(?, ?)";

    debug!("prepare & execute");
    let mut insert_stmt = connection.prepare(insert_stmt_str).await?;
    insert_stmt.add_batch(&("conn1-auto1", 45_i32))?;
    insert_stmt.add_batch(&("conn1-auto2", 46_i32))?;
    insert_stmt.execute_batch().await?;

    debug!("prepare & execute on second connection");
    let connection2 = connection.spawn().await?;
    let mut insert_stmt2 = connection2.prepare(insert_stmt_str).await?;
    insert_stmt2.add_batch(&("conn2-auto1", 45_i32))?;
    insert_stmt2.add_batch(&("conn2-auto2", 46_i32))?;
    let affrows = insert_stmt2.execute_batch().await?.into_affected_rows()?;
    debug!("affected rows: {affrows:?}");

    debug!(
        "prepare & execute on first connection with auto_commit off, \
         rollback, do it again and commit"
    );
    connection.set_auto_commit(false).await;
    connection.reset_statistics().await;
    let mut insert_stmt = connection.prepare(insert_stmt_str).await?;
    insert_stmt.add_batch(&("conn1-rollback1", 45_i32))?;
    insert_stmt.add_batch(&("conn1-rollback2", 46_i32))?;
    insert_stmt.add_batch(&("conn1-rollback3", 47_i32))?;
    insert_stmt.add_batch(&("conn1-rollback4", 48_i32))?;
    insert_stmt.add_batch(&("conn1-rollback5", 49_i32))?;
    insert_stmt.add_batch(&("conn1-rollback6", 50_i32))?;
    let affrows = insert_stmt.execute_batch().await?.into_affected_rows();
    debug!(
        "affected rows: {:?}, callcount: {}",
        affrows,
        connection.statistics().await.call_count()
    );
    assert_eq!(connection.statistics().await.call_count(), 2);
    connection.rollback().await?;

    insert_stmt.add_batch(&("conn1-commit1", 45_i32))?;
    insert_stmt.add_batch(&("conn1-commit2", 46_i32))?;
    insert_stmt.execute_batch().await?;
    connection.commit().await?;

    // prepare, execute batch, rollback in new spawn
    let connection3 = connection.spawn().await?;
    let mut insert_stmt3 = connection3.prepare(insert_stmt_str).await?;
    insert_stmt3.add_batch(&("conn3-auto1", 45_i32))?;
    insert_stmt3.add_batch(&("conn3-auto2", 46_i32))?;
    insert_stmt3.add_batch(&("conn3-auto3", 47_i32))?;
    insert_stmt3.execute_batch().await?;
    connection3.rollback().await?;

    let typed_result: Vec<TestStruct> = connection
        .query("select * from TEST_PREPARE")
        .await?
        .try_into()
        .await?;
    assert_eq!(typed_result.len(), 6);
    for ts in typed_result {
        let s = ts.f1_s.as_ref().unwrap();
        assert!(!s.contains("rollback"));
        assert!(s.contains("comm") || s.contains("auto"));
    }
    Ok(())
}

async fn prepare_statement_use_parameter_row(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("statement preparation with direct use of a parameter row");
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_PREPARE"])
        .await;
    let stmts = vec!["create table TEST_PREPARE (F1_S NVARCHAR(20), F2_I INT)"];
    connection.multiple_statements(stmts).await?;

    let insert_stmt_str = "insert into TEST_PREPARE (F1_S, F2_I) values(?, ?)";

    debug!("prepare & execute with rust types");
    let mut insert_stmt = connection.prepare(insert_stmt_str).await?;
    debug!("connection: {}", connection.server_usage().await);
    debug!("insert_stmt: {}", insert_stmt.server_usage());

    insert_stmt.add_batch(&("conn1-auto1", 45_i32))?;
    insert_stmt.add_batch(&("conn1-auto2", 46_i32))?;
    insert_stmt.execute_batch().await?;
    debug!("connection: {}", connection.server_usage().await);
    debug!("insert_stmt: {}", insert_stmt.server_usage());

    let typed_result: i32 = connection
        .query("select sum(F2_I) from TEST_PREPARE")
        .await?
        .try_into()
        .await?;
    assert_eq!(typed_result, 91);

    debug!("prepare & execute with HdbValues");
    let my_string = String::from("foo");
    insert_stmt.add_row_to_batch(vec![
        HdbValue::STRING(my_string.clone()),
        HdbValue::INT(1000_i32),
    ])?;
    debug!("add row to batch...");
    // create HdbValue instances manually
    insert_stmt.add_row_to_batch(vec![
        HdbValue::STRING(my_string.clone()),
        HdbValue::INT(2100_i32),
    ])?;
    // use the ParameterDescriptors to create HdbValue instances
    let values: Vec<HdbValue<'static>> = insert_stmt
        .parameter_descriptors()
        .iter_in()
        .zip([my_string, "25".to_string()].iter())
        .map(|(descriptor, s)| descriptor.parse_value(s).unwrap())
        .collect();
    insert_stmt.add_row_to_batch(values)?;

    debug!("execute...");
    insert_stmt.execute_batch().await?;
    debug!("connection: {}", connection.server_usage().await);
    debug!("insert_stmt: {}", insert_stmt.server_usage());

    connection.commit().await?;
    debug!("checking...");
    let typed_result: i32 = connection
        .query("select sum(F2_I) from TEST_PREPARE")
        .await?
        .try_into()
        .await?;
    assert_eq!(typed_result, 3216);
    Ok(())
}

async fn prepare_multiple_errors(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("test multiple errors from failing batches");
    connection
        .multiple_statements_ignore_err(vec!["drop table TEST_PREPARE"])
        .await;
    let stmts = vec!["create table TEST_PREPARE (F1_S NVARCHAR(20) primary key, F2_I INT)"];
    connection.multiple_statements(stmts).await?;

    connection.set_auto_commit(true).await;
    let insert_stmt_str = "insert into TEST_PREPARE (F1_S, F2_I) values(?, ?)";
    let mut insert_stmt = connection.prepare(insert_stmt_str).await?;

    insert_stmt.add_batch(&("multi_error1", 41_i32))?;
    insert_stmt.add_batch(&("multi_error2", 42_i32))?;
    insert_stmt.add_batch(&("multi_error3", 43_i32))?;
    insert_stmt.add_batch(&("multi_error4", 44_i32))?;
    insert_stmt.add_batch(&("multi_error5", 45_i32))?;
    insert_stmt.execute_batch().await?;

    insert_stmt.add_batch(&("multi_error1", 141_i32))?;
    insert_stmt.add_batch(&("multi_error12", 142_i32))?;
    insert_stmt.add_batch(&("multi_error3", 143_i32))?;
    insert_stmt.add_batch(&("multi_error14", 144_i32))?;
    insert_stmt.add_batch(&("multi_error5", 145_i32))?;
    let result = insert_stmt.execute_batch().await;
    assert!(result.is_err());

    match result.err().unwrap() {
        HdbError::ExecutionResults(execution_results) => {
            assert!(execution_results[0].is_failure());
            assert!(!execution_results[1].is_failure());
            assert!(execution_results[2].is_failure());
            assert!(!execution_results[3].is_failure());
            assert!(execution_results[4].is_failure());
        }
        _ => panic!("bad err"),
    }
    Ok(())
}

async fn prepare_select_with_pars(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("prepared select statement with parameters");
    let sum_of_big_values: i64 = connection
        .prepare_and_execute(
            "select sum(F2_I) from TEST_PREPARE where F2_I > ?",
            &(45_i32),
        )
        .await?
        .into_result_set()?
        .try_into()
        .await?;
    assert_eq!(sum_of_big_values, 286_i64);
    Ok(())
}

async fn prepare_select_without_pars(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("prepared select statement without parameters");
    let stmt_str = "select sum(F2_I) from TEST_PREPARE";
    let mut stmt = connection.prepare(stmt_str).await?;

    // two ways to do the same
    let result_set = stmt.execute(&()).await?.into_result_set()?;
    let sum_of_big_values: i64 = result_set.try_into().await?;
    assert_eq!(sum_of_big_values, 501_i64);

    let result_set = stmt.execute_batch().await?.into_result_set()?;
    let sum_of_big_values: i64 = result_set.try_into().await?;
    assert_eq!(sum_of_big_values, 501_i64);

    Ok(())
}

async fn prepare_and_execute_with_fetch(
    _log_handle: &mut LoggerHandle,
    connection: &Connection,
) -> HdbResult<()> {
    info!("call prepare_and_execute() with implicit fetch");

    let _rs = connection
        .prepare_and_execute("select * from M_TABLES", &())
        .await?
        .into_result_set()?
        .into_rows()
        .await?;
    Ok(())
}
