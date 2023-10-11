extern crate serde;

mod test_utils;

use flexi_logger::LoggerHandle;
use hdbconnect::{Connection, HdbResult};
use log::{debug, info};

const QUERY: &str = "select * FROM TEST_NUMERIC_CONVERSION";

// cargo test test_042_numeric_conversion -- --nocapture
#[test]
fn test_042_numeric_conversion() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let start = std::time::Instant::now();
    let connection = test_utils::get_authenticated_connection()?;

    info!("create numeric fields and try different number conversions");
    debug!("setup...");

    test_tiny_int(&mut log_handle, &connection)?;
    test_small_int(&mut log_handle, &connection)?;
    test_integer(&mut log_handle, &connection)?;
    test_big_int(&mut log_handle, &connection)?;
    test_decimal(&mut log_handle, &connection)?;
    conversion_error(&mut log_handle, &connection)?;

    test_utils::closing_info(connection, start)
}

#[allow(clippy::cognitive_complexity)]
fn test_tiny_int(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (TINYINT TINYINT)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    debug!("execute...");
    insert_stmt.execute(&(1u8))?;
    insert_stmt.execute(&(1u16))?;
    insert_stmt.execute(&(1u32))?;
    insert_stmt.execute(&(1u64))?;
    insert_stmt.execute(&(1i8))?;
    insert_stmt.execute(&(1i16))?;
    insert_stmt.execute(&(1i32))?;
    insert_stmt.execute(&(1i64))?;

    debug!("query...");
    let resultset = connection.query(QUERY)?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<u8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<u16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<u32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<u64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<i8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<i16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<i32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<i64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    insert_stmt.execute(&(true))?;
    insert_stmt.execute(&(false))?;

    let rows: Vec<bool> = connection.query(QUERY)?.try_into()?;
    assert_eq!(rows, vec![true, false]);

    assert_eq!(connection.query(QUERY)?.try_into::<Vec<u8>>()?, vec![1, 0]);
    assert_eq!(connection.query(QUERY)?.try_into::<Vec<u16>>()?, vec![1, 0]);
    assert_eq!(connection.query(QUERY)?.try_into::<Vec<u32>>()?, vec![1, 0]);
    assert_eq!(connection.query(QUERY)?.try_into::<Vec<u64>>()?, vec![1, 0]);
    assert_eq!(connection.query(QUERY)?.try_into::<Vec<i8>>()?, vec![1, 0]);
    assert_eq!(connection.query(QUERY)?.try_into::<Vec<i16>>()?, vec![1, 0]);
    assert_eq!(connection.query(QUERY)?.try_into::<Vec<i32>>()?, vec![1, 0]);
    assert_eq!(connection.query(QUERY)?.try_into::<Vec<i64>>()?, vec![1, 0]);

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    insert_stmt.execute(&(true))?;
    insert_stmt.execute(&(false))?;

    let num_rows: Vec<bool> = connection.query(QUERY)?.try_into()?;
    assert_eq!(num_rows, vec![true, false]);

    //negative values not allowed
    assert!(insert_stmt.execute(&(-1i8)).is_err());
    assert!(insert_stmt.execute(&(-1i16)).is_err());
    assert!(insert_stmt.execute(&(-1i32)).is_err());
    assert!(insert_stmt.execute(&(-1i64)).is_err());

    //in range tinyint
    assert!(insert_stmt.execute(&(255_u16)).is_ok());
    assert!(insert_stmt.execute(&(255_u32)).is_ok());
    assert!(insert_stmt.execute(&(255_u64)).is_ok());
    assert!(insert_stmt.execute(&(255i16)).is_ok());
    assert!(insert_stmt.execute(&(255i32)).is_ok());
    assert!(insert_stmt.execute(&(255i64)).is_ok());

    //out of range tinyint
    assert!(insert_stmt.execute(&(256u16)).is_err());
    assert!(insert_stmt.execute(&(256u32)).is_err());
    assert!(insert_stmt.execute(&(256u64)).is_err());
    assert!(insert_stmt.execute(&(256i16)).is_err());
    assert!(insert_stmt.execute(&(256i32)).is_err());
    assert!(insert_stmt.execute(&(256i64)).is_err());

    let query = QUERY;
    assert!(connection.query(query)?.try_into::<Vec<u8>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<u16>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<u32>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<u64>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<i16>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<i32>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<i64>>().is_ok());

    assert!(connection.query(query)?.try_into::<Vec<i8>>().is_err());
    Ok(())
}

#[allow(clippy::cognitive_complexity)]
fn test_small_int(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (SMALLINT SMALLINT)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    debug!("execute...");
    insert_stmt.execute(&(1u8))?;
    insert_stmt.execute(&(1u16))?;
    insert_stmt.execute(&(1u32))?;
    insert_stmt.execute(&(1u64))?;
    insert_stmt.execute(&(1i8))?;
    insert_stmt.execute(&(1i16))?;
    insert_stmt.execute(&(1i32))?;
    insert_stmt.execute(&(1i64))?;

    debug!("query...");
    let query = QUERY;
    let resultset = connection.query(query)?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(query)?.try_into::<Vec<u8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    insert_stmt.execute(&(true))?;
    insert_stmt.execute(&(false))?;

    let rows: Vec<bool> = connection.query(query)?.try_into()?;
    assert_eq!(rows, vec![true, false]);

    assert_eq!(connection.query(query)?.try_into::<Vec<u8>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u16>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u32>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u64>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i8>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i16>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i32>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i64>>()?, vec![1, 0]);

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    insert_stmt.execute(&(true))?;
    insert_stmt.execute(&(false))?;

    let num_rows: Vec<bool> = connection.query(query)?.try_into()?;
    assert_eq!(num_rows, vec![true, false]);

    insert_stmt.execute(&(-1i8))?;
    insert_stmt.execute(&(-1i16))?;
    insert_stmt.execute(&(-1i32))?;
    insert_stmt.execute(&(-1i64))?;

    //in range
    assert!(insert_stmt.execute(&(32767u16)).is_ok());
    assert!(insert_stmt.execute(&(32767u32)).is_ok());
    assert!(insert_stmt.execute(&(32767u64)).is_ok());
    assert!(insert_stmt.execute(&(32767i16)).is_ok());
    assert!(insert_stmt.execute(&(32767i32)).is_ok());
    assert!(insert_stmt.execute(&(32767i64)).is_ok());

    //out of range
    assert!(insert_stmt.execute(&(32768u16)).is_err());
    assert!(insert_stmt.execute(&(32768u32)).is_err());
    assert!(insert_stmt.execute(&(32768u64)).is_err());
    assert!(insert_stmt.execute(&(32768i32)).is_err());
    assert!(insert_stmt.execute(&(32768i64)).is_err());

    //in range
    assert!(insert_stmt.execute(&(-32767i16)).is_ok());
    assert!(insert_stmt.execute(&(-32767i32)).is_ok());
    assert!(insert_stmt.execute(&(-32767i64)).is_ok());

    //out of range
    assert!(insert_stmt.execute(&(-32769i32)).is_err());
    assert!(insert_stmt.execute(&(-32769i64)).is_err());

    assert!(connection.query(query)?.try_into::<Vec<u8>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u16>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u32>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i8>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u64>>().is_err());

    assert!(connection.query(query)?.try_into::<Vec<i16>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<i32>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<i64>>().is_ok());

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
fn test_integer(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (INTEGER INTEGER)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    debug!("execute...");

    insert_stmt.execute(&(1u8))?;
    insert_stmt.execute(&(1u16))?;
    insert_stmt.execute(&(1u32))?;
    insert_stmt.execute(&(1u64))?;
    insert_stmt.execute(&(1i8))?;
    insert_stmt.execute(&(1i16))?;
    insert_stmt.execute(&(1i32))?;
    insert_stmt.execute(&(1i64))?;

    debug!("query...");
    let query = QUERY;
    let resultset = connection.query(query)?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(query)?.try_into::<Vec<u8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    insert_stmt.execute(&(true))?;
    insert_stmt.execute(&(false))?;

    let rows: Vec<bool> = connection.query(query)?.try_into()?;
    assert_eq!(rows, vec![true, false]);

    assert_eq!(connection.query(query)?.try_into::<Vec<u8>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u16>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u32>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u64>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i8>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i16>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i32>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i64>>()?, vec![1, 0]);

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    insert_stmt.execute(&(true))?;
    insert_stmt.execute(&(false))?;

    let num_rows: Vec<bool> = connection.query(query)?.try_into()?;
    assert_eq!(num_rows, vec![true, false]);

    insert_stmt.execute(&(-1i8))?;
    insert_stmt.execute(&(-1i16))?;
    insert_stmt.execute(&(-1i32))?;
    insert_stmt.execute(&(-1i64))?;

    //in range
    assert!(insert_stmt.execute(&(2_147_483_647u32)).is_ok());
    assert!(insert_stmt.execute(&(2_147_483_647u64)).is_ok());
    assert!(insert_stmt.execute(&(2_147_483_647i64)).is_ok());

    //out of range
    assert!(insert_stmt.execute(&(2_147_483_648u32)).is_err());
    assert!(insert_stmt.execute(&(2_147_483_648u64)).is_err());
    assert!(insert_stmt.execute(&(2_147_483_648i64)).is_err());

    //in range
    assert!(insert_stmt.execute(&(-2_147_483_648i32)).is_ok());
    assert!(insert_stmt.execute(&(-2_147_483_648i64)).is_ok());

    //out of range
    assert!(insert_stmt.execute(&(-2_147_483_649i64)).is_err());

    assert!(connection.query(query)?.try_into::<Vec<u8>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u16>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u32>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i8>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i16>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u64>>().is_err());

    assert!(connection.query(query)?.try_into::<Vec<i32>>().is_ok());
    assert!(connection.query(query)?.try_into::<Vec<i64>>().is_ok());

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
fn test_big_int(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (BIGINT BIGINT)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    debug!("execute...");

    insert_stmt.execute(&(1u8))?;
    insert_stmt.execute(&(1u16))?;
    insert_stmt.execute(&(1u32))?;
    insert_stmt.execute(&(1u64))?;
    insert_stmt.execute(&(1i8))?;
    insert_stmt.execute(&(1i16))?;
    insert_stmt.execute(&(1i32))?;
    insert_stmt.execute(&(1i64))?;

    debug!("query...");
    let query = QUERY;
    let resultset = connection.query(query)?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(query)?.try_into::<Vec<u8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<u64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(query)?.try_into::<Vec<i64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    insert_stmt.execute(&(true))?;
    insert_stmt.execute(&(false))?;

    let rows: Vec<bool> = connection.query(query)?.try_into()?;
    assert_eq!(rows, vec![true, false]);

    assert_eq!(connection.query(query)?.try_into::<Vec<u8>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u16>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u32>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<u64>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i8>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i16>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i32>>()?, vec![1, 0]);
    assert_eq!(connection.query(query)?.try_into::<Vec<i64>>()?, vec![1, 0]);

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    insert_stmt.execute(&(true))?;
    insert_stmt.execute(&(false))?;

    let num_rows: Vec<bool> = connection.query(query)?.try_into()?;
    assert_eq!(num_rows, vec![true, false]);

    insert_stmt.execute(&(-1i8))?;
    insert_stmt.execute(&(-1i16))?;
    insert_stmt.execute(&(-1i32))?;
    insert_stmt.execute(&(-1i64))?;

    //in range
    assert!(insert_stmt.execute(&(9_223_372_036_854_775_807u64)).is_ok());
    assert!(insert_stmt.execute(&(9_223_372_036_854_775_807i64)).is_ok());

    //out of range
    assert!(insert_stmt
        .execute(&(9_223_372_036_854_775_808u64))
        .is_err());

    //in range
    assert!(insert_stmt
        .execute(&(-9_223_372_036_854_775_808i64))
        .is_ok());

    assert!(connection.query(query)?.try_into::<Vec<u8>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u16>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u32>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i8>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i16>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i32>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u64>>().is_err());

    assert!(connection.query(query)?.try_into::<Vec<i64>>().is_ok());

    assert!(connection.query(query)?.try_into::<Vec<u8>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u16>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u32>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i8>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i16>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<i32>>().is_err());
    assert!(connection.query(query)?.try_into::<Vec<u64>>().is_err());

    assert!(connection.query(query)?.try_into::<Vec<i64>>().is_ok());

    Ok(())
}

#[allow(clippy::cognitive_complexity)]
fn test_decimal(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (DECIMAL DECIMAL)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    debug!("execute...");

    insert_stmt.execute(&(1u8))?;
    insert_stmt.execute(&(1u16))?;
    insert_stmt.execute(&(1u32))?;
    insert_stmt.execute(&(1u64))?;
    insert_stmt.execute(&(1i8))?;
    insert_stmt.execute(&(1i16))?;
    insert_stmt.execute(&(1i32))?;
    insert_stmt.execute(&(1i64))?;

    debug!("query...");
    let resultset = connection.query(QUERY)?;
    debug!("deserialize...");
    let rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<u8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<u16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<u32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<u64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<i8>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<i16>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<i32>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(
        connection.query(QUERY)?.try_into::<Vec<i64>>()?,
        vec![1, 1, 1, 1, 1, 1, 1, 1]
    );

    let rows: Result<Vec<bool>, _> = connection.query(QUERY)?.try_into();
    assert!(rows.is_err());

    connection.multiple_statements(vec!["TRUNCATE TABLE TEST_NUMERIC_CONVERSION"])?;
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;

    // currently no boolean to decimal conversion
    assert!(insert_stmt.execute(&(true)).is_err());
    assert!(insert_stmt.execute(&(false)).is_err());

    insert_stmt.execute(&(-1i8))?;
    insert_stmt.execute(&(-1i16))?;
    insert_stmt.execute(&(-1i32))?;
    insert_stmt.execute(&(-1i64))?;

    assert!(insert_stmt.execute(&(9_223_372_036_854_775_807u64)).is_ok());
    assert!(insert_stmt.execute(&(9_223_372_036_854_775_807i64)).is_ok());
    assert!(insert_stmt
        .execute(&(-9_223_372_036_854_775_808i64))
        .is_ok());

    assert!(connection.query(QUERY)?.try_into::<Vec<u8>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<u16>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<u32>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<i8>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<i16>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<i32>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<u64>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<i64>>().is_ok());

    Ok(())
}

fn conversion_error(_log_handle: &mut LoggerHandle, connection: &Connection) -> HdbResult<()> {
    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (TEXT NVARCHAR(50))"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt = connection.prepare("insert into TEST_NUMERIC_CONVERSION values (?)")?;
    debug!("execute...");

    insert_stmt.execute(&("nan"))?;

    assert!(connection.query(QUERY)?.try_into::<Vec<u8>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<u16>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<u32>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<i8>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<i16>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<i32>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<u64>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<i64>>().is_err());
    assert!(connection.query(QUERY)?.try_into::<Vec<f32>>().is_ok());
    assert!(connection.query(QUERY)?.try_into::<Vec<f64>>().is_ok());

    Ok(())
}
