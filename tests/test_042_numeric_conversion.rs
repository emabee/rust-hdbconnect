mod test_utils;

use flexi_logger::ReconfigurationHandle;
use hdbconnect::{Connection, HdbResult};
use log::{debug, info};

// cargo test test_042_numeric_conversion -- --nocapture
#[test]
pub fn test_042_numeric_conversion() -> HdbResult<()> {
    let mut log_handle = test_utils::init_logger();
    let mut connection = test_utils::get_authenticated_connection()?;

    info!("create numeric fields and try different number conversions");
    debug!("setup...");

    test_tiny_int(&mut log_handle, &mut connection)?;
    test_small_int(&mut log_handle, &mut connection)?;
    test_integer(&mut log_handle, &mut connection)?;
    test_big_int(&mut log_handle, &mut connection)?;

    info!("{} calls to DB were executed", connection.get_call_count()?);
    Ok(())
}


fn test_tiny_int(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("create numeric fields and try different number conversions");
    debug!("setup...");

    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (TINYINT TINYINT, SMALLINT SMALLINT, INTEGER INTEGER, BIGINT BIGINT)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt =
        connection.prepare("insert into TEST_NUMERIC_CONVERSION (TINYINT) values (?)")?;
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
    let resultset = connection.query("select TINYINT FROM TEST_NUMERIC_CONVERSION")?;
    debug!("deserialize...");
    let num_rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(num_rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    //negative values not allowed
    assert!(insert_stmt.execute(&(-1i8)).is_err());
    assert!(insert_stmt.execute(&(-1i16)).is_err());
    assert!(insert_stmt.execute(&(-1i32)).is_err());
    assert!(insert_stmt.execute(&(-1i64)).is_err());

    //in range tinyint
    assert!(insert_stmt.execute(&(255u16)).is_ok());
    assert!(insert_stmt.execute(&(255u32)).is_ok());
    assert!(insert_stmt.execute(&(255u64)).is_ok());
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

    Ok(())
}

fn test_small_int(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("create numeric fields and try different number conversions");
    debug!("setup...");

    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (SMALLINT SMALLINT)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt =
        connection.prepare("insert into TEST_NUMERIC_CONVERSION (SMALLINT) values (?)")?;
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
    let resultset = connection.query("select * FROM TEST_NUMERIC_CONVERSION")?;
    debug!("deserialize...");
    let num_rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(num_rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

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

    Ok(())
}

fn test_integer(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("create numeric fields and try different number conversions");
    debug!("setup...");

    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (INTEGER INTEGER)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt =
        connection.prepare("insert into TEST_NUMERIC_CONVERSION (INTEGER) values (?)")?;
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
    let resultset = connection.query("select * FROM TEST_NUMERIC_CONVERSION")?;
    debug!("deserialize...");
    let num_rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(num_rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

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

    Ok(())
}

fn test_big_int(
    _log_handle: &mut ReconfigurationHandle,
    connection: &mut Connection,
) -> HdbResult<()> {
    info!("create numeric fields and try different number conversions");
    debug!("setup...");

    connection.multiple_statements_ignore_err(vec!["drop table TEST_NUMERIC_CONVERSION"]);
    let stmts = vec!["create table TEST_NUMERIC_CONVERSION (BIGINT BIGINT)"];
    connection.multiple_statements(stmts)?;

    debug!("prepare...");
    let mut insert_stmt =
        connection.prepare("insert into TEST_NUMERIC_CONVERSION (BIGINT) values (?)")?;
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
    let resultset = connection.query("select * FROM TEST_NUMERIC_CONVERSION")?;
    debug!("deserialize...");
    let num_rows: Vec<usize> = resultset.try_into()?;
    assert_eq!(num_rows, vec![1, 1, 1, 1, 1, 1, 1, 1]);

    insert_stmt.execute(&(-1i8))?;
    insert_stmt.execute(&(-1i16))?;
    insert_stmt.execute(&(-1i32))?;
    insert_stmt.execute(&(-1i64))?;

    //in range
    assert!(insert_stmt.execute(&(9_223_372_036_854_775_807u64)).is_ok());
    assert!(insert_stmt.execute(&(9_223_372_036_854_775_807i64)).is_ok());

    //out of range
    assert!(insert_stmt.execute(&(9_223_372_036_854_775_808u64)).is_err());

    //in range
    assert!(insert_stmt.execute(&(-9_223_372_036_854_775_808i64)).is_ok());

    Ok(())
}
